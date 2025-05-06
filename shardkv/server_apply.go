package shardkv

import (
	"MyRaft/shardctrler"
)

func (kv *ShardKV) notifyWaitCommand(reqId int64, err Err, value string) {
	if ch, ok := kv.commandNotifyCh[reqId]; ok {
		ch <- CommandResult{
			Err:   err,
			Value: value,
		}
	}
}

func (kv *ShardKV) getValueByKey(key string) (err Err, value string) {
	if v, ok := kv.data[key2shard(key)][key]; ok {
		err = OK
		value = v
	} else {
		err = ErrNoKey
		value = ""
	}
	return
}

// 判断能否执行客户端发来的命令，主要判断是否在当前 config 下，是否在自己的 shard 上
func (kv *ShardKV) ProcessKeyReady(configNum int, key string) Err {
	//config不对
	if configNum == 0 || configNum != kv.config.Num {
		return ErrWrongGroup
	}
	shardId := key2shard(key)
	//没有分配该shard
	if _, ok := kv.meShards[shardId]; !ok {
		return ErrWrongGroup
	}
	//正在迁移，这里有优化的空间，如果没有迁移完成，可以直接请求目标节点完成操作并返回，但是这样就太复杂了，这里简略了
	if _, ok := kv.inputShards[shardId]; ok {
		return ErrWrongGroup
	}
	return OK
}

// 消费 Raft applyCh，应用各类命令（客户端读写、config 变更、mergeShard、cleanShard）
func (kv *ShardKV) handleApplyCh() {
	for {
		select {
		case <-kv.stopCh:
			return
		case cmd := <-kv.applyCh:
			// 处理快照命令，读取快照的内容
			if cmd.SnapshotValid {
				kv.mu.Lock()
				kv.readPersist(cmd.Snapshot)
				kv.mu.Unlock()
				continue
			}
			// 处理普通命令
			if !cmd.CommandValid {
				continue
			}
			cmdIdx := cmd.CommandIndex
			// 处理不同的命令
			if op, ok := cmd.Command.(Op); ok {
				kv.handleOpCommand(cmdIdx, op)
			} else if config, ok := cmd.Command.(shardctrler.Config); ok {
				// pullConfig --> Query(lastNum + 1) --> Start(config) --> HandleApplyCh
				// 	--> handleConfigCommand --> 保存 outputShards和 inputShards
				kv.handleConfigCommand(cmdIdx, config)
			} else if mergeData, ok := cmd.Command.(MergeShardData); ok {
				// fetchShards() --> fetchShard() --> FetchShardData 获得新增的部分
				// 	--> Start(mergeData) --> HandleApplyCh
				// 	--> handleMergeShardDataCommand --> 保存 inputShards
				// 	--> callPeerCleanShardData --> CleanShardData --> HandleApplyCh
				// 	--> handleCleanShardDataCommand --> 清除outputShards
				kv.handleMergeShardDataCommand(cmdIdx, mergeData)
			} else if cleanData, ok := cmd.Command.(CleanShardDataArgs); ok {
				kv.handleCleanShardDataCommand(cmdIdx, cleanData)
			} else {
				panic("apply command,NOT FOUND COMMDN！")
			}

		}

	}

}

// 处理get、put、append命令
func (kv *ShardKV) handleOpCommand(cmdIdx int, op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardId := key2shard(op.Key)
	if err := kv.ProcessKeyReady(op.ConfigNum, op.Key); err != OK {
		// 如果不在当前 config 下，或者不在自己的 shard 上，则直接返回错误
		kv.notifyWaitCommand(op.ReqId, err, "")
		return
	}
	if op.Method == "Get" {
		// 处理读，直接从 kv.data 读取并 notify
		if v, ok := kv.data[shardId][op.Key]; ok {
			kv.notifyWaitCommand(op.ReqId, OK, v)
		} else {
			kv.notifyWaitCommand(op.ReqId, ErrNoKey, "")
		}
	} else if op.Method == "Put" || op.Method == "Append" {
		if op.CommandId != kv.lastApplies[shardId][op.ClientId] {
			switch op.Method {
			case "Put":
				kv.data[shardId][op.Key] = op.Value
				kv.lastApplies[shardId][op.ClientId] = op.CommandId
			case "Append":
				kv.data[shardId][op.Key] += op.Value
				kv.lastApplies[shardId][op.ClientId] = op.CommandId
			default:
				panic("unknown method " + op.Method)
			}

		}
		//命令处理成功
		kv.notifyWaitCommand(op.ReqId, OK, "")
	} else {
		panic("unknown method " + op.Method)
	}

	//每应用一条命令，就判断是否进行持久化
	kv.saveSnapshot(cmdIdx)
}

// 处理config命令，即更新config
// 主要是处理 meshard、inputshard、outputshard
func (kv *ShardKV) handleConfigCommand(cmdIdx int, newConfig shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if newConfig.Num <= kv.config.Num {
		kv.saveSnapshot(cmdIdx)
		return
	}

	if newConfig.Num != kv.config.Num+1 {
		panic("applyConfig err")
	}

	oldConfig := kv.config.Copy()
	outputShardIds := make([]int, 0, shardctrler.NShards)
	inputShardIds := make([]int, 0, shardctrler.NShards)
	meShards := make([]int, 0, shardctrler.NShards)

	for i := 0; i < shardctrler.NShards; i++ {
		if newConfig.Shards[i] == kv.gid {
			meShards = append(meShards, i)
			if oldConfig.Shards[i] != kv.gid {
				// 对“移入”（config.Shards[i]==gid 且 oldConfig.Shards[i]!=gid）的 shard，
				// 把 i 加入 inputShards
				// 即，从新配置开始，本 group 负责这个 shard
				inputShardIds = append(inputShardIds, i)
			}
		} else {
			if oldConfig.Shards[i] == kv.gid {
				// 对“移出”的 shard，构造 MergeShardData（保存原 shard 数据和 lastApplies），
				// 存入 outputShards[oldConfig.Num][i]，并清空本地 data/lastApplies
				outputShardIds = append(outputShardIds, i)
			}
		}
	}

	//处理当前的shard
	kv.meShards = make(map[int]bool)
	for _, shardId := range meShards {
		kv.meShards[shardId] = true
	}

	// 处理移出的shard
	// 保存原 shard 数据和 lastApplies
	d := make(map[int]MergeShardData)
	for _, shardId := range outputShardIds {
		mergeShardData := MergeShardData{
			ConfigNum:      oldConfig.Num,
			ShardNum:       shardId,
			Data:           kv.data[shardId],
			CommandIndexes: kv.lastApplies[shardId],
		}
		d[shardId] = mergeShardData
		// 清空数据
		kv.data[shardId] = make(map[string]string)
		kv.lastApplies[shardId] = make(map[int64]int64)
	}
	kv.outputShards[oldConfig.Num] = d

	// 处理移入的shard
	kv.inputShards = make(map[int]bool)
	if oldConfig.Num != 0 {
		for _, shardId := range inputShardIds {
			kv.inputShards[shardId] = true
		}
	}

	kv.config = newConfig
	kv.oldConfig = oldConfig
	kv.saveSnapshot(cmdIdx)
}

// 负责将其他节点的 output shard 数据迁移到自己的 data 中
func (kv *ShardKV) handleMergeShardDataCommand(cmdIdx int, data MergeShardData) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num != data.ConfigNum+1 {
		return
	}

	// 如果将要迁移进来的 shard 并不在 inputShards 所标记的 shard 中，
	// 则说明该 shard 已经被迁移走了，直接返回
	// 这里的逻辑是，kv.inputShards 是一个 map[int]bool，表示当前节点需要迁移的 shard
	if _, ok := kv.inputShards[data.ShardNum]; !ok {
		return
	}

	kv.data[data.ShardNum] = make(map[string]string)
	kv.lastApplies[data.ShardNum] = make(map[int64]int64)

	for k, v := range data.Data {
		kv.data[data.ShardNum][k] = v
	}
	for k, v := range data.CommandIndexes {
		kv.lastApplies[data.ShardNum][k] = v
	}
	// 迁移完成后，删除 inputShards 中的该 shard
	delete(kv.inputShards, data.ShardNum)

	kv.saveSnapshot(cmdIdx)
	// 通知源节点可以清理该分片
	// kv.oldConfig 是上一个配置
	// data.ShardNum 是刚迁移完成的的 shard 原来所属的 shardId
	go kv.callPeerCleanShardData(kv.oldConfig, data.ShardNum)
}

// 处理已经迁移走的shard，即output shard
func (kv *ShardKV) handleCleanShardDataCommand(cmdIdx int, data CleanShardDataArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//如果要清除的shard确实是在outputShard中，且没有被清除，则需要清除
	if kv.OutputDataExist(data.ConfigNum, data.ShardNum) {
		delete(kv.outputShards[data.ConfigNum], data.ShardNum)
	}

	//通知等待协程
	//if ch, ok := kv.cleanOutputDataNotifyCh[fmt.Sprintf("%d%d", data.ConfigNum, data.ShardNum)]; ok {
	//	ch <- struct{}{}
	//}

	kv.saveSnapshot(cmdIdx)
}
