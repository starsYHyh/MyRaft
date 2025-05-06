package shardkv

import (
	"MyRaft/shardctrler"
	"time"
)

// 判断是否存在指定config和指定shardId的output shard
func (kv *ShardKV) OutputDataExist(configNum int, shardId int) bool {
	if _, ok := kv.outputShards[configNum]; ok {
		if _, ok = kv.outputShards[configNum][shardId]; ok {
			return true
		}
	}
	return false
}

/*
RPC，针对output shard
*/
//请求获取shard
func (kv *ShardKV) FetchShardData(args *FetchShardDataArgs, reply *FetchShardDataReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 必须是过去的config
	if args.ConfigNum >= kv.config.Num {
		return
	}

	reply.Success = false
	if configData, ok := kv.outputShards[args.ConfigNum]; ok {
		if shardData, ok := configData[args.ShardNum]; ok {
			reply.Success = true
			reply.Data = make(map[string]string)
			reply.CommandIndexes = make(map[int64]int64)
			for k, v := range shardData.Data {
				reply.Data[k] = v
			}
			for k, v := range shardData.CommandIndexes {
				reply.CommandIndexes[k] = v
			}
		}
	}
}

// 请求清除shard
func (kv *ShardKV) CleanShardData(args *CleanShardDataArgs, reply *CleanShardDataReply) {
	kv.mu.Lock()

	//必须是过去的 config
	if args.ConfigNum >= kv.config.Num {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	_, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		return
	}

	// 简单处理下。。。
	for i := 0; i < 10; i++ {
		kv.mu.Lock()
		exist := kv.OutputDataExist(args.ConfigNum, args.ShardNum)
		kv.mu.Unlock()
		if !exist {
			reply.Success = true
			return
		}
		time.Sleep(time.Millisecond * 20)
	}
}

// 非空的 inputShards 周期性触发向旧主 FetchShardData
func (kv *ShardKV) fetchShards() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.pullShardsTimer.C:
			//判断是否有要input的shard
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.mu.Lock()
				// 根据 inputShards 中的 shardId 来请求数据
				// 数据来源于另一个 group 的 outputShards
				for shardId := range kv.inputShards {
					// 注意要从上一个 config 中请求 shard 的源节点
					go kv.fetchShard(shardId, kv.oldConfig)
				}
				kv.mu.Unlock()
			}
			kv.pullShardsTimer.Reset(PullShardsInterval)

		}
	}
}

// 获取指定的 shard
func (kv *ShardKV) fetchShard(shardId int, config shardctrler.Config) {
	args := FetchShardDataArgs{
		ConfigNum: config.Num,
		ShardNum:  shardId,
	}

	t := time.NewTimer(CallPeerFetchShardDataTimeOut)
	defer t.Stop()

	for {
		//依次请求group中的每个节点,但只要获取一个就好了
		for _, s := range config.Groups[config.Shards[shardId]] {
			reply := FetchShardDataReply{}
			srv := kv.make_end(s)
			done := make(chan bool, 1)
			go func(args *FetchShardDataArgs, reply *FetchShardDataReply) {
				done <- srv.Call("ShardKV.FetchShardData", args, reply)
			}(&args, &reply)

			t.Reset(CallPeerFetchShardDataTimeOut)

			select {
			case <-kv.stopCh:
				return
			case <-t.C:
			case isDone := <-done:
				if isDone && reply.Success {
					kv.mu.Lock()
					if _, ok := kv.inputShards[shardId]; ok && kv.config.Num == config.Num+1 {
						replyCopy := reply.Copy()
						mergeShardData := MergeShardData{
							ConfigNum:      args.ConfigNum,
							ShardNum:       args.ShardNum,
							Data:           replyCopy.Data,
							CommandIndexes: replyCopy.CommandIndexes,
						}
						kv.mu.Unlock()
						kv.rf.Start(mergeShardData)
						//不管是不是leader都返回
						return
					} else {
						kv.mu.Unlock()
					}
				}
			}

		}
	}

}

/*
处理好input shard，请求源节点清除output shard
*/

// 发送给shard源节点，可以删除shard数据了
// 一般在apply command中处理好input的shard，发送给源节点删除保存的shard数据
func (kv *ShardKV) callPeerCleanShardData(config shardctrler.Config, shardId int) {
	// 这里的 config 是上一个配置
	// shardId 是刚刚迁移完毕的 shard 的 shardId
	// 根据这两个信息获得上一个配置中，该 shard 属于哪一个 group
	// 并向其发送请求，以清除源节点中的 shard 数据
	args := CleanShardDataArgs{
		ConfigNum: config.Num,
		ShardNum:  shardId,
	}

	t := time.NewTimer(CallPeerCleanShardDataTimeOut)
	defer t.Stop()

	for {
		//因为并不知道哪一个节点是leader，因此群发吧
		for _, group := range config.Groups[config.Shards[shardId]] {
			reply := CleanShardDataReply{}
			srv := kv.make_end(group)
			done := make(chan bool, 1)

			go func(args *CleanShardDataArgs, reply *CleanShardDataReply) {
				done <- srv.Call("ShardKV.CleanShardData", args, reply)
			}(&args, &reply)

			t.Reset(CallPeerCleanShardDataTimeOut)

			select {
			case <-kv.stopCh:
				return
			case <-t.C:
			case isDone := <-done:
				if isDone && reply.Success {
					return
				}
			}

		}
		kv.mu.Lock()
		if kv.config.Num != config.Num+1 || len(kv.inputShards) == 0 {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
	}
}
