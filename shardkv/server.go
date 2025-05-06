package shardkv

import (
	"sync"
	"time"

	"MyRaft/labgob"
	"MyRaft/labrpc"
	"MyRaft/raft"
	"MyRaft/shardctrler"
)

const (
	PullConfigInterval            = time.Millisecond * 100
	PullShardsInterval            = time.Millisecond * 200
	WaitCmdTimeOut                = time.Millisecond * 500
	CallPeerFetchShardDataTimeOut = time.Millisecond * 500
	CallPeerCleanShardDataTimeOut = time.Millisecond * 500
	MaxLockTime                   = time.Millisecond * 10 // debug
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stopCh          chan struct{}
	commandNotifyCh map[int64]chan CommandResult           // 用于命令apply后的唤醒
	lastApplies     [shardctrler.NShards]map[int64]int64   // k-v：ClientId-CommandId，记录每一个 shard 的最新 apply 的 commandId
	config          shardctrler.Config                     // 记录当前的config
	oldConfig       shardctrler.Config                     // 保存上一个config，进行shard迁移时，目标节点根据这个config来获取源节点，从而获取shard数据和请求清除shard数据
	meShards        map[int]bool                           // 记录自己分配到的shard，meShards[shardId] = true 表示该节点分配到 shardId
	data            [shardctrler.NShards]map[string]string // 存储数据，有 NShards 个 shard，每个 shard 存储一个 map，在 map 中存储 key-value 对

	inputShards  map[int]bool                   //当前这个config相较于上一个config新指派的shard，只有input为空了才能更新下一个config
	outputShards map[int]map[int]MergeShardData // configNum -> shard -> data。当某一个config，当前节点的shard移除，则记录当前config的所有移除shard的mergeShardData
	//cleanOutputDataNotifyCh map[string]chan struct{}  //用来通知等待协程clean完成
	scc *shardctrler.Clerk //保存一个shardctrler的客户端，因为要向shardctrler发送query获取配置信息

	//持久化
	persister *raft.Persister

	//定时任务计时器
	pullConfigTimer *time.Timer //定期获取config
	pullShardsTimer *time.Timer //定期检查inputShard并请求数据
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.stopCh)
}

// Leader 周期性向 shardctrler.Query 获取最新配置，并通过 raft.Start 提交 config 变更命令
func (kv *ShardKV) pullConfig() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.pullConfigTimer.C:
			//只有leader才能获取
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.pullConfigTimer.Reset(PullConfigInterval)
				break
			}
			kv.mu.Lock()
			lastNum := kv.config.Num
			kv.mu.Unlock()

			// pullConfig 看到 newConfig.Num+1 存在且当前没有待拉取的 inputShards，
			// 就通过 raft.Start 提交新的 shardctrler.Config
			// 配置号必须连续递增
			newConfig := kv.scc.Query(lastNum + 1)
			// 如果确实存在更新的配置
			if newConfig.Num == lastNum+1 {
				// 找到新的config
				kv.mu.Lock()
				// 必须当前shard全部迁移完成才能获取下一个 config
				if len(kv.inputShards) == 0 && kv.config.Num+1 == newConfig.Num {
					kv.mu.Unlock()
					//请求该命令
					kv.rf.Start(newConfig.Copy())
				} else {
					kv.mu.Unlock()
				}
			}
			// 只有当获取到新的config时，才重置定时器
			kv.pullConfigTimer.Reset(PullConfigInterval)
		}
	}
}

func (kv *ShardKV) ticker() {
	//处理applyCh
	go kv.handleApplyCh()
	//定时获取config信息
	go kv.pullConfig()
	//定时获取input shard(如果有的话)
	go kv.fetchShards()
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.persister = persister
	kv.scc = shardctrler.MakeClerk(kv.ctrlers)
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan struct{})
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//初始化自身数据
	kv.data = [shardctrler.NShards]map[string]string{}
	for i := range kv.data {
		kv.data[i] = make(map[string]string)
	}
	kv.lastApplies = [shardctrler.NShards]map[int64]int64{}
	for i := range kv.lastApplies {
		kv.lastApplies[i] = make(map[int64]int64)
	}

	kv.inputShards = make(map[int]bool)
	kv.outputShards = make(map[int]map[int]MergeShardData)
	//kv.cleanOutputDataNotifyCh = make(map[string]chan struct{})
	config := shardctrler.Config{
		Num:    0,
		Shards: [shardctrler.NShards]int{},
		Groups: map[int][]string{},
	}
	kv.config = config
	kv.oldConfig = config

	//读取快照内容
	kv.readPersist(kv.persister.ReadSnapshot())

	kv.commandNotifyCh = make(map[int64]chan CommandResult)
	//设置定时器
	kv.pullConfigTimer = time.NewTimer(PullConfigInterval)
	kv.pullShardsTimer = time.NewTimer(PullShardsInterval)

	kv.ticker()

	return kv
}
