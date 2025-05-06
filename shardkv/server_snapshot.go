package shardkv

import (
	"MyRaft/labgob"
	"MyRaft/shardctrler"
	"bytes"
	"log"
)

// 保存快照
func (kv *ShardKV) saveSnapshot(logIndex int) {
	//判断条件，满足一定的日志量才能进行持久化
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}

	//生成快照数据
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.data) != nil ||
		e.Encode(kv.lastApplies) != nil ||
		e.Encode(kv.inputShards) != nil ||
		e.Encode(kv.outputShards) != nil ||
		e.Encode(kv.config) != nil ||
		e.Encode(kv.oldConfig) != nil ||
		e.Encode(kv.meShards) != nil {
		panic("gen snapshot data encode err")
	}
	data := w.Bytes()
	kv.rf.Snapshot(logIndex, data)
}

// 读取快照
// 两处调用：初始化阶段；收到Snapshot命令，即接收了leader的Snapshot
func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	//对数据进行同步
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvData [shardctrler.NShards]map[string]string
	var lastApplies [shardctrler.NShards]map[int64]int64
	var inputShards map[int]bool
	var outputShards map[int]map[int]MergeShardData
	var config shardctrler.Config
	var oldConfig shardctrler.Config
	var meShards map[int]bool

	if d.Decode(&kvData) != nil ||
		d.Decode(&lastApplies) != nil ||
		d.Decode(&inputShards) != nil ||
		d.Decode(&outputShards) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&oldConfig) != nil ||
		d.Decode(&meShards) != nil {
		log.Fatal("kv read persist err")
	} else {
		kv.data = kvData
		kv.lastApplies = lastApplies
		kv.inputShards = inputShards
		kv.outputShards = outputShards
		kv.config = config
		kv.oldConfig = oldConfig
		kv.meShards = meShards
	}
}
