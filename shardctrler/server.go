package shardctrler

import (
	"sort"
	"sync"
	"time"

	"MyRaft/labgob"
	"MyRaft/labrpc"
	"MyRaft/raft"
)

const WaitCmdTimeOut = time.Millisecond * 500 // cmd执行超过这个时间，就返回timeout
const MaxLockTime = time.Millisecond * 10     // debug

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	stopCh          chan struct{}
	commandNotifyCh map[int64]chan CommandResult
	lastApplies     map[int64]int64 //k-v: ClientId-CommandId

	configs []Config // indexed by config num
}

type CommandResult struct {
	Err    Err
	Config Config
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqId     int64 //用来标识commandNotify
	CommandId int64
	ClientId  int64
	Args      interface{}
	Method    string
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	close(sc.stopCh)
	// Your code here, if desired.
}

func (sc *ShardCtrler) removeCh(reqId int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.commandNotifyCh, reqId)
}

func (sc *ShardCtrler) getConfigByIndex(idx int) Config {
	if idx < 0 || idx >= len(sc.configs) {
		// 因为会在config的基础上进行修改形成新的config，又涉及到map需要深拷贝
		// 如果 idx 小于 0 或者大于等于 len(sc.configs)，则返回最后一个配置，即为最新的配置
		return sc.configs[len(sc.configs)-1].Copy()
	}
	return sc.configs[idx].Copy()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	res := sc.waitCommand(args.ClientId, args.CommandId, "Join", *args)
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = res.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	res := sc.waitCommand(args.ClientId, args.CommandId, "Leave", *args)
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = res.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	res := sc.waitCommand(args.ClientId, args.CommandId, "Move", *args)
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = res.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("server %v query:args %+v", sc.me, args)

	// 如果是查询已经存在的配置可以直接返回，因为存在的配置是不会改变的；
	// 如果是-1，则必须在handleApplyCh中进行处理，按照命令顺序执行，不然不准确。
	sc.mu.Lock()
	if args.Num >= 0 && args.Num < len(sc.configs) {
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = sc.getConfigByIndex(args.Num)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	res := sc.waitCommand(args.ClientId, args.CommandId, "Query", *args)
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = res.Err
	reply.Config = res.Config
}

func (sc *ShardCtrler) waitCommand(clientId int64, commandId int64, method string, args interface{}) (res CommandResult) {
	op := Op{
		ReqId:     nrand(),
		ClientId:  clientId,
		CommandId: commandId,
		Method:    method,
		Args:      args,
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	ch := make(chan CommandResult, 1)
	sc.commandNotifyCh[op.ReqId] = ch
	sc.mu.Unlock()
	DPrintf("server %v wait cmd notify,index: %v,term: %v,op: %+v", sc.me, index, term, op)

	t := time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()

	select {
	case <-t.C:
		res.Err = ErrTimeout
	case res = <-ch:
	case <-sc.stopCh:
		res.Err = ErrServer
	}
	sc.removeCh(op.ReqId)
	return

}

// 配置的调整
// 我们的策略是尽量不改变当前的配置
func (sc *ShardCtrler) adjustConfig(conf *Config) {
	//针对三种情况分别进行调整
	if len(conf.Groups) == 0 {
		conf.Shards = [NShards]int{}
	} else if len(conf.Groups) == 1 {
		for gid := range conf.Groups {
			for i := range conf.Shards {
				conf.Shards[i] = gid
			}
		}
	} else if len(conf.Groups) <= NShards {
		//group数小于shard数，因此某些group可能会分配多一个或多个shard
		avgShardsCount := NShards / len(conf.Groups)
		otherShardsCount := NShards - avgShardsCount*len(conf.Groups)
		isTryAgain := true

		for isTryAgain {
			isTryAgain = false
			DPrintf("adjust config,%+v", conf)
			//获取所有的gid
			var gids []int
			for gid := range conf.Groups {
				gids = append(gids, gid)
			}
			sort.Ints(gids)
			//遍历每一个server
			for _, gid := range gids {
				count := 0
				for _, val := range conf.Shards {
					if val == gid {
						count++
					}
				}

				//判断是否要改变配置
				if count == avgShardsCount {
					//不需要改变配置
					continue
				} else if count > avgShardsCount && otherShardsCount == 0 {
					//多出来的设置为0
					temp := 0
					for k, v := range conf.Shards {
						if gid == v {
							if temp < avgShardsCount {
								temp += 1
							} else {
								conf.Shards[k] = 0
							}
						}
					}
				} else if count > avgShardsCount && otherShardsCount > 0 {
					//此时看看多出的shard能否全部分配给该server
					//如果没有全部分配完，下一次循环再看
					//如果全部分配完还不够，则需要将多出的部分设置为0
					temp := 0
					for k, v := range conf.Shards {
						if gid == v {
							if temp < avgShardsCount {
								temp += 1
							} else if temp == avgShardsCount && otherShardsCount != 0 {
								otherShardsCount -= 1
							} else {
								conf.Shards[k] = 0
							}
						}
					}

				} else {
					//count < arg
					for k, v := range conf.Shards {
						if v == 0 && count < avgShardsCount {
							conf.Shards[k] = gid
							count += 1
						}
						if count == avgShardsCount {
							break
						}
					}
					//因为调整的顺序问题，可能前面调整的server没有足够的shard进行分配，需要在进行一次调整
					// 例如gid为 [1, 3]，但是 已分配: [3, 3, 3, 3, 2, 2]
					// 由于顺序关系，当检测gid为1时，没有可以供分配的shard
					// 因为空闲的shard是在检测到gid为3时才空出来
					if count < avgShardsCount {
						DPrintf("adjust config try again.")
						isTryAgain = true
						continue
					}
				}
			}

			// 调整完成后，可能会有所有group都打到平均的shard数，但是多出来的shard没有进行分配
			// 此时可以采用轮询的方法
			// 例如，在remove group时，可能会有多出来的shard没有进行分配
			// 例如原来为[1, 1, 2, 2, 3, 3]，现在变为[1, 1, 2, 2, 0, 0]
			// 需要将多出来的shard进行分配
			// 变成[1, 1, 2, 2, 1, 2]
			cur := 0
			for k, v := range conf.Shards {
				//需要进行分配的
				if v == 0 {
					conf.Shards[k] = gids[cur]
					cur += 1
					cur %= len(conf.Groups)
				}
			}

		}
	} else {
		//group数大于shard数，每一个group最多一个shard，会有group没有shard
		// 标记已经占用了 shard的gid
		gidsFlag := make(map[int]int)
		emptyShards := make([]int, 0, NShards)
		// 本循环是为了保证每个 gid 最多占用一个 shard
		// 且记录下空闲的 shard
		for k, gid := range conf.Shards {
			if gid == 0 {
				emptyShards = append(emptyShards, k)
				continue
			}
			// 判断本 gid 是否已经占用了一个shard
			// 例如[0, 0, 0, 0, 1, 1]，gid为1的group占用了两个shard
			// 需要将多余的shard设置为0
			if _, ok := gidsFlag[gid]; ok {
				conf.Shards[k] = 0
				emptyShards = append(emptyShards, k)
			} else {
				// 如果 ok == false，说明该 gid 还没有占用一个 shard
				// 需要将该 gid 标记为已经占用
				gidsFlag[gid] = 1
			}
		}
		if len(emptyShards) > 0 {
			var gids []int
			for k := range conf.Groups {
				gids = append(gids, k)
			}
			sort.Ints(gids)
			temp := 0
			// 轮询分配
			for _, gid := range gids {
				if _, ok := gidsFlag[gid]; !ok {
					conf.Shards[emptyShards[temp]] = gid
					temp += 1
				}
				if temp >= len(emptyShards) {
					break
				}
			}

		}
	}
}

/*
applych处理代码
*/

func (sc *ShardCtrler) handleJoinCommand(args JoinArgs) {
	// 例如:
	// ck.Join(map[int][]string{
	// 	2: []string{"10.0.0.2:3000", "10.0.0.3:3000"},  // 添加GID为2的复制组
	// 	3: []string{"10.0.0.4:3000", "10.0.0.5:3000"}   // 添加GID为3的复制组
	// })
	conf := sc.getConfigByIndex(-1)
	conf.Num += 1

	//加入组
	for k, v := range args.Servers {
		conf.Groups[k] = v
	}

	sc.adjustConfig(&conf)
	sc.configs = append(sc.configs, conf)
}

func (sc *ShardCtrler) handleLeaveCommand(args LeaveArgs) {
	conf := sc.getConfigByIndex(-1)
	conf.Num += 1

	//删掉server，并重置分配的shard
	for _, gid := range args.GIDs {
		delete(conf.Groups, gid)
		for i, v := range conf.Shards {
			if v == gid {
				conf.Shards[i] = 0
			}
		}
	}

	sc.adjustConfig(&conf)
	sc.configs = append(sc.configs, conf)
}

func (sc *ShardCtrler) handleMoveCommand(args MoveArgs) {
	conf := sc.getConfigByIndex(-1)
	conf.Num += 1
	conf.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, conf)
}

func (sc *ShardCtrler) notifyWaitCommand(reqId int64, err Err, conf Config) {
	if ch, ok := sc.commandNotifyCh[reqId]; ok {
		ch <- CommandResult{
			Err:    err,
			Config: conf,
		}
	}
}

// 处理applych
func (sc *ShardCtrler) handleApplyCh() {
	for {
		select {
		case <-sc.stopCh:
			DPrintf("get from stopCh,server-%v stop!", sc.me)
			return
		case cmd := <-sc.applyCh:
			//处理快照命令，读取快照的内容
			if cmd.SnapshotValid {
				continue
			}
			//处理普通命令
			if !cmd.CommandValid {
				continue
			}
			cmdIdx := cmd.CommandIndex
			DPrintf("server %v start apply command %v: %+v", sc.me, cmdIdx, cmd.Command)
			op := cmd.Command.(Op)
			sc.mu.Lock()

			if op.Method == "Query" {
				//处理读
				conf := sc.getConfigByIndex(op.Args.(QueryArgs).Num)
				sc.notifyWaitCommand(op.ReqId, OK, conf)
			} else {
				//处理其他命令
				//判断命令是否重复
				if op.CommandId != sc.lastApplies[op.ClientId] {
					switch op.Method {
					case "Join":
						sc.handleJoinCommand(op.Args.(JoinArgs))
					case "Leave":
						sc.handleLeaveCommand(op.Args.(LeaveArgs))
					case "Move":
						sc.handleMoveCommand(op.Args.(MoveArgs))
					default:
						panic("unknown method")
					}
				}
				sc.lastApplies[op.ClientId] = op.CommandId
				sc.notifyWaitCommand(op.ReqId, OK, Config{})
			}

			DPrintf("apply op: cmdId:%d, op: %+v", cmdIdx, op)
			sc.mu.Unlock()
		}
	}
}

/*
初始化代码
*/

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})

	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.stopCh = make(chan struct{})
	sc.commandNotifyCh = make(map[int64]chan CommandResult)
	sc.lastApplies = make(map[int64]int64)

	go sc.handleApplyCh()

	return sc
}
