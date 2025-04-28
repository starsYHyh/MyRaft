# ShardKV 配置变更处理机制详解

ShardKV 是一个分片键值存储系统，它通过 Raft 共识算法来保证数据一致性，同时使用分片技术支持水平扩展。配置变更是 ShardKV 的核心功能之一，它管理着分片在不同服务器组之间的分配与迁移。

## 配置相关的数据结构

```go
type ShardKV struct {
    // ...
    config          shardctrler.Config        // 当前配置
    oldConfig       shardctrler.Config        // 上一个配置 
    meShards        map[int]bool              // 当前服务器负责的分片
    inputShards     map[int]bool              // 需要从其他组迁入的分片
    outputShards    map[int]map[int]MergeShardData // 需要迁出到其他组的分片
    // ...
}
```

## 配置变更的整体流程

1. **配置拉取**: Leader 定期从 shardctrler 拉取最新配置
2. **配置变更提交**: 通过 Raft 提交配置变更命令
3. **分片迁移计算**: 计算需要迁入和迁出的分片
4. **分片数据迁移**: 处理分片数据的传输
5. **清理工作**: 完成迁移后的清理工作

## 配置拉取机制

```go
func (kv *ShardKV) pullConfig() {
    for {
        // ...
        _, isLeader := kv.rf.GetState()
        if isLeader {
            lastNum := kv.config.Num
            newConfig := kv.scc.Query(lastNum + 1)
            
            // 关键: 只有当所有迁移任务完成才能获取下一个配置
            if newConfig.Num == lastNum+1 && len(kv.inputShards) == 0 {
                kv.rf.Start(newConfig.Copy())
            }
        }
        // ...
    }
}
```

关键点：
- 只有 Leader 才能拉取配置
- 只有当所有待迁入分片处理完成才能拉取下一个配置
- 配置号必须连续递增

## 配置变更处理

```go
func (kv *ShardKV) handleConfigCommand(cmdIdx int, newConfig shardctrler.Config) {
    // 检查配置号是否正确
    if newConfig.Num <= kv.config.Num {
        return
    }
    if newConfig.Num != kv.config.Num+1 {
        panic("applyConfig err")
    }
    
    oldConfig := kv.config.Copy()
    outputShardIds := make([]int, 0, shardctrler.NShards)
    inputShardIds := make([]int, 0, shardctrler.NShards)
    meShards := make([]int, 0, shardctrler.NShards)
    
    // 计算需要迁入、迁出的分片
    for i := 0; i < shardctrler.NShards; i++ {
        if newConfig.Shards[i] == kv.gid {
            meShards = append(meShards, i)
            if oldConfig.Shards[i] != kv.gid {
                inputShardIds = append(inputShardIds, i)
            }
        } else if oldConfig.Shards[i] == kv.gid {
            outputShardIds = append(outputShardIds, i)
        }
    }
    
    // 处理分片迁移
    // ...
}
```

## 分片迁移处理

### 输出分片处理
```go
// 保存需要迁出的分片数据
d := make(map[int]MergeShardData)
for _, shardId := range outputShardIds {
    mergeShardData := MergeShardData{
        ConfigNum:      oldConfig.Num,
        ShardNum:       shardId,
        Data:           kv.data[shardId],
        CommandIndexes: kv.lastApplies[shardId],
    }
    d[shardId] = mergeShardData
    
    // 清空本地数据
    kv.data[shardId] = make(map[string]string)
    kv.lastApplies[shardId] = make(map[int64]int64)
}
kv.outputShards[oldConfig.Num] = d
```

### 输入分片处理
```go
// 记录需要迁入的分片
kv.inputShards = make(map[int]bool)
if oldConfig.Num != 0 {
    for _, shardId := range inputShardIds {
        kv.inputShards[shardId] = true
    }
}
```

## 分片数据获取

```go
func (kv *ShardKV) fetchShards() {
    for {
        // ...
        _, isLeader := kv.rf.GetState()
        if isLeader {
            for shardId, _ := range kv.inputShards {
                go kv.fetchShard(shardId, kv.oldConfig)
            }
        }
        // ...
    }
}

func (kv *ShardKV) fetchShard(shardId int, config shardctrler.Config) {
    // 从源节点请求分片数据
    // 如果成功获取，通过 Raft 提交合并分片数据的命令
    // ...
}
```

## 分片数据合并与清理

```go
func (kv *ShardKV) handleMergeShardDataCommand(cmdIdx int, data MergeShardData) {
    // 将获取到的分片数据合并到本地
    for k, v := range data.Data {
        kv.data[data.ShardNum][k] = v
    }
    for k, v := range data.CommandIndexes {
        kv.lastApplies[data.ShardNum][k] = v
    }
    
    // 从待迁入集合中移除处理完毕的分片
    delete(kv.inputShards, data.ShardNum)
    
    // 通知源节点可以清理该分片
    go kv.callPeerCleanShardData(kv.oldConfig, data.ShardNum)
}
```

## 客户端请求处理

当客户端发送请求时，ShardKV 通过以下机制确保请求处理的正确性：

```go
func (kv *ShardKV) ProcessKeyReady(configNum int, key string) Err {
    // 检查配置号
    if configNum != kv.config.Num {
        return ErrWrongGroup
    }
    
    shardId := key2shard(key)
    // 检查分片是否由该服务器管理
    if _, ok := kv.meShards[shardId]; !ok {
        return ErrWrongGroup
    }
    
    // 检查分片是否正在迁移中
    if _, ok := kv.inputShards[shardId]; ok {
        return ErrWrongGroup
    }
    
    return OK
}
```

## 配置变更的关键约束

1. **配置连续性**: 配置必须按顺序处理，配置号必须连续递增
2. **迁移完成性**: 只有当前所有迁移任务完成才能处理下一个配置
3. **操作原子性**: 通过 Raft 确保配置变更操作在所有副本上一致执行
4. **迁移期间请求处理**: 对于迁移中的分片，拒绝客户端请求，确保数据一致性

这种设计确保了在分布式环境下，即使在配置变更和分片迁移期间，系统也能保持数据一致性，同时支持系统的水平扩展和负载均衡。