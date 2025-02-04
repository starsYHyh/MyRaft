目前的情况是，重新启动的服务器会重播完整的 Raft 日志以恢复其状态。但是，对于长期运行的服务来说，永远记住完整的 Raft 日志是不切实际的。相反，您将修改 Raft 以与不时持久存储其状态“快照”的服务合作，此时 Raft 会丢弃快照之前的日志条目。结果是持久数据量更少，重启速度更快。但是，现在追随者可能会落后太多，以至于领导者已经丢弃了它需要赶上的日志条目；然后领导者必须发送快照以及从快照时开始的日志。扩展 Raft 论文的第 7 节概述了该方案；您必须设计细节。

您可能会发现参考 Raft 交互图来了解复制服务和 Raft 如何通信会有所帮助。

你的 Raft 必须提供以下函数，服务可以使用其状态的序列化快照来调用该函数：`Snapshot(index int, snapshot []byte)`
在实验 2D 中，测试人员定期调用 `Snapshot()`。在实验 3 中，您将编写一个调用 `Snapshot()` 的键/值服务器；快照将包含完整的键/值对表。服务层在每个对等点（而不仅仅是领导者）上调用 `Snapshot()`。

index 参数表示快照中反映的最高日志条目。 Raft 应该丢弃该点之前的日志条目。您需要修改 Raft 代码以在仅存储日志尾部的情况下运行。

您需要实现本文中讨论的 `InstallSnapshot RPC`，它允许 Raft 领导者告诉滞后的 Raft 对等点用快照替换其状态。您可能需要仔细考虑 InstallSnapshot 应如何与图 2 中的状态和规则进行交互。

当追随者的 Raft 代码收到 `InstallSnapshot RPC` 时，它可以使用`applyCh`将快照发送到`ApplyMsg`中的服务。`ApplyMsg`结构定义已经包含您需要的字段（以及测试人员期望的字段）。请注意，这些快照只会推进服务的状态，而不会导致其倒退。

如果服务器崩溃，它必须从持久数据重新启动。您的 Raft 应该同时保留 Raft 状态和相应的快照。使用`persister.SaveStateAndSnapshot()`，它为 Raft 状态和相应的快照分别接受参数。如果没有快照，请传递nil作为快照参数。

当服务器重新启动时，应用程序层会读取持久快照并恢复其保存的状态。

之前，本实验建议您实现一个名为`CondInstallSnapshot`的函数，以避免需要协调在`applyCh`上发送的快照和日志条目。这个残留的 API 接口仍然存在，但不建议您实现它：相反，我们建议您直接让它返回 true。

+ 一个好的起点是修改代码，以便它能够仅存储从某个索引 X 开始的日志部分。最初，您可以将 X 设置为零并运行 2B/2C 测试。然后让`Snapshot(index)`丢弃index之前的日志，并将 X 设置为等于index。如果一切顺利，您现在应该可以通过第一个 2D 测试。
+ 您将无法将日志存储在 Go 切片中，也无法将 Go 切片索引与 Raft 日志索引交替使用；您需要以一种考虑日志丢弃部分的方式对切片进行索引。
+ 下一步：如果没有使跟随者更新所需的日志条目，则让领导者发送 `InstallSnapshot RPC`。
+ 在单个` InstallSnapshot RPC `中发送整个快照。不要实现图 13 中用于分割快照的偏移机制。
+ Raft 必须以允许 Go 垃圾收集器释放和重新使用内存的方式丢弃旧的日志条目；这要求丢弃的日志条目没有可访问的引用（指针）。
+ 即使日志被修剪，您的实现仍然需要在`AppendEntries RPC` 中的新条目之前正确发送条目的术语和索引；这可能需要保存和引用最新快照的`lastIncludedTerm/lastIncludedIndex`（考虑是否应该持久保存）。
+ 不使用-race时，完成整个 Lab 2 测试 (2A+2B+2C+2D) 所需的合理时间是 6 分钟实际时间和 1 分钟 CPU 时间。使用-race运行时，大约需要 10 分钟实际时间和 2 分钟 CPU 时间。

full: 10 11 12 13 14 15 16
part: 0(nil) 1 2 3 4 5 6
10: lastIdx
16: recvdIdx
14: index
15: commitIdx
6: recvdIdx - lastIdx == len(log) - 1
4: index - lastIdx
log[5:]: log[index - lastIdx + 1: ]

newLastTerm: log[4].term
newLastIndex: index
log: log[5:]

```go
rf.log = append([]LogEntry{{Term: rf.lastIncludedTerm, Command: nil}}, rf.log[index-rf.lastIncludedIndex:]...)


```
6: recvdIdx - lastIdx == len(log)
4: index - lastIdx
log[4~6]: log[index - lastIdx:]

进行以下修改：

1. `SnapShot()`：用于处理接收到的快照
2. `InstallSnapShot()`：用于向跟随着发送落后的快照
3. `AppendEntries()`：
    + 需要修改日志的索引表示方法，因为截断后的日志下标和实际的index有区别。
    + 需要修改日志的处理方法，判断当前日志是否落后于领导者的快照。如果落后，需要请求领导者的快照
    + 领导者需要根据回复来判断是否需要执行`InstallSnapShot()`。