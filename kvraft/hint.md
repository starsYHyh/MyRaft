# KV

## 介绍

在本实验中，您将使用实验 2中的 Raft 库构建容错键/值存储服务。您的键/值服务将是一个复制状态机，由多个使用 Raft 进行复制的键/值服务器组成。只要大多数服务器处于活动状态并且可以通信，您的键/值服务就应该继续处理客户端请求，而不管其他故障或网络分区如何。完成实验 3 后，您将实现Raft 交互图中显示的所有部分（Clerk、Service 和 Raft）。

客户端可以向键/值服务发送三种不同的 RPC：`Put(key, value)`、`Append(key, arg)`和`Get(key)`。该服务维护一个包含键/值对的简单数据库。键和值都是字符串。`Put(key, value)`替换数据库中特定键的值，`Append(key, arg)`将 arg 附加到键的值，`Get(key)`获取键的当前值。对不存在的键的Get应该返回一个空字符串。对不存在的键的Append应该像Put一样运行。每个客户端都通过具有 `Put/Append/Get` 方法的Clerk与服务通信。 Clerk管理与服务器的 RPC 交互。

您的服务必须安排应用程序对Clerk `Get/Put/Append` 方法的调用是线性化的。如果一次调用一个，`Get/Put/Append` 方法应该表现得好像系统只有一个状态副本，并且每个调用都应该观察前面调用序列所暗示的状态修改。对于并发调用，返回值和最终状态必须相同，就像操作按某种顺序一次执行一个一样。如果调用在时间上重叠，则它们是并发的：例如，如果客户端 X 调用`Clerk.Put()`，客户端 Y 调用`Clerk.Append()`，然后客户端 X 的调用返回。调用必须观察在调用开始之前已完成的所有调用的效果。

线性一致性对应用程序来说很方便，因为它是单个服务器每次处理一个请求时的行为。例如，如果一个客户端从服务获得更新请求的成功响应，则随后从其他客户端启动的读取肯定会看到该更新的效果。对于单个服务器来说，提供线性一致性相对容易。如果服务是复制的，那就更难了，因为所有服务器都必须为并发请求选择相同的执行顺序，必须避免使用非最新状态回复客户端，并且必须在发生故障后以保留所有已确认的客户端更新的方式恢复其状态。

本实验分为两部分。在部分 A 中，您将使用 Raft 实现来实现键/值服务，但不使用快照。在部分 B 中，您将使用实验 2D 中的快照实现，这将允许 Raft 丢弃旧日志条目。请在相应的截止日期前提交每个部分。

您应该查看扩展的 Raft 论文，特别是第 7 和 8 节。从更广阔的视角来看，请看一下 Chubby、Paxos Made Live、Spanner、Zookeeper、Harp、Viewstamped Replication 和Bolosky 等。

## 入门

我们在src/kvraft中为您提供了骨架代码和测试。您需要修改`kvraft/client.go`、`kvraft/server.go`，也许还有`kvraft/common.go`。

要启动并运行，请执行以下命令。不要忘记使用`git pull`来获取最新软件。

```sh
$ cd ~/6.824 
$ git pull 
... 
$ cd src/kvraft 
$ go test -race 
... 
$
```

## A 部分：无快照的键/值服务（中等/困难）

您的每个键/值服务器（“kvservers”）都将有一个关联的 Raft 对等点。 Clerks 向其关联的 Raft 为领导者的 kvserver 发送`Put()`、`Append()`和 `Get()` RPC。 kvserver 代码将 `Put/Append/Get` 操作提交给 Raft，以便 Raft 日志保存一系列 `Put/Append/Get` 操作。 所有 kvserver 按顺序执行 Raft 日志中的操作，并将操作应用于其键/值数据库；目的是让服务器维护键/值数据库的相同副本。

Clerk有时不知道哪个 kvserver 是 Raft 领导者。如果Clerk向错误的 kvserver 发送 RPC，或者无法到达 kvserver，则Clerk应通过发送到其他 kvserver 进行重试。如果键/值服务将操作提交到其 Raft 日志（从而将操作应用于键/值状态机），则领导者通过响应其 RPC 将结果报告给 Clerk 。如果操作提交失败（例如，如果领导者被替换），服务器将报告错误，并且Clerk将使用其他服务器重试。

您的 kvservers 不应直接通信；它们只应通过 Raft 相互交互。

> 您的首要任务是实施一个在未丢失消息且服务器未发生故障的情况下仍能起作用的解决方案。
您需要在client.go中的 Clerk `Put/Append/Get` 方法中添加 RPC 发送代码，并在server.go中实现`PutAppend()`和`Get()` RPC 处理程序。这些处理程序应使用`Start()`在 Raft 日志中输入`Op` ；您应该在server.go中填写Op结构定义，以便它描述 Put/Append/Get 操作。每个服务器都应在 Raft 提交`Op`命令时执行它们，即当它们出现在applyCh上时。RPC 处理程序应该注意到 Raft 何时提交其`Op`，然后回复 RPC。
当您可靠地通过测试套件中的第一个测试“一个客户端”时，您就完成了此任务。

+ 调用`Start()`后，您的 kvservers 将需要等待 Raft 完成协商。已协商好的命令将到达 `applyCh` 。您的代码将需要继续读取`applyCh`，同时`PutAppend()`和`Get()`处理程序使用`Start()`将命令提交到 Raft 日志。注意 kvserver 和其 Raft 库之间的死锁。
+ 你可以向 Raft ApplyMsg添加字段，也可以向 Raft RPC 添加字段（例如AppendEntries），但对于大多数实现来说这不是必需的。
如果 kvserver 不属于多数派，则不应完成`Get()` RPC（这样它就不会提供过时的数据）。一个简单的解决方案是将每个`Get()`（以及每个`Put()`和`Append()`）输入到 Raft 日志中。您不必实现第 8 节中描述的只读操作优化。
+ 最好从一开始就添加锁定，因为避免死锁的需求有时会影响整体代码设计。使用`go test -race`检查您的代码是否无竞争。
现在，您应该修改解决方案，以便在网络和服务器发生故障时继续运行。您将面临的一个问题是，Clerk可能必须多次发送 RPC，直到找到一个可以肯定回复的 kvserver。如果领导者在将条目提交到 Raft 日志后立即失败，Clerk可能不会收到回复，因此可能会将请求重新发送给另一个领导者。每次调用`Clerk.Put()`或`Clerk.Append()`都应该只导致一次执行，因此您必须确保重新发送不会导致服务器执行请求两次。

添加代码来处理故障，并应对重复的Clerk请求，包括Clerk在一个任期内向 kvserver 领导者发送请求、等待回复超时以及在另一个任期内将请求重新发送给新领导者的情况。请求应该只执行一次。您的代码应该通过`go test -run 3A -race`测试。

+ 您的解决方案需要处理已为 Clerk 的 RPC 调用 Start() 但在将请求提交到日志之前失去领导权的领导者。在这种情况下，您应该安排 Clerk 将请求重新发送到其他服务器，直到找到新的领导者。一种方法是让服务器检测到它已失去领导权，方法是注意到 Start() 返回的索引处出现了不同的请求，或者 Raft 的任期发生了变化。如果前任领导者自己分区，它将不知道有新的领导者；但同一分区中的任何客户端也无法与新领导者交谈，因此在这种情况下，服务器和客户端无限期地等待直到分区恢复是可以的。
+ 您可能需要修改 Clerk，使其记住哪个服务器是上一次 RPC 的领导者，并首先将下一个 RPC 发送到该服务器。这将避免浪费时间在每次 RPC 上搜索领导者，这可能会帮助您足够快地通过一些测试。
+ 您需要唯一地标识客户端操作以确保键/值服务只执行一次。
+ 您的重复检测方案应快速释放服务器内存，例如通过让每个 RPC 暗示客户端已看到其上一个 RPC 的回复。可以假设客户端每次只会调用 Clerk 一次。

你的代码现在应该可以通过实验室 3A 测试，如下所示：

```sh
$ go test -run 3A -race
Test: one client (3A) ...
  ... Passed --  15.5  5  4576  903
Test: ops complete fast enough (3A) ...
  ... Passed --  15.7  3  3022    0
Test: many clients (3A) ...
  ... Passed --  15.9  5  5884 1160
Test: unreliable net, many clients (3A) ...
  ... Passed --  19.2  5  3083  441
Test: concurrent append to same key, unreliable (3A) ...
  ... Passed --   2.5  3   218   52
Test: progress in majority (3A) ...
  ... Passed --   1.7  5   103    2
Test: no progress in minority (3A) ...
  ... Passed --   1.0  5   102    3
Test: completion after heal (3A) ...
  ... Passed --   1.2  5    70    3
Test: partitions, one client (3A) ...
  ... Passed --  23.8  5  4501  765
Test: partitions, many clients (3A) ...
  ... Passed --  23.5  5  5692  974
Test: restarts, one client (3A) ...
  ... Passed --  22.2  5  4721  908
Test: restarts, many clients (3A) ...
  ... Passed --  22.5  5  5490 1033
Test: unreliable net, restarts, many clients (3A) ...
  ... Passed --  26.5  5  3532  474
Test: restarts, partitions, many clients (3A) ...
  ... Passed --  29.7  5  6122 1060
Test: unreliable net, restarts, partitions, many clients (3A) ...
  ... Passed --  32.9  5  2967  317
Test: unreliable net, restarts, partitions, random keys, many clients (3A) ...
  ... Passed --  35.0  7  8249  746
PASS
ok   6.824/kvraft 290.184s
```

每个Passed后的数字是实际时间（以秒为单位）、对等体的数量、发送的 RPC 数量（包括客户端 RPC）以及执行的键/值操作数量（Clerk Get/Put/Append 调用）。

## B 部分：带快照的键/值服务（困难）

目前，您的键/值服务器不会调用 Raft 库的Snapshot()方法，因此重新启动的服务器必须重播完整的持久化 Raft 日志才能恢复其状态。现在，您将使用实验 2D 中的Raft 的Snapshot()修改 kvserver 以与 Raft 配合使用，以节省日志空间并减少重新启动时间。

测试人员将`maxraftstate`传递给您的`StartKVServer()`。`maxraftstate`表示持久 Raft 状态的最大允许大小（以字节为单位）（包括日志，但不包括快照）。您应该将`maxraftstate`与`persister.RaftStateSize()`进行比较。每当您的键/值服务器检测到 Raft 状态大小接近此阈值时，它都应该通过调用 Raft 的 Snapshot 来保存快照。如果`maxraftstate`为-1，则您不必快照。`maxraftstate`适用于您的 Raft 传递给`persister.SaveRaftState()`的GOB 编码字节。

> 修改您的 kvserver，使其能够检测持久化的 Raft 状态何时变得过大，然后将快照交给 Raft。当 kvserver 服务器重新启动时，它应该从持久化器读取快照并从快照中恢复其状态。

+ 考虑一下 kvserver 何时应该对其状态进行快照以及快照中应包含哪些内容。Raft 使用`SaveStateAndSnapshot()`将每个快照以及相应的 Raft 状态存储在持久对象中。您可以使用ReadSnapshot()读取最新存储的快照。
+ 您的 kvserver 必须能够检测跨检查点日志中的重复操作，因此您用来检测它们的任何状态都必须包含在快照中。
+ 将快照中存储的结构的所有字段大写。
+ 您的 Raft 库中可能存在本实验中暴露的错误。如果您对 Raft 实现进行了更改，请确保它继续通过所有实验 2 测试。
+ 实验室 3 测试的合理时间是 400 秒实际时间和 700 秒 CPU 时间。此外，`go test -run TestSnapshotSize`应花费少于 20 秒的实际时间。

您的代码应该通过 3B 测试（如此处的示例）以及 3A 测试（并且您的 Raft 必须继续通过实验室 2 测试）。

```sh
$ go test -run 3B -race
Test: InstallSnapshot RPC (3B) ...
  ... Passed --   4.0  3   289   63
Test: snapshot size is reasonable (3B) ...
  ... Passed --   2.6  3  2418  800
Test: ops complete fast enough (3B) ...
  ... Passed --   3.2  3  3025    0
Test: restarts, snapshots, one client (3B) ...
  ... Passed --  21.9  5 29266 5820
Test: restarts, snapshots, many clients (3B) ...
  ... Passed --  21.5  5 33115 6420
Test: unreliable net, snapshots, many clients (3B) ...
  ... Passed --  17.4  5  3233  482
Test: unreliable net, restarts, snapshots, many clients (3B) ...
  ... Passed --  22.7  5  3337  471
Test: unreliable net, restarts, partitions, snapshots, many clients (3B) ...
  ... Passed --  30.4  5  2725  274
Test: unreliable net, restarts, partitions, snapshots, random keys, many clients (3B) ...
  ... Passed --  37.7  7  8378  681
PASS
ok   6.824/kvraft 161.538s
```
