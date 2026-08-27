# Presto 查询引擎内核详解：Spill-to-Disk 执行

#### Presto Spill-to-Disk Execution：从内存压力到算子溢写

---

在分布式查询引擎中，内存通常是决定查询能够处理多大数据规模的关键资源。Hash Aggregation、Hash Join、Sort、Window 等算子都可能随着输入数据增长而维护越来越大
的中间状态。当这些状态超过 Worker 可用内存时，如果查询引擎只能继续申请内存，最终就只能以 OOM 或 Query Failure 结束。

Spill-to-Disk 是解决这一问题的重要机制：当内存压力达到一定程度时，将部分可以外置的中间状态写入本地磁盘，释放内存，并让查询继续执行。

但 Presto 的 Spill 并不是简单的“把 Operator 的内存状态序列化到磁盘，之后再全部加载回来”。它实际上是一套由 Memory Management、Memory Revocation、
Driver Scheduling 和 Operator-specific External-Memory Execution 共同构成的执行机制。

从整体上看，可以将它概括为：

```
    Memory Pressure
          ↓
    Memory Revocation
          ↓
    Driver Execution
          ↓
    Operator-specific Spill
          ↓
    Memory Reclamation
          ↓
    Execution Resumes
```

本文从 Worker 的内存管理开始，逐步分析 Presto 如何发现内存压力、如何选择需要 Spill 的 Operator、为什么必须主动唤醒 Driver，以及 Operator 如何完成实际的
磁盘溢写，最终形成完整的执行闭环。

## 1. 为什么需要 Spill：从 Worker 内存压力说起

### 1.1 查询执行中的内存为什么会不断增长？

很多查询算子都不是简单地读取一条数据、处理一条数据然后立即输出。

例如，对于如下的聚合查询：

```
SELECT key, count(*) FROM table GROUP BY key;
```

Hash Aggregation 需要维护一个类似这样的结构：

```
                  Hash Table
        ┌────────────────────────────────┐
        │                                │
        │  (key₁, aggregation state₁)    │
        │  (key₂, aggregation state₂)    │
        │  (key₃, aggregation state₃)    │
        │  (key₄, aggregation state₄)    │
        │             ...                │
        │                                │
        └────────────────────────────────┘
```

随着输入数据不断到达，不同的 key 越来越多，Hash Table 也不断增长。

Hash Join 也类似：Build Side 通常需要首先建立 Hash Table，然后再与 Probe Side 执行连接。Sort 则需要保留需要排序的数据，Window Operator 也可能需要保存
大量中间状态。

因此，一个 Query 的执行过程实际上可能持续消耗大量内存：

```
     Input
       ↓
    Operator
       ↓
    Intermediate State
       ↓
    Memory Usage ↑
```

当 Query 数据规模扩大以后，单个 Operator 或整个 Worker 的内存使用都可能达到很高水平。

### 1.2 为什么不能简单地继续申请内存？

假设一个 Worker 总共只有 100 GB 内存。

现在多个 Query 同时执行：

```
Query A → 40 GB
Query B → 30 GB
Query C → 20 GB
----------------
Total    90 GB
```

此时某个 Hash Aggregation 又需要 20 GB：

```
90 GB + 20 GB = 110 GB
```

如果查询引擎简单地允许继续申请，那么最终可能因为无法满足内存需求而触发 OOM，甚至导致 Worker 进程失败；而如果仅仅通过严格的 Query 级内存上限来限制查询，一旦查询的
内存需求超过该上限就直接失败，对于需要处理大规模数据的查询来说同样不够友好，也限制了查询引擎处理超出单机内存规模的数据集的能力。

对于一个成熟的查询引擎而言，更合理的方式是让部分不需要持续驻留在内存中的执行状态能够被外置。

例如，Hash Aggregation 的部分中间聚合状态可以被写入磁盘，之后再通过特定的外部内存算法重新处理这些磁盘上的中间结果，最终生成查询结果。

这就是 Spill-to-Disk 的基本思想。

### 1.3 Presto Spill 的核心并不是“保存 Operator 状态”

Presto 的 Spill 并非某种通用的 Operator Suspension 机制，其更接近：

```
               Operator
             /         \
           Memory       Disk
            │            │
      current state   spilled state
             \          /
              \        /
          continue execution
```

在该机制中，Operator 并没有整体被“暂停并保存”。相反，Operator 会将部分中间数据转化为磁盘上的 spill data，同时保留继续执行所需的必要状态，并在内存释放完成后
继续推进后续计算。

因此，从执行模型上说，Presto Spill 更准确地描述为：

> Operator-specific external-memory execution

即：由具体 Operator 自己实现的外部内存执行算法。这也是理解 Presto Spill 后续所有机制的基础。

## 2. Presto 的内存管理：Memory 如何控制 Operator 执行

理解 Spill，首先必须理解 Presto 的 Memory Management。这部分的详情可以参见我写的另一篇文章：《Presto 查询引擎内核详解：集群资源管理机制解析》。

### 2.1 Operator 的内存申请与 Worker MemoryPool

Operator 在执行过程中需要申请和释放内存。这些内存申请通过 Presto 的层次化 Memory Context 进行管理，并最终汇聚到 Worker 的 MemoryPool。

从概念上可以理解为：

```
                 Operator
                     ↓
             OperatorContext
                     ↓
          MemoryTrackingContext
                     │
          ┌──────────┼──────────┐
          ▼          ▼          ▼
       Local      Aggregated
       Memory       Memory     ...
       Context      Context
          └──────────┼──────────┘
                     ↓
           Leveled Memory Accounting
                     ↓
                 MemoryPool
```

这种层次化设计使得 Operator 的局部内存需求能够最终纳入 Worker 全局的内存管理体系。当多个 Operator 和 Query 的内存使用共同导致 MemoryPool 面临压力时，
上层 Memory Management 机制便可以进一步进行 Memory Blocking 或 Memory Revocation。

例如，在算子内部，当使用了一定数量的内存之后，执行：

```
memoryContext.setBytes(bytes);
```

这个操作会逐层向上传递并最终影响 Worker MemoryPool 中的内存状态。

因此，Operator 的一次普通内存申请实际上已经与整个 Worker 的内存管理体系建立了联系。

### 2.2 Memory 不仅仅是 Accounting

如果 Memory Context 只是记录：

```
Operator A = 10 GB
Operator B = 20 GB
Operator C = 30 GB
```

那么它只是一个 accounting system。

但 Presto 的 Memory Management 更进一步：Memory Management 会直接参与执行流控。

当 Operator 申请内存时，如果 MemoryPool 能够满足，则后续执行继续进行：

```
     reserve
        ↓
    Memory available
        ↓
     success
        ↓
    Operator continues
```

但是，如果 MemoryPool 无法满足，则会通过返回的 blocked future 来阻塞 Operator 的后续执行：

```
     reserve
        ↓
    Memory insufficient
        ↓
     blocked Future
        ↓
     Operator waits
```

于是 MemoryPool 的状态就可以反向影响到 Operator 的执行了。

### 2.3 Memory Blocked Future

可以将这个 Future 理解为：“当前内存不足，等有人释放内存后再继续”。例如：

```
    Operator
        │ request 10 MB
        ▼
    MemoryPool
        │ insufficient
        ▼
    blocked Future
```

OperatorContext 持有与之相关的内存等待状态：`memoryFuture` 和 `revocableMemoryFuture`，分别代表了在申请 user memory 和 revocable memory 时由于内存不足
而导致的阻塞状态。

Driver 在判断某个 Operator 是否处于阻塞状态，以及 Driver 本身是否整体处于阻塞状态时，都会去检查 OperatorContext 中的这种状态。这也说明 Memory Pool 已经进入了
Query Execution 的控制路径。

### 2.4 Revocable Memory

对于 Spill 来说，更重要的是 Revocable Memory。但并不是所有 Operator 使用的内存都可以随意回收。

- 有些内存：一旦释放，就会破坏当前 Operator 的执行状态。
- 另外一些内存：可以通过 Spill 转化为磁盘上的中间数据，因此具有可回收性。

这其中的后者就是 Revocable Memory。

可以粗略理解为：

```
Worker Memory
    ├── User / Non-revocable Memory
    │       └── 一般不能直接要求 Operator 释放
    │
    └── Revocable Memory
            └── 执行状态可以被 externalize，因此可以通过 Spill reclaim 的内存
```

所以 Memory Revocation 的目标并不是：

> “随便找一个 Operator，让它释放内存。”

而是：

> 找到持有可回收状态的 Operator，让它通过 Spill 主动释放 Revocable Memory，并以 external-memory execution 的模式继续执行。

## 3. Memory Revocation：内存压力如何转化为 Spill 请求

### 3.1 Memory Pressure

随着多个 Operator 持续执行：

```
    Memory Reservation ↑
        ↓
    MemoryPool Usage ↑
```

当内存使用达到需要 Revocation 的条件时，Presto 的 MemoryRevokingScheduler 会介入。

MemoryRevokingScheduler 会将 onMemoryReserved() 监听逻辑注册到其管理的每一个 MemoryPool 上。因此，当 Operator 的内存 reservation 导致 MemoryPool
状态发生变化时，相关状态变化会进入 MemoryRevokingScheduler 的监听路径，由 MemoryRevokingScheduler 判断当前是否需要启动 Revocation。

```
                         registers listener
                  ┌──────────────────────────┐
                  │                          │
                  ▼                          │
        ┌───────────────────┐     ┌──────────┴─────────────┐
        │    MemoryPool     │────►│ MemoryRevokingScheduler│
        └───────────────────┘     └───────────┬────────────┘
                  ▲                           │
                  │ reservation change        │ evaluate pressure
                  │                           ▼
             Operator reserve          Trigger Revocation
```

### 3.2 MemoryRevokingScheduler 做什么？

MemoryRevokingScheduler 的核心职责并不是实际执行 Spill。它主要解决的是：

> 现在是否需要释放内存？如果需要的话，应该让哪些 Operator 来释放？

因此可以把它理解成一个 Memory-Reclamation Coordinator。流程大致是：

```
                 ┌──────────────────────────┐
                 │ MemoryRevokingScheduler  │
                 │                          │
Memory Pressure ─► Evaluate pressure        │
                 │         ↓                │
                 │ Calculate required       │
                 │ revocation               │
                 │         ↓                │
                 │ Select suitable operators│
                 └──────────┬───────────────┘
                            │ request revocation
                            ▼
                     Selected Operators
```

这里的职责边界非常重要：Scheduler 决定“谁应该释放内存”，而 Operator 本身决定“如何释放这些内存”。

### 3.3 requestMemoryRevoking()

当 Scheduler 找到合适的 Operator 后，会调用：

```
operatorContext.requestMemoryRevoking();
```

这会在 OperatorContext 中设置 Memory Revocation Request 状态。在逻辑上可以将其理解为：

> “这个 Operator 下一次获得执行机会时，需要先执行 Memory Revocation。”

于是，一个看似简单的问题随之出现：既然 Scheduler 已经确定了需要释放内存的 Operator，为什么不直接调用 Operator 的 startMemoryRevoke()，而是只设置一个
Revocation Request？

答案涉及 Presto 的核心执行模型：

> Memory Revocation 并不是由 MemoryRevokingScheduler 直接驱动 Operator 执行，而是需要回到 Driver 的统一调度执行路径，由 Driver 在合适的执行时机处理
> Revocation Request。

这也就引出了下一章要讨论的问题：

Revocation Request 是如何从 MemoryRevokingScheduler 进入 Driver，并最终驱动 Operator 执行实际的 Spill？

## 4. Driver 如何驱动 Memory Revocation

### 4.1 一个看似矛盾的问题

假设 Operator 当前因为内存不足而 blocked，而现在 MemoryRevokingScheduler 又要求：“请你执行 Spill，释放内存。”

可是此时 Operator 甚至整个 Driver 可能都已经 blocked 了，如果它完全依赖正常执行流程：

```
    Driver blocked
          ↓
    被调度到了也无法运行
          ↓
 不能驱动 Operator 执行 Spill
          ↓
    不能释放 Memory
          ↓
 其他 Operators 也无法获得 Memory
```

于是就可能进入一种无法破解的整体阻塞状态。

Presto 对此的解决方式是：Memory Revocation Request 会主动唤醒 Driver。

### 4.2 Driver 的 driverBlockedFuture

Driver 中维护：

```
AtomicReference<SettableFuture<?>> driverBlockedFuture
```

它代表 Driver 当前是否处于 blocked 状态。

在 Driver 初始化时，会为其持有的每一个 active Operators 注册 Memory Revocation Listener。该 listener 监听每一个算子上的 requestMemoryRevoking 动作，
并在该动作发生的时候执行`driverBlockedFuture.set(null)`以取消当前 Driver 的阻塞状态，如下图所示：

```
    OperatorContext
        │
        │ requestMemoryRevoking()
        ▼
    Memory Revocation Listener
        │
        ▼
    driverBlockedFuture.set(null)
        │
        ▼
    Driver becomes runnable
```

因此：

> Memory Revocation Request 会打破 Driver 原本因为某些等待条件而形成的 blocked 状态，使其重新进入可运行/可调度路径，从而有机会执行 Memory Revoke。

注意这里一个非常容易产生的误解：

> Driver 被唤醒并不意味着内存已经恢复。

恰恰相反：

> Driver 被唤醒，是为了让它驱动具体的 Operator 执行释放内存的动作。

### 4.3 Driver processFor() 中的判断

Driver 每次获得执行机会时，会首先检查自己的 blocked 状态。如果 Driver 当前确实因为某个 Future 等待：

```
driverBlockedFuture not completed
```

那么它不会继续执行。

而当 Memory Revocation Request 触发时设置了：

```
driverBlockedFuture.set(null)
```

Driver 就可以在下一次调度时继续执行。于是：

```
    Memory Pressure
        ↓
    requestMemoryRevoking()
        ↓
    wake Driver
        ↓
    Driver executes
        ↓
    handleMemoryRevoke()
```

### 4.4 handleMemoryRevoke()

Driver 获得执行机会后，首先会执行方法 `handleMemoryRevoke()`。其中会遍历 active Operators，对于满足如下条件的 Operator：

```
not already revoking + isMemoryRevokingRequested()
```

调用如下方法以启动异步的溢写及内存回收行为：

```
operator.startMemoryRevoke();
```

返回的 Future 会维护在 revokingOperators 中。于是整个状态变成：

```
    requestMemoryRevoking()
            ↓
    Driver awakened
            ↓
    handleMemoryRevoke()
            ↓
    startMemoryRevoke()
            ↓
    spill in progress
            ↓
 溢出执行的状态维护在 revokingOperators
```

### 4.5 为什么需要 revokingOperators？

因为 Spill 是异步执行的。如下所示：

```
    startMemoryRevoke()
          ↓
      serialize
          ↓
      write disk
          ↓
        flush
          ↓
    Future complete
```

Driver 不能在 Spill 尚未完成时继续正常操作这个 Operator。因此，在 Future 完成之前将 Operator 维护在 revokingOperators 中。这样一来，Driver 会把这个
Operator 视为处于 Memory Revocation 状态。然后在其 `checkOperatorFinishedRevoking()` 方法中，检查并确认 Spill 成功之后，将 Operator 从
revokingOperators 里移除，并调用：

```
finishMemoryRevoke()
```

最后清除 memoryRevokingRequest 标志，整个过程就完成了。

### 4.6 几种 Future 的职责必须区分

这一套代码中涉及到的异步操作 Future 很多，很容易混淆。可以用下面的方式理解：

| Future 类型                      | 含义                                          |
|:-------------------------------|:--------------------------------------------|
| **driverBlockedFuture**        | 表示Driver当前是否整个处于阻塞状态                        |
| **memoryFuture**               | 表示Operator是否因为普通内存（User Memory）不足而等待        |
| **revocableMemoryFuture**      | 表示Operator是否因为可撤销内存（Revocable Memory）不足而等待  |
| **memory revoke/spill Future** | 表示当前内存回收（Memory Revocation）或溢出（Spill）操作是否完成 |

它们解决的是不同层次的问题：

- Memory Future: “我没有内存，不能继续。”
- Revoke Future: “我要释放内存，但 Spill 还没完成。”
- Driver Future: “当前 Driver 是否处于阻塞状态。”

理解这一点之后，Presto 的整个控制流就会清晰很多。

## 5. Operator-Specific Spill：Presto 如何把算子状态溢写到磁盘

### 5.1 Spill 并不是通用的 Operator Snapshot

Presto 为 Operator 定义了如下的接口方法：

```
ListenableFuture<?> startMemoryRevoke();
void finishMemoryRevoke();
```

但是这两个接口本身并没有规定：

> “所有 Operator 应该如何执行 Spill。”

换句话说，Presto 的 Memory Revocation API 抽象的是“释放可撤销内存”的生命周期，而不是“如何持久化 Operator State”的数据模型。具体实现由 Operator 自己负责，即：

- HashAggregationOperator -> Aggregation-specific Spill
- OrderByOperator         -> Sort-specific Spill
- HashBuilderOperator     -> Join-specific Spill
- ......

因此，Presto Spill 的一个核心设计思想是：

> Spilling is operator-specific.

### 5.2 为什么不做一个通用的 Spill Framework？

因为不同 Operator 的中间状态完全不同。

- 对于 Hash Aggregation： Hash Table
- 对于 Sort： Rows / sorted runs
- 对于 Hash Join： Build-side hash structures / partitions

这些数据不仅结构不同，而且最终如何重新合并的算法也各有不同。

例如，Sort 可以采用：External Merge Sort 算法；而 Hash Aggregation 则需要：Re-aggregate spilled data。这些差异并不是简单的实现细节，
而是不同 Operator 的核心执行算法本身，因此很难由一个通用 Spill Framework 完全抽象。

因此，Presto 选择了另一条路线：

> 由 Operator 自己实现适合其执行逻辑的数据外置与恢复算法。

### 5.3 Spill 后 Operator 如何继续推进

这是理解 Presto Spill 的关键。在 Presto 中，Spill 并不是：

```
Memory -> Disk -> Operator suspended
```

而是：

```
                Operator
              /         \
          Memory        Disk
             │            │
       current state   spilled data
             │            │
             └─────┬──────┘
                   ↓
           continue execution
```

Spill 并不意味着整个 Operator 状态被序列化写出并暂停。对于支持增量 Spill 的 Operator，startMemoryRevoke() 触发其将当前可外置的中间数据转换为 Spill Data，
并释放相应的 Revocable Memory。Operator 随后可以继续处理新的输入直至产生最终输出结果。

因此，一个 Operator 在运行过程中可以同时维护：

```
In-memory intermediate state + Disk-resident intermediate data
```

甚至可以多次执行 Spill：

```
        Memory
          ↓
    Spill #1 -> Continue
          ↓
    Memory grows again
          ↓
    Spill #2 -> Continue
          ↓
        ......
```

最终，算子的中间状态包含了内存中和溢写到磁盘上的状态：

```
Memory State + Spill File #1 + Spill File #2 + Spill File #3 + ...
```

这是一种典型的 External-Memory Execution 模式。

### 5.4 Spill Data 如何参与后续计算？

最终产生输出时，Operator 不能只处理当前内存中的状态（例如 Hash Table）。它还必须处理之前 Spill 到磁盘的中间数据。因此最终过程可以抽象为：

```
    Spill Data + Memory State
            ↓
    Operator-specific processing
            ├── External Merge
            ├── Re-aggregate
            ├── Partition Processing
            └── ...
```

这正是 Operator-specific External-Memory Algorithm 的体现。

### 5.5 HashAggregationOperator

以 Hash Aggregation 为例。当其正常执行时：

```
          Input
            ↓
    HashAggregationOperator
            ↓
    InMemoryHashAggregationBuilder
            ↓
        Hash Table
```

随着后续输入不断到达，Hash Table 变得越来越大，占用的内存也越来越大。当 MemoryRevokingScheduler 要求它释放内存时：

```
    MemoryRevokingScheduler
            │ requestMemoryRevoking()
            ▼
    HashAggregationOperator
            │ startMemoryRevoke()
            ▼
    启动异步 Spill
            │
            ├──────────────► Spiller
            │                    │
            │                    ▼
            │              以 group key 有序的方式
            │              写出 Aggregation State
            ▼
    释放可撤销内存
            │
            ▼
    清理 / 重置内存中的 Aggregation State
```

此时 Hash Aggregation 的逻辑状态就变成：

```
HashAggregationBuilder
    │
    ├── In-memory aggregation state
    │
    └── Spilled intermediate data
            ├── spill file #1
            ├── spill file #2
            └── ...
```

溢写完成之后 Operator 可以继续接收新的输入，并且有可能随着内存的不断增加再次触发 requestMemoryRevoking()。

> 注意：发生 Spill 后，内存中的聚合状态不再能够直接作为最终聚合结果输出。由于相同的 Group By Key 可能同时存在于内存中的聚合状态以及已经 Spill 到磁盘的
> 中间聚合结果中，因此最终需要对这些 intermediate aggregation states 再次进行合并聚合。为此，在执行 Spill 时，Presto 会将当前内存中的聚合状态转换为
> 适合外部处理的 intermediate/partial aggregation representation，并通过 buildHashSortedResult() 等逻辑构造有序的中间结果，然后写入 Spill 文件。

最终生成结果时，将内存中的以及多个 Spill 文件中的有序中间聚合结果进行 Merge，并对属于相同 Group By Key 的 aggregation states 执行最终合并。通过这种方式，
可以流式的产生最终聚合结果，从而避免在内存中维护一个覆盖全部数据的巨大 Hash Table。

最后需要强调的是，HashAggregation Spill 的关键并不是单纯的把 Hash Table 写到磁盘，而是通过 buildHashSortedResult() 将内存中的
Hash-based aggregation state 转换成有序的 intermediate representation，使后续能够通过 External Merge 和流式 Re-aggregation
在有限内存下完成最终聚合。

### 5.6 不同 Operator 的 Spill 算法不同

前面的 HashAggregation 只是 Operator-Specific Spill 的一个具体例子。Presto 没有规定所有 Operator 必须采用统一的 Spill Algorithm，而是由具体的
Operator 根据自身的数据结构、计算语义以及最终结果的生成方式，选择合适的 External-Memory Processing 方案。因此，不同 Operator 的 Spill 行为可能存在
很大的差异。

例如，OrderBy 的核心问题是如何将无法完全放入内存的数据转换为多个有序数据集，并最终通过 External Merge 产生有序输出：

```
    Input
      ↓
    In-memory sorting
      ↓
    sorted run
      ↓
    spill
      ↓
    multiple sorted runs
      ↓
    external merge
      ↓
    output
```

而 Hash Join 通常通过 Partitioning 将数据划分成多个可以独立处理的 Partition，当内存不足时将部分 Partition 外置，之后逐个处理这些 Partition：

```
    Build data
      ↓
    partition
      ↓
    spill partitions
      ↓
    process partitions
      ↓
    join
```

因此不能简单地说：

> “Presto 可以把 Operator 的 state 溢写到磁盘。”

更准确的说法是：

> Presto 将 Operator 中可以外置的中间数据转换成磁盘上的 Spill Data，并由 Operator 自己负责后续的 External-Memory Processing。

## 6. Spill 完成后，内存如何真正释放并恢复执行？

前面的流程描述的是：

> “如何让 Operator 执行 Spill？”

现在还需要解决另外一个问题：

> Spill 完成之后，释放出来的内存如何让此前因内存不足而阻塞的 Operator 恢复执行？

### 6.1 finishMemoryRevoke()

Driver 被调度执行时，会在其 `checkOperatorFinishedRevoking()` 方法中，检查 Operator 的 Spill future 是否完成。在确认 startMemoryRevoke() 返回的
future 完成以后，调用该算子的：

```
finishMemoryRevoke()
```

这个阶段主要完成本次 Memory Revocation 的收尾工作。其中，最重要的一步是：更新 Operator 的 Memory Accounting，释放已经被溢写到外部存储的 Revocable Memory。

于是形成如下的反馈：

```
    Operator
        │ finishMemoryRevoke()
        ▼
Update Memory Accounting
        │
        ▼
Revocable Memory Reservation ↓
        │
        ▼
  MemoryPool.free()
        │
        ▼
  Available Memory ↑
```

Memory Revocation 真正完成的标志，不是 Spill File 已经写入磁盘，而是 Operator 已经完成 Memory Accounting 的更新，使对应的
Revocable Memory Reservation 被释放。

### 6.2 内存释放如何解除被阻塞的 Operator

之前内存不足时：

```
         MemoryPool
             ↓
 Available Memory insufficient
             ↓
  Memory Future not completed
             ↓
   Operator blocked on memory
```

现在，当 MemoryPool 的可用内存因为 Revoke 而增加后，之前等待内存资源的 Operator 所关联的 Memory Future 得以重新满足完成条件，从而解除其内存阻塞：

```
    MemoryPool.free()
          ↓
     free bytes ↑
          ↓
  Operator memory future
      completed
          ↓
   Operator unblocked
          ↓
    Driver runnable
          ↓
   continue execution
```

## 7. 总结：Presto Spill 是一个 Memory-Driven Execution Feedback Loop

如果只从“数据落盘”的角度看，Spill 似乎只应该是：Memory → Disk。但从 Worker Execution Engine 的角度看，Spill 的核心并不是数据落盘，而是通过
Operator-specific External-Memory Processing 将内存压力转化为可执行的资源回收动作，并最终恢复系统的执行能力。

- MemoryRevokingScheduler 负责发现内存压力并选择需要释放 Revocable Memory 的 Operator；
- Driver 负责响应 Revocation Request，并获得执行机会以驱动 Operator 执行 startMemoryRevoke()；
- 最终由 Operator 根据自身的 External-Memory Algorithm 完成状态外置，并在释放内存后继续执行。

因此，Presto Spill 最准确的架构抽象并不是：

> “Presto 在内存不足时把数据写到磁盘。”

而是：

> Presto 实现了一套由内存压力驱动的外部内存执行机制，通过将算子中可外置的中间数据转换为 Spill Data 并释放相应的 Revocable Memory，使查询能够在有限内存下继续执行。

这也是理解 Presto Spill 最重要的一点：Spill 并不是一个独立的磁盘 I/O 功能，而是整个执行引擎围绕内存压力形成的一套反馈控制机制。

而 Operator-specific External-Memory Execution，则是这套机制能够真正落地的关键：

> Memory Management 决定“什么时候需要释放”，Driver 负责“如何驱动释放”，Operator 决定“如何将自己的中间数据外置并继续执行”。

三者共同构成从 Memory Pressure → Memory Revocation → Spill → Memory Release → Execution Recovery 的完整闭环。



---

## 欢迎交流

本文基于作者当前的理解与实践经验整理而成，难免存在疏漏或值得进一步探讨之处。

如果您对文中的观点有不同看法，发现任何问题，或有相关实践经验，欢迎通过 [GitHub Issue](https://github.com/hantangwangd/hantangwangd.github.io/issues/new) 与作者交流讨论。

期待与更多同行围绕数据基础设施相关技术展开交流，分享实践经验，共同学习、共同进步。