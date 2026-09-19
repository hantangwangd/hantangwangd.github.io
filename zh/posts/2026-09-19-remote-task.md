# Presto 查询引擎内核详解：RemoteTask——Coordinator 如何驱动分布式查询执行

---

## 1. Coordinator 视角下的 Task

理解 RemoteTask 之前，首先需要明确一个问题：

> 从 Coordinator 的角度看，一个 Task 到底是什么？

在 Presto 的分布式执行体系中，一个 Query 会被划分为多个 Stage，每个 Stage 又会在不同 Worker 上创建多个 Task。从整体层次来看：

```
    Query
      ↓
    Stage
      ↓
    Task
      ↓
Pipeline / Driver / Operator
```

这个层次结构的前半段（Query → Stage → Task）体现的是**分布式执行的组织关系**，后半段（Task → Pipeline → Driver → Operator）体现的是 **Worker 内部的本地执行体系**。两者的分界线恰好落在 Task 上——这正是理解 RemoteTask 的关键。

### 1.1 Worker Task：真正的执行实体

Task 运行在 Worker 上，负责执行分配给自己的 PlanFragment，并在内部组织 Pipeline、Driver 和 Operator 完成实际的数据处理。这些本地执行对象负责具体的数据读取、计算和输出，但 Coordinator 并不会直接管理它们。对于 Coordinator 来说，它直接面对的是一个更高层、更稳定的执行抽象：Task。

### 1.2 RemoteTask：Coordinator 侧的管理代理

由于 Task 实际运行在远程 Worker 上，因此 Coordinator 需要在本地维护一个对象来代表这个远程 Task，并通过它与 Worker 建立控制和状态同步关系——这个对象就是 RemoteTask。


```
Coordinator                         Worker
───────────                         ──────
RemoteTask  ───── control ────────→  Task
     ↑                                │
     └─────────── observe ────────────┤
                                      ├── Pipeline
                                      ├── Driver
                                      └── Operator
```

由此可以建立全文最重要的一个概念：

> Worker Task 是实际执行实体，RemoteTask 是 Coordinator 侧对 Worker Task 的管理与通信代理。RemoteTask 不是 Worker Task 的另一种实现，而是 Coordinator 与 Worker 之间的**管理边界**。

### 1.3 为什么以 Task 为管理粒度

一个 Query 可能包含多个 Stage，每个 Stage 又可能包含大量分布在不同 Worker 上的 Task。Coordinator 需要逐个管理这些 Task 的生命周期和执行状态：是否启动、当前状态、是否完成或失败、是否需要继续下发执行信息、消耗了多少资源。

因此，Task 是 Coordinator 与 Worker 之间建立直接管理和状态同步关系的基本粒度。但这并不意味着 Coordinator 只能看到 Task 级别的信息——Worker 会将 Pipeline、Driver、Operator 的执行统计聚合到 Task 级别再反馈给 Coordinator。这里存在一个贯穿全文的重要区别：

> **管理粒度 ≠ 观测粒度。** Coordinator 管理到 Task，观测信息却可以来自 Task 内部——只是必须经过 Worker 的聚合，以 Task 级信息的形态呈现。

### 1.4 认知模型

到这里，可以形成一个完整的认知模型：

```
                    Coordinator
                         │
                    RemoteTask
                         │
                    管理 / 状态同步
                         ↓
                    Worker Task
                         │
              ┌──────────┼──────────┐
              ↓          ↓          ↓
           Pipeline    Driver    Operator
                         │
                         ↓
                     实际数据执行
```

后续讨论将始终围绕这条主线展开：Coordinator 通过 RemoteTask 驱动 Worker Task 执行（第 2 章），持续观察其运行状态（第 3 章），并让 Task 级信息进入 Stage 和 Query 的全局执行管理（第 4 章）。

## 2. 下行链路：RemoteTask 如何驱动 Task 执行

本章关注 RemoteTask 的下行控制链路：

```
    StageScheduler
          ↓
     RemoteTask
          ↓
     Worker Task
          ↓
       实际执行
```

对这一过程的核心认识是：

> RemoteTask 不是一次性的远程调用，而是 Coordinator 与 Worker Task 之间**持续存在的执行控制对象**。

### 2.1 创建与 start()：持续同步机制的起点

在 Stage 调度过程中，Coordinator 会根据执行计划和 Worker 分配情况，为每个待启动的 Worker Task 创建对应的 RemoteTask。RemoteTask 在 Coordinator 侧维护该 Task 的全部管理状态：Task ID、Worker Node 与 Task URI、当前状态、待发送的 Splits、OutputBuffers，以及 TaskInfo、TaskStatus 等状态信息。

因此，RemoteTask 并不只是一个保存 HTTP 地址的 Client，而是 Coordinator 中对应一个远程 Worker Task 的**生命周期管理对象**。

RemoteTask.start() 会触发第一次 update。这次 update 会将 Task 执行所需的核心信息发送给 Worker，并在 Worker 侧创建和初始化对应的 Task：

- **PlanFragment**：Task 执行什么；
- **TaskSources**：Task 当前有哪些数据源；
- **Splits**：Task 需要处理哪些数据；
- **OutputBuffers**：Task 的输出如何组织和传递。

start() 的意义不在于发送一条"开始执行"的命令，而在于启动 Coordinator 与 Worker Task 之间的持续同步过程。后续的 addSplits()、OutputBuffer 更新以及其他控制信息，都会经由这条 update 通道逐步同步给 Worker：

> Task 的启动不是一次性的 RPC，而是持续 update 机制的起点。

### 2.2 addSplits()：Split 同步的产生、发送与确认解耦

Task 启动之后，Coordinator 仍需随调度过程不断向 Worker Task 提供新的 Split。对于 Source Stage，StageScheduler 逐步产生并分配 Split，RemoteTask.addSplits() 负责将这些 Split 纳入 Coordinator 侧的待同步状态。

这里的关键设计是：**Split 的产生与网络发送是解耦的**。addSplits() 并不每次都立即发起 HTTP 请求，而是先更新 RemoteTask 本地维护的 pending 状态，再由统一的 update 机制将尚未同步的信息发送给 Worker。这样，StageScheduler 可以按照自己的调度节奏高频产生 Split，而多个连续到达的 Split 会先在 Coordinator 侧累积，再通过一次 update 批量推进：

```
    StageScheduler
        │
        │ 高频产生 Split
        ↓
    RemoteTask
        │
        ├── Split #1
        ├── Split #2
        ├── Split #3
        └── ...
        │
        │ update（串行推进）
        ↓
    Worker Task
        │
        │ ACK
        ↓
RemoteTask（清理已确认 Split）
```

整体形成一种**"高频产生、批量累积、串行推进"**的同步模式：Coordinator 的调度节奏不需要与 Coordinator–Worker 之间的网络通信频率绑定。

为了跟踪同步进度，Presto 会将 Split 封装为 ScheduledSplit，并为其分配 Task 内唯一的 sequenceId。Worker 确认后通过 ACK 反馈已确认的序号，RemoteTask 据此清理 pending 状态。如果请求失败或部分 Split 尚未确认，RemoteTask 会保留相应的本地状态，在后续 update 中继续推进。

因此，addSplits() 背后的核心机制不是简单地"把 Split 发给 Worker"：

> RemoteTask 将 Split 的**产生、发送与确认**解耦，在 Coordinator 侧维护待同步状态，通过有序的 update 与 ACK 持续推进 Split 同步。它维护的不只是"发送什么"，还有"哪些执行输入已经与 Worker 完成同步"——这是明确的 Task 管理语义。

### 2.3 setOutputBuffers()：输出侧的控制状态

addSplits() 解决的是 Worker Task 的输入问题，setOutputBuffers() 解决的则是输出问题：Task 产生的结果如何组织、分发给哪些下游 Task。

在 Presto 的分布式执行模型中，Task 并不会独立完成计算后统一返回结果。对于存在 Remote Exchange 的执行计划，上游 Task 通过 Output Buffer 保存和发送计算结果，下游 Task 通过 Exchange 主动消费这些数据。因此，Coordinator 需要向 Worker Task 提供 Output Buffer 配置信息，描述输出数据如何面向下游组织和分发——它与执行计划中的 Exchange、Partitioning 以及下游 Task 的消费关系密切相关。

从 HttpRemoteTask 的实现来看，setOutputBuffers() 与 addSplits() 遵循同一个模式：先在 RemoteTask 侧更新本地状态，再通过统一的 update 机制同步给 Worker。由此，RemoteTask 同时维护着 Task 的两侧控制状态：

> addSplits() 维护输入侧控制状态，setOutputBuffers() 维护输出侧控制状态；二者最终都通过同一条持续同步机制作用于 Worker Task。

### 2.4 生命周期控制与下行职责小结

除上述机制外，RemoteTask 还承担 Task 生命周期中的其他控制操作：

- noMoreSplits()：通知某个 Source 不再产生新的 Split；
- removeRemoteSource()：移除不再需要的 Remote Source；
- cancel() / abort()：终止 Task 执行。

把这些内容放在一起，可以得到 RemoteTask 下行职责的完整模型：

```
                             Coordinator
                                  │
                             StageScheduler
                                  ↓
                             RemoteTask
                                  │
          ┌───────────────────────┼────────────────────────┐
          ↓                       ↓                        ↓
     PlanFragment               Splits                OutputBuffers
          │                       │                        │
          └─ 生命周期控制（start / noMoreSplits / cancel…）───┘
                                  ↓
                             Worker Task
                                  ↓
                      Pipeline / Driver / Operator
```

> RemoteTask 在 Coordinator 与 Worker Task 之间维护一条持续的执行控制通道：触发 Task 创建与初始化，持续提供 Split 等执行输入，维护输出组织方式，并在 Coordinator 侧跟踪这些控制信息与 Worker 的同步状态。

## 3. 上行链路：RemoteTask 如何观察 Worker Task

分布式 Query 执行期间，Coordinator 不仅需要控制 Task，还需要持续掌握其生命周期、执行统计和输入输出情况。因此，RemoteTask 同时承担两个方向的职责：

```
             Coordinator
                  │
          ┌───────┴───────┐
          │               │
       Control        Observation
          ↓               ↑
      Worker Task ────────┘
```

### 3.1 TaskInfo：Task 的综合执行视图

Worker Task 在运行过程中不断产生执行信息，并以 TaskInfo 的形式提供给 Coordinator。TaskInfo 是 Coordinator 周期性的观察一个远程 Task 的主要信息载体，包含：

- TaskStatus（生命周期与运行状态）；
- TaskStats（执行统计）；
- OutputBuffer 相关信息；
- Source / Split 状态；
- heartbeat 等生命周期信息。

它与第 2 章的控制链路正好形成对应：

```
控制：Coordinator → RemoteTask → Worker Task
观察：Worker Task → TaskInfo → RemoteTask → Coordinator
```

需要注意的是，Observation 并不意味着 Coordinator 直接读取 Worker 内部对象（Pipeline/Driver/Operator）——Worker 将 Task 内部的状态和统计组织成 TaskInfo，再反馈给 Coordinator。

### 3.2 TaskStatus：关键状态的双通道感知设计

TaskStatus 回答的是"这个 Task 现在什么状态"：当前的 TaskState、是否运行、是否完成/失败/取消，以及 Driver、OutputBuffer 等运行状态。

对 Coordinator 来说，Task 状态并不只是展示信息——状态变化本身就是调度推进和异常处理的触发条件。Task 从 RUNNING 变为 FINISHED，可能意味着某个 Stage 的部分执行已经完成；Task 进入 FAILED，则必须及时触发 Stage 或 Query 层面的失败处理。

这里存在一个设计上的矛盾：完整信息适合周期性同步，但周期性轮询无法及时感知状态变化——提高轮询频率又会带来不必要的通信开销。Presto 的解法是将其拆分为两条互补的通道：

```
                         Coordinator
                              │
                 ┌────────────┴────────────┐
                 ↓                         ↓
          周期性获取完整信息              低延迟状态感知
                 │                         │
              TaskInfo               TaskStatus 请求
                 │                   （等待状态变化）
                 ↓                         ↓
          TaskInfoFetcher      ContinuousTaskStatusFetcher
                 └────────────┬────────────┘
                              ↓
                         Worker Task
```

- TaskInfoFetcher：周期性获取相对完整的 TaskInfo，构建 Coordinator 对 Task 的整体执行视图；
- ContinuousTaskStatusFetcher：以更低的通信开销持续感知关键 Task 状态变化，使 FINISHED、FAILED、CANCELED 等状态能够及时反馈到调度与执行控制。

这体现了一个重要的设计思想：

> 完整执行信息不需要以最低延迟获得，而影响调度推进的关键状态必须及时感知。信息完整性、通信开销与调度响应速度三者之间的权衡，被 Presto 显式拆分为两条各司其职的通道，而不是压缩到一个轮询机制里。

### 3.3 TaskStats：Worker 内部执行统计的聚合

TaskStats 回答的是"这个 Task 到目前为止执行得怎么样"。

Pipeline、Driver 和 Operator 在 Worker 上执行实际数据处理并产生统计信息，Worker 将其聚合后通过 TaskStats 反馈给 Coordinator，涵盖 CPU、Memory、Input/Output、Driver、Operator、Processing 等维度。

这正是 1.3 节"管理粒度 ≠ 观测粒度"的体现：

> Coordinator 直接管理的只是 Task，但通过 Worker 的聚合上报，其观测能力可以深入到 Task 内部的 Driver / Operator 执行统计。

### 3.4 小结：从一次快照到持续观察

Task 的状态和统计随执行不断变化，Coordinator 需要的不是一次性快照，而是持续更新的执行视图。这种持续观察由两条通道共同构成：

```
    Task（State / Statistics / I/O 持续变化）
                 │
           ┌─────┴─────┐
           ↓           ↓
       TaskInfo     TaskStatus
           │           │
    TaskInfoFetcher  ContinuousTaskStatusFetcher
           └─────┬─────┘
                 ↓
            Coordinator
```

> RemoteTask 的 Observation 职责，是让 Coordinator 持续获得 Worker Task 的运行状态和执行信息：TaskInfo 是主要信息载体，TaskStatus 反映当前状态，TaskStats 反映内部执行情况。对于完整执行视图和关键状态变化，Presto 分别采用周期性同步和低延迟状态感知机制，以兼顾信息完整性、通信开销和调度响应速度。

## 4. 状态反馈：从 Task 局部状态到 Stage 与 Query 的全局调度

Coordinator 不仅要了解 Worker Task 的运行状态，还要用这些状态推进 Stage 和 Query 的执行。本章讨论：Worker Task 的局部执行状态，如何经由 RemoteTask 进入 Coordinator 的全局执行控制体系？

### 4.1 Task 状态：调度的输入，而非决策本身

Worker Task 在执行中经历 PLANNED → RUNNING → FINISHED 的生命周期，异常时进入 FAILED。这些状态首先发生在 Worker 上，通过 TaskStatus 经由 RemoteTask 进入 Coordinator，最终汇聚到 SqlStageExecution。

一个 Stage 通常包含多个 Task，因此单个 Task 的状态不能直接决定 Stage 的状态。**Task FINISHED ≠ Stage FINISHED**——某个 Task 完成只意味着 Stage 的部分执行工作完成。SqlStageExecution 需要综合所有 Task 的执行情况和 Stage 自身的调度条件，判断 Stage 的生命周期并决定下一步动作：

```
      Task 状态变化
           ↓
    SqlStageExecution
           ↓
    重新评估 Stage 执行条件
           ↓
    继续调度 / 等待 / 完成
```

例如，随着上游 Task 执行并产生输出，下游 Stage 会逐步获得执行条件；当 Stage 中的 Task 达到完成条件时，Stage 的生命周期也随之推进。

因此需要区分：

> Task 状态是调度的输入，而不是调度决策本身。Worker Task 负责产生执行事实，RemoteTask 负责管理和观察 Task，SqlStageExecution 则根据这些事实结合整体执行条件，决定如何推进执行图。

### 4.2 失败传播：局部异常进入全局控制

Task 的失败首先是 Worker 上的局部执行异常，但由于 Task 隶属于 Stage、Stage 又属于 Query，这一状态会经由 RemoteTask → SqlStageExecution 逐级上升：

```
    Worker Task (FAILED)
          ↓
      RemoteTask
          ↓
    SqlStageExecution
          ↓
   Stage / Query 失败处理
```

Coordinator 据此更新 Stage 执行状态、进行失败传播，并根据执行策略决定重试或终止 Query。具体的恢复机制属于更高层的执行策略，这里不展开。核心关系是：

> Worker Task 的局部失败，能够通过 RemoteTask 进入 Coordinator 的全局执行控制体系。

### 4.3 信息汇聚：Task → Stage → Query 的全局视图

除了驱动调度，Task 级信息还需要逐级汇聚，形成全局执行视图：

```
        Worker Task
            ↓
    TaskInfo / TaskStats
            ↓
   StageExecution / StageInfo
            ↓
    QueryInfo / QueryStats
```

Task 层反映单个执行实体的状态和统计，Stage 层形成一组 Task 的整体执行视图，Query 层进一步形成整个查询的全局视图。这里表示的是执行信息的组织与汇聚关系，而非对象之间简单的一对一构造关系。

这些信息的价值并不止于监控。Task 的 CPU、Memory、Input/Output 统计经过汇聚后，为 Coordinator 的资源管理提供数据基础——资源管理涉及 Query、Resource Group、Memory Pool 等多个层次，会据此实施并发限制、内存控制、Memory Revoking，乃至必要的 Query 终止。产生于 Worker 的底层执行统计，最终参与的是 Coordinator 的全局资源管理。

### 4.4 Control–Observation 闭环

将下行控制与上行观测结合起来，可以得到完整的执行反馈过程：

```
        Coordinator
             │ Control
             ↓
        RemoteTask
             ↓
        Worker Task
             │ 执行
             ↓
        Task 状态变化
             │ Observation
             ↓
        RemoteTask
             ↓
     SqlStageExecution
             ↓
        调度决策 ────→ Control（回到起点）
```

在更宏观的视角下，这条闭环继续向外延伸：

```
    Worker Task
          ↓ Observation
    Task → Stage → Query（全局执行视图）
          ↓
    Resource Management
          ↓
    Execution Control ────→ Worker Task
```

由此，RemoteTask 的 Control 与 Observation 并不是两条独立的链路，而是同一个反馈闭环的两段弧：

> Control 驱动 Worker 执行，Observation 将执行状态反馈给 Coordinator，Coordinator 依据这些状态推进调度和资源管理，再形成新的控制。RemoteTask 是这个闭环在 Task 层面的连接点。

## 5. 总结：RemoteTask——分布式执行的管理边界

Presto 的分布式执行，本质上是 Coordinator 的全局控制与 Worker 的局部执行之间持续协同的过程。Worker Task 负责真正执行计算，Coordinator 负责从全局角度组织和推进整个 Query，而 RemoteTask 以 Task 为边界建立两者的管理关系：

```
        Coordinator
             │
         RemoteTask
             │
     ┌───────┴────────┐
     ↓                ↓
   Control        Observation
     ↓                ↑
     └────────→ Worker Task
            Pipeline / Driver / Operator
```

向下，RemoteTask 将 PlanFragment、Splits、OutputBuffers 和生命周期控制持续同步到 Worker，并在 Coordinator 侧跟踪同步状态（高频产生、批量累积、串行推进）；向上，它通过 TaskInfo 周期同步与 TaskStatus 低延迟感知构成的双通道，持续获得 Worker 的执行反馈。

这些 Task 级信息进一步沿 **Task → Stage → Query** 逐级汇聚，将分散在不同 Worker 上的局部执行组织成 Coordinator 的全局执行视图，为调度推进和资源管理提供基础，最终形成完整的 Control–Observation 反馈闭环。

因此，从更高层次看：

> RemoteTask 不是简单的 RPC 代理，而是 Presto 分布式执行控制与状态反馈在 Task 层面的管理边界：向下控制、向上观测、逐级汇聚、反馈调节——将分布式 Worker 上的局部执行，组织成 Coordinator 可以统一推进和管理的全局 Query。


---

## 欢迎交流

本文基于作者当前的理解与实践经验整理而成，难免存在疏漏或值得进一步探讨之处。

如果您对文中的观点有不同看法，发现任何问题，或有相关实践经验，欢迎通过 [GitHub Issue](https://github.com/hantangwangd/hantangwangd.github.io/issues/new) 与作者交流讨论。

期待与更多同行围绕数据基础设施相关技术展开交流，分享实践经验，共同学习、共同进步。
