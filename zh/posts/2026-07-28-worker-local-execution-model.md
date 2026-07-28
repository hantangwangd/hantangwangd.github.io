# Presto 查询引擎内核详解：Worker 本地执行模型——从物理计划到可调度执行单元

本文档深入探讨 Presto Worker 节点内部的核心执行机制。我们将追踪一个 Stage 的 PlanFragment 如何从物理计划片段一步步转化为可并发执行的本地任务，并最终被调度器高效执行。

涉及到的核心概念包括 Driver、SplitRunner、TaskExecutor 以及多级优先级队列。

## 1. 核心概念：从物理计划片段到本地执行单元

### 1.1 Driver：执行片段与数据分组的二维切分

当一个Stage的PlanFragment被发送到Worker节点并规划成本地可执行计划后，它会被切分成多个Pipeline。每个Pipeline是组可以流水线化执行的物理算子（Operator）。
随后，每个Pipeline会根据执行策略被实例化为多个 Driver 实例。

Driver 是 Presto 中的核心执行概念，它被通过两个维度进行定义：

- Pipeline 维度（算子链形状）：属于同一个 Pipeline 的所有 Driver 实例拥有完全相同的算子列表（List<Operator>），即执行相同的处理逻辑。
- Lifespan 维度（数据分组）：Lifespan 代表了数据的分组方式。同一个 Pipeline 下，不同 Lifespan 的 Driver 处理不同的数据组（例如，对应一个Split，或一个 Grouped 数据块）。而相同 Lifespan 下也可能创建多个 Driver 实例来并行处理同一组数据（单纯增加并行度），以提升性能。

> Pipeline 决定执行逻辑，Lifespan 决定数据隔离边界，而 Driver 是二者组合后产生的实际执行实例。

关键规则：切分后，每个 Driver 最多只有一个 SourceOperator 类型的输入节点（如 TableScanOperator 或 ExchangeOperator）。

### 1.2 DriverSplitRunner & PrioritizedSplitRunner：可调度的最小执行单元

Driver本身不会直接被调度执行，它会被封装到DriverSplitRunner中：

- DriverSplitRunner：一个具体的Driver实例与其所属的Lifespan、DriverContext绑定。它代表了“一个Pipeline片段在特定Lifespan上的一个运行实例”。
- PrioritizedSplitRunner：这是DriverSplitRunner的进一步封装，是调度器可以管理并调度的最小单位。它在DriverSplitRunner之上增加了优先级和分时调度的概念。
    - PrioritizedSplitRunner实现了SplitRunner接口，其核心方法是processFor(Duration)。

## 2. 核心调度器：TaskExecutor

TaskExecutor 是每个 Worker 节点内全局唯一的调度器，负责管理和调度本节点上所有 SQL 任务（SqlTask）产生的所有 PrioritizedSplitRunners。

### 2.1 核心组件（属性）

- 线程池 (executor)：启动固定数量（maxWorkerThreads，默认为 CPU核心数 * 2）的常驻TaskRunner工作线程。

- 任务管理列表 (List<TaskHandle> tasks)：维护所有注册到本Worker的SqlTask对应的TaskHandle。

- Splits 调度队列 (MultiLevelSplitQueue waitingSplits)：一个多级优先级队列，用于存放等待被调度的 PrioritizedSplitRunner。这是实现公平调度的关键。

- 运行中/阻塞中的 Splits 管理：通过runningSplits、blockedSplits等集合追踪不同状态的Split。

### 2.2 调度模型：时间片轮转

PrioritizedSplitRunner的执行采用类似操作系统的分时调度策略。

- 时间片 (SPLIT_RUN_QUANTA)：每个 Split 每次被调度执行的默认时间片为1秒。
- 调度流程：TaskRunner 线程从 MultiLevelSplitQueue 中取出一个 PrioritizedSplitRunner，调用其 processFor(1秒) 方法。如果执行时间超过1秒，该 Split 会被放回队列尾部，等待下一次调度。这保证了没有单个慢查询能独占CPU资源。

### 2.3 多级优先级队列 (MultiLevelSplitQueue)

为了实现公平调度，TaskExecutor使用了一个多级队列来管理等待中的Split。

- 层级划分：根据一个任务的累积调度时间将其分配到不同层级（Level）。例如，Level 0：< 1秒，Level 1：1-10秒，以此类推。累积调度时间越长，任务的层级越高。
- 调度策略：调度器会优先选择与目标调度时间差距最大的层级（说明该层级被调度亏欠的最多）中的Split。这防止了高层级（长时间运行）的任务被低层级（短时间）的任务无限期饿死。每个层级内部是一个优先级队列，运行时间更长的Split（优先级值更大）会排在后面。

> note: 多级优先级队列涉及到的算法较为精细与复杂，后面会有专门的一篇文章详解

## 3. 执行流程：从创建到运行

### 3.1 创建阶段

SqlTaskManager 启动：Worker 节点的 Presto Server 进程启动时，通过依赖注入框架（Guice）初始化全局唯一的 SqlTaskManager，它内部持有 TaskExecutor。

接收 Task 请求：当 Worker 接收到一个 Task 请求（TaskRequest）时，SqlTaskManager 会创建或获取对应的 SqlTask 和 QueryContext。

构建 SqlTaskExecution：SqlTask 的核心是 SqlTaskExecution，它负责管理一个 Task 的生命周期。在构造时，它会：

- 向 TaskExecutor 注册自身，获得一个 TaskHandle。
- 使用 LocalExecutionPlanner 将 PlanFragment 物理计划片段转换为本地执行计划片段，并将其拆分成多个 Pipeline (DriverFactory)。

创建 Driver 实例：SqlTaskExecution 会根据数据来源（如 Split）调用 DriverSplitRunnerFactory.createDriverRunner() 来创建实际的 DriverSplitRunner。

- 该方法首先为指定的 Lifespan 创建 DriverContext。
- 然后通过 DriverFactory.createDriver()，利用其内部的 OperatorFactory 列表，创建出真正的 Operator 实例和 Driver 对象。
- 最后，将新创建的 DriverSplitRunner 封装后，交给 TaskExecutor 进行调度。

### 3.2 调度与执行阶段

入队 (enqueueSplits)：SqlTaskExecution 调用 TaskExecutor.enqueueSplits()，将新创建的 PrioritizedSplitRunner 放入 TaskHandle 的队列中，并最终进入 MultiLevelSplitQueue。

工作线程 (TaskRunner) 轮询：一个空闲的 TaskRunner 线程会调用 MultiLevelSplitQueue.take()，根据多级优先级算法获取下一个要执行的 PrioritizedSplitRunner。

执行时间片 (PrioritizedSplitRunner.process())：

- 记录等待时间、开始时间等统计信息。
- 调用其内部持有的DriverSplitRunner.processFor(1秒)。

Driver执行 (Driver.processFor(duration))：

- 获取排它锁，确保线程安全。
- 设置 Yield Signal，当达到1秒时间片时，主动让出 CPU。
- 调用 processInternal() 执行实际的数据处理逻辑。

核心数据处理循环 (Driver.processInternal())：

- 处理内存回收请求：检查并执行需要溢写（Spill）的算子。
- 处理新数据源：将新到达的Split合并到当前Driver。
- 拉取数据并传递：这是最核心的循环。它遍历activeOperators，对每个算子对 (current, next)：
    - 调用 current.getOutput() 获取一个Page。
    - 如果Page不为空，调用 next.addInput(page)，将数据传递给下游算子。
    - 这个过程实现了一次调用中一个Page的数据流经整个Pipeline。
- 处理算子完成：如果一个算子isFinished()，则关闭它及其之前的所有算子，并可能触发结果缓存。
- 处理阻塞：如果没有任何Page被移动（movedPage == false），则检查是否有算子被阻塞（如等待网络IO、等待内存），并返回一个ListenableFuture表示阻塞状态。

调度器响应：PrioritizedSplitRunner.process() 会根据Driver.processFor()的返回值决定下一步：

- 返回NOT_BLOCKED：说明Split在时间片内没有阻塞，但未完成。将其重新放回MultiLevelSplitQueue，等待下次调度。
- 返回一个Future：说明Split被阻塞（如等待远程数据）。将其放入blockedSplitsMap中，并为该Future注册一个监听器。当Future完成（数据到达）时，监听器会将该Split重新放回MultiLevelSplitQueue。

说明：Presto Worker 内部并不是简单的“一个 Task 一个线程”，而是通过 TaskExecutor 将大量 Pipeline + Lifespan 对应的执行实例抽象为支持协作式抢占的执行单元，在 Worker 级别完成类似操作系统 CPU 调度的资源复用。

## 4. 并发控制与优先级管理

### 4.1 TaskHandle 与并发控制器 (SplitConcurrencyController)

每个 TaskHandle 管理其所属 Task 的所有 Split，并通过 SplitConcurrencyController 动态调整该Task的目标并发度。

调整依据：

- OutputBuffer 利用率：如果下游消费速度慢，导致 OutputBuffer 利用率高（> 0.5），则降低并发度，避免内存压力过大。
- 调度时间：每个 Split 执行完后，控制器会评估其执行时间，如果系统资源充足，可以适当提高并发度。

Leaf Split vs. Intermediate Split：

- Leaf Split（如 TableScan）：受并发控制器限制，确保 Task 不会过度占用资源。
- Intermediate Split（如 Exchange）：不受并发控制器限制，一旦产生就会被直接调度，以保证数据能够快速流通。

### 4.2 优先级追踪 (TaskPriorityTracker)

TaskPriorityTracker与MultiLevelSplitQueue协同工作。

- 每次Split执行完一个时间片，TaskPriorityTracker会更新其累积调度时间。
- MultiLevelSplitQueue根据这个累积时间判断该Task所属的Level，并计算其在新Level中的优先级。
- 这种机制确保长时间运行的任务会逐渐“降级”，让出更多执行机会给短任务，实现了查询级别的公平调度。

## 5. 执行路径优化：Fragment Result Cache

除了正常Driver执行路径之外，对于特定模式的查询（如聚合的 Partial 阶段），Presto 还支持将中间结果缓存起来，从而进一步加速后续相同查询。

适用场景：Partial AggregationNode，且其子孙节点是 TableScan、Filter、Project 等。

工作流程：

- 标准化与哈希：LocalExecutionPlanner 会为可缓存的 PlanFragment 生成一个标准化的表示（CanonicalPlanFragment），并结合 Split 的信息计算一个唯一的哈希值作为缓存 Key。
- 缓存写入：当 Driver 的最后一个算子执行完毕时（outputOperatorFinished），如果开启了缓存，它会将输出的 outputPages 通过 FragmentResultCacheManager（例如 FileFragmentResultCacheManager）写入本地文件系统。
- 缓存读取：在 Driver.processNewSources() 中，当接收到一个 Split 时，会首先根据 Split 和标准化计划去缓存中查找。如果命中，则直接读取缓存数据，绕过整个执行过程，极大地提升了性能。

## 6. 总结

Presto Worker端的执行模型是一个精妙设计的分层异步系统：

- 逻辑到物理：PlanFragment -> Pipeline -> Driver -> (Prioritized) SplitRunner，层层细化，最终得到可被独立调度的最小单元。
- 中心化调度：全局唯一的 TaskExecutor 通过 MultiLevelSplitQueue 和分时调度策略，在任务和查询级别实现了高吞吐与公平性的平衡。
- 流水线执行：Driver 在一个时间片内，驱动一个个的 Page 流经整个算子链，最大化 CPU 和内存效率。
- 非阻塞设计：通过 ListenableFuture 和回调机制，将IO、等待内存等耗时操作异步化，让工作线程可以立即处理其他就绪的 Split。
- 动态控制：SplitConcurrencyController 和 TaskPriorityTracker 根据运行时状态（内存压力、执行时间）动态调整并发度和优先级，使系统具有很强的自适应能力。

其核心思想是将静态的物理计划片段转换为动态执行流水线：通过 Pipeline 和 Driver 将执行片段拆解为大量可并行推进的细粒度执行实体，再由 TaskExecutor 在有限计算资源上进行统一调度，从而同时获得节点内流水线并行能力、高吞吐执行效率以及多查询资源公平性。
