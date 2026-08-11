# Presto 查询引擎内核详解：分布式规划机制

#### 从 Physical Plan 到 Distributed Plan

---

## 一、引言：为什么需要分布式规划？

在前面的文章中，我们了解到 Presto 如何完成：

```
SQL --> AST --> Analysis --> LogicalPlan --> PhysicalPlan
```

优化器最终生成的是一棵 Physical Plan Tree（PlanNode Tree）。例如：

```
           Output
              |
           Exchange
              |
            Join
         /        \
    Scan Orders   Exchange
                     \
                    Scan Customer
```

从单机执行角度看，这棵树已经描述了：

- 数据如何读取
- 算子如何连接
- 数据如何流动

但是，对于一个 MPP 查询引擎而言，集群中的 Worker 节点无法直接执行整棵计划树。原因在于：

- 数据需要跨节点重新分布；
- 不同计算阶段需要分布到不同 Worker；
- 查询需要拆分为多个独立调度单元；
- 不同阶段之间需要建立依赖关系。

因此，Presto 需要完成一次关键转换：

```
PhysicalPlan --> Distributed Plan
```

这个过程就被称之为：

> Distributed Planning（分布式规划）

完成这一转换的核心组件是 PlanFragmenter，它负责将 Physical Plan 切分为多个 PlanFragment，并构建后续分布式调度所需的 Distributed Plan。

## 二、整体认知：Presto 的分布式规划及调度执行模型

在Presto中，一个SQL查询从优化后的物理计划转化为可执行的分布式任务，需要经历几个核心阶段：将物理计划切分为计划片段、规划各个片段的分布式执行特性、为分布式计划构造调度器并实际调度和执行。

在深入细节之前，我们需要先建立一个分层的全局认知。

Presto 是一个以 Pipeline Streaming 执行为核心的 MPP 查询引擎，其默认执行模型强调数据流式传输和算子流水执行。同时，为支持 Exchange Materialization、
	CTE Materialization 等高级能力，现代 Presto 引入了 Section 等更高层调度抽象，用于统一描述纯 Streaming 执行和带物化依赖的混合执行模型。

- 随着物化执行等能力的发展，Presto 的调度执行模型逐渐形成 Section、Stage、Task 等多层抽象，用于描述调度执行的边界、生命周期以及依赖关系；
- 默认情况下，Stage 之间通过 Remote Streaming Exchange 进行流式数据交换，避免中间结果完全物化。在该模型下，整个 Query 的所有 Stage 通常位于同一个 Section 内，Stage 之间通过 Remote Streaming Exchange 形成流水依赖，而不存在跨 Section 的物化依赖 DAG。
- 在 Exchange Materialization 或 Cte Materialization 等模式下，查询会根据物化依赖边界被切分为多个 Section，Section 之间可以形成显式 DAG 调度关系。
- 调度以 Section 作为高层组织单元，以 Stage 作为生命周期管理和依赖调度实体，以 Task / Split 作为执行层实际调度对象。

关于 Section 这一层级概念的引入，有必要再专门介绍一下。Presto 最初的分布式执行模型主要围绕 Stage 和 Streaming Exchange 构建，Stage Tree 足以描述
	Producer-Consumer 之间的流水依赖关系。随着 Exchange Materialization 等能力的引入，Presto 开始支持具有持久化边界的阶段化执行模式，查询执行不再始终
	表现为单一 Streaming Pipeline，而可能被切分为多个具有独立生命周期的执行区域。因此，Section 作为位于 Stage 之上的执行组织抽象被引入，用于表示一组共享
	执行生命周期的 Stage，并描述这些独立执行区域之间基于物化数据形成的 DAG 依赖关系。

## 三、从 PlanNode 到 SubPlan：PlanFragmenter 的核心职责

### 3.1 PlanFragmenter 做什么？

在 Presto 中，负责完成分布式规划的核心组件为：PlanFragmenter。PlanFragmenter 不仅仅是一个“切 Stage 的工具”，它实际上承担的是分布式执行计划构造（Distributed Plan Construction），包括：

- 执行 Stage 边界划分
- 构造 PlanFragment
- 推导 Fragment 的数据分布属性
- 连接上下游 Stage 的数据流关系
- 反向推导并调整数据分布需求，协调 Source 与输出 Partitioning
- 标记 Grouped Execution 能力

用一句话来描述：

> PlanFragmenter 将优化器生成的 Physical Plan Tree 转换为面向分布式执行的 SubPlan Tree。在这个过程中，它不仅根据 Remote Exchange 边界切分 Stage，还负责确定每个
	PlanFragment 的分布式执行属性，包括内部数据分布（PartitioningHandle）、输出数据分布（PartitioningScheme）以及相关执行能力。

```
输入：PlanNode Tree
      |  PlanFragmenter
      v
输出：SubPlan Tree
```

SubPlan 是 Presto 分布式规划阶段生成的逻辑执行结构，它表示一个 Stage 及其依赖的子 Stage 组成的树形执行计划。一个 SubPlan 主要由两部分组成：

```
SubPlan
 +-- PlanFragment
 +-- children(SubPlan)
 ```

**PlanFragment 描述一个 Stage 的计算语义以及其在分布式执行中的数据交换语义**

它包含：

- fragmentId: 当前 Fragment 的唯一标识；
- root: 当前 Stage 内部执行的 PlanNode 子树根节点；
- partitioningHandle: 描述当前 Stage 内部数据分布方式；
- partitioningScheme: 描述当前 Stage 输出数据的分布方式；
- variables: 描述当前 Stage 输出的变量集合，即该 Stage 对外暴露的数据 schema；
- tableScanSchedulingOrder: 描述当前 Stage 中包含的按调度顺序排列的 TableScan 节点列表
- remoteSourceNodes: 描述当前 Stage 中包含的远程数据源的集合
- execution properties: 描述该 Stage 的执行属性，例如 grouped execution 等等。

因此：

> PlanFragment 不仅描述“这个 Stage 内部执行什么计算”，还定义了该 Stage 在分布式执行过程中的数据分布语义、输出接口以及执行能力等等信息。

**children 描述当前 Stage 依赖的上游 Stages**

children(SubPlan) 表示当前 Stage 所依赖的上游 Stage，这些 Stage 通常对应当前 Fragment 中 RemoteSourceNode 背后的 Producer Stage。

后续阶段中，SqlQueryScheduler 会根据 SubPlan Tree 创建对应的 StageExecution，并按照其中描述的依赖关系划分并推进各个 Section / Stage 的调度执行。

### 3.2 PlanFragmenter 的整体流程

PlanFragmenter 的整体处理过程可以抽象为：

```
createSubPlans()
        |
        v
FragmentProperties 初始化
        |
        v
Visitor 遍历 PlanNode
        |
        +--------------------------------+
        |                                |
        v                                v
普通 PlanNode 处理              ExchangeNode 处理
        |                                |
        |                 +--------------+--------------+
        |                 |                             |
        |                 v                             v
        |        Streaming Exchange           Materialized Exchange
        |                 |                             |
        |                 v                             v
        v         切断 Fragment 边界          创建物化数据边界（构建临时表写入计划）
   设置分布属性             |                             |
        |                 v                             v
        |       生成 RemoteSourceNode           生成 TableScanNode
        |                 |                             |
        |                 +--------------+--------------+
        |                                |
        |                                v
        |                      生成 PlanFragment
        |                                |
        +------buildRootFragment---------+
                                         v
                                     SubPlan
                                         |
                                         v
                               Grouped Execution 分析
                                         |
                                         v
                               Partitioning 修正
                                         |
                                         v
```

PlanFragmenter 是 Presto 从单体 Physical Plan 到 Distributed Execution Plan 的关键转换层。它通过识别 Exchange 边界，将连续的计算逻辑转换为由多个 Fragment 组成的 Distributed Plan，并同时确定 Fragment 间的数据交换方式、Partitioning 语义以及执行属性。

## 四、FragmentProperties：Stage 构建过程中的状态模型

PlanFragmenter 在遍历 PlanNode 时，需要维护当前 Stage 的各种属性。这些状态封装在：FragmentProperties 中。从更加工程实现的角度来说，FragmentProperties
	可以看作 Fragmenter Visitor 在遍历 PlanNode 过程中维护的“当前 Stage 构建上下文”。

其核心字段包括：

```
FragmentProperties
    +--List<SubPlan> children;
    +--PartitioningScheme partitioningScheme;
    +--Optional<PartitioningHandle> partitioningHandle;
    +--Set<PlanNodeId> partitionedSources;
```

各个字段的说明如下：

| 字段 | 含义                               |
|------|----------------------------------|
| children | 遍历过程中推导出来的当前 Stage 依赖的子 Stage 列表 |
| partitioningHandle | 遍历过程中推导出来的当前 Stage 内部数据分布方式      |
| partitioningScheme | 切分新 Stage 时传递来的当前 Stage 输出数据分布方式 |
| partitionedSources | 遍历过程中推导出来的当前 Stage 内的数据源         |

其中最容易混淆的是：partitioningHandle 和 partitioningScheme 两个概念。

### 4.1 PartitioningHandle

PartitioningHandle 描述当前 Stage 所对应的数据分布（Partitioning）语义。

它并不是一个被直接指定的决定 Stage 数据布局的策略对象，而是 Distributed Planning 过程中，结合 Stage 内各个 PlanNode 的数据分布特性推导出的最终分布语义。

在遍历一个 Stage 所包含的 PlanNode 时，Presto 会结合不同节点的数据分布信息，例如：

- TableScan 所对应 Connector 提供的源端 Partitioning；
- RemoteExchange 所定义的数据交换及 Partitioning 语义；
- 其他 PlanNode 对上游 Partitioning 的保持、转换或破坏；

逐步推导当前 Stage 的 Partitioning，最终形成该 Stage 对应的 PartitioningHandle。

因此，PartitioningHandle 的核心作用是描述“当前 Stage 的数据应该如何被理解和组织”。后续的分布式规划、Connector 数据访问以及 Exchange 和调度过程，可以基于这一 Partitioning 语义决定数据如何分布以及 Task 如何承载这些数据。

例如：

```
Hive Bucket(customer_id, 8)
```

表示当前 Stage 的数据分布语义为：数据按照 Hive Bucket 规则基于 customer_id 被划分为 8 个 Bucket。该语义可以被后续的 Source、Exchange 以及调度逻辑用于判断数据的分布方式和计算并行度。

### 4.2 PartitioningScheme

PartitioningScheme 描述当前 Fragment 输出数据时所采用的 Partitioning 方式以及输出布局信息。

与 PartitioningHandle 不同，PartitioningScheme 并不是在遍历当前 Stage 内部 PlanNode 的过程中，根据各节点的分布特性独立推导出来的。它与
	Remote Exchange 所定义的数据交换语义直接相关。

在进行分布式计划切分时，当 PlanFragmenter 跨越一个 Remote Exchange 创建新的 Fragment / Stage 时，会根据该 Exchange 定义的 PartitioningScheme
	初始化新的 FragmentProperties。因此，PartitioningScheme 本质上是沿着 Exchange 边界传递到新 Fragment 的数据布局描述，并在后续的 Fragment 构建过程中
	继续参与分布式规划。

例如：

```
HASH(order_id, 4)
```

表示该 Exchange 输出的数据按照 order_id 进行 Hash Partitioning，并划分为 4 个分区。下游 Fragment 将基于这一 Partitioning Scheme 接收和处理
	来自该 Exchange 的数据。

两者更准确的区别可以理解为：

- PartitioningHandle：描述“当前 Fragment 的数据分布语义是什么”
- PartitioningScheme：描述“跨越 Exchange 后，数据以什么分区方式和布局传递给下游 Fragment”

因此，PartitioningScheme 与 PartitioningHandle 虽然并不是严格的一一对应关系，但二者存在明确的数据分布语义传递关系：

```
上游 Stage
    │
    │ PartitioningScheme
    ▼
Remote Exchange
    │
    ▼
RemoteSource
    │
    │ 参与下游 Stage 的 Partitioning 推导
    ▼
下游 Stage
    │
    │ PartitioningHandle
    ▼
当前 Stage 的 Partitioning 
```

也就是说，上游 Stage 的 PartitioningScheme 描述其通过 Remote Exchange 输出数据时所采用的分区方式；当该 Exchange 在下游 Stage 中表现为 RemoteSource 时，
	这一分布信息会成为下游 Fragment Properties 推导 Partitioning 的输入之一，并最终参与形成下游 Stage 的 PartitioningHandle。

## 五、Exchange：Stage 切分的核心机制

Presto 分布式规划阶段最重要的概念为：

> Exchange 决定 Stage 边界。

需要说明的是，“Exchange 决定 Stage 边界”描述的是分布式规划阶段的职责——即根据已经存在的 Remote Exchange 节点进行 Stage 切分。

而在更早的 AddExchanges 优化阶段，优化器会基于不同算子的语义特征决定是否需要以及在何处插入 Remote Exchange 节点。

换言之：

- AddExchanges 阶段：根据算子特点决定要不要加 Exchange（原因层）
- 分布式规划阶段：根据已有的 Exchange 执行 Stage 切分（结果层）

本文主要聚焦于后者。理解了这一分层，就不难理解为什么会有如下结论：

不是：

```
Join = Stage
Aggregation = Stage
```

而是：

```
Remote Exchange = Stage Boundary
```

> Stage 的边界由数据交换节点定义，而交换节点的插入则由上游算子的计算需求驱动。

### 5.1 Local Exchange

Local Exchange 表示同一个 Stage 内的数据交换（也即是在同一个 Worker 节点上的同一个 Task 内，不同 Pipeline 之间的数据交换）。

例如在一个 Task 内部：

```
Driver A
   |
Local Exchange
   |
Driver B
```

不会产生新的 Stage。详细机制请参见另一篇文章《Presto 查询引擎内核详解：AddLocalExchanges——基于数据流属性约束的本地执行规划》。

### 5.2 Remote Streaming Exchange

Remote Streaming Exchange 表示跨 Worker 的流式数据交换。

例如，原始的物理执行计划为：

```
           Output
              |
           Exchange
              |
            Join
         /        \
    Scan Orders   Exchange
                     \
                    Scan Customer
```

Stage 切分转换之后：

```
   Stage 0              Stage 1                 Stage2

   Output                Join                  Scan Customer
     |                /         \
RemoteSource     Scan Orders  RemoteSource
```

在 Stage 切分过程中，Exchange 被替换为：RemoteSourceNode。同时，创建新的：Child SubPlan。最终形成如下形状的一个 Stage Tree：

```
    Stage0
      |
    Stage1
      |
    Stage2
```

## 六、Materialized Exchange 与 Section

前文提到的 Streaming Exchange 和 Stage 切分，构成了经典的 Stage Tree 执行模型。但当引入物化边界后，调度执行模型可以从树演进为有向无环图（DAG），这便是引入 Section 这一层级的核心价值所在。

### 6.1 从 Streaming 到 Materialized：物化依赖的引入

Streaming Exchange 的语义是流水线式的：

```
Producer Stage
    |（Network Stream，不落盘）
    ▼
Consumer Stage
```

Producer 和 Consumer 之间通过网络直接传输数据，不存在中间状态持久化。

Materialized Exchange 则引入了物化依赖：

```
Producer Stage
    |（写入临时表，落盘）
    ▼
Temporary Table
    |（从临时表读取）
    ▼
Consumer Stage
```

此时 Producer Stage 必须完全执行完毕并物化结果后，Consumer Stage 才能开始执行。这种先写后读的依赖关系，在物化交换边界上引入了同步 barrier，使 Producer 和 Consumer 生命周期解耦，但也牺牲了一部分端到端流水执行能力。

### 6.2 Section：物化边界定义的执行切分

Materialized Exchange 的出现，在 Stage Tree 中切分出了Section：

```
     Stage Tree                          Section DAG
┌─────────────────┐                ┌─────────────────┐
│    Stage 0, 1   │                │   Section A     │
│       │         │                │  (Stages 0-1)   │
│       ▼         │                └────────┬────────┘
│  Materialized   │                         │ （物化依赖）
│   Exchange      │                         ▼ 
│       │         │                ┌─────────────────┐
│       ▼         │                │   Section B     │
│    Stage 2, 3   │                │  (Stages 2-3)   │
└─────────────────┘                └─────────────────┘
```

Section 的定义：

> Section 是由 Materialized Exchange 边界划分出的调度执行组织单元。同一个 Section 内的 Stage 通过 Streaming Exchange 连接，形成流水线；不同 Section 之间通过 Materialized Exchange 连接，形成物化依赖。

相比早期 Presto 的单纯流式 Stage Tree 模型，Section DAG 带来了几个关键能力：

- 容错恢复：Materialized Exchange 物化的中间结果可作为检查点，某个 Section 失败后无需重跑整个查询，只需重跑该 Section 及其下游；此外，结合分组执行（grouped execution），可以支持分组级（任务级）的失败重试。
- 运行时自适应优化：下游 Section 的计划可以基于上游 Section 物化后的真实数据统计信息（如行数、数据分布、NDV、Null 比例等）进行动态调整，引入类似于 Spark AQE 的能力。
- 资源解耦：不同 Section 可独立调度，无需同时持有所有 Stage 的资源，对大规模 ETL 场景尤其友好。

这一演进扩展了 Presto 的执行模型，使其在保持交互式查询能力的同时，具备更强的长链路查询、大规模数据处理以及复杂工作负载支持能力。

## 七、Fragment 创建与 SubPlan 组装

在 PlanFragmenter 遍历 PlanNode 的过程中，随着发现不同类型的 Exchange 边界，会逐步完成 Fragment 切分和创建。

因此，PlanFragment 并不是在整个 Plan 遍历完成后统一生成，而是在 Fragment 边界识别过程中逐步构建：

```
Visitor 遍历 PlanNode
        |
        v
发现 Exchange 边界
        |
        v
递归构建 Exchange Source 对应的 Child Fragment
        |
        v
当前 Fragment 替换 Exchange 为 RemoteSourceNode
  或建立物化读取节点
        |
        v
继续处理父节点
        |
        v
构建 Root Fragment
        |
        v
组装 SubPlan Tree
```

在 Fragment / SubPlan 构建阶段，主要完成以下工作：

### 1. 确定 Fragment 的执行结构

PlanFragmenter 根据当前 Stage 内部的 PlanNode 拓扑关系，确定：

- 当前 Fragment 的根节点；
- Source 节点关系；
- 数据交换关系。

确保该 Fragment 描述的是一个完整、可执行的计算区域。

### 2. 汇总执行属性

在前面的遍历过程中，FragmentProperties 已经逐步收集了当前 Stage 的执行信息，包括：

- 数据分布方式（PartitioningHandle）；
- 输出分区方案（PartitioningScheme）；
- 直属子 Fragment 对应的 SubPlan 依赖关系（children）；
- 相关执行能力属性。

这些信息最终都会被封装到 PlanFragment / SubPlan 中。

### 3. SubPlan Tree 组装

当一个 Fragment 构建完成后，PlanFragmenter 会根据 FragmentProperties 中维护的子 Fragment 关系构造对应的 SubPlan：

```
        Parent SubPlan
              |
        ----------------
        |              |
Child SubPlan  Child SubPlan
                       |
                     ......
```

其中：

- Parent Fragment 通过 RemoteSource 或物化读取节点消费 Child Fragment 输出；
- Child Fragment 是 Producer Stage；
- SubPlan Tree 描述 Fragment 之间的数据依赖关系。

至此，PlanFragmenter 完成了 Distributed Plan 的初步构建。生成的 SubPlan Tree 作为后续分布式执行优化和调度规划的基础，接下来的阶段还将进一步分析 Fragment 的执行能力，并调整其数据分布策略。

## 八、Grouped Execution 能力分析

除了构建 Stage 拓扑和确定数据分布之外，PlanFragmenter 还会对部分执行能力进行分析，其中包括 Grouped Execution。

Grouped Execution 主要用于降低大规模 Join 或 Aggregation 场景下的内存压力。

传统执行模式下，一个 Task 通常需要一次性加载完整的数据分组：

```
   Task
    |
    v
   读取全部 Bucket
    |
    v
   Build Hash Table
    |
    v
   执行 Join
```

当 Build Side 数据规模较大时，会导致较高的峰值内存消耗。

Grouped Execution 则将执行过程拆分为多个独立的数据组（通常对应 Bucket 或 Lifespan）：

```
   Bucket n
    |
    v
   执行 Join
    |
    v
   释放资源
```

通过限制单次参与计算的数据规模，降低 Hash Build 等算子的内存压力。

在分布式规划阶段，PlanFragmenter 并不负责执行 Grouped Execution，而是通过 GroupedExecutionTagger 分析当前 Fragment 是否具备该能力，并将分析结果记录到：
	StageExecutionDescriptor 中以供后续 Stage 调度和 Task 执行阶段使用。

其核心关注点包括：

- 当前 PlanNode 是否支持 Grouped Execution；
- 子树是否适合采用 Grouped Execution；
- 哪些 TableScan 节点可以提供分组数据；
- 分组数量等执行信息。

关于 Grouped Execution 的完整机制，我们将在后续专门的文章中单独展开。

## 九、计算需求反向影响数据布局：Partitioning Reassignment

在分布式规划过程中，Presto 不仅需要决定计算如何分布执行，有时还需要根据计算阶段的需求反向调整数据源提供的数据布局。

这一机制称为：**Partitioning Reassignment**

它体现了 Presto 分布式规划中的一个重要思想：

> 执行计划中的数据分布需求，可以反向影响 Source 层的数据读取方式。

### 9.1 为什么需要 Partitioning Reassignment？

在默认情况下，TableScan 通常只提供数据源自身的数据分布方式。

例如，Hive Connector 的某一个表上已经存在的数据分布格式为：

```
bucket(customer_id, 16)
```

但是，在包含多个 Source 的 Stage 中，Stage 最终需要形成统一的数据分布，而各个 Source 所提供的数据布局未必完全一致。

例如：

```
Stage Requirement

Hive Bucket Partitioning

bucket(customer_id, 8)
```

这意味着：

数据需要以 Hive 的 Bucket Hash 算法按照 customer_id 的值被划分到 8 个 bucket 中，而不是该表原始划分的 16 个 bucket 中。

如果 Source 提供的数据布局无法满足该要求，则可能导致：

- 额外的数据 Shuffle；
- 增加 Network Exchange 开销；
- 降低 Join 等算子的执行效率。

因此，Presto 会尝试让数据源提供与 Stage 计算需求更加匹配的数据布局。

### 9.2 计算需求如何影响 Source？

Partitioning Reassignment 的核心过程可以抽象为：

```
Stage Execution Requirement
          |
          v
PartitioningHandle Reassignment
          |
          v
Table Layout 调整
          |
          v
Connector 提供匹配的数据分布
```

也就是说，分布式规划阶段首先遍历整个计划树，切分 Stage 并自底向上的确定各个 Stage 希望数据以什么方式组织。

然后再自顶向下的将数据输出需求传递给 Source 层，使 Connector 最终调整并提供满足条件的数据布局。

例如：

```
Stage:
HASH(customer_id, 8)
        ↓
TableScan:
Hive Bucket(customer_id, 8)
```

需要说明的是，在切分阶段（甚至是更早的 AddExchanges 优化阶段）已经确保了一个 Stage 与其内部持有的 Sources 之间的数据布局的兼容性。在优化阶段，优化器已经通过
	`ConnectorMetadata.getCommonPartitioningHandle` 等接口询问并确认过 Source Connector 可以调整并提供兼容的数据布局。否则会通过添加 Remote Exchange
    节点来调整数据布局（因此也会将其切分到不同的 Stage 中）。因此，在 Partitioning Reassignment 阶段，不会出现 Stage 要求的数据布局无法被 Source Connector
	满足而导致的规划失败。

### 9.3 体现的数据计算协同思想

传统查询优化通常是：

```
Data Source
    |
    v
Execution Plan
```

即：数据源提供数据，执行计划适配数据。

而 Partitioning Reassignment 体现的是另一种方向：

```
Execution Requirement
      |
      v
Data Layout Selection
      |
      v
Source Optimization
```

这是现代湖仓查询引擎中的重要设计趋势：

- 查询执行不再只是被动消费数据；
- 执行计划可以影响数据读取方式；
- 存储布局与计算模型形成协同优化。

在 Presto 中，这种机制连接了：

```
Optimizer
    ↓
Distributed Planning
    ↓
Connector Layer
    ↓
Physical Data Layout
```

使查询引擎能够根据执行目标选择更加高效的数据访问路径。这体现了一种：“**计算分布需求反向约束数据源布局**”的设计思想。

## 十、从 SubPlan 到 Distributed Scheduler

分布式规划阶段生成的 SubPlan 是一种具有 Stage 拓扑、数据分布策略和执行能力描述的 Distributed Plan，其为后续 Query Scheduler 创建 Task、分配 Split 和驱动 Worker 执行奠定基础。

```
Optimizer
    |
    v
PlanNode Tree
    |
    v
PlanFragmenter
    |
    v
SubPlan Tree
    |
    v
SqlQueryScheduler
    |
    v
StageExecution
```

关于 Query Scheduler 分布式调度部分，将在后续的文章中专门讲解说明，此处不再展开。

## 总结：Presto 分布式规划的核心思想

Presto 的分布式规划过程，本质上是将优化后的 Physical Plan 转换为包含执行边界、数据交换语义和调度拓扑的 Distributed Execution Model。

这一过程围绕三个核心抽象展开：

### 1. 通过 Exchange 定义计算边界

Presto 并不是简单按照算子类型切分执行阶段，而是通过识别 Exchange 边界，将连续的计算逻辑划分为多个独立执行区域。

- Remote Streaming Exchange 定义流式数据交换边界；
- Materialized Exchange 定义物化数据交换边界。

基于这些边界，PlanFragmenter 将 Physical Plan 切分为多个相互关联的 PlanFragment。

### 2. 通过 Partitioning 描述分布式数据语义

在 Fragment 切分之后，Presto 需要进一步确定：

- 数据如何在 Worker 之间分布；
- Fragment 之间如何交换数据；
- 下游计算如何消费上游结果。

其中：

- PartitioningHandle 描述 Fragment 数据分布策略；
- PartitioningScheme 描述 Fragment 输出数据的分区方案以及数据交换布局。

它们共同定义了分布式执行中的数据流转语义。

### 3. 通过分层执行模型连接规划与调度

基于 Fragment 依赖关系，Presto 构建 SubPlan，并进一步将其映射为运行时执行模型：

- Streaming Exchange 保留 Pipeline Streaming 的低延迟执行能力；
- Materialized Exchange 和 Section 引入持久化执行边界，使查询能够形成更复杂的执行拓扑；
- Scheduler 根据 SubPlan 管理 Stage 生命周期，并将计算任务调度到 Worker 执行。

最终，Presto 分布式规划的核心目标，是在保留 Pipeline Streaming 低延迟执行特性的同时，将优化后的计算计划转换为具有明确执行边界、数据交换语义和调度拓扑的分布式执行模型，从而连接查询优化与集群调度执行。



---

## 欢迎交流

本文基于作者当前的理解与实践经验整理而成，难免存在疏漏或值得进一步探讨之处。

如果您对文中的观点有不同看法，发现任何问题，或有相关实践经验，欢迎通过 [GitHub Issue](https://github.com/hantangwangd/hantangwangd.github.io/issues/new) 与作者交流讨论。

期待与更多同行围绕数据基础设施相关技术展开交流，分享实践经验，共同学习、共同进步。
