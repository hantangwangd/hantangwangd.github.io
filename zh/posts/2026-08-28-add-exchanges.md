# Presto 查询引擎内核详解：AddExchanges——基于物理属性的全局数据分布规划

---

## 引言

在 Presto 的分布式执行引擎中，查询优化器在将逻辑计划转换为物理执行计划时，面临一个核心问题：如何确保每个算子都能获得符合其执行要求的数据分布？

数据分布指的是数据在分布式环境中的组织和路由方式——数据是否按某列分区、相同 Key 是否路由到同一分区、是否需要汇聚到单节点或复制到多节点。不同算子对数据分布有
截然不同的要求，例如：

- Aggregation 要求相同 Group Key 的数据进入同一个 partition，从而保证同一个 Group 的数据能够由同一个执行实例完成聚合；
- Join 根据执行策略，可能要求两侧数据具有兼容的 Partitioning，也可能通过 Broadcast 将一侧数据复制到参与执行的 Worker；
- Single-node execution 则要求所有输入数据最终汇聚到同一个 Worker。

然而，上游算子产生的数据分布几乎不可能天然满足下游所有算子的需求，这种数据分布不匹配正是 AddExchanges 要解决的核心问题。

本文将从问题动机、属性机制、属性推导和具体实现四个层面，系统分析 Presto AddExchanges 如何基于物理属性完成全局数据分布规划。

## 一、数据分布不匹配问题与 AddExchanges 的定位

### 1.1 问题的典型场景

考虑如下的聚合查询：

```
SELECT a, b, COUNT(*) FROM t GROUP BY a, b;
```

对于 Aggregation 来说，它在分布式执行层面的核心要求是：所有具有相同 (a,b) 的数据行必须路由到同一个分区，保证同一 Group 由同一个执行实例完整处理：

```
Need:
    PARTITIONED BY (a, b)
```

然而上游 TableScan 提供的数据分布很可能是任意的，例如：

```
Have:
    Unknown distribution

Worker 1: (1,1), (2,2), (1,2)
Worker 2: (1,1), (2,1), (3,3)
Worker 3: (2,2), (3,3), (1,2)
```

相同 (a,b) 的数据分散在不同 Worker，无法直接进行最终聚合。因此需要在中间插入 Remote Exchange 重新组织数据分布：

```
              Aggregation (GROUP BY a, b)
                    ▲
                    │ 需要: PARTITIONED BY (a, b)
                    │
        Remote Partitioned Exchange  ←── 在此插入
                    ▲
                    │ 实际: Unknown Distribution
                    │
                   TableScan
```

Remote Exchange 通过网络将数据重新路由到目标 Worker，使相同 (a,b) 的数据进入同一个 partition。因此，Remote Exchange 改变的不是数据的逻辑内容，而是数据
在分布式执行环境中的物理分布和路由方式。

### 1.2 AddExchanges 的职责与定位

AddExchanges 是 Presto 物理规划阶段负责全局数据分布规划的核心组件，具体职责包括：

- 根据算子执行语义确定其对输入的数据分布需求；
- 规划子节点，并判断其实际具备的数据分布是否满足需求；
- 在必要时插入合适类型的 Remote Exchange，建立所需的数据分布；
- 基于新生成的计划推导相应物理属性，使后续 Optimizer 能够基于新的属性继续进行规划。

核心原则：与 AddLocalExchanges 一样，AddExchanges 的主要职责是根据算子的物理属性需求建立满足执行语义的数据分布，而不是直接进行 Exchange 的成本优化。
Exchange 是否可以进一步简化或消除，则由后续优化阶段处理。

### 1.3 在优化流程中的层次划分

在 Presto 的物理计划优化阶段，Exchange 相关处理由多个优化规则共同完成，各阶段关注的问题不同：

| 组件 | 关注层次 | Exchange 类型 | 核心问题 |
|:---|:---|:---|:---|
| **AddExchanges** | Worker 之间 | Remote Exchange | 数据如何在 Worker / Partition 之间重新分布？ |
| **AddLocalExchanges** | Worker 内部 | Local Exchange | Worker 内部不同数据流之间如何分发？ |

```
                 Exchange Planning
                        │
          ┌─────────────┴─────────────┐
          │                           │
    AddExchanges              AddLocalExchanges
          │                           │
          ▼                           ▼
   Worker 之间的数据分布        Worker 内部的数据流组织
          │                           │
          ▼                           ▼
    Remote Exchange              Local Exchange
```

这种分层设计使每个组件聚焦于自己层次的问题，逻辑保持清晰专注。

但仅仅知道“需要重新分布”还不够。Optimizer 还必须回答两个问题：当前算子需要什么样的数据分布？Child 当前实际具有什么样的数据分布？ Presto 通过
PreferredProperties 和 ActualProperties 分别描述这两类信息，并进一步通过属性推导和匹配机制决定是否需要插入 Remote Exchange。

## 二、数据分布约束机制

理解了“为什么需要 AddExchanges”之后，我们来看它背后的设计机制。

### 2.1 核心概念：PreferredProperties 与 ActualProperties

AddExchanges 的核心并不是针对某个 Operator 固定插入某一种 Exchange，而是通过一套数据分布约束机制（Data Distribution Property Enforcement），
分析 Operator 对输入数据分布的要求，并通过这些要求自顶向下规划 Child；Child 规划完成后，再结合其 ActualProperties 判断需求是否已经满足，必要时插入
Remote Exchange。

这套属性驱动的数据分布约束机制，其核心是两个互补的物理属性概念：

| 概念 | 方向 | 含义 | 示例 |
|:---|:---|:---|:---|
| **PreferredProperties** | Top-down（需求） | Parent 希望 Child 具有的分布 | PARTITIONED BY (a,b) |
| **ActualProperties** | Bottom-up（实际） | Child 规划完成后实际具备的分布 | PARTITIONED BY (a,b) 或 UNKNOWN |

因此，PreferredProperties 更准确地说是 Parent 对 Child 的物理属性偏好/要求；它首先用于指导 Child 的自顶向下规划，而不是简单地作为一个必须原样满足的硬约束。
Child 规划完成后，AddExchanges 再通过 ActualProperties 判断该偏好是否已经得到满足，并在必要时进行 Enforcement。

### 2.2 整体执行流程

AddExchanges 的核心执行路径形成一个完整的闭环：

```
                 Parent Operator
                       │
                       │ ① 构造 PreferredProperties（表达需求）
                       ▼
              planChild(preferred)
                       │
                       │ ② 自顶向下规划 Child
                       ▼
                  Child Plan
                       │
                       │ ③ PropertyDerivations 自底向上推导属性
                       ▼
               ActualProperties（描述实际）
                       │
                       ▼
               Property Matching（需求 vs 实际）
                       │
                  ┌────┴────┐
                  │         │
                满足       不满足
                  │         │
                  ▼         ▼
             直接复用   插入 Remote Exchange
                             │
                             ▼
                       建立所需分布
                             │
                             ▼
                   推导新的 ActualProperties
```

关键理解：

- 自顶向下：通过 PreferredProperties 引导子节点尽可能生成满足要求的物理计划
- 自底向上：在规划之后通过 PropertyDerivations 获取子节点实际具备的物理属性
- 属性匹配：决定是否需要强制执行（Enforcement）

Exchange 并非预先固定在某个算子前，而是当且仅当需求无法被现有计划满足时，作为强制转换机制动态产生。

### 2.3 PreferredProperties：需求的表达

PreferredProperties 是 Parent 算子向优化器表达其对 Child 数据分布期望的载体：

```
PreferredProperties
    ├── Global Properties（跨节点 / 分区分布）
    │   ├── Partitioned on columns K（按 K 分区）
    │   ├── Single（单节点汇聚）
    │   ├── Any（无特定要求）
    │   └── Broadcast（复制到所有节点）
    └── Local Properties（分区内分布）
        ├── Grouped on columns K（按 K 分组）
        └── Sorted on columns K（按 K 排序）
```

以 Aggregation 为例：

```
Aggregation (GROUP BY a, b)
    │
    ▼
PreferredProperties:
    Global: PARTITIONED BY (a, b)
    Local:  GROUPED BY (a, b)
```

同时，该 PreferredProperties 会与 Parent 的 PreferredProperties 合并，使相邻算子的属性要求尽可能共同满足，避免不必要的 Exchange。

### 2.4 ActualProperties：实际属性的描述

ActualProperties 描述算子在完成规划后实际具备的物理属性：

```
ActualProperties
    ├── Global Properties
    │   ├── nodePartitioning （数据在多个节点之间是如何分布的）
    │   └── streamPartitioning （数据在多个流 / 切片之间是如何分布的）
    ├── Local Properties （数据在单个流 / 切片内部的特性）
    │   ├── Grouped on columns K
    │   └── Sorted on columns K
    └── Constants
```

对于 AddExchanges，Global Properties 是最核心的属性，因为它描述了数据在分布式执行环境中的节点级和数据流级分布，是判断是否需要 Remote Exchange 的主要依据。

### 2.5 Property Matching：需求与实际的比对

Presto 的数据分布特性匹配并不是简单的字符串或集合相等判断，而是需要根据不同算子的执行语义以及 partitioning 的具体语义，判断 Child 当前具备的
ActualProperties 是否能够满足 Parent 的需求。例如：

| 匹配场景 | 示例 |  结果 |
|:---|:---|:---:|
| **完全匹配** | Need `(a,b)` / Have `(a,b)` |  匹配 |
| **可满足的属性** | Have 的 partitioning 能够满足 Need |  匹配 |
| **实际分布单节点** | Need `PARTITIONED` / Have `SINGLE` |  匹配 |
| **不兼容** | Need `(a)` / Have `(b)` | 不匹配 |

需要注意的是，AddExchanges 中并不存在一个统一的属性匹配入口。实际的匹配逻辑分散在 `AddExchanges.Rewriter` 针对不同 PlanNode 类型的 `visitXxx()` 方法中，
由各类算子的具体执行语义决定如何判断 Child 的实际属性是否满足要求。

因此，理解 AddExchanges 的关键并不是寻找一个统一的 `match()` 方法，而是结合具体的 `visitXxx()` 方法，分析它如何构造 PreferredProperties、规划 Child，
以及最终如何根据 Child 的 ActualProperties 决定是否插入 Remote Exchange。

## 三、PropertyDerivations：ActualProperties 的推导机制

如果无法准确知道 Child 实际具备什么属性，Property Matching 就无从谈起。PropertyDerivations 是 Presto 用于**自底向上推导 PlanNode 实际物理属性
（ActualProperties）**的核心机制。

### 3.1 自底向上递归推导

PropertyDerivations 的主体入口是：

```
derivePropertiesRecursively(
        PlanNode node,
        Metadata metadata,
        Session session)
```

该方法采用**自底向上**（Bottom-up）的方式遍历 Plan Tree。

对于当前 node，首先递归处理其所有 source，分别得到：

```
List<ActualProperties> inputProperties
```

然后调用：

```
deriveProperties(node, inputProperties, ...)
```

根据当前节点以及所有 Child 的 ActualProperties，推导当前节点的 ActualProperties。

整个过程可以概括为：

```
            Parent
              │
         ┌────┴────┐
         ▼         ▼
       Child1    Child2
         │         │
         ▼         ▼
    ActualProps ActualProps
         │         │
         └────┬────┘
              ▼
       deriveProperties()
              │
              ▼
        Parent ActualProps
```

deriveProperties() 会创建 PropertyDerivations.Visitor，随后通过：

```
node.accept(visitor, inputProperties)
```

调用对应 PlanNode 的处理逻辑。不同的 PlanNode 对数据分布和其他物理属性具有不同的语义，因此 Visitor 会针对不同的 PlanNode 分别推导其 ActualProperties。

这里需要特别区分两个层次：

- derivePropertiesRecursively()：负责整个 Plan Tree 的递归遍历；
- PropertyDerivations.Visitor：负责针对具体 PlanNode，根据已经推导出的 Child ActualProperties 计算当前节点的属性，并不负责递归。

因此，PropertyDerivations 的核心职责可以概括为：

> 自底向上遍历计划树，并根据不同 PlanNode 的执行语义逐层推导 ActualProperties。

### 3.2 Visitor：不同 PlanNode 的属性推导逻辑

deriveProperties() 会创建 PropertyDerivations.Visitor，随后通过：

```
node.accept(visitor, inputProperties)
```

调用当前 PlanNode 对应的 visitXxx() 方法。

不同 PlanNode 对物理属性具有不同的语义，因此 Visitor 针对不同节点实现相应的属性推导逻辑。例如：

- TableScan：可以根据 Connector 提供的 TableLayout 推导数据的初始分布；
- Exchange：根据 Exchange 自身的 partitioning scheme 推导其输出数据分布；
- Aggregation、Join 等算子：根据自身的执行阶段以及 Child 的 ActualProperties 推导输出属性。

因此，Visitor 并不是一个独立的属性系统，而是 ActualProperties 推导框架针对不同 PlanNode 的具体实现。

### 3.3 PropertyDerivations 与 AddExchanges 的关系

如 2.2 节中的整体执行流程图中所展示的，PropertyDerivations 与 AddExchanges 并不是两个相互独立的数据分布规划组件，而是规划机制与属性推导能力之间的关系。

AddExchanges 负责整个数据分布规划过程：它根据当前 Parent Operator 的执行语义构造 PreferredProperties，利用该属性自顶向下规划 Child；Child 规划完成后，
再借助 PropertyDerivations 推导 Child 当前实际具备的 ActualProperties，随后由 AddExchanges 根据具体算子的执行语义判断实际属性是否满足需求，并在不满足时
插入相应的 Remote Exchange。

更准确地说，PropertyDerivations 是一个通用的物理属性推导机制，而 AddExchanges 在执行数据分布规划时利用这一机制获取 Child 的 ActualProperties。
因此，PropertyDerivations 本身不负责决定是否插入 Remote Exchange，而是为 AddExchanges 的属性驱动规划提供实际属性推导能力。

## 四、以 AggregationNode 为例：AddExchanges 的完整规划流程

### 4.1 场景设定

AggregationNode 是理解 Presto AddExchanges 工作机制的一个典型例子。对于如下的聚合查询：

```
SELECT customer, SUM(amount) FROM orders GROUP BY customer;
```

在分布式执行环境中，相同 customer 的数据可能分散在不同 Worker 上：

```
Worker 1: (A,100), (B,200), (C,150)
Worker 2: (A,300), (B,250), (D,100)
Worker 3: (B,50), (C,400), (D,200)
```

如果直接执行 Aggregation，相同 customer 会产生多个局部聚合结果。因此，Aggregation 的核心分布要求是：

> 相同 grouping key 的数据必须进入同一个 partition。

需要强调的是，本章讨论的是 AddExchanges 阶段的物理属性规划。此时 AggregationNode 仍然作为一个完整的 PlanNode 参与 Property Planning，并不存在
Partial / Final Aggregation 的拆分。本节关注的是 Aggregation 对 Child 提出的 partitioning requirement，以及 AddExchanges 如何根据
Child 的 ActualProperties 判断是否需要插入 Exchange。

### 4.2 通过 PreferredProperties 表达 Aggregation 的需求

在 visitAggregation() 中，首先根据 grouping keys 构造 preferred properties：

```
Set<VariableReferenceExpression> groupingKeys = node.getGroupingKeys();
// groupingKeys = {customer}

PreferredProperties preferred =
    PreferredProperties.partitionedWithLocal(
        partitioningRequirement.forKeys(groupingKeys),
        grouped(groupingKeys)
    );
```

这意味着 Aggregation 希望 child：

```
Global: partitioned by customer
Local: grouped by customer
```

其中，partitioned by customer 是当前 AddExchanges 阶段重点处理的分布属性；grouped by customer 属于 Local Property，主要由后续的 AddLocalExchanges 处理。

同时，Aggregation 的 PreferredProperties 还会与 parent 的 PreferredProperties 合并，使相邻 Operator 的物理属性要求尽可能得到共同满足，从而避免
不必要的 Exchange。

### 4.3 递归规划 Child 并获取实际属性

构造好 preferredProperties 后，执行：

```
PlanWithProperties child = planChild(node, preferredProperties);
ActualProperties childProperties = child.getProperties();
```

Optimizer 将 Aggregation 的 preferredProperties 自顶向下传递给 child，并在 child 规划完成后获得其 ActualProperties。

此时得到了明确的 Preferred 和 Actual 信息：

```
Preferred: partitioned(customer)
Actual: child 当前的数据分布
```

Optimizer 后续根据 Child 的 ActualProperties 判断其是否已经满足当前 Operator 所需的物理属性；如果不能满足，则进入 Property Enforcement。其中，
可能呈现以下几种情况：

| 情况 | Child ActualProperties | 是否满足当前 Property Requirement |
|:---:|:---|:---------------------------:|
| 1 | `PARTITIONED BY customer` |             满足              |
| 2 | `SINGLE` |             满足              |
| 3 | `PARTITIONED BY order_date` |           通常无法满足            |
| 4 | `UNKNOWN / ARBITRARY` |             不满足             |

注意：这里的 SINGLE 分布意味着所有数据已经位于同一个 partition 中，因此当然可以满足 PARTITIONED BY customer 的要求，因为不管 customer 是什么，
所有 customer 数据都在同一个地方。这也说明 Property Matching 判断的并不是两个 Property 是否完全相同，而是 Actual Property 是否能够满足当前
Operator 的 Property Requirement。

### 4.4 属性匹配与插入 Remote Exchange

执行判断，如果 Child 当前的 Actual 数据分布无法满足，例如：

```
!isStreamPartitionedOn(customer) 并且
!isNodePartitionedOn(customer)
```

则 AddExchanges 会插入：Remote Partitioned Exchange，从而形成：

```
                 Aggregation
                      ▲
                      │
            partitioned by customer
                      │
          Remote Partitioned Exchange
               hash(customer)
                      ▲
         ┌────────────┼────────────┐
         │            │            │
        W1           W2           W3
```

Exchange 改变数据在 Worker 之间的分布，使相同 customer 的数据进入同一个 partition，从而建立 Aggregation 所需要的 Global Property。插入 Exchange 后，
Exchange 所产生的数据分布属性也会被重新推导为相应的 ActualProperties，使得其上层 Aggregation 能够看到满足要求的 partitioning property。

### 4.5 执行路径总结

通过 AggregationNode 可以看到，AddExchanges 的核心并不是简单地“检查节点或属性并添加 Exchange”，而是一个物理属性需求向下传播、实际属性向上推导，再根据两者进行匹配
和必要时进行属性 enforcement 的递归规划过程。

对于 Aggregation，核心执行路径可以概括为：

```
                  Aggregation
                       │
                       │ ① 构造 PreferredProperties: partitioned(customer)
                       ▼
                 planChild(preferred)
                       │
                       │ ② 自顶向下传递需求
                       ▼
                    Child Plan
                       │
                       │ ③ 自底向上推导 ActualProperties
                       ▼
               ActualProperties
                       │
                       │ ④ Property Matching
                       ▼
             是否满足 partitioning requirement？
                  │             │
                 Yes            No
                  │             │
                  ▼             ▼
             直接复用     Remote Partitioned Exchange
                                │
                                ▼
                          partitioned(customer)
                                │
                                ▼
                           推导新的物理 Properties
```

其中最重要的是三个相互配合的机制：

- 自顶向下（Top-down）：Operator 根据自身的执行需求构造 PreferredProperties，并通过 planChild() 将该 PreferredProperties 传递给 child，引导 child 尽可能产生满足要求的物理属性。
- 自底向上（Bottom-up）：child 规划完成后，Optimizer 根据实际生成的 Plan 结构推导其 ActualProperties，描述 child 最终实际具备的数据分布等物理性质。
- 属性匹配（Property Matching）：Optimizer 比较 child 的 ActualProperties 与当前 Operator 的物理需求。如果现有属性已经满足要求，则直接复用；否则通过 Exchange 强制建立缺失的物理属性。

这也是 Presto AddExchanges 与简单的规则式 Exchange 插入机制之间最重要的区别：Exchange 并不是预先固定在某个 Operator 前面，而是作为物理属性需求无法通过现有 Plan 满足时的 enforcement mechanism 动态产生。

## 五、总结：从 Exchange 插入到属性驱动的物理规划

通过前面的分析可以看到，AddExchanges 表面上负责的是 Exchange 的规划与插入，但其真正解决的问题并不是“什么时候插入 Exchange”，而是如何让分布式执行计划满足
Operator 所需要的物理属性。

在这一过程中，PreferredProperties 表达 Operator 对 Child 的物理属性期望，PropertyDerivations 从实际 Plan 结构中推导 Child 的 ActualProperties，
Property Matching 判断两者是否能够满足，而 Exchange 则作为 Property Enforcement 的一种手段，在必要时改变数据分布，建立新的物理属性。几个机制共同
形成了一个完整的 Property Planning 闭环。

这套设计的关键价值，在于它将 “Operator 需要什么” 与 “Child 已经有什么” 解耦。Optimizer 不再需要针对每一种 Operator 固定定义 Exchange 插入规则，而是通过
统一的 Property System 描述、推导和匹配物理执行约束。因此，当 Child 已经具备满足要求的属性时，可以直接复用已有的数据分布，而无需再次进行 Shuffle；
当属性无法满足时，才通过 Exchange 等 Enforcement 机制进行调整。

从更高的层次来看，AddExchanges 实际上完成了一个重要的抽象：将分布式执行中隐含的数据分布约束，从具体 Operator 的执行逻辑中提取出来，转化为 Optimizer 可以显式描述、
推导和优化的 Physical Properties。

因此，理解 AddExchanges 最重要的并不是记住某个 visitXXX() 在什么情况下插入什么 Exchange，而是理解它背后的设计思想：

> Exchange 是手段，Property 是核心。AddExchanges 本质上是一套以物理属性为中心，将逻辑计算需求逐步转化为包含了数据分布特性的物理执行计划的规划机制。


---

## 欢迎交流

本文基于作者当前的理解与实践经验整理而成，难免存在疏漏或值得进一步探讨之处。

如果您对文中的观点有不同看法，发现任何问题，或有相关实践经验，欢迎通过 [GitHub Issue](https://github.com/hantangwangd/hantangwangd.github.io/issues/new) 与作者交流讨论。

期待与更多同行围绕数据基础设施相关技术展开交流，分享实践经验，共同学习、共同进步。
