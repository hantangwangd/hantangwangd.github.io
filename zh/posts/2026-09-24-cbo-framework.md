# Presto 查询引擎内核详解：优化器体系与 CBO 框架

#### 从 PlanOptimizers 流水线到 Memo 上的代价取舍

---

## 引言

一条 SQL 在 Presto 中被优化的过程，并不是"交给优化器跑一遍"这么简单。Presto 的优化体系是一条**串行的 `PlanOptimizers` 流水线**：计划依次流过有序排列的若干优化阶段，每一站各自改写一部分，然后交还下一站。流水线上混编着两类决策——依据代数定律的 RBO 与依据统计估计的 CBO；此外还有一层不占据流水线站次的输入侧校正（HBO）。第一章先给出这条流水线的全景，确定坐标系。

了解了这条流水线之后，本文重点描述其中的 CBO 骨架，即 `IterativeOptimizer` 这一族实现。要理解一个 CBO 框架，需要回答几个具体的问题：

- 同一条 SQL 的等价计划是一个**组合爆炸的空间**——例如，仅 Join 顺序一项，10 张表的排列就是 10! 量级。优化器如何组织这个空间，才能既不重复计算，又不遗漏候选？
- 计划树的节点是**不可变对象**（immutable），任何一次改写都要重建整棵树的祖先链。优化器如何在几秒钟内完成成百上千次改写而不被对象拷贝拖垮？
- "代价"本身从哪里来？统计信息的计算代价不低，计划又在不断被改写——**旧计划上算出的统计与代价，凭什么在新计划上还有效？**

这三个问题分别对应 CBO 框架的三大支柱：**搜索空间的组织（Memo）、候选计划的生成（Rule）、候选之间的裁决（Stats + Cost）**。本文沿着这条主线，从 Cascades 的经典设计出发，落到 `IterativeOptimizer` 的具体实现，并特别关注 Presto 在"教科书设计"与"工程落地"之间做出的一系列取舍——尤其是它如何用串行流水线与受限规则集，把搜索空间从运行时的赌注变成设计时的承诺。

需要强调的是：**Presto 的整个 CBO 体系并不以"找到全局最优计划"为首要目标**，它追求的是受控的空间与时间尺度内的相对较优路径。因此文中凡提到"最优"，都指这一意义上的相对最优，而非全部等价计划中的全局最优。

## 一、Presto 优化器体系：串行的 PlanOptimizers 流水线

在讨论 CBO 之前，需要先确定坐标系：**Presto 并不存在一个"全局 CBO 阶段"。** 核心优化器本质上是一条串行的 `PlanOptimizers` 流水线——一个有序的 `PlanOptimizer` 列表，其中既有纯粹的规则式优化器实现，也有多个 `IterativeOptimizer` 实例；后者每个只封装**有限的一组规则**，在自己的 Memo 空间上做一次局部 CBO 探索，然后交还计划，由流水线的下一站继续处理。

```
PlanNode ─►[ RBO 站 ]─►[ CBO 站 ]─►[ CBO 站 ]─►[ RBO 站 ]─►[ CBO 站 ]─► ... ─► 最终计划
            确定性改写   受限规则集    受限规则集   确定性改写    受限规则集
                           |            |                     |
                           ▼            ▼                     ▼
                  （每个规则集持有独立的 IterativeOptimizer 实例 和 Memo 空间）
```

也就是说，代价驱动的探索被切分为若干个受控的小段，嵌入在确定性的串行流程之中。这个设计从两个层面约束了搜索空间：外层的流水线顺序是硬边界——每个阶段能看什么、能改什么，由它在列表中的位置预先决定；内层的规则集是软边界——每个 Memo 上 fixpoint 迭代的发散程度，由该实例携带的规则数量决定。两层边界合在一起，把"搜索空间会不会爆炸"这个 Cascades 最常被提及的风险，从运行时的问题转换为设计时可以界定的范畴。

代价是明确且被接受的：流水线某一站做出的改写，可能使另一站本可发现的更优形态永久不可达——**Presto 追求的不是全局最优计划，而是在受控的空间与时间尺度上的相对较优路径。** 这是贯穿后面所有实现细节的前提。

### 1.1 流水线上的两类决策：RBO 与 CBO

流水线上各站做出决策的依据并不相同，整体是 **RBO 与 CBO 的混编**：

| 类型 | 决策依据 | 在 Presto 中的典型代表 |
|:---|:---|:---|
| **RBO** | 代数定律与执行语义的**必然要求**，与数据无关 | `AddExchanges` / `AddLocalExchanges`：依据算子对数据分布的物理属性要求，规划 Remote / Local Exchange；谓词下推、列裁剪等结构性改写 |
| **CBO** | 基于统计的**代价估计** | `IterativeOptimizer` 实例上的一组受限规则，如 `DetermineJoinDistributionType`、`ReorderJoins` |

例如，AddExchanges 体现的是典型的 RBO 决策：它并不比较“Broadcast 还是 Repartition 哪个代价更低”，而是根据下游算子要求的物理属性，判断当前数据分布是否满足要求；如果不满足，就插入相应的 Exchange。这一决策主要由算子的执行语义与物理属性要求决定，而不是由当前表有多少行、网络代价是多少来决定。

而 `ReorderJoins` 是 CBO 阶段如何在受限空间内进行代价取舍的典型样本：它先通过确定性的约束划定候选边界——JoinNodeFlattener 只展平 INNER Join，展平数量受 joinLimit 限制，并跳过已经确定 distribution type 的子树；然后在这个受限集合上由 `JoinEnumerator` 枚举划分组合（枚举时强制左集合包含首节点，以规避 `Join(A,B)` 与 `Join(B,A)` 的对称重复），最后使用 `CostProvider` 比较候选 Join 树的代价（一次完整的示例见第八章）。

RBO 与 CBO 的混编发生在 PlanOptimizers 流水线层面，而 CBO 阶段内部则通过确定性的搜索约束控制候选空间，再由代价模型完成候选之间的取舍。这种分层设计使得 CBO 不必面对整个等价计划空间，而只需在当前阶段允许探索的有限空间内进行代价比较。

### 1.2 输入侧的第三种依据：HBO

除流水线上的两类决策之外，Presto 还有一层基于历史执行的统计信息校正。需要注意，它与 RBO、CBO 不在同一个维度上：**RBO 与 CBO 回答的是"由谁、依据什么在流水线上做改写决策"，是结构层面的划分；HBO 回答的是"CBO 所依据的那个估计值从哪里来"，是输入层面的修正。** 换言之，流水线的站次序列不会因为启用 HBO 而增减，变化的只是 CBO 那一站读到的数字更准了一些（详见 6.3）。

### 1.3 本文的范围

本文聚焦上述体系中的 CBO 骨架：从 Cascades 的经典设计（第三章）出发，落到 `IterativeOptimizer` 的实现（第四、五章），展开它赖以决策的统计与代价基础设施（第六章），讨论 fixpoint 迭代的收敛保障（第七章），并以一个四表 Join 的完整示例把各机制跑通一遍（第八章）。流水线上纯粹的 RBO 阶段（如 `AddExchanges` 依据物理属性规划 Exchange）各有独立主题，本文只在需要交代它与 CBO 的分工时提及。

## 二、CBO 要解决什么问题：在等价计划空间中取舍

### 2.1 启发式规则的失效边界

RBO 用一组确定性规则改写计划：谓词下推、列裁剪、常量折叠……这类改写的正确性主要由关系代数与执行语义保证，通常不需要依赖数据分布的统计估计。

但另一类决策没有这种性质。典型的例子包括：

**Join 数据分发方式**。Broadcast Join 把一侧数据复制到所有 Worker，代价与“数据量 × Worker 数”成正比；Repartitioned Join 对两侧重新哈希分区，代价与“两侧数据量”成正比。哪种方式更便宜，取决于两侧的数据规模以及具体的代价模型参数——这正是 `DetermineJoinDistributionType` 需要回答的问题。

**Join 顺序**。多表 Join 的执行代价对连接顺序高度敏感，而顺序的取舍依赖于中间结果基数的估计；其中，等值 Join 的 Join Key NDV 是影响基数估算的关键统计量。

> 一旦决策依据从"逻辑必然"变成"数据特征"，优化器就必须回答一个新问题：**在一个等价的计划空间里，如何系统地选出代价更低的那个？** 这便是 CBO 需要回答的问题。

### 2.2 三个子问题，三大组件

把上述命题拆开，可以看到三个相互配合的核心问题：

| 子问题 | 含义 | 对应组件 |
|:---|:---|:---|
| **组织** | 等价计划如何存放，才能共享子结构、避免重复计算？ | **Memo** 空间 |
| **生成** | 等价候选从哪里来？ | **Rule**（转换规则 + 实现规则） |
| **裁决** | 候选之间如何比较？ | **Stats + Cost**（统计与代价模型） |

三者缺一不可：没有 Memo，每条等价改写都要复制整棵树；没有 Rule，空间里只有一个计划；没有代价模型，再大的空间也只能随机挑选。Presto 的实现沿用了 Cascades 的这套分工——而 Cascades 又源自上世纪 90 年代的 Volcano 体系，这条谱系在工业界延续为 SQL Server、Orca 与 Calcite 的优化器框架。

## 三、Cascades 框架：Memo、Rule 与代价搜索

### 3.1 Memo：用等价类组织搜索空间

Memo 的关键洞察是：不同候选计划之间往往共享大量子结构。例如多个 Join 顺序不同的候选，可能反复使用相同的 A ⋈ B、B ⋈ C 等子计划。如果让每个子计划只计算一次，搜索空间就可以从“计划森林”压缩为“共享子结构的 DAG”。

Memo 用 **Group（等价类）** 来表达这一点：一个 Group 表示一组语义等价的表达式，每个具体表达式的子节点则通过对其他 Group 的引用连接：

```
计划:  A ── B ── C ── D
            └── E ── F

Memo:  G0: { A → G1 }
       G1: { B → [G2, G3] }
       G2: { C → G4 }
       G3: { E → G5 }
       G4: { D }
       G5: { F }
```

在经典的 Cascades 中，一个 Group 内会保留搜索过程中发现的等价表达式候选（如 `G1 = {Join(A,B), Join(B,A), HashJoin(A,B), MergeJoin(A,B), ...}`），逻辑候选与物理实现共存于同一等价类。这带来 DP 式的收益：一个 Group 的最优计划算出一次即可缓存，引用它的上层表达式可以直接复用——这正是 memoization 的本义。

不过"复用"二字需要精确理解：缓存的键不是 Group 本身，而是 **（Group, Required Properties）**。OptimizeGroup(G, RP, UB) 的优化上下文包含 Group、Required Properties 和 Cost Upper Bound，但 winner 的缓存主要按 (Group, RP) 组织；UB 是当前搜索的代价约束，用于判断已有 winner 是否可以复用，以及在不满足时继续搜索。

这个限定并非小题大做。考虑一个 Group 内的两个候选：`P1` 代价 100 但输出无序，`P2` 代价 120 但输出按 k 有序。若父节点是要求输入有序的 Merge Join，那么"取最便宜的 `P1` + 补一个 Sort(60)"合计 160，反而不如直接采用 `P2` 的 120——**单独看更贵的子计划，在特定的父节点语境下可能给出更低的总代价。** 属性需求之所以必须参与 winner 的区分，原因正在于此。

### 3.2 Rule：模式匹配驱动的候选生成

等价候选由规则生成。规则由三部分构成：

- **Pattern（模式）**：描述可匹配的计划树形状，如 `filter(x: project(<any>))`；
- **命名绑定（named arguments / capture）**：把匹配到的子结构绑定为变量，供规则体直接使用——`Join(left, right)` 中的 `left`、`right` 绑定的就是实际计划中符合条件的子 Group；
- **规则体**：消费绑定，产出零到多个新表达式。

规则通常不需要通过一个固定的类型标签来区分；从优化作用看，可以概括为三类：

| 类型 | 例子 | 作用 |
|:---|:---|:---|
| 逻辑转换 | Join 交换律 / 结合律、谓词下推 | 扩充逻辑等价空间 |
| 物理实现 | `LogicalJoin → HashJoin / NestedLoopJoin` | 生成可执行计划 |
| 属性强制 | 在 Merge Join 之前强制排序 | 建立物理属性要求 |

### 3.3 代价搜索：自顶向下触发，自底向上聚合

Cascades 的搜索方向常被概括为"自顶向下"，但这个说法只对了一半。准确地说，体系中存在**两个方向**：

```
规则展开 / 计划搜索        自顶向下     决定"先优化哪个 Group"
代价计算                  自底向上     聚合子计划代价以得到父计划代价
```

优化入口是 `optimize(rootGroup, costBound, requiredProperties)`。表面上，它从 Root Group 出发挑选候选表达式，看起来是自顶向下的；但任何一个表达式的代价都满足：

```
cost(Join(A, B)) = local_cost(Join) + best_cost(A) + best_cost(B)
```

`best_cost(A)` 必须先算出来——于是优化器递归优化子 Group，代价沿递归返回的方向自底向上聚合。**最终行为是：递归向下展开，代价向上返回。**

那为什么不干脆用纯自底向上的动态规划？两个原因：

1. **剪枝的时机**。自顶向下携带 cost bound（代价上界），可以在展开之前就放弃注定超界的分支，不必探索整个搜索空间。具体机制是动态收紧的边界：先用 `costBound` 优化左子树得到 `cost(L)`，再用 `costBound - cost(L)` 优化右子树，最后用剩余边界约束当前节点。这构成了 branch-and-bound 式的剪枝机制；
2. **物理属性的传递方向**。required properties（如"输出需按 k 分区"）天然由父算子对子算子提出，只能自顶向下传播。

> Cascades 是一个"**自顶向下触发 + 自底向上聚合**"的深度优先 DP 搜索框架。方向的选择并非随意为之，而是由两件事决定：何时能够剪枝，以及物理属性沿哪个方向传递。

## 四、Presto 的落地：IterativeOptimizer 的组织与控制流

以上介绍的是经典 Cascades 的基本搜索框架。Presto 的实际实现——`IterativeOptimizer`——保留了 Memo、Rule 和 Cost/Stats 这几个核心组件，但在搜索空间组织和控制流上进行了明显的工程化简化。

### 4.1 双轨入口

`IterativeOptimizer` 同时持有两套规则：

- `legacyRules`：传统的顺序重写优化器（`PlanOptimizer` 列表），逐个应用；
- `newRules`：Cascades 风格的规则集合，通过 `RuleIndex` 组织——一个从**模式顶层算子类 → 规则**的 multimap 索引。匹配某条规则时，只需先按根节点类型过滤候选集，避免了对全部规则的线性扫描。

当会话未启用新优化器（`isNewOptimizerEnabled`）且存在 legacy 规则时，走传统顺序路径；否则进入 Cascades 风格的探索路径。

需要说明的是，`IterativeOptimizer` 始终是 PlanOptimizers 流水线上的一站——而且在整个流水线中出现多次，每次携带不同的规则集。它的探索范围因此受双重约束：内层是本实例携带的规则集（决定探索的发散程度），外层是它在流水线中的位置（前序阶段已经确定的形态不再重新考虑）。这两层约束，正是第一章所述"搜索空间在设计期即可界定"的具体含义。

### 4.2 optimize() 的组织流程

进入新路径后，`optimize()` 是整个 CBO 探索过程的入口。它首先为本次优化建立 Memo、RuleIndex、统计与代价 Provider 以及 Context 等基础设施，然后从 Root Group 进入 exploreGroup() 的递归探索，最终从 Memo 提取优化后的计划。

```
                         optimize(plan, ...)
                               │
             ┌─────────────────┼────────────────┐
             ▼                 ▼                ▼
           Memo            RuleIndex        Calculator
             │                                  │
             │                         ┌────────┴────────┐
             │                         ▼                 ▼
             │                 StatsCalculator     CostCalculator
             │                         │                 │
             │                         ▼                 ▼
             │               CachingStatsProvider  CachingCostProvider
             │                         └────────┬────────┘
             │                                  │
             └──────────────┬───────────────────┘
                            ▼
                         Context
                            │
                            ▼
                       exploreGroup()
                            │
                            ▼
                       memo.extract()
                            │
                            ▼
                       最终 PlanNode
```

其中有两个值得注意的生命周期设计：

- `statsCalculator` / `costCalculator` 是**全局单例**（依赖注入，所有查询共享），只负责"怎么算"；
- `CachingStatsProvider` / `CachingCostProvider` 是**单次优化生命周期**的缓存层（内部是按引用判等的 `IdentityHashMap`），缓存"算出来的结果"，生命周期与 Memo 一致——Memo 销毁，缓存随之废弃。

计算逻辑与计算缓存的分离，使得缓存策略（失效传播，见 6.2）可以完全内聚在 Provider 一侧。

### 4.3 控制流：exploreGroup 的 fixpoint 循环

实际探索由三个方法构成递归的三角控制流：

```
exploreGroup(G)                      ← 整体控制器：驱动当前子树整体收敛
    │
    ├── exploreNode(G)               ← 对当前 Group 反复试规则，直至局部收敛
    │
    └── while (有进展):
            exploreChildren(G)       ── 递归 ──► exploreGroup(child)
                │
                └── 子树变化后回到 exploreNode(G)   ← 变化反馈，父节点重新尝试
```

`exploreGroup` 的循环逻辑精确刻画了 fixpoint 语义：

1. 先 `exploreNode`：对当前 Group 反复应用所有可匹配规则，直到不再有规则触发（局部收敛）；
2. 再 `exploreChildren`：递归优化所有子 Group。**只要任何一个子树发生变化，就返回当前 Group 重新执行 `exploreNode`**——因为子树变了，父节点可能又能匹配新的规则了；
3. 若父节点重试无进展，跳出循环。

> Presto 新优化器框架整体上是一个：**自顶向下驱动 + 子树变化反馈 + 当前节点重试**的 fixpoint 框架。收敛的判据不是"所有规则都试过一遍"，而是"再来一轮也不会有任何变化"。

### 4.4 规则执行与统计等价性的保持

单条规则的执行在 `transform()` 中：`matcher.match(rule.getPattern(), node)` 先做模式匹配，命中后调用 `rule.apply(match.value(), match.captures(), ruleContext(context))` 产出新节点，最终通过 `memo.replace(group, transformedNode, ...)` 写回 Memo。

一个容易被忽略、但在设计上颇有讲究的细节是 **statsEquivalentPlanNode 的保持**：如果规则改写了节点却没有显式声明统计等价信息，框架会自动把原节点的统计等价声明迁移到新节点上。这维护了一个不变式——**凡是规则没有明确声明“统计特征变了”的改写，都默认视为统计上等价**。统计缓存因此得以在大量纯结构性改写（如变量重命名）中安全复用。

这条不变式的价值远不止缓存复用：它同时也是 HBO 能够工作的前提——只有存在一个不随纯结构性改写漂移的统计身份，当前计划节点才能在经过多轮优化改写后，仍与历史执行中对应的计划节点建立关联，从而复用其历史实测统计（详见 6.3）。

## 五、Presto 的 Memo 数据结构：单成员 Group 与引用计数

### 5.1 GroupReference：计划节点与 Group 的桥接

Memo 将计划树中的计划节点分别组织到 Group 中，其子节点被替换为 `GroupReference`——一个形式上仍是 PlanNode、实质上指向 Group ID 的引用节点。这个设计让规则本身无需直接感知 Memo 的内部数据结构：规则的 Pattern 匹配在普通 PlanNode 世界进行，`Lookup` 负责在 Group 引用与实际节点之间透明解析。

### 5.2 引用计数与垃圾回收

Group 维护 `incomingReferences`（谁引用了我）。当改写使某个 Group 从根节点不可达时，其引用计数可能归零，Group 随之被级联删除；这一机制更接近引用计数式的垃圾回收。

```
memo.replace(G, newNode)
    │
    ├── incrementReferenceCounts(newNode, G)   ← 建立 G 到新子 Group 的引用
    ├── membership = newNode                   ← 替换 Group 当前成员
    └── decrementReferenceCounts(oldNode, G)   ← 撤销旧引用
            │
            └── 子 Group 引用计数归零 → deleteGroup → 递归清理其子引用
```

### 5.3 关键差异：单成员 Group

这里存在一个关键差异。经典 Cascades 的 Group 可以保留搜索过程中发现的等价表达式候选，并在这一搜索空间上进行 cost-guided exploration 与 branch-and-bound 剪枝；而 Presto 的 `Memo.Group` 只保留一个字段：

```java
private PlanNode membership;   // 该分组当前关联的实际计划节点（唯一）
```

`memo.replace()` 的语义不是“往等价类里追加一个候选”，而是**“用新节点覆盖当前成员”**。Group 退化成了一个可原地修改的槽位。

> Presto 的 Memo 不是搜索空间，而是**改写工作台**：它不保存“所有等价计划”供代价比较，只保存“当前选定的形态”供继续改写。等价空间的枚举被放弃了，换来的是实现简单、内存可控、调试可追踪。

这个取舍的深层含义在于**裁决权的下放**：既然框架不负责全空间枚举和统一的 branch-and-bound 搜索，代价比较就主要下沉到具体规则中——例如 `DetermineJoinDistributionType` 从 `Rule.Context` 获取 `StatsProvider` / `CostProvider`，比较 Broadcast 与 Repartitioned 两种形态的代价；`ReorderJoins` 则在规则内部实现自己的 Join 顺序 DP。框架提供的是决策**基础设施**（统计、代价、缓存），而具体规则负责**决策逻辑**。

反过来看，单成员 Group 也使 3.1 节讨论的多 winner 问题不再出现在 Memo 层：一个 Group 只维护一个当前成员，因此不需要在同一 Group 内同时维护针对不同 Required Properties 的多个 winner。经典 Cascades 通过这种属性感知的 winner 复用扩大搜索与复用能力；Presto 则不在 Memo 层维护这套机制，而是将具体的属性与代价权衡下沉到规则内部，在规则自己构造出的较小候选集上完成。

## 六、统计与代价：生产、缓存与失效传播

### 6.1 计算链

统计信息的计算链是典型的"规则化计算 + 透明缓存"两层结构：

```
StatsCalculator（全局单例，负责实际的统计信息计算）
    ├── ComposableStatsCalculator（基础实现：按根节点类型分派统计规则）
    │       └── Rule<T>: getPattern() + calculate(node, sourceStats, ...)
    └── HistoryBasedPlanStatisticsCalculator（启用 HBO 时的可选外壳）
            └── delegate = ComposableStatsCalculator
                    先由 delegate 估算，再用历史实测覆盖可用分量

StatsProvider（单次优化生命周期，向调用方提供统计信息）
    └── CachingStatsProvider（提供缓存机制，已算过的统计信息不用再算一遍）
            ├── GroupReference → memo.getStats(group) 命中即返回
            └── 未命中 → statsCalculator.calculateStats(node, this, ...)
                     → memo.storeStats(group, stats) 写回 Memo
```

`ComposableStatsCalculator` 的分派逻辑与优化器的 `RuleIndex` 在组织方式上具有相似性：都是先按计划节点类型缩小候选范围，再选择适用的规则。也就是说，**统计推导本身也被建模为一套规则系统**——Aggregation 的基数估计、Filter 的选择率推导，各自是一条独立的统计规则。

代价一侧的结构完全对称：`CostCalculator` 负责计算，`CachingCostProvider` 负责缓存于 Group，此处不再赘述。

### 6.2 失效传播：改写如何使缓存作废

计划被不断改写，而统计与代价是**针对特定计划形态**算出来的。`memo.replace()` 换掉 Group 的成员后，旧统计立刻作废——而且不止本 Group：任何以本 Group 为子节点的上层 Group，其统计同样依赖这个子树，也一并作废。`evictStatisticsAndCost()` 实现了这个向上递归的失效传播：

```
memo.replace(G5, newNode)
    │
    ▼
evictStatisticsAndCost(G5)          ← G5.stats = null, G5.cost = null
    │
    ├── 遍历 incomingReferences: {G2, G7}
    ├── evictStatisticsAndCost(G2)  ← 引用本 Group 的父 Group 也失效
    └── evictStatisticsAndCost(G7)  ── 递归向上，直到根
```

反向引用边（`incomingReferences`）在此处发挥了第二个作用：它不仅是 GC 的引用计数，也是**缓存失效的传播路径**。子树一旦被改写，沿引用边向上污染的所有相关统计被整体清除，下次询问时按需重算。

> 统计与代价不依附于计划树的数据结构，而以 Group 为粒度旁挂在 Memo 上：**计划是主体，统计是影子；影子随主体改写而作废，作废沿引用边向上蔓延。**

需要注意的是：类似的失效机制也作用于 Logical Properties：当计划结构发生变化时，依赖该结构推导出的逻辑属性同样需要重新计算。这里不再展开。

### 6.3 HBO：让上一次的真实执行修正这一次的估计

CBO 的精度瓶颈通常不在搜索，而在**输入**。估计建立在一系列假设之上（列间独立、取值均匀），多表 Join 的选择率误差会被逐层放大；若输入本身偏差过大，再细致的搜索也难以选出合适的计划。HBO（History Based Optimization）正是对这一薄弱环节的补偿，而它的接入方式相当克制——**它不是流水线上的一个新阶段，而是作用在 CBO 的输入侧**：

> **HBO 不在流水线上占据新的阶段，而是作用在 CBO 的统计输入侧。** `HistoryBasedPlanStatisticsCalculator` 内部持有一个 delegate `StatsCalculator`（通常是 `ComposableStatsCalculator`）：先用规则估算出 `delegateStats`，再用历史实测值覆盖其中可用的分量，返回合并结果。流水线上的任何规则都不知道自己拿到的输入是"算出来的"还是"跑出来的"。

要把第 N 次执行的经验迁移给第 N+1 次查询，先要解决一个身份问题：**计划树经历多轮优化改写之后，如何认定"这就是上次那个 Join"？** 这正与前文那条不变式相呼应——节点上的 `statsEquivalentPlanNode`（见 4.4）提供不随纯结构性改写漂移的统计身份；`HistoricalStatisticsEquivalentPlanMarkingOptimizer` 在优化早期为各节点赋予该属性，`registerPlan()` 随即对每个节点做规范化并计算哈希（`CanonicalPlanGenerator` 按指定的 `PlanCanonicalizationStrategy` 生成规范化计划后序列化取摘要），得到一个跨查询稳定的 key。

匹配并非"哈希相同即可"。同一个计划在不同数据规模下输出行数可以相差几个数量级，因此 HBO 还要比对**输入表的统计信息**：只有当历史上那次执行的输入表统计与本次的偏差落在 `historyMatchingThreshold` 之内，这条历史记录才被认为是可参考。命中且置信度大于零时，`delegateStats.combineStats(predicatedPlanStatistics, ...)` 用历史值覆盖对应的估计分量；未命中则原样回退——**HBO 的设计是增强而非替代：历史统计不可用时，仍回退到常规统计估计。**

写回侧构成闭环：查询创建时，`HistoryBasedPlanStatisticsTracker` 通过 `addFinalQueryInfoListener` 挂到 `QueryExecution` 上；执行完毕后，把各规范化节点的实际统计写回 `HistoryBasedPlanStatisticsProvider`（可用 Redis、内存或空实现）。一条跨越多次查询的反馈回路就此成型：

```
第 N 次查询 ── 执行 ──► 实际行数 / 数据量
                            │ addFinalQueryInfoListener 写回
                            ▼
                历史统计仓库（按规范化计划哈希索引）
                            │ 第 N+1 次优化时读取
                            ▼
              StatsCalculator 的估计值被实测值修正
```

由此可以看清三者的关系——但它们并不处在同一个层次上：

> **RBO 主要依据规则与执行语义，CBO 依据统计信息与代价模型，HBO 则利用历史真实执行反馈修正统计估计。** 前两者是流水线上的两类决策：确定性最强的先做，估计驱动的取舍紧随其后；而 HBO 不改变流水线的站次与顺序，它只决定 CBO 拿到的那个估计值有多准。三者不是替代关系，而是"谁来做决策"与"决策依据从哪来"这两个问题的不同答案。

值得强调的是，HBO 提高的是**输入质量**，而不是**空间覆盖**：它让同等规模搜索下的比较更准，却不扩大被搜索的范围——"较优"向真正的"最优"推近一圈，但受控空间的边界并未移动。

## 七、收敛性：fixpoint 的保障与最后的刹车

规则驱动的 fixpoint 迭代首先需要回答一个问题：如何防止规则组合把优化过程拖入无限迭代？设计文档中列出了几类必须防范的情况：

- **不收敛的规则**：如 `limit(union(x)) → limit(union(limit(x)))`，规则输出会再次满足自身触发条件，无限自激；
- **恒等改写**：规则产出与输入完全相同的表达式，引擎原地打转；
- **重复触发**：`union(union(union(x, y), z), w) → union(x, y, z, w)` 这类展平规则，若不加以抑制，会对每个子表达式不必要地重复触发；
- **匹配过程中的重复访问**：模式匹配在遍历计划结构时需要记录已访问的 Group，避免同一 Group 被重复遍历。

这些约束意味着**写一条新规则时，规则作者必须考虑它与现有规则组合后的收敛性。**，框架只提供两类兜底：模式匹配的访问去重，以及——最后的刹车——`Context.checkTimeoutNotExhausted()`。每次节点探索前检查耗时，超过 `optimizer_timeout`（默认 3 分钟，设置得较长是因为规则可能需要从 Connector 元数据获取信息）即抛出异常终止优化。超时兜底的意义在于把最坏情况从"优化器长时间空转"降级为"本次查询放弃优化"——及早放弃，而不是无限等待。

还需要区分一点：fixpoint 的收敛并不等于全局最优。 它只意味着在当前规则集合、搜索范围和探索过程下，继续应用规则已经无法产生新的变化；它并不构成对整个等价计划空间的最优性证明。这也是 Presto 与经典 Cascades 全局代价搜索模型之间的重要差异。

## 八、示例：一个四表 Join 如何被重排

框架至此已经完整，但还没有"动起来"。本章不再抽象地讨论组件，而是让一个具体的 ReorderJoins 跑完整条链路：Stats 提供输入，Rule 生成候选，内部 Memo 复用子问题，Cost 完成候选裁决。

接下来用一个最小例子把 `ReorderJoins` 的完整链路走一遍：四张表 `A ⋈ B ⋈ C ⋈ D`，谓词为 `A.k = B.k ∧ B.k = C.k ∧ C.k = D.k`；经过谓词下推后的基数估计为：A 一千行，B 十万行，C 一万行，D 五百万行。

**第一步 · 展平。** `JoinNodeFlattener` 将满足条件的连续 INNER Join 子树拆成 sources + predicates，最终形成 `MultiJoinNode`。只有 INNER、deterministic、尚未确定 distribution type 且未超过 joinLimit 的 Join 才会继续展开；否则整个子树作为一个 source 保留。

**第二步 · 划定空间。** 如果把 4 张表的左右顺序和二叉树形都分别计入，理论上的有序 Join tree 数量是 120；考虑 Join 交换律后，对无方向的二叉 Join tree 可归并为 15 类。generatePartitions 生成左右子集合的划分，并通过固定一个元素进入左集合，消除左右交换产生的对称重复。

**第三步 · 递归与 memo。** 枚举对每个划分递归求解两侧，对 4 张表而言，理论上存在 11 个大小至少为 2 的子集合子问题。这些子问题通过 `JoinEnumerator` 内部的 `Map<Set<PlanNode>, JoinEnumerationResult>` 进行复用。

**第四步 · 剪枝。** 某划分两侧不存在可用于连接的跨侧 Join 条件，返回 `INFINITE_COST_RESULT`，直接丢弃；任一子问题无法得到可用的代价结果（`UNKNOWN_COST_RESULT`），则整个重排放弃并保留原始顺序：宁可不改，也不在代价信息不可靠时强行做选择。

**第五步 · 定价与选择。** 下面为了直观展示 Join reorder 的决策过程，使用“移动行数”作为简化代价模型；这不是 Presto 实际 CostCalculator 的完整计算公式。在这个简化模型下，三个代表性候选：

| 候选形态 | 关键中间结果 | 累计代价 |
|:---|:---|:---|
| `A ⋈ (B ⋈ (C ⋈ D))` | `C⋈D` = 50 万 | 5,671,000 |
| `A ⋈ ((B ⋈ C) ⋈ D)` | `B⋈C` = 4 万 | 5,152,200 |
| `((A ⋈ B) ⋈ C) ⋈ D` | `A⋈B` = 2 千 → 1 千 | 5,114,000 |

以第一行为例：最内层 `C⋈D` 移动 1 万 + 500 万，中间层 `B⋈(C⋈D)` 移动 10 万 + 50 万，最外层移动 1 千 + 6 万，合计 5,671,000。其余两行同理可验算。

差距几乎全部来自 D 的位置：让它先参与，就得先把五百万行重分区；让它最后参与，届时中间结果只剩一千行。

**第六步 · 物理形态的二次选择。** 上表仍是逻辑层面的比较。`setJoinNodeProperties` 还会为每个 Join 在 PARTITIONED 与 REPLICATED 之间再取一次 min：最后一层左侧仅一千行，在这个简化模型下，若低于 broadcast 阈值，改为 REPLICATED 之后代价约为"一千 × Worker 数"，D 那一侧无需进行这一步的 repartition shuffle——总代价随之降到十万量级，与最贵的候选相差近五十倍。

注意这个规模：**一个数量级以上的收益，产生在一个理论上只有 15 类无方向二叉 Join 结构、11 个非平凡子问题的搜索空间里。** 边界由 `JoinNodeFlattener` 的 INNER-only、deterministic 条件、已定 distribution type 的提前退出，以及 joinLimit 共同划出；边界之内，DP 按子集合逐步计算候选代价并保留当前最优结果。

## 九、总结：规则生成候选，代价完成裁决，Memo 组织搜索

回头看，CBO 框架的三大支柱在 Presto 中的最终形态：

| 支柱 | 经典 Cascades                            | Presto IterativeOptimizer |
|:---|:---------------------------------------|:---|
| **搜索空间** | 全局统一的 memo 搜索空间                        | 串行流水线切分为多个受限的局部空间 |
| **Memo** | 等价类保留全部候选，是搜索空间；按（Group、属性需求、上界）缓存多份胜者 | 单成员 Group，是改写工作台，无候选缓存 |
| **Rule** | 框架统一调度，含代价边界剪枝                         | fixpoint 重写；裁决权下放到规则内部 |
| **Cost** | branch-and-bound 全局搜索的评分函数             | `StatsProvider` / `CostProvider` 作为规则可查询的基础设施 |
| **收敛保障** | Memo 去重与搜索控制约束搜索范围                     | 流水线分段 + 规则自证收敛 + 超时兜底 |

对照表背后的取舍可以归结为三个层面的收紧：外层用串行的 `PlanOptimizers` 流水线把代价驱动的探索切成有限的几段，每段只携带有限的规则集；中层放弃全空间枚举与代价剪枝，把框架简化为"Memo 上的 fixpoint 重写引擎"；底层把裁决权下放到具体规则，让每条代价敏感的规则（`ReorderJoins`、`DetermineJoinDistributionType`……）自带决策逻辑。代价是无法保证探索到全局最优的计划；换来的是搜索空间在设计期即可界定、优化时间在运行期可约束、优化行为在故障时可追溯。

这条路径在 Cascades 的思想谱系中有清楚的位置：SQL Server、Orca 等实现采用统一的 Memo 搜索空间，而 Presto 则选择了轻量的 Memo 化重写，将搜索边界更多交给 PlanOptimizers 与具体 Rule。Calcite 则提供了不同类型的 Planner 实现，可采用不同的优化策略。

> **Memo 解决状态怎么存，Rule 解决候选从哪来，Cost 决定最终选哪个。** CBO 的本质不是把代价算得多么准——统计永远只是估计——也不是把最优计划找出来——搜索空间永远只是全空间的一个受控切片——而是把"这个计划为什么更好"，从直觉变成一条可计算、可复现、可失效的推导链。


---

## 欢迎交流

本文基于作者当前的理解与实践经验整理而成，难免存在疏漏或值得进一步探讨之处。

如果您对文中的观点有不同看法，发现任何问题，或有相关实践经验，欢迎通过 [GitHub Issue](https://github.com/hantangwangd/hantangwangd.github.io/issues/new) 与作者交流讨论。

期待与更多同行围绕数据基础设施相关技术展开交流，分享实践经验，共同学习、共同进步。
