# 能力联邦：Presto的一种可能演进方向

####  从 Data Access 到 Capability Provider：关于联邦查询引擎演进的一些思考

---

## 引言

长期以来，业界对联邦查询引擎（Federated Query Engine）的认知，大多停留在一个经典定义上：

> **"统一访问多个异构数据源的分布式 SQL 查询层"。**

无论是 PrestoDB、Trino，还是早期联邦数据库系统，其核心价值通常被概括为如下几点：

- **统一 SQL 接口**：屏蔽不同系统之间的数据访问差异
- **统一元数据视图**：多数据源连接能力，将分散在不同系统中的数据组织为统一的逻辑空间
- **统一计算层**：在查询引擎中完成跨系统计算

在这样的定位下，Connector 层的职责也被自然界定为 **Storage Adapter Layer**，即：

- Schema Mapping
- Data Access
- Predicate Pushdown
- Split Enumeration

本质上，Connector 更多承担的是一个面向异构数据源的数据访问适配层（Data Access Adapter）的角色。

然而，现代数据基础设施的演进正在悄然改变这一格局。

Lakehouse 表格式、AI Native 数据格式、向量数据库、分布式缓存系统、特征平台与数据编排系统等等，这些技术方向正逐渐成为数据平台的核心组件。它们带来的不仅仅是新的数据来源，更是**新的语义、新的能力和新的优化机会**。

最近在阅读代码、Review PR，并参与 Presto 生态中 Iceberg、Lance 等 Connector 的开发过程中，我逐渐注意到一个有意思的现象：Connector 的职责似乎正在发生一些值得关注的变化。

> **Connector 正在变得越来越厚。** 其正在从传统的"数据访问适配层"，逐渐演化为"能力提供层（Capability Provider Layer）"；
>
> Federated Query Engine 也开始具备越来越强的能力聚合与编排能力，并逐渐承担起一种本文暂且称其为**能力聚合层** (Capability Aggregation Layer) 的角色。

这不是对原有架构的否定，而是对其职责的自然延伸。接下来，我们将从 PrestoDB 的视角出发，梳理这一演化的技术背景、已有基础与未来可能的方向。

声明：本文并非试图定义或限定 Federated Query Engine 的演进方向，也不代表任何社区或项目的官方立场，而只是记录自己在参与相关项目过程中形成的一些观察与思考。

---

## 一、从传统定位到新背景下的挑战：Federated Engine 的能力边界正在扩展

### 1.1 传统定位：统一访问异构数据的 SQL 层

传统联邦查询系统的核心目标，可以归结为一个简单的问题：

> 如何在统一 SQL 语义下，高效查询多个异构数据源？

围绕这一目标，Connector 的职责通常包括：

- 如何获取 metadata
- 如何读写数据
- 如何下推 predicate 和 projection

由此衍生出的典型能力包括：

- Predicate Pushdown
- Projection Pushdown
- Partition Pruning
- Split Generation

这些能力虽然重要，但本质上仍然归属于同一个范畴：

> **Access Semantics**——即"如何访问数据"。

在这一阶段，Connector 的价值主要体现在"连接"与"适配"：它屏蔽了底层数据源的差异，向上层呈现统一的关系型视图。至于这些数据源本身是否具备更丰富的语义或能力，并不在传统联邦引擎的关注范围内。

### 1.2 根本变化：Lakehouse 与 AI Data Infra 带来的新语义

然而，近年来数据系统和表格式的能力边界正在显著扩展。

以 Apache Iceberg 为代表的 Lakehouse 表格式，使得数据管理层开始具备越来越丰富的语义：

- Snapshot Semantics
- Transaction Semantics
- Metadata Graph
- Partition Evolution
- Delete Semantics
- Fine-grained Metadata Indexing

与此同时，AI Data Infra 的兴起又引入了全新的数据形态与访问模式：

- Embedding
- Vector Index
- ANN Retrieval
- AI Dataset Management
- Multimodal Metadata（图像、视频、音频等）

这些变化共同反映出一个趋势：

> Connector 所面对的，已经不再只是"文件"或"表"，而是具备丰富语义的数据系统与数据格式。

### 1.3 正在发生的演进：当 Connector 开始超越传统边界

从我观察到的一些实践来看，联邦查询引擎的职责似乎也正在随之演进。它不再仅仅满足于"读取某个 Iceberg、Hive 或 Lance 表"，而是开始：

- 利用 Snapshot 进行增量计算
- 利用 Metadata 进行查询优化
- 利用 Partition Evolution 实现透明演化
- 利用表级语义进行 Materialized View 的改写与维护
- 利用 Vector Index 进行 AI-native 检索

这意味着：

> Connector 已经开始向 Query Planner / Optimizer / Execution 暴露越来越多的能力。

其角色已经超出了传统意义上的 Data Access Adapter。当越来越多的数据系统能力通过 Connector 暴露给 Query Planner、Optimizer 与 Execution 层时，Federated Query Engine 所能够利用和编排的能力边界也随之扩展。它开始不仅负责统一访问数据，也开始承担不同数据系统能力的协调、聚合与利用——这也正是本文后续章节希望深入探讨的核心方向。

---

## 二、从 Data Access 到 Capability：Connector 的新角色

在 Presto 的现有生态中，我们已经能够观察到若干 Connectors 开始展现出超越传统"数据访问适配"边界的能力。它们不再单纯地向引擎暴露查询访问的基础能力，而是开始向整个系统提供其底层数据系统或存储格式所特有的执行语义与优化机会。

尤为关键的是，其中一部分能力具备**泛化**的可能性。当然，并非所有 Connector 特性都具备泛化价值。只有那些对多种数据源具有普遍意义、能够在抽象层面被统一表达和复用的高级执行语义，才真正值得也具备被泛化的条件。一旦此类能力由某个 Connector 提供，Presto 生态中所有 Connectors 所管理的数据与系统，都将有可能共享这一能力。

以 Alluxio、Iceberg、Lance 为例，这三个 Connector 分别代表了三种不同方向的 Capability Provider。

### 2.1 Alluxio：缓存能力的泛化

Alluxio 的传统定位是分布式缓存层（Distributed Cache Layer）。但从 Federated Engine 的视角来看，它提供的并不仅仅是某个存储系统的缓存加速，而是一组可以被查询执行层直接利用的基础设施能力：

- Data Locality
- Distributed Cache
- Remote IO Acceleration

这些能力本质上解决的是：**如何更加高效地访问远端数据**。因此，它们并不依赖具体的数据格式或者数据源。无论底层数据来自：

- Hive
- Iceberg
- PostgreSQL
- Object Storage

只要查询通过 Federated Engine 执行，都可以受益于 Alluxio 提供的缓存与数据局部性能力。这些能力的意义已经超越了"为某个特定存储系统提供缓存"的范畴，而是演变为 **"整个查询执行层的 IO 能力增强"**。换言之，Alluxio Connector 将分布式缓存作为一种可泛化的系统能力，附加在任何后端数据源之上。

### 2.2 Iceberg：Data Management Capability 高阶语义的泛化

Apache Iceberg 已经不再仅仅是一个开放表格式（Open Table Format）。随着现代 Lakehouse 架构的发展，以 Iceberg 为代表的数据表格式逐渐具备更加丰富的数据管理语义：

- Snapshot-based Semantics
- Transaction Semantics
- Metadata Graph
- Partition Evolution
- Delete Semantics
- Fine-grained Metadata Indexing

这些语义的价值已不再局限于数据读取层面，而是开始扩展到：

- 查询规划（Query Planning）
- 增量计算（Incremental Computation）
- 数据维护（Data Maintenance）
- 表演进管理（Table Evolution Management）
- 数据一致性管理（Consistency Management）

换句话说，Iceberg 提供的不只是：

> Access to Table Data

而是：

> Access to Data Management Semantics

例如，在 Materialized View 场景中，Iceberg 所提供的价值并不仅仅是"存储物化视图结果"，而是作为 Materialized View 的**统一状态管理层**（State Management Layer），为物化视图维护提供了一组关键的数据管理语义：

- Snapshot 与 Metadata 提供版本化的视图状态管理能力
- Schema Evolution 与 Partition Evolution 提供透明的数据演化能力
- Atomic Commit 与 Snapshot Isolation 提供一致性的刷新发布语义
- Fine-grained Metadata Indexing 提供高效的查询访问能力

这些能力共同解决了 Materialized View 最核心的一系列问题：

- 如何维护视图状态的一致性与可见性
- 如何尽可能的利用变化数据的信息，以较低代价维护视图最新状态
- 如何支持视图数据的持续演化与长期维护
- 如何提供面向查询优化的高效访问路径

因此，Presto 不再需要依赖底层数据源自身提供 Materialized View 能力，而是可以利用 Iceberg 作为统一的 Materialized View 状态管理层：

- 上游数据可以来自 Hive、PostgreSQL、MySQL 或对象存储
- Materialized View 的实际数据、版本状态以及生命周期管理，统一由 Iceberg 承担
- 增量刷新、查询改写以及 Predicate Stitching 等高级优化能力，则由 Presto Federated Engine 统一提供

这些能力的本质在于：Iceberg Connector 具备了为整个 Presto Federation 提供统一 Materialized View 状态管理能力的基础，而 Presto Engine 则负责将这一能力进一步扩展为跨 Connector 的高级物化视图语义。无论底层数据来自 Hive、PostgreSQL 还是对象存储，只要通过 Presto 进行查询，都有可能共享这一能力所带来的优化收益。

### 2.3 Lance：AI Workload 高阶语义的泛化

Lance 代表了 AI Data Infra 方向的新范式。Lance 提供的不只是列式存储（Columnar Storage），更包括：

- Vector Index
- ANN Retrieval
- Embedding-aware Access
- AI Dataset Management
- 多模态数据原生存储

基于这些能力，Lance Connector 正在展现出另一个重要的泛化方向：为整个 Presto 生态中的 AI Workload 提供统一的高阶语义支持。具体而言，Lance Connector 具备了向整个 Federated Engine 提供以下几类 AI-native Capability：

- **向量存储与检索能力**：无论原始数据来自哪个 Connector，都可以通过 Lance 构建和维护统一的向量存储、向量索引以及 ANN 检索能力
- **AI 数据集管理能力**：训练集、验证集、特征工程中间结果以及采样数据，都可以作为版本化的物化资产统一维护
- **多模态数据组织能力**：来自文本、图像、音频以及视频等不同模态的数据，可以被统一管理、索引与检索

例如，在 AI Retrieval 场景中：

- 原始数据可以来自 Hive、Iceberg、PostgreSQL 或对象存储
- Embedding 与 Vector Index 则统一由 Lance 管理
- 向量检索、ANN Search 以及 Retrieval Planning，则由 Presto Federated Engine 统一编排与执行

这些能力的本质在于：Lance Connector 不再只是向 Federated Engine 暴露数据访问接口，而是开始向整个 Presto Federation 提供统一的 AI Retrieval 与 AI Dataset Management 能力。原始数据的管理仍然由各 Source Connector 负责，而 Embedding、Vector Index 以及 AI Dataset State 则由 Lance 统一维护。Presto Engine 则负责将这些能力进一步组合与编排，最终形成跨 Connector 的 AI Workload 高阶语义。后续章节我们会进一步看到，诸如 Distributed Procedure 与 Table Function 等机制，正在使这些能力逐渐成为 Federated Execution Graph 中的一等公民。

---

这三种泛化方向分别作用于查询执行的不同层次：Alluxio 增强的是 IO 基础设施层，Iceberg 拓展的是数据语义管理层，而 Lance 则面向 AI 工作负载层。三者互不重叠、互为补充，共同勾勒出 Federated Engine 能力演进的完整图景。它们共同指向一个明确的趋势：在 Lakehouse 与 AI Data Infra 持续深化的背景下，Connector 作为能力提供者的角色将变得越来越突出，也越来越不可替代。Connector 不再仅仅是"适配"不同数据源的访问协议，而是开始成为整个 Presto 生态中高级执行语义的载体与放大器。从这个意义上讲，Federated Engine 所联邦的对象，也正在从单纯的 Data Source 本身，逐渐扩展到 Data Capability。

---

## 三、Presto 近期实践中的一些演化迹象

上述关于 Capability Federation 的讨论，并不仅仅来源于理论推演。事实上，PrestoDB 近年来的一系列架构演化，也能够为这一思路提供一些有趣的实践参考。

在我看来，以下几个方向的发展，共同体现出一个值得关注的趋势：

- Distributed Procedure
- Table Function
- Connector Optimizer
- Predicate Stitching
- Incremental MV Refresh
- Metadata-aware Rewrite

这些能力共同反映出一种变化：

> Connector 已经不仅仅只是回答"如何读取数据"，而是开始定义"如何向整个 Federated Engine 暴露自身的 specialized capability"。

### 3.1 Distributed Procedure：能力暴露的天然接口

Distributed Procedure 的意义，并不仅仅是"支持分布式 Procedure 调用"。更重要的是，它开始允许 Connector 以一种语义更加简洁、更加自然的方式，向整个 Federated Engine 暴露自身特有能力。

传统 Connector API 更多描述：

> How to access data

而 Distributed Procedure 可以更自然的表达：

> What operations this connector can provide

未来，Connector 或许可以更加自然地通过 Distributed Procedure 暴露自身能力，例如：

| Connector | 可暴露的能力 |
|-----------|-------------|
| Iceberg Connector | MV Refresh、Metadata Optimization、Data Optimization、Snapshot Maintenance |
| Lance Connector | Vector Index Build、Embedding Materialization、ANN Maintenance |
| Alluxio Connector | Cache Warmup、Cache Cleanup、Locality Optimization |

这意味着 Connector 的能力不再隐藏在内部实现中，而是可以被引擎统一调用。在这一视角下，Federated Engine 正逐渐从 Unified Query Engine 演化为 Unified Capability Execution Platform。

### 3.2 Table Function：Specialized Retrieval 进入统一执行计划

Table Function 的价值，也远不止于"SQL 扩展语法"。其真正重要的地方在于：它允许非传统关系操作，以一种自然方式进入统一 Query Plan。

过去，特殊计算通常位于 SQL 引擎之外。例如：

- 向量搜索服务
- 推荐系统
- 特征检索系统

而 Table Function 使这些能力能够进入 Federated Execution Graph，例如：

- Vector Retrieval
- ANN Search
- Embedding Retrieval

未来可以：

- 参与优化
- 参与 Join Planning
- 参与 Predicate Pushdown
- 参与统一调度执行

这意味着，AI-native Retrieval 正在真正成为 Federated Execution Graph 的一等公民，而不再是一个独立于 SQL 引擎之外的"附加组件"。

### 3.3 Connector Optimizer：优化能力的联邦化

传统 Query Engine 中，Optimizer 通常属于 Centralized Engine Intelligence——优化逻辑集中在引擎核心，由它来理解所有数据源的特性。

然而，随着数据格式及数据系统能力不断增强：

- Iceberg Metadata
- Vector Index
- Cache Locality
- AI-native Semantics

Engine Core 已经越来越难完全理解所有底层系统特性。如果 Capability Provider 能够定义自身能力，却无法参与能力选择与执行路径决策，那么 Federated Capability 将无法真正实现最优组合。Connector Optimizer 或许正是这一趋势在 Optimizer 层面的体现。

Connector Optimizer 的核心价值在于：

> 将 Specialized System 的 Optimization Intelligence 引入统一 Optimizer。

也就是说，未来的 Optimizer 可能会越来越像 Federated Optimization Intelligence，而不再只是 Single-engine Optimizer。每个 Connector 都可以贡献自己对底层数据系统最优访问路径的理解，由联邦优化器统一协调与决策。

### 3.4 Materialized View：Capability Federation 的典型实例

Presto 在 Materialized View 能力上的演化，已经具体体现了 Capability Federation 的典型特征。

传统数据库中，Materialized View 通常是存储系统内部能力，其依赖于：

- MV Metadata Management
- Refresh strategy
- Query Rewrite
- Optimization

这些能力往往紧耦合于单一数据源系统，难以跨系统复用。

但在 Presto 的近期演进中，基于 Federated Capability Composition，分工正在发生变化：

| 组件 | 提供的能力 |
|------|-----------|
| Apache Iceberg | MV State Management、MV storage Management、Atomic Publication |
| Source Connectors | Base Table Access、Change Awareness、Incremental Read Semantics |
| Presto | Predicate Stitching、Incremental Refresh Algorithm、CBO-based Rewrite &amp; Selection、Federated Query Planning |

于是，Materialized View 不再属于单一系统，而成为多个组件能力组合后的结果。

#### 查询阶段：

Presto 基于 CBO 自动选择执行路径：

- 全表查询
- MV Rewrite
- Predicate Stitching

并结合实时查询条件动态调整执行计划。

#### 刷新阶段：

Iceberg 作为 Materialized View 的状态管理基础设施，提供：

- Snapshot Metadata
- Metadata Graph
- Atomic Commit
- Snapshot Isolation

Presto 则负责：

- Change Detection（借助 Source Connectors 的增量感知能力）
- Incremental Refresh Planning
- Refresh Execution

两者结合共同实现：

- Incremental Refresh
- Fine-grained Maintenance
- Consistent View Publication

最终形成的是：一种面向整个 Presto Federation 的、泛化的、更细粒度控制的 Materialized View 能力。

### 核心启示

这一案例的真正重要之处在于：

> MV 能力已经不再紧耦合于单一存储系统，而开始成为 Federated Engine + Specialized Metadata System 共同组合出的平台能力。

沿着这一思路来看，Federated Engine 所联邦的对象，似乎已经开始从 Data Source 本身，逐渐扩展到 Data Capability。

---

## 四、一个更加直观的例子：迁移能力，而非迁移数据

为了更直观地理解 Capability Federation 的价值，不妨考虑一个典型的企业数据平台演进场景。

### 4.1 初始状态：能力孤立的异构系统

企业内部通常已经存在大量异构数据系统：

- Hive 数据湖
- PostgreSQL 业务数据库
- Oracle 数仓
- 对象存储中的 Parquet 数据集

这些系统往往长期稳定运行，并承担着各自明确的职责。然而在传统模式下，这些系统彼此之间处于**能力隔离、元数据隔离、查询能力隔离**的状态。每个系统只能提供自身固有的能力，跨系统的能力复用几乎不存在。

如果企业希望进一步获得：

- 分布式缓存加速
- 物化视图维护与自动查询改写
- 向量检索能力
- AI Dataset 管理能力

通常意味着：数据迁移、平台重建以及业务改造。不仅成本高昂，而且建设周期长、业务风险高。

### 4.2 新的模式：能力附着，数据不动

在 Federated Engine + Capability Provider 的架构中，可以呈现出一种不同的数据平台演进方式。**企业无需迁移现有业务数据**，而可以通过逐步引入新的 Capability Provider，便能够让现有数据逐渐"附着"上新的能力。

#### 第一步：引入统一 Federated Engine

首先，引入统一的 Federated Engine，例如 PrestoDB，作为整个企业的数据访问与执行层。此时：

- Hive 仍然是 Hive
- PostgreSQL 仍然是 PostgreSQL
- Oracle 仍然是 Oracle

原有系统的职责、边界以及运行模式均无需改变。变化的仅仅是：

> 企业开始拥有一个统一的 Federated Engine 作为整个企业数据的联邦查询层及数据能力的组合层（Capability Composition Layer）。

#### 第二步：引入 Alluxio —— 获得统一的 IO 加速能力

随后，引入 Alluxio 作为统一的数据缓存与加速层。

Alluxio 提供：

- Distributed Cache
- Data Locality
- Remote IO Acceleration

于是，原本存储在 Hive、对象存储、HDFS 中的数据，即使完全保留在原位置，也开始共享统一的数据缓存与局部性优化能力。原始数据仍然由各 Source Connector 管理，而缓存状态与数据局部性则由 Alluxio 统一维护。

IO Optimization 开始从某个存储系统的内部能力，演化为整个 Federated Engine 的共享能力。

#### 第三步：引入 Iceberg —— 获得统一的数据管理能力

接着，引入 Iceberg 作为统一的数据管理能力提供者。

Iceberg 提供：

- Snapshot Metadata
- Transaction Semantics
- Metadata Graph
- Fine-grained Metadata Indexing

这些能力使得 Presto 开始具备构建跨系统数据管理能力的基础，例如：

- Materialized View State Management
- Incremental Refresh
- Query Rewrite
- Metadata-aware Optimization

此时：

- 原始数据仍然来自 Hive、PostgreSQL、Oracle 或对象存储
- Materialized View 的状态管理则统一由 Iceberg 承担
- 查询改写、谓词缝合、增量刷新以及优化决策，则由 Presto Federated Engine 负责完成

这意味着：Materialized View 不再必须绑定于某个单独的数据系统，而开始呈现出一种由 Federated Engine 与底层数据管理系统共同提供的平台能力。

#### 第四步：引入 Lance —— 获得统一的 AI Data Management 能力

最后，引入 Lance 作为统一的 AI Data Management Layer。

Lance 提供：

- Vector Storage
- Vector Index
- ANN Retrieval Infrastructure
- AI Dataset Management
- Multimodal Asset Management

于是：

- 原始数据仍然保留在 Hive、PostgreSQL 或对象存储中
- Embedding、Vector Index 以及 AI Dataset State 则统一由 Lance 维护
- Retrieval Planning、ANN Execution 以及 Federated Optimization，则由 Presto Engine 统一编排

在这样的能力组合方式下，即使原始数据从未迁移，整个系统仍然有机会获得如下的 AI-native 能力：

- Vector Retrieval
- Semantic Search
- RAG Retrieval
- AI Dataset Lifecycle Management

这意味着：AI Capability 有望成为整个 Federated Engine 的共享能力，而不再专门属于某个独立的 AI 数据平台或 AI 表格式。

### 4.3 核心启示：能力开始变得可迁移

这个例子的核心价值，并不在于引入了多少新的组件，而是企业建设数据平台的方式开始发生变化：

| 传统模式 | Capability Federation |
|----------|----------------------|
| 能力绑定于特定存储系统 | 能力作为可插拔的服务层附着于数据之上 |
| 获取新能力需要迁移数据 | 获取新能力更多依赖新增 Capability Provider |
| 平台建设是"替换式"的 | 平台建设是"增量式"的 |
| 数据与能力强耦合 | 数据与能力逐渐解耦 |
| 数据集中化 | 能力联邦化 |

> 能力附着于数据，而非绑定于某一种数据系统——这正是 Capability Federation 架构在工程实践中的核心体现。

在这种模式下，企业建设数据平台的方式也开始发生变化：平台演进的重点，或许将逐渐从持续建设新的数据系统，转向持续为现有数据系统附着新的能力。

---

## 五、为什么 Capability Federation 可能成为一种重要路线

### 5.1 Unified Platform 与 Capability Federation

在讨论 Capability Federation 时，一个自然的问题是：为什么不直接构建一个统一平台，将所有能力都内建到同一个系统之中？事实上，这也是当前许多主流商业数据平台所采用的路线。典型代表包括 Databricks、Snowflake 以及 BigQuery。

这些系统的核心思路并非联邦化能力组合，而是：将存储、元数据、优化器、执行引擎以及上层能力统一纳入同一个平台之中。

从工程角度来看，这一路线在某些方面具有明显优势：

- 更强的一致性
- 更统一的用户体验
- 更大的全局优化空间
- 更清晰的系统边界

因此，Capability Federation 并不意味着 Unified Platform 的失败。相反，这两种路线实际上是在尝试解决同一个问题：如何让越来越复杂的数据能力，被更加高效地利用。

区别仅在于：

- Unified Platform 更倾向于**吸收能力（Capability Absorption）**
- Capability Federation 更倾向于**组合能力（Capability Composition）**

### 5.2 Specialized Capability Explosion

过去很多年中，数据平台建设几乎始终围绕着同一个目标展开：**Data Unification**。其核心思想是：将数据迁移到统一的平台中，再统一进行治理、分析与计算。

这一思路背后的基本假设是：只有数据集中，能力才能集中。

然而，随着 Lakehouse、AI Workload 以及多模态数据的发展，一个新的变化正在逐渐出现：越来越多的数据能力开始从传统数据库内部独立出来，并演化为专门的数据系统。例如：

- Open Table Format
- Metadata Catalog
- Distributed Cache
- Vector Database
- Feature Store
- AI-native Table Format

与此同时，这些系统之间的差异，也正在从简单的数据存储差异，逐渐演化为语义能力差异。例如：

Iceberg 更关注：

- Snapshot
- Transaction
- Branch
- Metadata Graph

而 Lance 更关注：

- Vector Index
- ANN Retrieval
- Embedding Dataset

两者所关注的问题域已经存在明显差异。这意味着：如果统一平台希望同时支持所有能力，并做到与专业系统同样优秀，其复杂度将急剧增加。

于是，一个新的问题开始变得越来越重要：是否一定要将数据迁移并集中，才能获得新的能力？

在这种背景下，一种新的思路开始逐渐浮现：**Capability Unification**。

其关注的重点，不再仅仅是：

> Where is the data?

而是：

> What capability does the system provide?
>
> How can these capabilities be exposed?
>
> How can these capabilities be composed?

换句话说，系统所追求的目标不再是强制统一存储、统一格式以及统一平台，而是统一能力的暴露方式、统一能力的组合机制，以及统一能力的利用路径。

在这一模式下，数据仍然可以保留在最适合自身 workload 的系统之中，而 Federated Engine 则负责发现、组合并利用这些分散在不同系统中的 specialized capabilities，从而将原本孤立的系统能力，逐渐演化为整个数据平台的共享能力。

### 5.3 两种路线的长期共存

未来很长时间内，这两条路线很可能长期共存。

Unified Platform 路线在以下场景中仍然具有显著优势：

- 强一致性与事务保障要求极高的核心业务场景
- 追求极致用户体验与开箱即用能力的场景
- 技术栈高度统一的组织

Capability Federation 路线则更适合：

- 已经存在大量异构系统的企业环境
- 希望渐进式演进而非推倒重来的场景
- 希望组合不同专业系统最佳能力的复杂场景

因此，Capability Federation 更适合作为一种趋势观察，而非必然结论。未来很长时间内，两条路线大概率会长期共存，并根据不同约束条件，各得其所。

---

## 六、为什么这一趋势对 Presto 尤其重要

Presto 自诞生之初，就以统一访问多个异构数据源作为核心设计目标。围绕这一目标，其形成了以 Connector 为核心扩展点、以 Federation 为中心的系统架构。

而是：

- 多数据源
- 多 Connector
- 多系统协作

因此，当 Connector 开始从 Storage Adapter 演化为 Capability Provider 时，Presto 的 Federation Architecture 也开始具备了承载这一演进方向的基础。

它不需要像许多统一平台那样重新设计系统边界。因为：聚合异构系统，本来就是 Presto 的原始设计目标。

如果未来越来越多的数据系统开始向外暴露 specialized capability，那么 Presto 天然具备成为这些能力统一入口与统一编排层的基础条件。

从这个意义上讲，Presto 的 Federation-first Architecture，或许正在获得新的时代语义。

---

## 结语：从 Query Federation 到 Capability Federation

过去很多年中，Federated Engine 所联邦的对象，始终是 Data Source。因此，Connector 的职责被定义为：

> Access to Data。

然而，随着越来越多的数据系统开始提供丰富的数据语义与 specialized capability，联邦引擎未来所关注的对象，可能已经不再只是数据本身，而是越来越多具备独立能力与语义的表格式及数据系统。

Connector 的价值不再只是：如何读取数据。其正在变得越来越厚，开始逐渐演化为：如何将底层系统擅长的能力释放出来。如果这一趋势持续发展，那么未来 Federated Engine 的核心竞争力，可能越来越取决于：

- 吸收新能力的效率
- 组合新能力的能力
- 利用新能力的深度

当然，Capability Federation 并不意味着所有问题都已经有答案。能力发现、能力编排、能力选择以及统一优化等问题，未来仍然存在大量值得探索的空间。但这些问题更多属于工程实现层面的挑战，而非方向层面的障碍。

如果说传统 Federated Engine 主要围绕 Data Source 构建抽象，通过 Connector 实现对不同数据系统的统一访问，那么 Capability Federation 则是在这一基础之上的进一步演进：Data Source 仍然是核心能力来源之一，但引擎关注的对象不再局限于数据访问本身，而是进一步扩展到数据系统所提供的更丰富能力，包括事务与版本管理、增量计算、元数据智能、查询优化、数据维护、向量检索、语义搜索以及 AI 数据管理能力。

因此，围绕 Capability 的暴露、组合与利用，将很可能成为下一阶段数据基础设施演进的重要主题。

而这，也许正是 Lakehouse 与 AI Data Infra 时代，Federated Engine 的又一次战略机会所在。

---

## 欢迎交流

本文基于作者当前的理解与实践经验整理而成，难免存在疏漏或值得进一步探讨之处。

如果您对文中的观点有不同看法，发现任何问题，或有相关实践经验，欢迎通过 [GitHub Issue](https://github.com/hantangwangd/hantangwangd.github.io/issues/new) 与作者交流讨论。

期待与更多同行围绕数据基础设施相关技术展开交流，分享实践经验，共同学习、共同进步。