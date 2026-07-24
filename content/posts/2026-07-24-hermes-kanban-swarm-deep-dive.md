---
title: "Hermes Agent v0.16 Kanban Swarm 深度解析：AI 驱动的自动化博客流水线"
date: 2026-07-24
tags: ["Hermes", "Kanban", "Swarm", "AI", "多代理协作"]
author: "Woody"
draft: false
---

# Hermes Agent v0.16 Kanban Swarm 深度解析：AI 驱动的自动化博客流水线

> "如果你的 AI 代理团队开会讨论谁先干活，那你就白费力气了。"

你有没有遇到过这种场景：你想让 AI 写完一篇文章→审查代码→修复 bug→发布上线，结果同一个代理在同一个会话里，刚写完研究笔记就开始自我审查，刚审查完又自己批准——全程自导自演，每一步"人工确认"实际上都是自己在确认自己？

这不是 AI 的问题，是架构的问题。当单一代理身兼研究者、写作者、审查者、发布者四重角色时，你得到的不是协作，是精神分裂。

Hermes Agent v0.16 引入的 **Kanban Swarm**，就是为了解决这个核心矛盾：**如何在同一个模型驱动下，让多个专用代理像真实团队一样并行协作，同时保持对最终产出的可控性与可审计性。**

---

## 一、Kanban 架构设计原理：SQLite 扛起的分布式协调

### 为什么是 SQLite？

传统认知中，多代理协作需要 Redis 队列、Kafka 流、或者至少一张 PostgreSQL 表。但 Hermes 的设计者做了一个看似"朴素"的选择：**SQLite WAL 模式 + IMMEDIATE 事务**。

```yaml
# ~/.hermes/config.yaml — Kanban 相关配置
kanban:
  dispatch_in_gateway: true                   # Dispatcher 跑在 gateway 进程中
  dispatch_stale_timeout_seconds: 14400        # 4 小时无心跳超时回收
  failure_limit: 2                             # 连续失败后自动 block
  orchestrator_profile: orchestrator           # 自动分解时使用的 Orchestrator Profile
  auto_decompose: true                         # Orchestrator 任务自动分解为子任务
```

**Why it matters:** SQLite 不是"不够好"——对于单主机场景，它比外部队列更优。没有网络往返、不需要 Docker Compose、不需要运维 Kafka 集群。WAL 模式允许多个进程同时读，IMMEDIATE 事务避免写冲突，单文件备份即迁移。这不是技术上的退步，而是对 YAGNI 原则的极致贯彻——**在需要 Redis 之前，SQLite 就是 Redis**。

### Board 与 Tenant 的两层隔离

Kanban 的数据模型分为两层：

| 概念 | 作用 | 隔离级别 |
|------|------|----------|
| **Board** | 独立 SQLite 文件，完全物理隔离 | 项目级（硬边界） |
| **Tenant** | Board 内的命名空间，用于 workspace_path + memory_key 隔离 | 子项目级（软边界） |

这意味着你可以一个 Board 跑"博客文章流水线"，另一个 Board 跑"代码审查流水线"，互不干扰。Worker 启动时 `HERMES_KANBAN_BOARD` 环境变量直接钉死它属于哪个 Board——一个 Worker 不可能"串场"。

### 任务状态机

每个任务经历的状态变迁是一个**有向无环图**：

```
  triage → todo → ready → running → done
                 ↑          ↕
              blocked    running ← (心 beat 超时 → re-claim)
```

关键设计：`ready` 到 `running` 的转换是**原子性的**——Dispatcher 通过 SQLite 的 `UPDATE ... WHERE status='ready'` 加乐观锁实现，不会出现两个 Worker 抢到同一个任务。

> **补充：** 完整的任务状态还包括 `failed`（连续失败 N 次后自动标记）和 `cancelled`（人工干预取消），两者为终结状态，不会自动恢复。

---

## 二、Swarm 工作流拓扑：DAG 驱动的流水线

### 不只是"串行执行"

Kanban Swarm 的拓扑不是简单的 A→B→C 线性链。它通过 `parents` 字段声明依赖关系，形成一个 DAG（有向无环图）：

```
           ┌─────────┐
           │ 选题定纲 │  ← Orchestrator
           └────┬────┘
                │
     ┌──────────┼──────────┐
     ▼          ▼          ▼
 ┌───────┐ ┌───────┐ ┌───────┐
 │Research│ │Research│ │Research│  ← 并行研究（可选）
 └───┬───┘ └───┬───┘ └───┬───┘
     └─────────┼──────────┘
               ▼
          ┌─────────┐
          │  Writer  │         ← 等待所有 Researcher 完成
          └────┬────┘
               │
     ┌─────────┼─────────┐
     ▼         ▼         ▼
 ┌──────┐ ┌──────┐ ┌──────┐
 │Review│ │Review│ │Review│  ← 三路并行审查
 └──┬───┘ └──┬───┘ └──┬───┘
    └────────┼─────────┘
             ▼
       ┌──────────┐
       │Synthesizer│          ← 审查结果合
       └─────┬────┘
             ▼
       ┌──────────┐
       │Publisher  │          ← 预发布修复 + 部署
       └──────────┘
```

**Why it matters:** 这个拓扑体现了两个关键理念：

1. **扇出（Fan-out）** → Orchestrator 将大目标分解为多个并行子任务，充分利用模型并发能力
2. **扇入（Fan-in）** → 子任务通过 `parents` 字段等待所有前置任务完成，自动触发下一步

### 依赖链如何工作？

当 Researcher 任务标记为 `done` 或 `archived` 时，Writer 并不会立刻变成 `ready`。它只在**所有 parent 任务都完成（done 或 archived）**时才晋升。这个逻辑由 Dispatcher 的 `promote_ready_tasks()` 实现：

```sql
-- 伪代码逻辑
UPDATE tasks SET status = 'ready'
WHERE status = 'todo'
  AND id NOT IN (
    SELECT child_id FROM task_links l
    JOIN tasks p ON l.parent_id = p.id
    WHERE p.status NOT IN ('done', 'archived')
  );
```

没有复杂的 workflow engine，没有状态机 DSL——一条 SQL 查询就完成了任务晋升。

---

## 三、多 Profile 协作机制：专才而非通才

### Profile 即角色

在 Hermes 中，一个 Profile 是一个完全独立的代理实例——独立的配置文件、技能集、会话历史、内存。Kanban Swarm 利用了这一点：每个任务分配给一个专门的 Profile，每个 Profile 只做一件事。

典型的博客流水线 Profile 分配：

```bash
# 创建专用 Profile
hermes profile create orchestrator --clone default
hermes profile create researcher   --clone default
hermes profile create writer       --clone default
hermes profile create reviewer     --clone default
hermes profile create publisher    --clone default

# 每个 Profile 只保留需要的 toolset
hermes -p researcher tools disable file    # Researcher 不需要写文件
hermes -p reviewer tools disable web       # Reviewer 不需要上网搜索
```

每个 Profile 可以有自己专用的系统提示词，通过 `SOUL.md`（全局身份文件，Profile 共用）或技能（Skills）注入。以 Researcher 为例：

```markdown
# researcher/.hermes.md  ← 项目上下文文件，非 Profile 自动加载；需要通过 Profile 的 skills 或 system prompt 配置注入
你是一名技术研究员。你的任务：
- 深度搜索主题的权威资料
- 提炼核心概念、代码示例、常见陷阱
- 输出结构化 Markdown 笔记
- 不要撰写最终文章——那是 Writer 的工作
```

**Why it matters:** 每个 Profile 有**边界认知**——Researcher 知道自己不写文章，Reviewer 知道自己不发布。这比在 system prompt 里写"你是一个研究者，但必要时也可以写作"要可靠得多。认知边界越清晰，越不容易出现"越俎代庖"式的幻觉。

### 任务间通信：Shared Blackboard 模式

Profile 之间是独立进程，没有共享内存。任务间的数据传递通过 **附件（attachments）** 和 **任务 body/comment** 实现：

```
Researcher 输出:
  → 附件: research-rust-async-programming.md (19KB)
  → metadata: {sections: 9, covered_concepts: ["Future", "Pin", "async/await", ...]}
  
Writer 输入:
  → kanban_show() 读取 Researcher 的 handoff
  → 读取附件获取完整研究笔记
  → 输出附件: rust-async-best-practices.md (303 行, 8 个代码示例)
```

这就是 **Shared Blackboard 模式**——一个中央数据存储（Kanban 的 SQLite + 文件系统），所有工作者读写同一块黑板，不存在点对点的消息路由。简单、可靠、可审计。

---

## 四、Dispatcher 调度策略：让机器管机器

### 心跳与超时回收

Worker 在长时间运行的任务中必须定期调用 `kanban_heartbeat()`。Dispatcher 的巡检逻辑检查每个 `running` 任务的最后心跳时间：

| 参数 | 默认值 | 作用 |
|------|--------|------|
| claim TTL（硬编码） | 900s (15min) | Worker 占锁过期后重新 claim |
| `dispatch_stale_timeout_seconds` | 14400 | 无心跳超时回收 + 重新 dispatch |
| `failure_limit` | 2 | 连续失败 N 次 → 自动 block 等待人工介入 |

### Worker 生命周期

```
Dispatcher 巡检 →
  发现 ready 任务 →
  原子 claim（UPDATE ... WHERE status='ready' LIMIT 1）→
  启动 hermes 进程（HERMES_KANBAN_TASK 环境变量传入任务 ID）→
  Worker 读取任务 → 执行 → kanban_complete()
  │
  └→ 如果 Worker 崩溃（进程退出未 complete）→
     下次巡检发现 stale 任务 → 重新 dispatch → 新的 Worker 实例
```

**极简的容错模型：** Worker 不需要处理复杂的错误恢复逻辑——它只要完成或失败。Dispatcher 巡检处理所有边界情况：崩溃、超时、死锁。

### 任务的 4 小时天然生存期

默认 `dispatch_stale_timeout_seconds` 为 4 小时不是一个随意值：它假设一个 Worker 如果在 4 小时内没有完成、没有心跳、没有阻塞，就应该被回收。对于多数内容生产流水线（研究→写作→审查→发布），4 小时绰绰有余。需要更长时间的任务可以调整 `max_runtime_seconds` 或提高 `dispatch_stale_timeout_seconds`。

---

## 五、实战验证：Rust 异步编程文章全流程纪实

> 理论说够了，看看真的跑起来是什么样。

### 实验参数

| 项目 | 数据 |
|------|------|
| 主题 | Rust 异步编程最佳实战 |
| 管道拓扑 | Orchestrator → Researcher → Writer → Reviewer×3 → Synthesizer → Publisher |
| 启动时间 | 15:52（Orchestrator 分解） |
| 上线时间 | 16:28 |
| 总耗时 | **~24.5 分钟** |
| 人工介入 | **0 次** |

### 时间线

```
16:07  🔬 Research 完成           (t_4b5f7094)   ← 首任务实际启动 15:52
16:10  ✍️  Writer 完成            (t_1920d0eb)   ← 实际启动 16:07
16:17  🔍 Review ×3 并行启动      (t_f6b3b90b / t_78f5db5c / t_59e5b9a8)  ← 完成 16:19~16:21
16:22  📋 Review 合成完成          (t_cde60549)   ← 实际启动 16:17
16:25  🚀 Publisher 启动           (t_6527b613)
16:28  ✅ Orchestrator 收束        (t_52958c00)   ← Orchestrator 完成
```

> **说明：** 上表中的时间点为任务**完成时间**（除明确标注"启动"的条目外），这是 Dispatcher 事件日志的自然记录方式。从首任务的分解启动（15:52）到 Publisher 完成上线（~16:28），实际端到端总耗时约 **24.5 分钟**。

### 关键发现

**1. 三路并行审查实际有效**

三个 Reviewer 各自聚焦不同维度：
- **Reviewer A** — Future 与 async/await 概念准性
- **Reviewer B** — 代码示例逻辑正确性（发现 1 个编译错误）
- **Reviewer C** — 文章结构与读者友好度（发现 8 项改进项）

三者独立运行，互不干扰，审查合成阶段的 Synthesizer 将它们统一为 14 项问题的分级报告。

**2. 代码修正闭环自动完成**

Reviewer B 发现在 `best_locking` 代码块中缺少 `std::time::Duration` 导入：

```rust
// ❌ 编译错误版本
use tokio::sync::Mutex;
// 缺少: use std::time::Duration;

async fn best_locking() {
    let lock = Mutex::new(42);
    let _guard = lock.lock().await;
    // Duration::from_millis(10) — 这里编译报错！
}
```

Publisher 阶段自动修复该问题，在发布前补全了所有 4 个 issues。

**3. 六阶段 24.5 分钟，零人工干预**

从选题（Orchestrator 分解）到线上发布（GitHub Pages 200 OK），全程由 Kanban Swarm 自动调度、自动执行、自动修复。这验证了一个关键假设：**对于结构化内容生产，AI 代理可以全自动完成从研究到发布的全流程，且产出质量达到可发布标准。**

最终文章地址：[Rust 异步编程最佳实战](https://woody88888.github.io/tech-blog/posts/rust-async-best-practices/)

---

## 六、技术优势与局限分析

### 核心优势

| 优势 | 说明 |
|------|------|
| **无单点瓶颈** | 多个 Reviewer 可以并行审查，不因一个慢任务阻塞整个流水线 |
| **自动容错** | Worker 崩溃后由 Dispatcher 回收重调度，不丢失任务 |
| **可审计** | 每个任务都有独立 run 记录、事件日志、附件存储 |
| **单主机场景下零外部依赖** | 不需要 Redis、Kafka、Docker——一个 SQLite 文件就是一个 Board |
| **Profile 天然隔离** | 错误不会从一个 Profile 扩散到另一个，任务 Body 中的 prompt injection 不会影响下游 Profile |

### 限制与改进方向

1. **无增量重跑**：如果 Publisher 阶段失败，整个管道需要从 Orchestrator 重新分解。目前没有"仅重试失败阶段"的增量机制。

2. **Review 结果合并开销**：三路并行 Review 各自输出独立报告，需要第 4 个 Synthesizer 手工合并。如果能自动合并到同一任务线程中会更高效。

3. **Worker 信息拉取依赖文件系统**：Reviewer 需要从 Writer 的附件中读取文章，但部分 Reviewer 缺少 file-read 工具。更可靠的方案是在任务 Body 中嵌入文章摘要。

4. **多主机场景缺失**：当前 Kanban 基于 SQLite 文件，所有 Worker 必须在同一主机或共享文件系统上运行。对于分布式集群场景，SQLite 的并发模型不再适用。

5. **缺乏人工审批闸门**：Publisher 自动修复并发布了 Reviewer 发现的问题。对于"需要人工确认修改"的保守场景，缺少 R3 门的机制。

---

## 七、从 Kanban Swarm 学到什么

Kanban Swarm 的设计哲学可以总结为：

- **用数据库状态机替代 Workflow Engine** — 一条 SQL 查询抵得上一个 BPMN 图
- **用文件系统做消息队列** — 附件就是消息，文件就是 blackboard
- **用进程隔离做安全边界** — Profile 不是概念，是操作系统的进程
- **让 Dispatcher 做唯一的状态管理者** — Worker 只有读和执行两个职能

这是一种**低技术高可用**的设计思路（low-tech, high-reliability）。在 AI 代理领域，我们太容易被"分布式"、"事件驱动"、"消息队列"这些词汇吸引，而忘记了一个朴素的真理：**如果 SQLite 能解决你的问题，用 SQLite。**

对于想要自行搭建类似流水线的开发者，核心建议是：

1. **从 RDBMS 开始**——SQLite 或 Postgres 够用了
2. **用 profile/workspace 做隔离**——不要把所有角色塞进同一个上下文
3. **DAG 是拓扑，串行是特例**——设计时就考虑并行
4. **心跳是唯一需要的 liveness 检测**——不需要服务发现和注册中心

Kanban Swarm 还远不是 AI 代理协作的终局方案——它缺乏分布式协调、没有 R3 人工门、不支持跨主机调度。但它在"一台机器上用 SQLite 让 5 个 AI 代理像团队一样写一篇文章"这件事上，做到了令人惊讶的优雅。

毕竟，好的架构不是方案有多么豪华，而是方案有多么合适。

---

*本文由 Hermes Agent v0.16 Kanban Swarm 的 Researcher → Writer 流水线协作研究产出的素材撰写，写作过程为手动创作（本文为解释性文章，需要人类叙事控制以确保概念清晰度）。*
