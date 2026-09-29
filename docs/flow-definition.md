# 流程定义格式

jeeflow 使用 LogicFlow JSON 作为流程定义格式，与 Java 版完全一致——**同一份流程 JSON 六语言可移植**
（Java / Go / Python / Node / PHP / Rust 各仓带 `flows/` 副本驱动测试，唯一编辑源在 `jeeflow-java` 仓，执行时由 resolver 精确镜像）。

## 顶层结构

```json
{
  "name": "leave",
  "displayName": "请假审批",
  "type": "approval",
  "nodes": [...],
  "edges": [...]
}
```

## 节点类型

| type | 说明 | 必填属性 |
|------|------|----------|
| `snaker:start` | 开始 | - |
| `snaker:end` | 结束 | - |
| `snaker:task` | 任务 | `assignee` |
| `snaker:decision` | 条件分支 | `expr`（节点级） |
| `snaker:fork` | 并行分支 | - |
| `snaker:join` | 并行合并 | - |
| `snaker:custom` | 自定义（记录类） | `clazz` |

`snaker:custom` 是**记录类**节点（规范 `spec/02` §6.1／§6.2）：引擎执行 `clazz` 处理器 →
落一条 `task_state=20` 的历史行（**真落库**）→ 令牌沿出边继续流转；它不解析参与者、不产生待办，
也不 fire `PROCESS_TASK_START`。Rust 没有反射，`clazz` 走**按名注册**：

```rust
ctx.register_custom_handler("com.mldong.jeeflow.test.TestCustomHandler",
                            Arc::new(MyCustomHandler));  // MyCustomHandler: impl CustomNodeHandler
```

未配置 `clazz` 或注册表里查不到该名字 ⇒ 各记一条可诊断日志后照常落历史行、继续流转（不打断建单）；
处理器**自身**返回 `Err`/panic 属业务错误，照旧外抛。处理器返回 `Some(value)` 时，值按节点
`val` 键写进流程变量，`val` 缺省用 `custom_return_val`。
其余 custom 属性：`methodName` / `args` / `val`（与规范 §6 同名字，`methodName`/`args` 由处理器自读）。

## 任务节点属性

```json
{
  "id": "t1",
  "type": "snaker:task",
  "properties": {
    "assignee": "leader",
    "form": "leave-form",
    "taskType": 0,
    "performType": 0,
    "countersignType": "PARALLEL"
  },
  "text": { "value": "组长审批" }
}
```

参与者优先级：`assignee` → `assignmentHandler`。`candidateUsers` 仅供前端设计器展示，不生成 actor 记录。

> ⚠️ **本栈现状与上一句、与规范 `spec/02` §4 分叉**：`resolve_assignee` 的 Priority 4 现在会把
> `candidateUsers` **折进参与者集合**（u1,u2 直接成为 actor）。规范与本文都写"不生成 actor"，
> 改动面涉及所有把 `candidateUsers` 当"预分配处理人"用的存量流程 ⇒ 另批裁定。
> 现状由 `engine.rs::custom_node_tests::test_i142_candidate_users_current_shape_folds_into_actors_pending_ruling`
> 按**实得形状**钉住（将来谁落实规范，这一格会红并提醒改判），别照本文那句以为已经生效。
> 对照：`candidateGroups` 确实不折进参与者，那类节点走下面这条零参与者建单。

> **零参与者的任务节点也会建单**（`spec/02` §6.1 表第一行／§6.2 第 3 条，issues/142 A 批）：
> 解析不出参与者时引擎建**一行 `actor_ids` 为空的 DOING 待办**并停在这里，
> 不再像旧形状那样"一行不建、令牌沿出边跑掉"（那等于库里查不到实例到过哪个节点）。
> 这一行谁也办不动是**设计如此**——它的价值是"实例停在哪"可查，且仍能被
> `transfer`／`nextNodeOperator` 救活；引擎**不会**兜底把它挂给当前操作人（§6.1 硬结论 1）。
> 会签（`performType=1`）按 java 同形逐成员建行，名册为空时天然 0 行，不落在这一档。

## 条件分支

```json
{
  "nodes": [
    {"id":"decision","type":"snaker:decision","properties":{"expr":"amount > 1000"},"text":{"value":"金额>1000?"}},
    {"id":"manager","type":"snaker:task","properties":{"assignee":"manager"},"text":{"value":"经理审批"}},
    {"id":"director","type":"snaker:task","properties":{"assignee":"director"},"text":{"value":"总监审批"}}
  ],
  "edges": [
    {"id":"e3","sourceNodeId":"decision","targetNodeId":"manager",
     "properties":{"expr":"amount > 1000"},"text":{"value":"金额>1000"}},
    {"id":"e4","sourceNodeId":"decision","targetNodeId":"director",
     "properties":{"expr":"amount <= 1000"},"text":{"value":"金额≤1000"}}
  ]
}
```

边的 `text.value` 用于钉钉模式分支标签展示，`properties.expr` 用于引擎求值
（默认内置求值器；可经 `ServiceContext::with_expression_evaluator` 替换）。

## Rust 代码加载

流程 JSON 直接存 `ProcessDefine.content`（`Vec<u8>`），引擎 `start_process_instance` 时解析：

```rust
use jeeflow_core::model::ProcessDefine;
use jeeflow_core::spi::ProcessRepository;

let def = ProcessDefine {
    name: "leave".into(),
    display_name: "请假审批".into(),
    content: flow_json_bytes.clone(),   // LogicFlow JSON 原文
    ..Default::default()
};
repo.add_define(def);
```

`jeeflow-core::parser` 提供 JSON → `ProcessModel`（节点/边/条件）的解析；解析失败的负向断言
（未知节点类型 / 断边 / 环路）见 `jeeflow-core` 单测与合规测试。
