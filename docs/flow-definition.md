# 流程定义格式

jeeflow 使用 LogicFlow JSON 作为流程定义格式，与 Java 版完全一致——**同一份流程 JSON 六语言可移植**
（Java / Go / Python / Node / PHP / Rust 共享 `jeeflow-java/jeeflow-core/src/test/resources/flows/` 驱动测试）。

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
| `snaker:custom` | 自定义 | `customClass` |

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
