# 引擎 API

## 两层入口

| 层 | crate | 入口 | 适用 |
|----|-------|------|------|
| 门面 | `jeeflow-facade` | `JeeflowFacade::flow(action, args) -> Json` | 业务系统 / mldong 框架对接（42 action，与其余五语言一致） |
| 引擎 | `jeeflow-core` | `JeeflowEngine` trait（`JeeflowEngineImpl` 实现） | 需要细粒度控制或自定义门面时 |

## JeeflowEngine trait

```rust
pub trait JeeflowEngine: Send + Sync {
    fn start_process_instance(&self, define_id: i64, operator: &str, args: &FlowData)
        -> JeeflowResult<ProcessInstance>;
    fn start_process_instance_with_parent(&self, define_id: i64, operator: &str, args: &FlowData,
                                          parent_id: i64, parent_node_name: &str)
        -> JeeflowResult<ProcessInstance>;
    fn execute_process_task(&self, task_id: i64, operator: &str, args: &FlowData)
        -> JeeflowResult<Vec<ProcessTask>>;
    fn execute_and_jump_task(&self, task_id: i64, operator: &str, args: &FlowData,
                             target_task_name: Option<&str>)
        -> JeeflowResult<Vec<ProcessTask>>;
    fn execute_and_jump_to_end(&self, task_id: i64, operator: &str, args: &FlowData)
        -> JeeflowResult<Vec<ProcessTask>>;
    fn execute_and_jump_to_first_task_node(&self, task_id: i64, operator: &str, args: &FlowData)
        -> JeeflowResult<Vec<ProcessTask>>;
}
```

`JeeflowEngineImpl` 同步方法外另有 `*_async` 版本（`start_async` / `execute_task_async` /
`execute_and_jump_async` / `execute_and_jump_to_end_async` / `execute_and_jump_to_first_async`），
tokio 环境推荐用 async 版本避免 `block_in_place` 线程穿透开销。

## 核心操作

### start_process_instance

启动流程：加载定义 → 解析 LogicFlow JSON → 创建实例 → 执行 start 节点 → 创建第一批任务。

```rust
let mut args = FlowData::new();
args.insert("amount", json!(5000));
args.insert("BUSINESS_NO", json!("BIZ-001"));
let inst = engine.start_process_instance(define_id, "张三", &args)?;
```

### execute_process_task

完成任务并驱动流程前进：校验权限 → 完成任务 → 执行当前节点输出边 → 创建下一批任务 / 结束。

```rust
let mut args = FlowData::new();
args.insert("submitType", json!(1));   // 1=同意
args.insert("comment", json!("同意报销"));
let _next = engine.execute_process_task(task_id, "张三", &args)?;
```

**resume mergeVars 语义**（1.0.5 对齐五语言）：`execute_task` / `jump` / `jump_to_end` 三路径
以实例 `variables` 为底、本次提交覆盖——发起时写入的 `f_*` 表单字段在后续 resume 中保持可达
（对齐 Go `engine_impl.go:261`）。

### execute_and_jump_to_end

驳回——完成任务后将实例标记为已拒绝（状态 45），其他进行中任务 → 99（已废弃）。

### execute_and_jump_task

跳转到指定任务节点（回退 / 跳过）：当前任务完成 → 其他任务废弃 → 重建 `target_task_name` 的任务。

## 门面契约（jeeflow-facade）

与五语言统一的出口约束（CI 测试覆盖）：

- **ID 字符串化**（C1/C14）：所有 id 出口为 string（雪花 64 位防 JS 精度丢失）
- **camelCase 输出**（C4）：VO 顶层 key camelCase；`ext` / `jsonObject` / `variable` 等自由 map 保留原键（`u_realName` / `PERMISSION_*`）
- **时间格式**（C5）：`yyyy-MM-dd HH:mm:ss`
- **分页 5 键信封**（C6）：`pageNum` / `pageSize` / `recordCount` / `totalPage` / `rows`
- **错误码**（C7）：统一 `99999999` + msg
- **args 透传**（1.0.5）：`args_to_flow_data` 数组/对象原样透传（旧版兜底 `to_string()` 会把多选
  ApiSelect 的 JSON 数组字符串化成 `"[...]"`，抄送人变字面量—— 根因）

## 流程变量

引擎自动注入以下变量（五语言同款）：

| 变量 | 说明 |
|------|------|
| `u_userId` | 操作人 ID |
| `u_realName` | 操作人姓名 |
| `u_deptId` / `u_deptName` | 部门 ID / 名称 |
| `u_postId` / `u_postName` | 岗位 ID / 名称 |
| `submitType` | 0=发起 1=同意 2=拒绝 3=退回上一步 4=跳转 5=重新提交 6=退回发起人 20=会签拒绝 |
| `BUSINESS_NO` | 业务流水号 |

## 状态码

| 常量 | 值 | 含义 |
|------|-----|------|
| `InstanceStateDoing` | 10 | 进行中 |
| `InstanceStateDone` | 20 | 已完成 |
| `InstanceStateReject` | 45 | 已拒绝 |
| `TaskStateDoing` | 10 | 进行中 |
| `TaskStateDone` | 20 | 已完成 |
| `TaskStateAbandoned` | 99 | 已废弃 |

## ID 生成（雪花）

`jeeflow-core::id_gen`：`AtomicIdGenerator`（测试/演示，固定基数）/ `DefaultIdGenerator`（雪花）。
**EPOCH = 1288834974657（2010-11-04，1.0.5 起五语言联邦统一 ID 空间，对齐 Go/Java/Python/Node）**——
共享库 `ORDER BY id DESC` 场景下新数据恒排存量种子前。
