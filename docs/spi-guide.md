# SPI 扩展指南

引擎核心零第三方依赖——仓储、用户、JSON、表达式、事务全部走 SPI trait（`jeeflow_core::spi`），
经 `ServiceContext` 装配（builder 风格 `with_*` 方法，全部 `Arc<dyn Trait>` 注入）。

## SPI 清单

| trait | 职责 | 内置实现 |
|-------|------|---------|
| `ProcessRepository` | 流程定义 / 实例 / 任务 8 表 CRUD | `MemoryRepository`（core）/ `SqlxRepository`（repository-sqlx） |
| `ProcessExtRepository` | 扩展表（抄送 / 审批记录 / 委托） | 同上 |
| `UserProvider` | 操作人信息（`u_userId` / `u_realName` / `u_dept*` / `u_post*` 注入源） | 无（业务方实现） |
| `OrgUserProvider` | 组织取人（部门 / 岗位 / 角色成员） | 无 |
| `UserSearchProvider` | 用户搜索（候选 / 转办 / 加签下拉） | 无 |
| `JsonProvider` | JSON 解析/序列化 | 内置 `serde_json` |
| `ExpressionEvaluator` | 条件表达式求值（decision 节点 / 边） | 内置求值器 |
| `TransactionTemplate` | 事务边界 | 无（仓储自带事务时可缺省） |
| `IdGenerator` | 雪花 ID 生成 | `DefaultIdGenerator` / `AtomicIdGenerator`（测试） |
| `ActionPermissionProvider` | action 级权限校验（mldong 框架权限码映射点） | 无 |
| `FlowInterceptor` | 流程节点拦截器（**前置 / 后置两条独立通道**，见下节） | 后置：persist 写库件（`jeeflow-persist`）；前置：无内置件，宿主自挂 |
| `BizDataReader` | 业务数据读取（persist 回查） | 无 |
| `AssignmentHandler` | 参与人处理器（`assignmentHandler` 节点属性路由） | 内置 7 个（Applicant / DeptLeader×main / TaskRoleAssignee 等） |
| `DecisionHandler` | 决策处理器 | 内置 |
| `CandidateHandler` | 候选人处理器 | 内置 |
| `ProcessEventListener` | 流程事件监听（完成 / 拒绝 / 废弃） | 无 |

## 拦截器：前置 / 后置两条通道（1.0.42 起）

`FlowInterceptor` 只有一个 `intercept(&mut Execution)`（与 Java / PHP / C# 同款，**没有** `preHandle`/`postHandle`
两个方法），所以前置与后置是**两条独立的注册数组**，各挂各的：

| 通道 | 注册方法 | 触发位 |
|------|----------|--------|
| 后置 | `ctx.register_interceptor(ic)` | Decision / Fork / Join / End 节点执行体**之后**，外加被完成任务自身（`processTask/execute` 办结腿） |
| 前置 | `ctx.register_pre_interceptor(ic)` | Decision / Fork / Join / End 节点执行体**之前** |

两档共用一份数组就等于同一次 `intercept()` 被调两遍 ⇒ persist 的写库腿会在节点执行前后各落一次，
所以写库件只许走 `register_interceptor`（后置）。两条数组都按 `order()` 升序派发，默认皆空 ⇒ 不注册就零行为变化。
拦截器抛错**不做隔离**（规范 12 §12.2 副作用面）：一支报错即中断整次动作。

> 历史：`1.0.41` 及以前 `fire_pre_interceptors` 是空实现、却有 4 个调用点 ⇒ 前置语义在本栈结构性不存在，
> 注册进 ctx 的拦截器只在后置位生效（八语言栈里其余七家均为真派发）。`1.0.42` 补齐，形状照 MoonBit 侧
> `Ctx::register_pre_interceptor`（issues/116 批次 D 的判例）。

## 最小装配示例

```rust
use std::sync::Arc;
use jeeflow_core::context::ServiceContext;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::spi::*;

let repo = Arc::new(MemoryRepository::new());
let ctx = ServiceContext::new()
    .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
    .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
    .with_user_provider(Arc::new(MyUserProvider))
    .with_org_user_provider(Arc::new(MyOrgUserProvider))
    .with_user_search_provider(Arc::new(MyUserSearchProvider))
    .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));
```

## 实现 UserProvider

```rust
impl UserProvider for MyUserProvider {
    fn user_info(&self, user_name: &str) -> Option<UserInfo> {
        // 返回操作人信息：id / real_name / dept_id / dept_name / post_id / post_name
        // 引擎在 execute_* 时注入 u_* 流程变量
        self.users.get(user_name).cloned()
    }
}
```

## 组织取人（AssignmentHandler）

节点属性 `"assignmentHandler": "ApplicantDeptLeader"` 路由到对应 handler。
内置 7 个（1.0.5 全实装，空结果跳过节点——对齐 Java 语义）：

- `Applicant` 发起人
- `ApplicantDeptLeader` / `ApplicantDeptMainLeader` 发起人部门（主）经理
- `TaskRoleAssignee` 任务角色
- `FormFieldAssignee` 表单字段取人（`f_` 前缀优先匹配，裸名回落，`_NN` 后缀去后缀——对齐 Go/Java）
- 抄送 `f_ccActors` 数组/字符串双形态解析（`parse_cc_actors`，对齐 Go facade.go:203）

自定义 handler 实现 `AssignmentHandler` trait 后注册进 `HandlerRegistry`
（mldong 框架集成侧扫描注册；引擎侧 `ServiceContext` 注入）。

## 与其余五语言的 SPI 对照

| 能力 | Java | Go | Python | Node | PHP | Rust |
|------|------|----|--------|------|-----|------|
| 仓储 | Spring Bean | interface | Protocol | class | interface | trait（`Arc<dyn>`） |
| 注册 | `@Bean` | `NewEngine(opts)` | `Engine(...)` | `new Engine({...})` | `new Engine([...])` | `ServiceContext::with_*` |

trait 方法签名与五语言语义一一对应（命名 snake_case）。跨语言行为差异一律以
`jeeflow-doc/docs/spec/` 规范为准。
