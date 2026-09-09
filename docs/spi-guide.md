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
| `FlowInterceptor` | 流程事件拦截器（FormFieldAssignee 等节点行为扩展） | 内置基础拦截器 |
| `BizDataReader` | 业务数据读取（persist 回查） | 无 |
| `AssignmentHandler` | 参与人处理器（`assignmentHandler` 节点属性路由） | 内置 7 个（Applicant / DeptLeader×main / TaskRoleAssignee 等） |
| `DecisionHandler` | 决策处理器 | 内置 |
| `CandidateHandler` | 候选人处理器 | 内置 |
| `ProcessEventListener` | 流程事件监听（完成 / 拒绝 / 废弃） | 无 |

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
