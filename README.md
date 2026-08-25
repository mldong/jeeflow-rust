# jeeflow-rust · Rust 版工作流引擎

[![Rust](https://img.shields.io/badge/Rust-1.97+-orange?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache-2.0-orange)](./LICENSE)

[jeeflow](https://jeeflow-doc.mldong.com) 引擎规范的 **Rust 语言实现**（多语言联邦，
与 Java/Go/Python/Node/PHP 共享同一套流程 JSON 与契约规范）。核心引擎**零第三方依赖**，
纯 Rust stdlib。

统一门面入口 + sqlx MySQL 仓储 + Salvo 演示服务；串行/并行/按比例会签与一票否决
（ONE_VOTE_VETO）语义对齐联邦契约。

---

## 快速开始

```rust
use std::collections::HashMap;
use std::sync::Arc;

use serde_json::json;
use jeeflow_core::{
    context::ServiceContext,
    id_gen::AtomicIdGenerator,
    memory::MemoryRepository,
    model::ProcessDefine,
    spi::{ProcessRepository, UserProvider},
};
use jeeflow_facade::JeeflowFacade;

// 1. 装配上下文：仓储 + 用户 Provider + ID 生成器
let repo = Arc::new(MemoryRepository::new());
let ctx = ServiceContext::new()
    .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
    .with_user_provider(Arc::new(my_user_provider()))
    .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));

// 2. 注册流程定义（LogicFlow JSON，与五语言共享同一套 flows 文件）
let mut define = ProcessDefine {
    id: 0, name: "leave".into(), display_name: "请假审批".into(),
    define_type: "approval".into(), state: 1,
    content: r#"{"nodes":[...],"edges":[...]}"#.as_bytes().to_vec(),
    version: 1,
    create_time: None, create_user: None,
    update_time: None, update_user: None,
};
repo.save_define(&mut define).unwrap();

// 3. 统一门面：一个入口转发全部 action
let facade = JeeflowFacade::new(ctx);

// 发起（startAndExecute：发起并自动完成发起节点）
let start_args: HashMap<String, serde_json::Value> = [
    ("name", json!("leave")), ("operator", json!("user1")), ("days", json!("3")),
].into_iter().collect();
let resp = facade.flow("processDefine/startAndExecute", &start_args).await;

// 查待办 / 办理
let todo_args: HashMap<String, serde_json::Value> = [
    ("operator", json!("user2")), ("page", json!("1")), ("size", json!("20")),
].into_iter().collect();
let todo = facade.flow("processTask/todoList", &todo_args).await;

let exec_args: HashMap<String, serde_json::Value> = [
    ("id", json!(100002)), ("operator", json!("user2")), ("submitType", json!("1")),
].into_iter().collect();
let done = facade.flow("processTask/execute", &exec_args).await;
```

## 安装

crates.io 正式版本（2026-08-25 首发 v1.0.5；打 tag `v*.*.*` 后 CI 按
`jeeflow-core → jeeflow-repository-sqlx → jeeflow-persist → jeeflow-facade` 拓扑序发布）。
按 crates.io 最新版本依赖即可，例如：

```toml
[dependencies]
jeeflow-facade = "1.0.5"      # 统一门面
jeeflow-core = "1.0.5"        # 引擎核心（零第三方依赖）
jeeflow-persist = "1.0.5"     # 持久层
jeeflow-repository-sqlx = "1.0.5"   # MySQL 仓储（sqlx）
```

> 语言指南（快速开始 / 引擎 API / 流程定义 / SPI / persist / salvo 集成 / demo）见 `docs/`，
> 聚合到[文档站语言指南](https://jeeflow-doc.mldong.com/languages/rust/)。

## 目录结构

```
jeeflow-rust/
├── jeeflow-core/            ← 引擎核心（零第三方依赖，对标 jeeflow-java jeeflow-core）
├── jeeflow-persist/         ← 持久层（元数据驱动动态表写入 + 持久后拦截器）
├── jeeflow-repository-sqlx/ ← MySQL 仓储（sqlx，T0 单测 + T1 冒烟）
├── jeeflow-facade/          ← 统一门面（对齐 spec/06）
├── jeeflow-demo-salvo/      ← Salvo 演示服务（:8091，内存仓，非宿主集成）
└── Dockerfile.demo          ← demo 镜像（CI demo-deploy 用）
```

## 节点支持

| 节点 | 状态 |
|------|------|
| start / end | ✅ |
| task（线性审批） | ✅ |
| decision（条件分支） | ✅ |
| fork / join（并行分支） | ✅ |
| subprocess（子流程） | ✅ |
| countersign（并行会签） | ✅ |
| countersign（串行会签） | ✅（完成一个建一个，任意时刻仅 1 个 DOING） |
| countersign（按比例表达式） | ✅（`#nrOfCompletedInstances==N` 等） |
| countersign（ONE_VOTE_VETO 一票否决） | ✅（软拒绝默认，否决需节点显式配置） |
| reject（驳回）/ jump（跳转） | ✅ |

会签契约与联邦其他语言一致：`submitType=20` 默认软拒绝（任务正常完成、
`countersignDisagreeFlag=1` 记录、不阻断）；仅当节点
`countersignCompletionCondition == "ONE_VOTE_VETO"` 时提前流转；
merged（任何路径）后废弃该节点剩余 DOING 任务（状态 99）。

## 测试

```bash
cargo test --workspace                # T0 内存仓（CI 同款，SKIP_MYSQL=1）
SKIP_MYSQL=0 cargo test --workspace --features mysql-smoke   # T1 本地 160 MySQL 冒烟
```

与 Java 版共享同一套流程 JSON 驱动测试（`jeeflow-java/jeeflow-core/src/test/resources/flows/`，
15 个共享 fixture，含会签/一票否决/比例场景）。

## 演示服务

```bash
cargo run -p jeeflow-demo-salvo       # :8091
```

- 启动时从共享 `jeeflow-java/.../flows/` 加载种子流程（id=1..N）
- 演示用户与其他语言 demo / jeeflow-ui 对齐（user1 / leader / manager …）
- `POST /wf/{action}` → `facade.flow(action, body)`（门面 action 全转发）
- `GET /healthz` / `GET /api/stats` / `POST /api/reset`（reset 会重载种子）

联调 jeeflow-ui：`pnpm --filter @jeeflow/demo dev`，分段控件选 **Rust**（代理 `/rust-api` → `:8091`），或打开 `?lang=rust`。

仅演示用（内存仓、无鉴权）；宿主集成走 mldong-salvo 框架。

## License

Copyright © 2025-2026 mldong

Licensed under the Apache License, Version 2.0.
See [LICENSE](./LICENSE) and [NOTICE](./NOTICE).
