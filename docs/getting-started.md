# 快速开始

## 安装

crates.io 正式版本（1.0.5 起）。核心引擎零第三方依赖，按需引入仓储/门面：

```toml
[dependencies]
jeeflow-core = "1.0.5"                 # 引擎核心（零第三方依赖）
jeeflow-repository-sqlx = "1.0.5"      # MySQL 仓储 + schema（sqlx）
jeeflow-persist = "1.0.5"              # 业务数据动态入库
jeeflow-facade = "1.0.5"               # 42-action 统一门面（对齐 mldong 框架接口）
```

`Cargo.lock` 锁死具体版本；引擎升版改 `Cargo.toml` 版本号 + `cargo update` 即可。

## 5 分钟上手

```rust
use std::collections::HashMap;
use std::sync::Arc;

use jeeflow_core::context::ServiceContext;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::*;
use jeeflow_core::spi::*;
use jeeflow_facade::JeeflowFacade;
use serde_json::{json, Value as Json};

#[tokio::main]
async fn main() {
    // 1. 内存仓储 + 具名用户（测试/演示用）
    let repo = Arc::new(MemoryRepository::new());

    // 2. 装配引擎上下文（仓储 + 用户 SPI）
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_user_provider(Arc::new(DemoUserProvider));

    // 3. 门面（42 action 统一入口）
    let facade = JeeflowFacade::new(ctx);

    // 4. 发起流程：action = "startProcess"（与 Java/Go/Python/Node/PHP 同款 action 名）
    let mut args: HashMap<String, Json> = HashMap::new();
    args.insert("defineId".into(), json!("1"));
    args.insert("operator".into(), json!("张三"));
    let resp = facade.flow("startProcess", &args).await;
    println!("发起: {}", serde_json::to_string(&resp).unwrap());

    // 5. 查待办
    let mut todo: HashMap<String, Json> = HashMap::new();
    todo.insert("operator".into(), json!("张三"));
    let resp = facade.flow("todoList", &todo).await;
    println!("待办: {}", serde_json::to_string(&resp).unwrap());
}
```

> 引擎核心（`JeeflowEngineImpl`）提供 `start_process` / `execute_task` / `jump_*` 等细粒度方法；
> 业务系统**对接 mldong 框架时走 `JeeflowFacade::flow(action, args)`**——42 个 action 与
> Java/Go/Python/Node/PHP 门面一一对应，前端无需改代码。

## 运行演示站

```bash
git clone https://github.com/mldong/jeeflow-rust
cd jeeflow-rust
cargo run -p jeeflow-demo-salvo
# 打开 http://localhost:8091
```

从本仓 `flows/` 副本加载示例流程（简单 / 多级 / 决策 / 会签 / 驳回 / 混合，15 个；唯一编辑源在 `jeeflow-java` 仓，维护者机器上执行时自动镜像同步）。
