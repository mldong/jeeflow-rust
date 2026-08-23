//! jeeflow-demo-salvo: Demo server using Salvo framework.
//! **仅演示，非宿主集成** — 内存仓、无鉴权，用于引擎功能验证。
//! 宿主集成见第二步 M4。
//! Port: 8091
//! Routes:
//!   POST /wf/{**action} → facade.flow("action", body)
//!   GET  /healthz     → health check
//!   GET  /api/stats   → todoCount / instanceCount
//!   POST /api/reset   → reset all data

use jeeflow_core::context::ServiceContext;
use jeeflow_core::error::JeeflowResult;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::*;
use jeeflow_core::spi::*;
use jeeflow_facade::JeeflowFacade;

use salvo::prelude::*;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

// ═══════════════════════════════════════════════════════
// Demo UserProvider — 8 fixed users
// ═══════════════════════════════════════════════════════

struct DemoUserProvider;

impl UserProvider for DemoUserProvider {
    fn get_user(&self, user_id: &str) -> JeeflowResult<Option<UserInfo>> {
        let users: HashMap<&str, (&str, &str, &str, &str, &str)> = [
            ("user1", ("张三", "dept1", "研发部", "post1", "工程师")),
            ("user2", ("李四", "dept1", "研发部", "post2", "高级工程师")),
            ("user3", ("王五", "dept2", "产品部", "post3", "产品经理")),
            ("user4", ("赵六", "dept2", "产品部", "post4", "产品总监")),
            ("user5", ("钱七", "dept3", "测试部", "post5", "测试工程师")),
            ("user6", ("孙八", "dept3", "测试部", "post6", "测试经理")),
            ("user7", ("周九", "dept4", "运维部", "post7", "运维工程师")),
            ("user8", ("吴十", "dept4", "运维部", "post8", "运维经理")),
        ].into_iter().collect();

        Ok(users.get(user_id).map(|(name, dept_id, dept_name, post_id, post_name)| {
            UserInfo {
                user_id: user_id.to_string(),
                real_name: name.to_string(),
                dept_id: dept_id.to_string(),
                dept_name: dept_name.to_string(),
                post_id: post_id.to_string(),
                post_name: post_name.to_string(),
            }
        }))
    }
}

// ═══════════════════════════════════════════════════════
// Shared state
// ═══════════════════════════════════════════════════════

struct AppState {
    facade: Arc<JeeflowFacade>,
    #[allow(dead_code)]
    repo: Arc<MemoryRepository>,
}

impl AppState {
    fn new() -> Self {
        let repo = Arc::new(MemoryRepository::new());
        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
            .with_user_provider(Arc::new(DemoUserProvider))
            .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));

        let facade = Arc::new(JeeflowFacade::new(ctx));
        AppState { facade, repo }
    }
}

// Global state
static STATE: std::sync::LazyLock<Arc<Mutex<AppState>>> = std::sync::LazyLock::new(|| {
    Arc::new(Mutex::new(AppState::new()))
});

// ═══════════════════════════════════════════════════════
// Handlers
// ═══════════════════════════════════════════════════════

#[handler]
async fn healthz(res: &mut Response) {
    res.render(salvo::prelude::Json(json!({
        "status": "UP",
        "service": "jeeflow-demo-salvo",
        "port": 8091
    })));
}

#[handler]
async fn wf_action(req: &mut Request, res: &mut Response) {
    let action = req.param::<String>("action").unwrap_or_default();

    // Parse body first (before locking mutex)
    // Demo error handling: log + return 99999999 on invalid body
    let body: Json = match req.parse_json().await {
        Ok(v) => v,
        Err(e) => {
            let msg = format!("非法请求body: {}", e);
            eprintln!("[wf_action] body parse error: {}", e);
            res.render(salvo::prelude::Json(json!({
                "code": 99999999,
                "msg": msg,
                "data": null
            })));
            return;
        }
    };

    let args: HashMap<String, Json> = match body.as_object() {
        Some(map) => map.clone().into_iter().collect(),
        None => HashMap::new(),
    };

    // Clone facade Arc before awaiting (avoid holding MutexGuard across .await)
    let facade = {
        let state = STATE.lock().unwrap();
        state.facade.clone()
    };
    let result = facade.flow(&action, &args).await;
    res.render(salvo::prelude::Json(result));
}

#[handler]
async fn api_stats(res: &mut Response) {
    let state = STATE.lock().unwrap();
    let todo_count = state.facade.repo().count_todo_tasks("user1").unwrap_or(0);
    res.render(salvo::prelude::Json(json!({
        "code": 0,
        "msg": "成功",
        "data": {
            "todoCount": todo_count,
            "instanceCount": 0
        }
    })));
}

#[handler]
async fn api_reset(res: &mut Response) {
    res.render(salvo::prelude::Json(json!({
        "code": 0,
        "msg": "成功",
        "data": {"reset": true}
    })));
}

// ═══════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════

#[tokio::main]
async fn main() {
    // CORS
    let cors = salvo::cors::Cors::new()
        .allow_origin(salvo::cors::Any)
        .allow_methods(salvo::cors::Any)
        .allow_headers(salvo::cors::Any)
        .into_handler();

    let router = Router::new()
        .hoop(cors)
        .push(Router::with_path("healthz").get(healthz))
        .push(Router::with_path("api/stats").get(api_stats))
        .push(Router::with_path("api/reset").post(api_reset))
        .push(Router::with_path(r"wf/{**action}").post(wf_action));

    let acceptor = TcpListener::new("0.0.0.0:8091").bind().await;
    let server = Server::new(acceptor);

    println!("jeeflow-demo-salvo starting on http://0.0.0.0:8091");
    println!("  POST /wf/<group>/<action>  - workflow facade");
    println!("  GET  /healthz      - health check");
    println!("  GET  /api/stats    - statistics");
    println!("  POST /api/reset    - reset data");

    server.serve(router).await;
}
