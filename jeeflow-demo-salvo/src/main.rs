//! jeeflow-demo-salvo: Demo server using Salvo framework.
//! **仅演示，非宿主集成** — 内存仓、无鉴权，用于引擎功能验证与 jeeflow-ui 联调。
//! 宿主集成见 mldong-salvo。
//! Port: 8091
//! Routes:
//!   POST /wf/{**action} → facade.flow("action", body)
//!   GET  /healthz     → health check
//!   GET  /api/stats   → todoCount / instanceCount
//!   POST /api/reset   → reset all data + reload shared flows

use jeeflow_core::context::ServiceContext;
use jeeflow_core::error::JeeflowResult;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::json::JsonValue;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::*;
use jeeflow_core::spi::*;
use jeeflow_facade::JeeflowFacade;

use salvo::prelude::*;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

/// 与 Java/Go/Python/Node/PHP demo + jeeflow-ui 统一的 8 个具名用户。
fn demo_users() -> HashMap<&'static str, (&'static str, &'static str)> {
    [
        ("user1", ("张三", "工程师")),
        ("userA", ("孙倩", "工程师")),
        ("userB", ("周明", "工程师")),
        ("userC", ("吴婷", "工程师")),
        ("leader", ("李四", "组长")),
        ("manager", ("王五", "经理")),
        ("director", ("赵六", "总监")),
        ("boss", ("钱七", "总经理")),
    ]
    .into_iter()
    .collect()
}

fn user_info(user_id: &str) -> UserInfo {
    let users = demo_users();
    let (real_name, post_name) = users
        .get(user_id)
        .copied()
        .unwrap_or(("用户", "工程师"));
    UserInfo {
        user_id: user_id.to_string(),
        real_name: if users.contains_key(user_id) {
            real_name.to_string()
        } else {
            format!("用户{user_id}")
        },
        dept_id: "D01".into(),
        dept_name: "研发部".into(),
        post_id: "P01".into(),
        post_name: post_name.to_string(),
    }
}

struct DemoUserProvider;

impl UserProvider for DemoUserProvider {
    fn get_user(&self, user_id: &str) -> JeeflowResult<Option<UserInfo>> {
        Ok(Some(user_info(user_id)))
    }
}

struct DemoOrgUserProvider;

impl OrgUserProvider for DemoOrgUserProvider {
    fn find_dept_leaders(&self, _dept_id: &str) -> JeeflowResult<Vec<String>> {
        Ok(vec!["leader".into()])
    }

    fn find_dept_main_leaders(&self, _dept_id: &str) -> JeeflowResult<Vec<String>> {
        Ok(vec!["manager".into()])
    }

    fn find_by_role(&self, role_code: &str) -> JeeflowResult<Vec<String>> {
        let map: HashMap<&str, Vec<String>> = [
            ("leader", vec!["leader".into()]),
            ("manager", vec!["manager".into()]),
            ("director", vec!["director".into()]),
            ("boss", vec!["boss".into()]),
        ]
        .into_iter()
        .collect();
        Ok(map.get(role_code).cloned().unwrap_or_default())
    }
}

struct DemoUserSearchProvider;

impl UserSearchProvider for DemoUserSearchProvider {
    fn page(&self, query: &PageQuery) -> JeeflowResult<PageResult<HashMap<String, JsonValue>>> {
        let keywords: Vec<String> = query
            .conditions
            .iter()
            .filter(|(k, v)| k.starts_with("m_") && !v.as_str().unwrap_or("").trim().is_empty())
            .map(|(_, v)| v.as_str().unwrap_or("").trim().to_lowercase())
            .collect();

        let mut all: Vec<HashMap<String, JsonValue>> = Vec::new();
        for uid in demo_users().keys() {
            let info = user_info(uid);
            if !keywords.is_empty() {
                let hay = format!("{}{}", info.user_id, info.real_name).to_lowercase();
                if !keywords.iter().all(|kw| hay.contains(kw)) {
                    continue;
                }
            }
            let mut row = HashMap::new();
            row.insert("userId".into(), JsonValue::Str(info.user_id));
            row.insert("realName".into(), JsonValue::Str(info.real_name));
            row.insert("deptId".into(), JsonValue::Str(info.dept_id));
            row.insert("deptName".into(), JsonValue::Str(info.dept_name));
            row.insert("postId".into(), JsonValue::Str(info.post_id));
            row.insert("postName".into(), JsonValue::Str(info.post_name));
            all.push(row);
        }

        let page_num = query.page_num.max(1);
        let page_size = query.page_size.max(1);
        let total = all.len() as i64;
        let start = ((page_num - 1) * page_size).min(total) as usize;
        let end = (start + page_size as usize).min(all.len());
        Ok(PageResult::new(page_num, page_size, total, all[start..end].to_vec()))
    }

    fn find_by_id(&self, user_id: &str) -> JeeflowResult<Option<HashMap<String, JsonValue>>> {
        let info = user_info(user_id);
        let mut row = HashMap::new();
        row.insert("userId".into(), JsonValue::Str(info.user_id));
        row.insert("realName".into(), JsonValue::Str(info.real_name));
        row.insert("deptId".into(), JsonValue::Str(info.dept_id));
        row.insert("deptName".into(), JsonValue::Str(info.dept_name));
        row.insert("postId".into(), JsonValue::Str(info.post_id));
        row.insert("postName".into(), JsonValue::Str(info.post_name));
        Ok(Some(row))
    }
}

fn flows_dir() -> PathBuf {
    jeeflow_core::flowsdir::dir()
}

/// 加载共享 LogicFlow JSON（id=1..N 文件名排序），与其他语言 demo 对齐。
/// 同时写入 define + design/his，使 listByType 与发起页契约完整。
fn load_seed(repo: &MemoryRepository) {
    let dir = flows_dir();
    let mut files: Vec<_> = match std::fs::read_dir(&dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("json"))
            .collect(),
        Err(e) => {
            eprintln!("[seed] flows dir missing {:?}: {}", dir, e);
            return;
        }
    };
    files.sort();
    for (i, path) in files.iter().enumerate() {
        let content = match std::fs::read(path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[seed] read {:?}: {}", path, e);
                continue;
            }
        };
        let raw: Json = serde_json::from_slice(&content).unwrap_or(Json::Null);
        let name = raw
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| path.file_stem().and_then(|s| s.to_str()).unwrap_or("flow"))
            .to_string();
        let display_name = raw
            .get("displayName")
            .and_then(|v| v.as_str())
            .unwrap_or(&name)
            .to_string();
        let define_type = raw
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("approval")
            .to_string();
        let now = jeeflow_core::model::current_time_str();
        let mut define = ProcessDefine {
            id: (i + 1) as i64,
            name: name.clone(),
            display_name: display_name.clone(),
            define_type: define_type.clone(),
            state: 1,
            content: content.clone(),
            version: 1,
            create_time: Some(now.clone()),
            create_user: Some("system".into()),
            update_time: None,
            update_user: None,
        };
        if let Err(e) = repo.save_define(&mut define) {
            eprintln!("[seed] save define {}: {}", display_name, e);
            continue;
        }
        // design + his（listByType / 设计器回显）
        let mut design = ProcessDesign {
            id: 0,
            name: name.clone(),
            display_name: display_name.clone(),
            design_type: define_type,
            icon: Some("doc".into()),
            is_deployed: 1,
            remark: Some(format!("v{}", define.version)),
            create_time: Some(now.clone()),
            create_user: Some("system".into()),
            update_time: None,
            update_user: None,
        };
        if let Err(e) = repo.save_design(&mut design) {
            eprintln!("[seed] save design {}: {}", display_name, e);
        } else {
            let mut his = ProcessDesignHis {
                id: 0,
                process_design_id: design.id,
                content: content.clone(),
                create_time: Some(now.clone()),
                create_user: Some("system".into()),
            };
            if let Err(e) = repo.save_design_his(&mut his) {
                eprintln!("[seed] save design his {}: {}", display_name, e);
            }
        }
        println!("  loaded: {} {}", define.id, display_name);
    }
    println!("[seed] {} flows from {:?}", files.len(), dir);
}

struct AppState {
    facade: Arc<JeeflowFacade>,
    #[allow(dead_code)]
    repo: Arc<MemoryRepository>,
}

impl AppState {
    fn new() -> Self {
        let repo = Arc::new(MemoryRepository::new());
        load_seed(&repo);
        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
            .with_user_provider(Arc::new(DemoUserProvider))
            .with_org_user_provider(Arc::new(DemoOrgUserProvider))
            .with_user_search_provider(Arc::new(DemoUserSearchProvider))
            .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));

        let facade = Arc::new(JeeflowFacade::new(ctx));
        AppState { facade, repo }
    }
}

static STATE: std::sync::LazyLock<Arc<Mutex<AppState>>> =
    std::sync::LazyLock::new(|| Arc::new(Mutex::new(AppState::new())));

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

    let facade = {
        let state = STATE.lock().unwrap();
        state.facade.clone()
    };
    let result = facade.flow(&action, &args).await;
    res.render(salvo::prelude::Json(result));
}

#[handler]
async fn api_stats(req: &mut Request, res: &mut Response) {
    let operator = req
        .query::<String>("operator")
        .or_else(|| req.query::<String>("userId"))
        .unwrap_or_else(|| "user1".into());
    let state = STATE.lock().unwrap();
    let todo_count = state.facade.repo().count_todo_tasks(&operator).unwrap_or(0);
    let mut q = jeeflow_core::model::PageQuery::new(1, 1);
    q.operator = Some(operator.clone());
    let instance_count = state
        .facade
        .repo()
        .page_instances(&q)
        .map(|p| p.record_count)
        .unwrap_or(0);
    res.render(salvo::prelude::Json(json!({
        "code": 0,
        "msg": "成功",
        "data": {
            "todoCount": todo_count,
            "instanceCount": instance_count
        }
    })));
}

#[handler]
async fn api_reset(res: &mut Response) {
    {
        let mut state = STATE.lock().unwrap();
        *state = AppState::new();
    }
    res.render(salvo::prelude::Json(json!({
        "code": 0,
        "msg": "成功",
        "data": {"reset": true}
    })));
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
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

    // 预热：加载共享 flows 种子
    let _ = &*STATE;

    println!("jeeflow-demo-salvo starting on http://0.0.0.0:8091");
    println!("  POST /wf/<group>/<action>  - workflow facade");
    println!("  GET  /healthz      - health check");
    println!("  GET  /api/stats    - statistics");
    println!("  POST /api/reset    - reset data + reload shared flows");

    server.serve(router).await;
}
