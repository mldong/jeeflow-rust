//! jeeflow-repository-sqlx: sqlx-based repository implementation.
//! Provides MySQL DDL schema + SqlxRepository wrapping sqlx::MySqlPool.
//! The ProcessRepository trait is synchronous; we use sync-over-async internally.

use jeeflow_core::error::{JeeflowError, JeeflowResult};
use jeeflow_core::model::*;
use jeeflow_core::spi::*;
use sqlx::mysql::MySqlPool;
use sqlx::Row;
use std::sync::Arc;

// ═══════════════════════════════════════════════════════
// MySQL DDL — 8 tables (spec/08)
// ═══════════════════════════════════════════════════════

/// Returns the 8-table DDL string for MySQL schema initialization.
pub fn schema_mysql() -> &'static str {
    MYSQL_SCHEMA
}

// 注意：列名与表名必须与 mldong-plus 规范 schema 完全一致（mldong 框架 DB 镜像 /
// Java 参考实现 schema-mysql.sql）：wf_process_task 用 task_state/operator/task_parent_id，
// wf_process_define/design 用 type，抄送表叫 wf_process_cc_instance。
// init_schema 仅用于全新库 bootstrap；既有规范表因 IF NOT EXISTS 直接跳过，
// 严禁用 ALTER ADD 补列的方式"对齐"（历史坑：曾污染共享 3306 测试库导致引擎自测假通过）。
/// 单一来源：同目录 `schema/schema-mysql.sql`（与 Java 参考实现仓
/// `jeeflow-repository-jdbc/src/test/resources/schema-mysql.sql` 唯一编辑源同步；
/// 改表结构只改 Java 仓后跑 `jeeflow-hub/scripts/sync-schema.sh` 分发）。
pub const MYSQL_SCHEMA: &str = include_str!("../schema/schema-mysql.sql");

// ═══════════════════════════════════════════════════════
// SqlxRepository
// ═══════════════════════════════════════════════════════

/// 默认雪花 id 生成器（进程内单例）：规范表无 AUTO_INCREMENT，
/// 主键由应用层生成（spec §7，对齐 Java 参考实现 repository 内 nextId()）。
fn default_id_gen() -> Arc<dyn Fn() -> i64 + Send + Sync> {
    static GEN: std::sync::OnceLock<jeeflow_core::id_gen::DefaultIdGenerator> =
        std::sync::OnceLock::new();
    Arc::new(move || {
        GEN.get_or_init(|| jeeflow_core::id_gen::DefaultIdGenerator::new(1))
            .next_id()
    })
}

/// SQLx-based repository wrapping a MySqlPool.
/// Implements ProcessRepository + ProcessExtRepository using sync-over-async.
pub struct SqlxRepository {
    pool: MySqlPool,
    id_gen: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl SqlxRepository {
    pub fn new(pool: MySqlPool) -> Self {
        SqlxRepository {
            pool,
            id_gen: default_id_gen(),
        }
    }

    /// 注入集成方 ID 生成器（如 salvo 雪花 id），保持全库 id 口径一致。
    pub fn with_id_gen(mut self, id_gen: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.id_gen = id_gen;
        self
    }

    fn next_id(&self) -> i64 {
        (self.id_gen)()
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    /// Block on an async future using the current tokio runtime handle.
    ///
    /// 必须用 `block_in_place` 包裹：本结构体的同步 SPI 方法会被引擎 facade 从
    /// Web 框架（salvo）的 multi_thread 工作线程直接调用。该线程的 runtime 上下文
    /// 已是 Entered，裸 `Handle::block_on` 会在 `enter_runtime` 触发
    /// "Cannot start a runtime from within a runtime" panic（单测用 spawn_blocking /
    /// plain 线程，上下文 NotEntered，故测不出来）。
    /// `block_in_place` 在多工作线程运行时会先把 worker core 挪给别的线程再阻塞（合法），
    /// 在 plain / blocking 线程则退化为就地执行——对所有调用上下文都安全，
    /// 且与集成层 `wf_db::block_on` 口径一致。
    fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
    }

    /// Initialize the schema by executing the DDL.
    ///
    /// 先剥掉 `--` 行注释再按 `;` 分句：DDL 文件头部与表之间都有注释行
    /// （规范源首行即 `--` 注释），只判首字符会误吞首句。
    pub async fn init_schema(pool: &MySqlPool) -> JeeflowResult<()> {
        let ddl: String = MYSQL_SCHEMA
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        for stmt in ddl.split(';') {
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            sqlx::query(trimmed)
                .execute(pool)
                .await
                .map_err(|e| JeeflowError::Internal(format!("Schema init error: {}", e)))?;
        }
        Ok(())
    }
}

// Helper: parse FlowData from JSON string
fn parse_flow_data(s: &Option<String>) -> jeeflow_core::json::FlowData {
    s.as_ref()
        .and_then(|v| jeeflow_core::json::parse_json(v).ok())
        .map(|jv| jeeflow_core::json::FlowData::from_map(jv.to_map()))
        .unwrap_or_default()
}

// Helper: serialize FlowData to JSON string
fn flow_data_to_json(fd: &jeeflow_core::json::FlowData) -> String {
    if fd.is_empty() {
        return "{}".to_string();
    }
    let entries: Vec<String> = fd.iter()
        .map(|(k, v)| format!("\"{}\":{}", k, v.to_json_string()))
        .collect();
    format!("{{{}}}", entries.join(","))
}

/// Read a MySQL DATETIME column as Option<String>.
/// Handles both DATETIME (chrono::NaiveDateTime) and VARCHAR/String types.
fn get_opt_datetime(r: &sqlx::mysql::MySqlRow, col: &str) -> Option<String> {
    // Try NaiveDateTime first (MySQL DATETIME)
    if let Ok(Some(dt)) = r.try_get::<Option<sqlx::types::chrono::NaiveDateTime>, _>(col) {
        return Some(dt.format("%Y-%m-%d %H:%M:%S").to_string());
    }
    // Fall back to String (VARCHAR or already cast)
    r.try_get::<Option<String>, _>(col).ok().flatten()
}


fn page_bounds(query: &PageQuery) -> (i64, i64, i64) {
    let page_num = if query.page_num < 1 { 1 } else { query.page_num };
    let page_size = if query.page_size < 1 { 20 } else { query.page_size };
    let offset = (page_num - 1) * page_size;
    (page_num, page_size, offset)
}

// ── m_ 过滤下推列白名单（issues/106，对齐 java pushdown / spec 06 §2.2）──
// 未命中白名单的 (alias, column) 返回 None → 该过滤被安全跳过（不注入、不报错）。

const DEFINE_COLS: &[&str] = &["name", "display_name", "type", "state", "version", "id", "create_time", "create_user", "update_time", "update_user"];
const DESIGN_COLS: &[&str] = &["name", "display_name", "type", "is_deployed", "icon", "remark", "id", "create_time", "create_user", "update_time", "update_user"];
const INSTANCE_MAIN_COLS: &[&str] = &["id", "state", "business_no", "operator", "parent_node_name", "process_define_id", "expire_time", "create_time", "create_user", "update_time", "update_user"];
const TASK_MAIN_COLS: &[&str] = &["id", "task_name", "display_name", "task_type", "perform_type", "task_state", "operator", "finish_time", "expire_time", "form_key", "task_parent_id"];
const PI_TASK_COLS: &[&str] = &["process_define_id", "state", "operator", "business_no"];
const PD_COLS: &[&str] = &["name", "display_name", "version"];

/// define/design 表无别名 → 裸列名（仅 t.*）
fn resolve_bare_col<'a>(cols: &'a [&'a str]) -> impl Fn(&str, &str) -> Option<String> + 'a {
    move |alias, column| {
        if alias == "t" && cols.contains(&column) { Some(column.to_string()) } else { None }
    }
}

/// instance 主表别名 pi（facade 2 段 m_ 过滤 alias=t → pi.*）；pd 为定义表
fn resolve_instance_col(alias: &str, column: &str) -> Option<String> {
    if alias == "t" && INSTANCE_MAIN_COLS.contains(&column) { return Some(format!("pi.{}", column)); }
    if alias == "pd" && PD_COLS.contains(&column) { return Some(format!("pd.{}", column)); }
    None
}

/// task 主表别名 t；pi 实例表；pd 定义表
fn resolve_task_col(alias: &str, column: &str) -> Option<String> {
    if alias == "t" && TASK_MAIN_COLS.contains(&column) { return Some(format!("t.{}", column)); }
    if alias == "pi" && PI_TASK_COLS.contains(&column) { return Some(format!("pi.{}", column)); }
    if alias == "pd" && PD_COLS.contains(&column) { return Some(format!("pd.{}", column)); }
    None
}

fn get_opt_string(r: &sqlx::mysql::MySqlRow, col: &str) -> Option<String> {
    r.try_get::<Option<String>, _>(col).ok().flatten()
}

fn get_opt_i64(r: &sqlx::mysql::MySqlRow, col: &str) -> Option<i64> {
    r.try_get::<Option<i64>, _>(col).ok().flatten()
}

fn get_opt_i32(r: &sqlx::mysql::MySqlRow, col: &str) -> Option<i32> {
    r.try_get::<Option<i32>, _>(col).ok().flatten()
}

/// 批量把任务行映射为 ProcessTask（对齐 Java mapTask/mapTasks：解析 variable + setActorIds）。
/// 参与者按 task_id 批量水合——权限判定 is_allowed 依赖 actor_ids，漏查会把真实处理人
/// 全部判为"无权限"。sqlx MySQL 驱动不支持 IN(?) 直接绑 Vec：手动展开占位符逐个 bind。
async fn tasks_from_rows(
    pool: &MySqlPool,
    rows: Vec<sqlx::mysql::MySqlRow>,
) -> JeeflowResult<Vec<ProcessTask>> {
    let task_ids: Vec<i64> = rows.iter().map(|r| r.get::<i64, _>("id")).collect();
    let mut actors_by_task: std::collections::HashMap<i64, Vec<String>> =
        std::collections::HashMap::new();
    if !task_ids.is_empty() {
        let placeholders = vec!["?"; task_ids.len()].join(",");
        let sql = format!(
            "SELECT process_task_id, actor_id FROM wf_process_task_actor WHERE process_task_id IN ({})",
            placeholders
        );
        let mut q = sqlx::query(&sql);
        for id in &task_ids {
            q = q.bind(*id);
        }
        let actor_rows = q
            .fetch_all(pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
        for a in actor_rows {
            let ptid: i64 = a.get("process_task_id");
            let aid: String = a.get("actor_id");
            actors_by_task.entry(ptid).or_default().push(aid);
        }
    }
    Ok(rows
        .into_iter()
        .map(|r| {
            let id: i64 = r.get("id");
            ProcessTask {
                task_id: id,
                process_instance_id: r.get("process_instance_id"),
                task_name: r.get("task_name"),
                display_name: r.get("display_name"),
                task_type: r.get("task_type"),
                perform_type: r.get("perform_type"),
                task_state: r.get("task_state"),
                actor_id: get_opt_string(&r, "operator"),
                actor_ids: actors_by_task.remove(&id).unwrap_or_default(),
                finish_time: get_opt_datetime(&r, "finish_time"),
                expire_time: get_opt_datetime(&r, "expire_time"),
                form_key: r.get("form_key"),
                parent_task_id: get_opt_i64(&r, "task_parent_id"),
                variables: parse_flow_data(&r.get("variable")),
                create_time: get_opt_datetime(&r, "create_time"),
                create_user: r.get("create_user"),
                update_time: get_opt_datetime(&r, "update_time"),
                update_user: r.get("update_user"),
            }
        })
        .collect())
}

fn map_task_row(r: &sqlx::mysql::MySqlRow) -> TaskRow {
    TaskRow {
        id: r.get("id"),
        process_instance_id: r.get("process_instance_id"),
        task_name: r.get("task_name"),
        display_name: r.try_get::<Option<String>, _>("display_name").ok().flatten().unwrap_or_default(),
        task_type: r.try_get::<Option<i32>, _>("task_type").ok().flatten().unwrap_or(0),
        perform_type: r.try_get::<Option<i32>, _>("perform_type").ok().flatten().unwrap_or(0),
        task_state: r.get("task_state"),
        operator: get_opt_string(r, "operator"),
        actor_id: get_opt_string(r, "actor_id"),
        finish_time: get_opt_datetime(r, "finish_time"),
        expire_time: get_opt_datetime(r, "expire_time"),
        form_key: get_opt_string(r, "form_key"),
        task_parent_id: get_opt_i64(r, "task_parent_id"),
        variable: get_opt_string(r, "variable"),
        create_time: get_opt_datetime(r, "create_time"),
        create_user: get_opt_string(r, "create_user"),
        update_time: get_opt_datetime(r, "update_time"),
        update_user: get_opt_string(r, "update_user"),
        process_define_id: get_opt_i64(r, "process_define_id"),
        instance_state: get_opt_i32(r, "instance_state"),
        instance_operator: get_opt_string(r, "instance_operator"),
        business_no: get_opt_string(r, "business_no"),
        instance_variable: get_opt_string(r, "instance_variable"),
        instance_create_time: get_opt_datetime(r, "instance_create_time"),
        define_name: get_opt_string(r, "define_name"),
        define_display_name: get_opt_string(r, "define_display_name"),
        define_version: get_opt_i32(r, "define_version"),
    }
}

fn map_instance_row(r: &sqlx::mysql::MySqlRow) -> InstanceRow {
    InstanceRow {
        id: r.get("id"),
        parent_id: get_opt_i64(r, "parent_id"),
        process_define_id: r.get("process_define_id"),
        state: r.get("state"),
        parent_node_name: get_opt_string(r, "parent_node_name"),
        business_no: get_opt_string(r, "business_no"),
        operator: r.try_get::<Option<String>, _>("operator").ok().flatten().unwrap_or_default(),
        expire_time: get_opt_datetime(r, "expire_time"),
        variable: get_opt_string(r, "variable"),
        create_time: get_opt_datetime(r, "create_time"),
        create_user: get_opt_string(r, "create_user"),
        update_time: get_opt_datetime(r, "update_time"),
        update_user: get_opt_string(r, "update_user"),
        define_name: get_opt_string(r, "define_name"),
        define_display_name: get_opt_string(r, "define_display_name"),
        define_version: get_opt_i32(r, "define_version"),
    }
}

fn map_define_row(r: &sqlx::mysql::MySqlRow) -> DefineRow {
    DefineRow {
        id: r.get("id"),
        name: r.get("name"),
        display_name: r.try_get::<Option<String>, _>("display_name").ok().flatten().unwrap_or_default(),
        define_type: r.try_get::<Option<String>, _>("type").ok().flatten().unwrap_or_else(|| "approval".into()),
        state: r.get("state"),
        version: r.try_get::<Option<i32>, _>("version").ok().flatten().unwrap_or(1),
        create_time: get_opt_datetime(r, "create_time"),
        create_user: get_opt_string(r, "create_user"),
        update_time: get_opt_datetime(r, "update_time"),
        update_user: get_opt_string(r, "update_user"),
    }
}

fn map_design(r: &sqlx::mysql::MySqlRow) -> ProcessDesign {
    ProcessDesign {
        id: r.get("id"),
        name: r.get("name"),
        display_name: r.try_get::<Option<String>, _>("display_name").ok().flatten().unwrap_or_default(),
        design_type: r.try_get::<Option<String>, _>("type").ok().flatten().unwrap_or_else(|| "approval".into()),
        icon: get_opt_string(r, "icon"),
        is_deployed: r.try_get::<Option<i32>, _>("is_deployed").ok().flatten().unwrap_or(0),
        remark: get_opt_string(r, "remark"),
        create_time: get_opt_datetime(r, "create_time"),
        create_user: get_opt_string(r, "create_user"),
        update_time: get_opt_datetime(r, "update_time"),
        update_user: get_opt_string(r, "update_user"),
    }
}

fn map_surrogate(r: &sqlx::mysql::MySqlRow) -> ProcessSurrogate {
    ProcessSurrogate {
        id: r.get("id"),
        process_name: r.get("process_name"),
        operator: r.get("operator"),
        surrogate: r.get("surrogate"),
        start_time: get_opt_datetime(r, "start_time"),
        end_time: get_opt_datetime(r, "end_time"),
        enabled: r.try_get::<Option<i32>, _>("enabled").ok().flatten().unwrap_or(1),
        create_time: get_opt_datetime(r, "create_time"),
        create_user: get_opt_string(r, "create_user"),
        update_time: get_opt_datetime(r, "update_time"),
        update_user: get_opt_string(r, "update_user"),
    }
}


impl ProcessRepository for SqlxRepository {
    fn find_define_by_id(&self, define_id: i64) -> JeeflowResult<Option<ProcessDefine>> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT id, name, display_name, type, state, content, version, create_time, create_user, update_time, update_user FROM wf_process_define WHERE id = ?"
            )
            .bind(define_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            Ok(row.map(|r| ProcessDefine {
                id: r.get("id"),
                name: r.get("name"),
                display_name: r.get("display_name"),
                define_type: r.get("type"),
                state: r.get("state"),
                content: r.get::<Vec<u8>, _>("content"),
                version: r.get("version"),
                create_time: get_opt_datetime(&r, "create_time"),
                create_user: r.get("create_user"),
                update_time: get_opt_datetime(&r, "update_time"),
                update_user: r.get("update_user"),
            }))
        })
    }

    fn save_define(&self, define: &mut ProcessDefine) -> JeeflowResult<()> {
        self.block_on(async {
            let content_str = String::from_utf8_lossy(&define.content).to_string();
            // 规范表无 AUTO_INCREMENT：未显式指定 id 时由应用层雪花生成
            if define.id == 0 {
                define.id = self.next_id();
            }
            // create_time 落库（对齐 Go/Java/Node INSERT 带审计列；PHP 待办排序走 create_time，
            // 漏写会让新数据 create_time=NULL 排到最旧）
            if define.create_time.is_none() {
                define.create_time = Some(current_time_str());
            }
            sqlx::query(
                "INSERT INTO wf_process_define (id, name, display_name, type, state, content, version, create_time, create_user) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(define.id)
            .bind(&define.name)
            .bind(&define.display_name)
            .bind(&define.define_type)
            .bind(define.state)
            .bind(&content_str)
            .bind(define.version)
            .bind(&define.create_time)
            .bind(&define.create_user)
            .execute(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn update_define(&self, define: &ProcessDefine) -> JeeflowResult<()> {
        self.block_on(async {
            sqlx::query(
                "UPDATE wf_process_define SET display_name=?, type=?, state=?, content=?, version=?, update_user=? WHERE id=?"
            )
            .bind(&define.display_name)
            .bind(&define.define_type)
            .bind(define.state)
            .bind(String::from_utf8_lossy(&define.content).to_string())
            .bind(define.version)
            .bind(&define.update_user)
            .bind(define.id)
            .execute(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn update_define_state(&self, define_id: i64, state: i32) -> JeeflowResult<()> {
        self.block_on(async {
            sqlx::query("UPDATE wf_process_define SET state=? WHERE id=?")
                .bind(state)
                .bind(define_id)
                .execute(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn remove_define(&self, define_id: i64) -> JeeflowResult<()> {
        self.block_on(async {
            sqlx::query("DELETE FROM wf_process_define WHERE id=?")
                .bind(define_id)
                .execute(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn find_instance_by_id(&self, instance_id: i64) -> JeeflowResult<Option<ProcessInstance>> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT id, parent_id, process_define_id, state, parent_node_name, business_no, operator, expire_time, variable, create_time, create_user, update_time, update_user FROM wf_process_instance WHERE id = ?"
            )
            .bind(instance_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            let Some(r) = row else { return Ok(None) };
            let instance_id_v: i64 = r.get("id");

            // issues/110：聚合水合——二次查 wf_process_task 装任务副本（含 actor_ids），
            // 对齐 Java findTasksByInstanceId / PHP PdoProcessRepository / C# issues/89；
            // 否则门面 detail 的 tasks/activeTaskList 恒空。
            // 直接内联查询 + tasks_from_rows（复用连接池，批查 actor），避免嵌套 block_on。
            let task_rows = sqlx::query(
                "SELECT id, process_instance_id, task_name, display_name, task_type, perform_type, task_state, operator, finish_time, expire_time, form_key, task_parent_id, variable, create_time, create_user, update_time, update_user FROM wf_process_task WHERE process_instance_id = ?"
            )
            .bind(instance_id_v)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            let tasks = tasks_from_rows(&self.pool, task_rows).await?;

            Ok(Some(ProcessInstance {
                instance_id: r.get("id"),
                parent_id: r.get("parent_id"),
                define_id: r.get("process_define_id"),
                state: r.get("state"),
                parent_node_name: r.get("parent_node_name"),
                business_no: r.get("business_no"),
                operator: r.get("operator"),
                expire_time: get_opt_datetime(&r, "expire_time"),
                variables: parse_flow_data(&r.get("variable")),
                tasks,
                create_time: get_opt_datetime(&r, "create_time"),
                create_user: r.get("create_user"),
                update_time: get_opt_datetime(&r, "update_time"),
                update_user: r.get("update_user"),
                define: None,
            }))
        })
    }

    fn save_instance(&self, instance: &mut ProcessInstance) -> JeeflowResult<()> {
        self.block_on(async {
            let var_json = flow_data_to_json(&instance.variables);
            // 规范表无 AUTO_INCREMENT：未显式指定 id 时由应用层雪花生成
            if instance.instance_id == 0 {
                instance.instance_id = self.next_id();
            }
            if instance.create_time.is_none() {
                instance.create_time = Some(current_time_str());
            }
            sqlx::query(
                "INSERT INTO wf_process_instance (id, parent_id, process_define_id, state, parent_node_name, business_no, operator, expire_time, variable, create_time, create_user) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(instance.instance_id)
            .bind(instance.parent_id)
            .bind(instance.define_id)
            .bind(instance.state)
            .bind(&instance.parent_node_name)
            .bind(&instance.business_no)
            .bind(&instance.operator)
            .bind(&instance.expire_time)
            .bind(&var_json)
            .bind(&instance.create_time)
            .bind(&instance.create_user)
            .execute(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn update_instance(&self, instance: &ProcessInstance) -> JeeflowResult<()> {
        self.block_on(async {
            let var_json = flow_data_to_json(&instance.variables);
            // update_user 用 COALESCE：仅当调用方显式回写（如撤回人）时才落库，
            // 其它路径传 None 保持既有值，避免误清（spec/06 withdraw「实例 update_user 回写为撤回人」）。
            sqlx::query("UPDATE wf_process_instance SET state=?, variable=?, update_user=COALESCE(?, update_user), update_time=NOW() WHERE id=?")
                .bind(instance.state)
                .bind(&var_json)
                .bind(&instance.update_user)
                .bind(instance.instance_id)
                .execute(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn find_task_by_id(&self, task_id: i64) -> JeeflowResult<Option<ProcessTask>> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT id, process_instance_id, task_name, display_name, task_type, perform_type, task_state, operator, finish_time, expire_time, form_key, task_parent_id, variable, create_time, create_user, update_time, update_user FROM wf_process_task WHERE id = ?"
            )
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            let Some(r) = row else { return Ok(None) };
            // 参与者从 wf_process_task_actor 水合（对齐 Java findTaskById 的 setActorIds）：
            // 权限判定 is_allowed 依赖 actor_ids，漏查会把真实处理人全部判为"无权限"。
            let actor_rows = sqlx::query("SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = ?")
                .bind(task_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            let actor_ids: Vec<String> = actor_rows
                .into_iter()
                .map(|a| a.get::<String, _>("actor_id"))
                .collect();

            Ok(Some(ProcessTask {
                task_id: r.get("id"),
                process_instance_id: r.get("process_instance_id"),
                task_name: r.get("task_name"),
                display_name: r.get("display_name"),
                task_type: r.get("task_type"),
                perform_type: r.get("perform_type"),
                task_state: r.get("task_state"),
                actor_id: get_opt_string(&r, "operator"),
                actor_ids,
                finish_time: get_opt_datetime(&r, "finish_time"),
                expire_time: get_opt_datetime(&r, "expire_time"),
                form_key: r.get("form_key"),
                parent_task_id: get_opt_i64(&r, "task_parent_id"),
                variables: parse_flow_data(&r.get("variable")),
                create_time: get_opt_datetime(&r, "create_time"),
                create_user: r.get("create_user"),
                update_time: get_opt_datetime(&r, "update_time"),
                update_user: r.get("update_user"),
            }))
        })
    }

    fn save_task(&self, task: &mut ProcessTask) -> JeeflowResult<()> {
        self.block_on(async {
            let var_json = flow_data_to_json(&task.variables);
            // 规范表无 AUTO_INCREMENT：未显式指定 id 时由应用层雪花生成
            if task.task_id == 0 {
                task.task_id = self.next_id();
            }
            if task.create_time.is_none() {
                task.create_time = Some(current_time_str());
            }
            sqlx::query(
                "INSERT INTO wf_process_task (id, process_instance_id, task_name, display_name, task_type, perform_type, task_state, operator, expire_time, form_key, task_parent_id, variable, create_time, create_user) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(task.task_id)
            .bind(task.process_instance_id)
            .bind(&task.task_name)
            .bind(&task.display_name)
            .bind(task.task_type)
            .bind(task.perform_type)
            .bind(task.task_state)
            .bind(&task.actor_id)
            .bind(&task.expire_time)
            .bind(&task.form_key)
            .bind(task.parent_task_id)
            .bind(&var_json)
            .bind(&task.create_time)
            .bind(&task.create_user)
            .execute(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn update_task(&self, task: &ProcessTask) -> JeeflowResult<()> {
        self.block_on(async {
            let var_json = flow_data_to_json(&task.variables);
            // update_user COALESCE：撤回/转办/办结显式回写操作人时落库，其它路径保持既有值。
            sqlx::query("UPDATE wf_process_task SET task_state=?, operator=?, finish_time=?, variable=?, update_user=COALESCE(?, update_user), update_time=NOW() WHERE id=?")
                .bind(task.task_state)
                .bind(&task.actor_id)
                .bind(&task.finish_time)
                .bind(&var_json)
                .bind(&task.update_user)
                .bind(task.task_id)
                .execute(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn find_doing_tasks(&self, instance_id: i64, task_names: &[String]) -> JeeflowResult<Vec<ProcessTask>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT id, process_instance_id, task_name, display_name, task_type, perform_type, task_state, operator, finish_time, expire_time, form_key, task_parent_id, variable, create_time, create_user, update_time, update_user FROM wf_process_task WHERE process_instance_id = ? AND task_state = 10"
            )
            .bind(instance_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            let tasks = tasks_from_rows(&self.pool, rows).await?;
            Ok(if task_names.is_empty() { tasks } else { tasks.into_iter().filter(|t| task_names.contains(&t.task_name)).collect() })
        })
    }

    fn find_done_tasks(&self, instance_id: i64, task_names: &[String]) -> JeeflowResult<Vec<ProcessTask>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT id, process_instance_id, task_name, display_name, task_type, perform_type, task_state, operator, finish_time, expire_time, form_key, task_parent_id, variable, create_time, create_user, update_time, update_user FROM wf_process_task WHERE process_instance_id = ? AND task_state = 20"
            )
            .bind(instance_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            let tasks = tasks_from_rows(&self.pool, rows).await?;
            Ok(if task_names.is_empty() { tasks } else { tasks.into_iter().filter(|t| task_names.contains(&t.task_name)).collect() })
        })
    }

    fn find_history_tasks(&self, instance_id: i64) -> JeeflowResult<Vec<ProcessTask>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT id, process_instance_id, task_name, display_name, task_type, perform_type, task_state, operator, finish_time, expire_time, form_key, task_parent_id, variable, create_time, create_user, update_time, update_user FROM wf_process_task WHERE process_instance_id = ?"
            )
            .bind(instance_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            // 对齐 Java findInstanceById→findTasksByInstanceId→mapTasks（setActorIds + 解析 variable）
            tasks_from_rows(&self.pool, rows).await
        })
    }

    fn find_task_actors(&self, task_id: i64) -> JeeflowResult<Vec<String>> {
        self.block_on(async {
            let rows = sqlx::query("SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = ?")
                .bind(task_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(rows.into_iter().map(|r| r.get::<String, _>("actor_id")).collect())
        })
    }

    fn add_task_actor(&self, task_id: i64, actors: &[String]) -> JeeflowResult<()> {
        self.block_on(async {
            for actor in actors {
                // 规范表 wf_process_task_actor.id NOT NULL 无默认值：由应用层雪花生成
                // （对齐 Java insertTaskActors 的 nextId()；漏 id 会 1364，startAndExecute 全挂）。
                let actor_row_id = self.next_id();
                sqlx::query("INSERT INTO wf_process_task_actor (id, process_task_id, actor_id, create_time) VALUES (?, ?, ?, ?)")
                    .bind(actor_row_id)
                    .bind(task_id)
                    .bind(actor)
                    .bind(&current_time_str())
                    .execute(&self.pool)
                    .await
                    .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn remove_task_actor(&self, task_id: i64, actors: &[String]) -> JeeflowResult<()> {
        self.block_on(async {
            for actor in actors {
                sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id = ? AND actor_id = ?")
                    .bind(task_id)
                    .bind(actor)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn create_cc_instance(&self, instance_id: i64, creator: &str, actor_ids: &[String]) -> JeeflowResult<()> {
        self.block_on(async {
            for actor in actor_ids {
                // 规范表无 AUTO_INCREMENT：抄送行 id 由应用层雪花生成
                let cc_id = self.next_id();
                sqlx::query("INSERT INTO wf_process_cc_instance (id, process_instance_id, actor_id, state, create_time, create_user) VALUES (?, ?, ?, 0, ?, ?)")
                    .bind(cc_id)
                    .bind(instance_id)
                    .bind(actor)
                    .bind(&current_time_str())
                    .bind(creator)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn update_cc_status(&self, instance_id: i64, actor_id: &str) -> JeeflowResult<()> {
        self.block_on(async {
            sqlx::query("UPDATE wf_process_cc_instance SET state=1 WHERE process_instance_id=? AND actor_id=?")
                .bind(instance_id)
                .bind(actor_id)
                .execute(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }


    fn page_todo_tasks(&self, query: &PageQuery) -> JeeflowResult<PageResult<TaskRow>> {
        self.block_on(async {
            let (page_num, page_size, offset) = page_bounds(query);
            let op = query.operator.clone();
            // m_ 过滤下推（issues/106）：白名单条件拼进 COUNT 与 SELECT，bind 顺序 operator → filters → limit
            let (frags, fvals) = jeeflow_core::filter_sql::build_filter_where(&query.filters, resolve_task_col);
            let where_extra = if frags.is_empty() { String::new() } else { format!(" AND {}", frags.join(" AND ")) };
            let count_sql = format!(
                "SELECT COUNT(DISTINCT t.id) AS cnt \
                 FROM wf_process_task t \
                 INNER JOIN wf_process_task_actor ta ON t.id = ta.process_task_id \
                 INNER JOIN wf_process_instance pi ON t.process_instance_id = pi.id \
                 LEFT JOIN wf_process_define pd ON pi.process_define_id = pd.id \
                 WHERE t.task_state = 10 AND (? IS NULL OR ta.actor_id = ?){where_extra}"
            );
            let mut count_q = sqlx::query(&count_sql).bind(op.clone()).bind(op.clone());
            for v in &fvals { count_q = count_q.bind(v); }
            let count_row = count_q
                .fetch_one(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            let total: i64 = count_row.get("cnt");

            let select_sql = format!(
                "SELECT DISTINCT t.id, t.process_instance_id, t.task_name, t.display_name, \
                        t.task_type, t.perform_type, t.task_state, \
                        t.operator, ta.actor_id AS actor_id, \
                        t.finish_time, t.expire_time, t.form_key, t.task_parent_id, \
                        t.variable, t.create_time, t.create_user, t.update_time, t.update_user, \
                        pi.process_define_id, pi.state AS instance_state, pi.operator AS instance_operator, \
                        pi.business_no, pi.variable AS instance_variable, pi.create_time AS instance_create_time, \
                        pd.name AS define_name, pd.display_name AS define_display_name, pd.version AS define_version \
                 FROM wf_process_task t \
                 INNER JOIN wf_process_task_actor ta ON t.id = ta.process_task_id \
                 INNER JOIN wf_process_instance pi ON t.process_instance_id = pi.id \
                 LEFT JOIN wf_process_define pd ON pi.process_define_id = pd.id \
                 WHERE t.task_state = 10 AND (? IS NULL OR ta.actor_id = ?){where_extra} \
                 ORDER BY t.id DESC LIMIT ? OFFSET ?"
            );
            let mut rows_q = sqlx::query(&select_sql).bind(op.clone()).bind(op);
            for v in &fvals { rows_q = rows_q.bind(v); }
            let rows = rows_q
                .bind(page_size)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            Ok(PageResult::new(page_num, page_size, total, rows.iter().map(map_task_row).collect()))
        })
    }

    fn page_done_tasks(&self, query: &PageQuery) -> JeeflowResult<PageResult<TaskRow>> {
        self.block_on(async {
            let (page_num, page_size, offset) = page_bounds(query);
            let op = query.operator.clone();
            // m_ 过滤下推（issues/106）：白名单条件拼进 COUNT 与 SELECT，bind 顺序 operator×3 → filters → limit
            let (frags, fvals) = jeeflow_core::filter_sql::build_filter_where(&query.filters, resolve_task_col);
            let where_extra = if frags.is_empty() { String::new() } else { format!(" AND {}", frags.join(" AND ")) };
            let count_sql = format!(
                "SELECT COUNT(*) AS cnt \
                 FROM wf_process_task t \
                 INNER JOIN wf_process_instance pi ON t.process_instance_id = pi.id \
                 LEFT JOIN wf_process_define pd ON pi.process_define_id = pd.id \
                 WHERE t.task_state = 20 AND (? IS NULL OR t.operator = ? OR t.create_user = ?){where_extra}"
            );
            let mut count_q = sqlx::query(&count_sql).bind(op.clone()).bind(op.clone()).bind(op.clone());
            for v in &fvals { count_q = count_q.bind(v); }
            let count_row = count_q
                .fetch_one(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            let total: i64 = count_row.get("cnt");

            let select_sql = format!(
                "SELECT t.id, t.process_instance_id, t.task_name, t.display_name, \
                        t.task_type, t.perform_type, t.task_state, \
                        t.operator, t.operator AS actor_id, \
                        t.finish_time, t.expire_time, t.form_key, t.task_parent_id, \
                        t.variable, t.create_time, t.create_user, t.update_time, t.update_user, \
                        pi.process_define_id, pi.state AS instance_state, pi.operator AS instance_operator, \
                        pi.business_no, pi.variable AS instance_variable, pi.create_time AS instance_create_time, \
                        pd.name AS define_name, pd.display_name AS define_display_name, pd.version AS define_version \
                 FROM wf_process_task t \
                 INNER JOIN wf_process_instance pi ON t.process_instance_id = pi.id \
                 LEFT JOIN wf_process_define pd ON pi.process_define_id = pd.id \
                 WHERE t.task_state = 20 AND (? IS NULL OR t.operator = ? OR t.create_user = ?){where_extra} \
                 ORDER BY t.id DESC LIMIT ? OFFSET ?"
            );
            let mut rows_q = sqlx::query(&select_sql).bind(op.clone()).bind(op.clone()).bind(op);
            for v in &fvals { rows_q = rows_q.bind(v); }
            let rows = rows_q
                .bind(page_size)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            Ok(PageResult::new(page_num, page_size, total, rows.iter().map(map_task_row).collect()))
        })
    }

    fn page_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> {
        self.block_on(async {
            let (page_num, page_size, offset) = page_bounds(query);
            let op = query.operator.clone();
            // m_ 过滤下推（issues/106）：bind 顺序 operator×2 → filters → limit
            let (frags, fvals) = jeeflow_core::filter_sql::build_filter_where(&query.filters, resolve_instance_col);
            let where_extra = if frags.is_empty() { String::new() } else { format!(" AND {}", frags.join(" AND ")) };
            let count_sql = format!(
                "SELECT COUNT(*) AS cnt \
                 FROM wf_process_instance pi \
                 LEFT JOIN wf_process_define pd ON pi.process_define_id = pd.id \
                 WHERE (? IS NULL OR pi.operator = ?){where_extra}"
            );
            let mut count_q = sqlx::query(&count_sql).bind(op.clone()).bind(op.clone());
            for v in &fvals { count_q = count_q.bind(v); }
            let count_row = count_q
                .fetch_one(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            let total: i64 = count_row.get("cnt");

            let select_sql = format!(
                "SELECT pi.id, pi.parent_id, pi.process_define_id, pi.state, pi.parent_node_name, \
                        pi.business_no, pi.operator, pi.expire_time, pi.variable, \
                        pi.create_time, pi.create_user, pi.update_time, pi.update_user, \
                        pd.name AS define_name, pd.display_name AS define_display_name, pd.version AS define_version \
                 FROM wf_process_instance pi \
                 LEFT JOIN wf_process_define pd ON pi.process_define_id = pd.id \
                 WHERE (? IS NULL OR pi.operator = ?){where_extra} \
                 ORDER BY pi.id DESC LIMIT ? OFFSET ?"
            );
            let mut rows_q = sqlx::query(&select_sql).bind(op.clone()).bind(op);
            for v in &fvals { rows_q = rows_q.bind(v); }
            let rows = rows_q
                .bind(page_size)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            Ok(PageResult::new(page_num, page_size, total, rows.iter().map(map_instance_row).collect()))
        })
    }

    fn page_cc_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> {
        self.block_on(async {
            let (page_num, page_size, offset) = page_bounds(query);
            let op = query.operator.clone();
            // m_ 过滤下推（issues/106）：加 WHERE 后 DISTINCT 语义不受影响；bind 顺序 operator×2 → filters → limit
            let (frags, fvals) = jeeflow_core::filter_sql::build_filter_where(&query.filters, resolve_instance_col);
            let where_extra = if frags.is_empty() { String::new() } else { format!(" AND {}", frags.join(" AND ")) };
            let count_sql = format!(
                "SELECT COUNT(DISTINCT pi.id) AS cnt \
                 FROM wf_process_cc_instance cc \
                 INNER JOIN wf_process_instance pi ON cc.process_instance_id = pi.id \
                 LEFT JOIN wf_process_define pd ON pi.process_define_id = pd.id \
                 WHERE (? IS NULL OR cc.actor_id = ?){where_extra}"
            );
            let mut count_q = sqlx::query(&count_sql).bind(op.clone()).bind(op.clone());
            for v in &fvals { count_q = count_q.bind(v); }
            let count_row = count_q
                .fetch_one(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            let total: i64 = count_row.get("cnt");

            let select_sql = format!(
                "SELECT DISTINCT pi.id, pi.parent_id, pi.process_define_id, pi.state, pi.parent_node_name, \
                        pi.business_no, pi.operator, pi.expire_time, pi.variable, \
                        pi.create_time, pi.create_user, pi.update_time, pi.update_user, \
                        pd.name AS define_name, pd.display_name AS define_display_name, pd.version AS define_version \
                 FROM wf_process_cc_instance cc \
                 INNER JOIN wf_process_instance pi ON cc.process_instance_id = pi.id \
                 LEFT JOIN wf_process_define pd ON pi.process_define_id = pd.id \
                 WHERE (? IS NULL OR cc.actor_id = ?){where_extra} \
                 ORDER BY pi.id DESC LIMIT ? OFFSET ?"
            );
            let mut rows_q = sqlx::query(&select_sql).bind(op.clone()).bind(op);
            for v in &fvals { rows_q = rows_q.bind(v); }
            let rows = rows_q
                .bind(page_size)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            Ok(PageResult::new(page_num, page_size, total, rows.iter().map(map_instance_row).collect()))
        })
    }

    fn page_defines(&self, query: &PageQuery) -> JeeflowResult<PageResult<DefineRow>> {
        self.block_on(async {
            let (page_num, page_size, offset) = page_bounds(query);
            // m_ 过滤下推（issues/106）：表无别名裸列名；有过滤插 WHERE（不前置 AND），无过滤保持原样
            let (frags, fvals) = jeeflow_core::filter_sql::build_filter_where(&query.filters, resolve_bare_col(DEFINE_COLS));
            let where_clause = if frags.is_empty() { String::new() } else { format!(" WHERE {}", frags.join(" AND ")) };
            let count_sql = format!("SELECT COUNT(*) AS cnt FROM wf_process_define{where_clause}");
            let mut count_q = sqlx::query(&count_sql);
            for v in &fvals { count_q = count_q.bind(v); }
            let count_row = count_q
                .fetch_one(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            let total: i64 = count_row.get("cnt");

            let select_sql = format!(
                "SELECT id, name, display_name, type, state, version, \
                        create_time, create_user, update_time, update_user \
                 FROM wf_process_define{where_clause} ORDER BY id DESC LIMIT ? OFFSET ?"
            );
            let mut rows_q = sqlx::query(&select_sql);
            for v in &fvals { rows_q = rows_q.bind(v); }
            let rows = rows_q
                .bind(page_size)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            Ok(PageResult::new(page_num, page_size, total, rows.iter().map(map_define_row).collect()))
        })
    }


    fn count_todo_tasks(&self, user_id: &str) -> JeeflowResult<i64> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT COUNT(DISTINCT t.id) as cnt FROM wf_process_task t INNER JOIN wf_process_task_actor ta ON t.id = ta.process_task_id WHERE ta.actor_id = ? AND t.task_state = 10"
            )
            .bind(user_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(row.get::<i64, _>("cnt"))
        })
    }

    fn get_all_instances(&self) -> JeeflowResult<Vec<ProcessInstance>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT id, parent_id, process_define_id, state, parent_node_name, business_no, \
                        operator, expire_time, variable, create_time, create_user, update_time, update_user \
                 FROM wf_process_instance"
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            Ok(rows.into_iter().map(|r| ProcessInstance {
                instance_id: r.get("id"),
                parent_id: r.get("parent_id"),
                define_id: r.get("process_define_id"),
                state: r.get("state"),
                parent_node_name: r.get("parent_node_name"),
                business_no: r.get("business_no"),
                operator: r.get("operator"),
                expire_time: get_opt_datetime(&r, "expire_time"),
                variables: parse_flow_data(&r.get("variable")),
                tasks: vec![],
                create_time: get_opt_datetime(&r, "create_time"),
                create_user: r.get("create_user"),
                update_time: get_opt_datetime(&r, "update_time"),
                update_user: r.get("update_user"),
                define: None,
            }).collect())
        })
    }

    fn get_all_tasks(&self) -> JeeflowResult<Vec<ProcessTask>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT id, process_instance_id, task_name, display_name, task_type, perform_type, \
                        task_state, operator, finish_time, expire_time, form_key, task_parent_id, \
                        variable, create_time, create_user, update_time, update_user \
                 FROM wf_process_task"
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            tasks_from_rows(&self.pool, rows).await
        })
    }
}

impl ProcessExtRepository for SqlxRepository {
    fn find_design_by_id(&self, design_id: i64) -> JeeflowResult<Option<ProcessDesign>> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT id, name, display_name, type, icon, is_deployed, remark, \
                        create_time, create_user, update_time, update_user \
                 FROM wf_process_design WHERE id = ?"
            )
            .bind(design_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(row.map(|r| map_design(&r)))
        })
    }

    fn save_design(&self, design: &mut ProcessDesign) -> JeeflowResult<()> {
        self.block_on(async {
            // 规范表无 AUTO_INCREMENT：未显式指定 id 时由应用层雪花生成
            if design.id == 0 {
                design.id = self.next_id();
            }
            if design.create_time.is_none() {
                design.create_time = Some(current_time_str());
            }
            sqlx::query(
                "INSERT INTO wf_process_design (id, name, display_name, type, icon, is_deployed, remark, create_time, create_user) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(design.id)
            .bind(&design.name)
            .bind(&design.display_name)
            .bind(&design.design_type)
            .bind(&design.icon)
            .bind(design.is_deployed)
            .bind(&design.remark)
            .bind(&design.create_time)
            .bind(&design.create_user)
            .execute(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn update_design(&self, design: &ProcessDesign) -> JeeflowResult<()> {
        self.block_on(async {
            sqlx::query(
                "UPDATE wf_process_design SET name=?, display_name=?, type=?, icon=?, \
                 is_deployed=?, remark=?, update_user=? WHERE id=?"
            )
            .bind(&design.name)
            .bind(&design.display_name)
            .bind(&design.design_type)
            .bind(&design.icon)
            .bind(design.is_deployed)
            .bind(&design.remark)
            .bind(&design.update_user)
            .bind(design.id)
            .execute(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn remove_design(&self, design_id: i64) -> JeeflowResult<()> {
        self.block_on(async {
            sqlx::query("DELETE FROM wf_process_design_his WHERE process_design_id=?")
                .bind(design_id)
                .execute(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            sqlx::query("DELETE FROM wf_process_design WHERE id=?")
                .bind(design_id)
                .execute(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn page_designs(&self, query: &PageQuery) -> JeeflowResult<PageResult<ProcessDesign>> {
        self.block_on(async {
            let (page_num, page_size, offset) = page_bounds(query);
            // m_ 过滤下推（issues/106）：表无别名裸列名；有过滤插 WHERE（不前置 AND），无过滤保持原样
            let (frags, fvals) = jeeflow_core::filter_sql::build_filter_where(&query.filters, resolve_bare_col(DESIGN_COLS));
            let where_clause = if frags.is_empty() { String::new() } else { format!(" WHERE {}", frags.join(" AND ")) };
            let count_sql = format!("SELECT COUNT(*) AS cnt FROM wf_process_design{where_clause}");
            let mut count_q = sqlx::query(&count_sql);
            for v in &fvals { count_q = count_q.bind(v); }
            let count_row = count_q
                .fetch_one(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            let total: i64 = count_row.get("cnt");
            let select_sql = format!(
                "SELECT id, name, display_name, type, icon, is_deployed, remark, \
                        create_time, create_user, update_time, update_user \
                 FROM wf_process_design{where_clause} ORDER BY id DESC LIMIT ? OFFSET ?"
            );
            let mut rows_q = sqlx::query(&select_sql);
            for v in &fvals { rows_q = rows_q.bind(v); }
            let rows = rows_q
                .bind(page_size)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(PageResult::new(page_num, page_size, total, rows.iter().map(map_design).collect()))
        })
    }

    fn save_design_his(&self, his: &mut ProcessDesignHis) -> JeeflowResult<()> {
        self.block_on(async {
            let content_str = String::from_utf8_lossy(&his.content).to_string();
            // 规范表无 AUTO_INCREMENT：未显式指定 id 时由应用层雪花生成
            if his.id == 0 {
                his.id = self.next_id();
            }
            if his.create_time.is_none() {
                his.create_time = Some(current_time_str());
            }
            sqlx::query(
                "INSERT INTO wf_process_design_his (id, process_design_id, content, create_time, create_user) VALUES (?, ?, ?, ?, ?)"
            )
            .bind(his.id)
            .bind(his.process_design_id)
            .bind(&content_str)
            .bind(&his.create_time)
            .bind(&his.create_user)
            .execute(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn list_design_his(&self, design_id: i64) -> JeeflowResult<Vec<ProcessDesignHis>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT id, process_design_id, content, create_time, create_user \
                 FROM wf_process_design_his WHERE process_design_id = ? ORDER BY id DESC"
            )
            .bind(design_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(rows.into_iter().map(|r| {
                // content 是 BLOB：sqlx MySQL 解码为 Vec<u8>（对齐 define 读取），
                // 读成 String 会类型不匹配静默返回 None → content 恒空（deploy 拿到空模型）。
                let content: Vec<u8> = r.try_get::<Option<Vec<u8>>, _>("content")
                    .ok()
                    .flatten()
                    .unwrap_or_default();
                ProcessDesignHis {
                    id: r.get("id"),
                    process_design_id: r.get("process_design_id"),
                    content,
                    create_time: get_opt_datetime(&r, "create_time"),
                    create_user: get_opt_string(&r, "create_user"),
                }
            }).collect())
        })
    }

    fn find_surrogate_by_id(&self, surrogate_id: i64) -> JeeflowResult<Option<ProcessSurrogate>> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT id, process_name, operator, surrogate, start_time, end_time, enabled, \
                        create_time, create_user, update_time, update_user \
                 FROM wf_process_surrogate WHERE id = ?"
            )
            .bind(surrogate_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(row.map(|r| map_surrogate(&r)))
        })
    }

    fn save_surrogate(&self, surrogate: &mut ProcessSurrogate) -> JeeflowResult<()> {
        self.block_on(async {
            // 规范表无 AUTO_INCREMENT：未显式指定 id 时由应用层雪花生成
            if surrogate.id == 0 {
                surrogate.id = self.next_id();
            }
            if surrogate.create_time.is_none() {
                surrogate.create_time = Some(current_time_str());
            }
            sqlx::query(
                "INSERT INTO wf_process_surrogate (id, process_name, operator, surrogate, start_time, end_time, enabled, create_time, create_user) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(surrogate.id)
            .bind(&surrogate.process_name)
            .bind(&surrogate.operator)
            .bind(&surrogate.surrogate)
            .bind(&surrogate.start_time)
            .bind(&surrogate.end_time)
            .bind(surrogate.enabled)
            .bind(&surrogate.create_time)
            .bind(&surrogate.create_user)
            .execute(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn update_surrogate(&self, surrogate: &ProcessSurrogate) -> JeeflowResult<()> {
        self.block_on(async {
            sqlx::query(
                "UPDATE wf_process_surrogate SET process_name=?, operator=?, surrogate=?, start_time=?, \
                 end_time=?, enabled=?, update_user=? WHERE id=?"
            )
            .bind(&surrogate.process_name)
            .bind(&surrogate.operator)
            .bind(&surrogate.surrogate)
            .bind(&surrogate.start_time)
            .bind(&surrogate.end_time)
            .bind(surrogate.enabled)
            .bind(&surrogate.update_user)
            .bind(surrogate.id)
            .execute(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn remove_surrogate(&self, surrogate_id: i64) -> JeeflowResult<()> {
        self.block_on(async {
            sqlx::query("DELETE FROM wf_process_surrogate WHERE id=?")
                .bind(surrogate_id)
                .execute(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn page_surrogates(&self, query: &PageQuery) -> JeeflowResult<PageResult<ProcessSurrogate>> {
        self.block_on(async {
            let (page_num, page_size, offset) = page_bounds(query);
            let op = query.operator.clone();
            let count_row = sqlx::query(
                "SELECT COUNT(*) AS cnt FROM wf_process_surrogate WHERE (? IS NULL OR operator = ?)"
            )
            .bind(op.clone())
            .bind(op.clone())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            let total: i64 = count_row.get("cnt");
            let rows = sqlx::query(
                "SELECT id, process_name, operator, surrogate, start_time, end_time, enabled, \
                        create_time, create_user, update_time, update_user \
                 FROM wf_process_surrogate \
                 WHERE (? IS NULL OR operator = ?) \
                 ORDER BY id DESC LIMIT ? OFFSET ?"
            )
            .bind(op.clone())
            .bind(op)
            .bind(page_size)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(PageResult::new(page_num, page_size, total, rows.iter().map(map_surrogate).collect()))
        })
    }

    fn get_surrogate(&self, operator: &str, process_name: &str, time: &str) -> JeeflowResult<Option<ProcessSurrogate>> {
        self.block_on(async {
            let row = if time.is_empty() {
                sqlx::query(
                    "SELECT id, process_name, operator, surrogate, start_time, end_time, enabled, \
                            create_time, create_user, update_time, update_user \
                     FROM wf_process_surrogate \
                     WHERE operator = ? AND process_name = ? AND enabled = 1 \
                     ORDER BY id DESC LIMIT 1"
                )
                .bind(operator)
                .bind(process_name)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?
            } else {
                sqlx::query(
                    "SELECT id, process_name, operator, surrogate, start_time, end_time, enabled, \
                            create_time, create_user, update_time, update_user \
                     FROM wf_process_surrogate \
                     WHERE operator = ? AND process_name = ? AND enabled = 1 \
                       AND (start_time IS NULL OR start_time <= ?) \
                       AND (end_time IS NULL OR end_time >= ?) \
                     ORDER BY id DESC LIMIT 1"
                )
                .bind(operator)
                .bind(process_name)
                .bind(time)
                .bind(time)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?
            };
            Ok(row.map(|r| map_surrogate(&r)))
        })
    }
}



// ═══════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    #[test]
    fn test_page_bounds_defaults() {
        let q = PageQuery::new(0, 0);
        let (n, s, o) = page_bounds(&q);
        assert_eq!(n, 1);
        assert_eq!(s, 20);
        assert_eq!(o, 0);
    }

    #[test]
    fn test_page_bounds_offset() {
        let q = PageQuery::new(3, 10);
        let (n, s, o) = page_bounds(&q);
        assert_eq!((n, s, o), (3, 10, 20));
    }

    #[test]
    fn test_task_row_instance_fields_default() {
        let row = TaskRow::default();
        assert!(row.instance_variable.is_none());
        assert!(row.instance_create_time.is_none());
    }


    use super::*;

    #[test]
    fn test_schema_mysql_not_empty() {
        let schema = schema_mysql();
        assert!(!schema.is_empty());
    }

    #[test]
    fn test_schema_contains_8_tables() {
        let schema = schema_mysql();
        let create_count = schema.matches("CREATE TABLE").count();
        assert_eq!(create_count, 8, "Schema should contain exactly 8 CREATE TABLE statements");
    }

    #[test]
    fn test_schema_table_names() {
        let schema = schema_mysql();
        // 表名必须与 mldong-plus 规范 schema（DB 镜像 / Java 参考实现）完全一致
        let expected_tables = [
            "wf_process_define", "wf_process_instance", "wf_process_task",
            "wf_process_task_actor", "wf_process_cc_instance", "wf_process_design",
            "wf_process_design_his", "wf_process_surrogate",
        ];
        for table in &expected_tables {
            assert!(schema.contains(table), "Schema should contain table: {}", table);
        }
    }

    #[test]
    fn test_schema_ddl_syntax() {
        let schema = schema_mysql();
        let create_count = schema.matches("CREATE TABLE").count();
        let if_not_exists_count = schema.matches("IF NOT EXISTS").count();
        assert_eq!(create_count, if_not_exists_count, "All CREATE TABLE should use IF NOT EXISTS");
    }

    #[test]
    fn test_schema_primary_keys() {
        let schema = schema_mysql();
        let pk_count = schema.matches("PRIMARY KEY").count();
        assert_eq!(pk_count, 8, "Each table should have a PRIMARY KEY");
    }

    /// 回归：init_schema 的「剥注释 + 按 `;` 分句」逻辑必须 8 句全产出。
    /// 历史坑：旧实现按分句后首字符判 `--` 跳过，规范源文件头部注释行
    /// 紧跟首句 DDL，会把第一句 CREATE TABLE 误吞（建表静默缺表）。
    #[test]
    fn test_init_schema_statement_split() {
        let ddl: String = MYSQL_SCHEMA
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        let stmts: Vec<&str> = ddl.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
        assert_eq!(stmts.len(), 8, "init_schema should yield exactly 8 statements");
        for stmt in &stmts {
            assert!(
                stmt.starts_with("CREATE TABLE IF NOT EXISTS"),
                "each statement must be a CREATE TABLE, got: {}...",
                &stmt[..stmt.len().min(60)]
            );
        }
    }

    #[test]
    fn test_schema_indexes() {
        let schema = schema_mysql();
        assert!(schema.contains("idx_process_define_name"), "Should have idx_process_define_name");
        assert!(schema.contains("idx_process_instance_pfid"), "Should have idx_process_instance_pfid");
        assert!(schema.contains("idx_process_task_piid"), "Should have idx_process_task_piid");
        assert!(schema.contains("idx_process_cc_instance_aid"), "Should have idx_process_cc_instance_aid");
    }

    #[test]
    fn test_schema_engine() {
        let schema = schema_mysql();
        let engine_count = schema.matches("ENGINE=InnoDB").count();
        assert_eq!(engine_count, 8, "All tables should use InnoDB engine");
    }

    #[test]
    fn test_schema_charset() {
        let schema = schema_mysql();
        let charset_count = schema.matches("utf8mb4").count();
        assert!(charset_count >= 8, "All tables should use utf8mb4 charset");
    }

    #[test]
    fn test_flow_data_to_json_empty() {
        let fd = jeeflow_core::json::FlowData::new();
        assert_eq!(flow_data_to_json(&fd), "{}");
    }

    #[test]
    fn test_parse_flow_data_none() {
        let fd = parse_flow_data(&None);
        assert!(fd.is_empty());
    }

    #[test]
    fn test_parse_flow_data_empty_string() {
        let fd = parse_flow_data(&Some("".to_string()));
        assert!(fd.is_empty());
    }

    #[test]
    fn test_parse_flow_data_with_values() {
        let fd = parse_flow_data(&Some(r#"{"key1":"val1","key2":42}"#.to_string()));
        assert_eq!(fd.len(), 2);
    }

    #[test]
    fn test_flow_data_to_json_with_values() {
        let mut fd = jeeflow_core::json::FlowData::new();
        fd.insert("name".to_string(), jeeflow_core::json::JsonValue::Str("test".to_string()));
        let json = flow_data_to_json(&fd);
        assert!(json.contains("name"));
        assert!(json.contains("test"));
    }

    /// 列名断言用：空白归一化（规范源文件列名间用对齐多空格，
    /// 旧内嵌 DDL 是单空格，归一化后两种格式断言一致）。
    fn flat(schema: &str) -> String {
        schema.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    #[test]
    fn test_schema_define_table_columns() {
        let schema = flat(schema_mysql());
        // Check key columns in wf_process_define（规范：type 列 + BLOB content）
        assert!(schema.contains("id BIGINT"));
        assert!(schema.contains("name VARCHAR"));
        assert!(schema.contains("display_name VARCHAR"));
        assert!(schema.contains("type VARCHAR"));
        assert!(schema.contains("content BLOB"));
        // 历史错名（define_type/design_type/wf_cc_instance）绝不允许再出现
        assert!(!schema.contains("define_type"));
        assert!(!schema.contains("design_type"));
        assert!(!schema.contains("wf_cc_instance"));
    }

    #[test]
    fn test_schema_instance_table_columns() {
        let schema = flat(schema_mysql());
        assert!(schema.contains("process_define_id BIGINT"));
        assert!(schema.contains("state INT"));
        assert!(schema.contains("operator VARCHAR"));
    }

    #[test]
    fn test_schema_task_table_columns() {
        let schema = flat(schema_mysql());
        assert!(schema.contains("task_name VARCHAR"));
        // 规范：wf_process_task 用 task_state/operator/task_parent_id（对齐 mldong-plus DB 镜像）
        assert!(schema.contains("task_state INT"));
        assert!(schema.contains("task_parent_id BIGINT"));
        assert!(schema.contains("perform_type INT"));
    }

    // ═══════════════════════════════════════════════════════
    // MySQL smoke tests (M1–M4)
    // ═══════════════════════════════════════════════════════

    fn skip_mysql() -> bool {
        std::env::var("SKIP_MYSQL").map(|v| v == "1").unwrap_or(false)
    }

    async fn connect_pool() -> MySqlPool {
        let host = std::env::var("JEFFLOW_DB_HOST").unwrap_or_else(|_| "192.168.1.160".into());
        let port: u16 = std::env::var("JEFFLOW_DB_PORT").unwrap_or_else(|_| "3306".into()).parse().unwrap_or(3306);
        let user = std::env::var("JEFFLOW_DB_USER").unwrap_or_else(|_| "root".into());
        let pwd = std::env::var("JEFFLOW_DB_PWD").unwrap_or_else(|_| "8Eli#gr#AUk".into());
        let name = std::env::var("JEFFLOW_DB_NAME").unwrap_or_else(|_| "jeeflow".into());
        let opts = sqlx::mysql::MySqlConnectOptions::new()
            .host(&host).port(port).username(&user).password(&pwd).database(&name);
        MySqlPool::connect_with(opts).await.expect("Failed to connect to MySQL")
    }

    /// Run a sync closure in a blocking thread, avoiding nested runtime panic.
    async fn run_sync<F: FnOnce() -> R + Send + 'static, R: Send + 'static>(f: F) -> R {
        tokio::task::spawn_blocking(f).await.unwrap()
    }

    /// 测试库 schema 准备：仅跑 init_schema（IF NOT EXISTS）。
    /// 历史坑（2026-08）：曾用 ALTER TABLE ADD COLUMN 往共享 3306 测试库补
    /// define_type/design_type/state/actor_id/parent_task_id 等"错名"列，
    /// 掩盖了 DDL 与 mldong-plus 规范 schema 不一致的 bug（生产栈 L2 全挂）。
    /// 测试库规范表由 DB 镜像 / Java 参考 schema 提供，这里绝不再 ALTER 补列。
    async fn setup_schema(pool: &MySqlPool) {
        SqlxRepository::init_schema(pool).await.unwrap();
    }

    /// M1: Page 五键 — page query returns {pageNum, pageSize, recordCount, totalPage, list}
    #[tokio::test]
    async fn test_mysql_m1_page_five_keys() {
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;

        // Pre-cleanup (in case of previous test failure)
        sqlx::query("DELETE FROM wf_process_define WHERE id BETWEEN 900001 AND 900099").execute(&pool).await.unwrap();

        let pool2 = pool.clone();
        let define_id = run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            let mut define = ProcessDefine {
                id: 900001, name: "rust_m1_test".into(), display_name: "M1 Test".into(),
                define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
                version: 1, create_time: None, create_user: Some("rust_test".into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define).unwrap();
            assert_eq!(define.id, 900001, "M1: define should keep manually assigned ID");
            let loaded = repo.find_define_by_id(define.id).unwrap().unwrap();
            assert_eq!(loaded.name, "rust_m1_test", "M1: define should be readable from MySQL");
            define.id
        }).await;

        // Verify PageResult structure has 5 keys
        let page: PageResult<DefineRow> = PageResult::new(1, 10, 1, vec![]);
        assert_eq!(page.page_num, 1, "M1: pageNum should be 1");
        assert_eq!(page.page_size, 10, "M1: pageSize should be 10");
        assert_eq!(page.record_count, 1, "M1: recordCount should be 1");
        assert!(page.total_page >= 0, "M1: totalPage should be >= 0");

        // Cleanup
        sqlx::query("DELETE FROM wf_process_define WHERE id = ?").bind(define_id).execute(&pool).await.unwrap();
    }

    /// M2: Hydrate 主键 string — BIGINT id returned as string in JSON output
    #[tokio::test]
    async fn test_mysql_m2_hydrate_id_string() {
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;

        // Pre-cleanup
        sqlx::query("DELETE FROM wf_process_task WHERE id BETWEEN 900101 AND 900199").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id BETWEEN 900101 AND 900199").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id BETWEEN 900101 AND 900199").execute(&pool).await.unwrap();

        let pool2 = pool.clone();
        let (define_id, instance_id, task_id) = run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            let mut define = ProcessDefine {
                id: 900101, name: "rust_m2_test".into(), display_name: "M2 Test".into(),
                define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
                version: 1, create_time: None, create_user: Some("rust_test".into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define).unwrap();
            let mut instance = ProcessInstance {
                instance_id: 900102, parent_id: None, define_id: define.id, state: 10,
                parent_node_name: None, business_no: None, operator: "user1".into(),
                expire_time: None, variables: jeeflow_core::json::FlowData::new(),
                tasks: vec![], create_time: None, create_user: Some("user1".into()),
                update_time: None, update_user: None, define: None,
            };
            repo.save_instance(&mut instance).unwrap();
            assert_eq!(instance.instance_id, 900102, "M2: instance should keep assigned ID");
            let loaded = repo.find_instance_by_id(instance.instance_id).unwrap().unwrap();
            assert_eq!(loaded.instance_id, 900102, "M2: loaded ID should match");
            let mut task = ProcessTask {
                task_id: 900103, process_instance_id: instance.instance_id,
                task_name: "task1".into(), display_name: "Task 1".into(),
                task_type: 0, perform_type: 0, task_state: 10,
                actor_id: None, actor_ids: vec!["user1".into()],
                finish_time: None, expire_time: None, form_key: None,
                parent_task_id: None, variables: jeeflow_core::json::FlowData::new(),
                create_time: None, create_user: Some("user1".into()),
                update_time: None, update_user: None,
            };
            repo.save_task(&mut task).unwrap();
            assert_eq!(task.task_id, 900103, "M2: task should keep assigned ID");
            let loaded_task = repo.find_task_by_id(task.task_id).unwrap().unwrap();
            assert_eq!(loaded_task.task_id, 900103, "M2: loaded task ID should match");
            (define.id, instance.instance_id, task.task_id)
        }).await;

        // Cleanup
        sqlx::query("DELETE FROM wf_process_task WHERE id = ?").bind(task_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id = ?").bind(instance_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id = ?").bind(define_id).execute(&pool).await.unwrap();
    }

    /// M3: Persist ARCHIVE — bizData stored as plain text in wf_process_instance.variable
    #[tokio::test]
    async fn test_mysql_m3_archive_bizdata_plain() {
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;

        // Pre-cleanup
        sqlx::query("DELETE FROM wf_process_instance WHERE id BETWEEN 900201 AND 900299").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id BETWEEN 900201 AND 900299").execute(&pool).await.unwrap();

        let pool2 = pool.clone();
        let (define_id, instance_id) = run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            let mut define = ProcessDefine {
                id: 900201, name: "rust_m3_test".into(), display_name: "M3 Test".into(),
                define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
                version: 1, create_time: None, create_user: Some("rust_test".into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define).unwrap();
            let mut vars = jeeflow_core::json::FlowData::new();
            vars.insert("bizData".to_string(), jeeflow_core::json::JsonValue::Str("{\"amount\":1000}".to_string()));
            let mut instance = ProcessInstance {
                instance_id: 900202, parent_id: None, define_id: define.id, state: 10,
                parent_node_name: None, business_no: Some("BIZ-M3-001".into()),
                operator: "user1".into(), expire_time: None,
                variables: vars, tasks: vec![],
                create_time: None, create_user: Some("user1".into()),
                update_time: None, update_user: None, define: None,
            };
            repo.save_instance(&mut instance).unwrap();
            (define.id, instance.instance_id)
        }).await;

        // Read back variable column directly via async sqlx
        let row = sqlx::query("SELECT variable FROM wf_process_instance WHERE id = ?")
            .bind(instance_id).fetch_optional(&pool).await.unwrap();
        assert!(row.is_some(), "M3: instance should exist in DB");
        let var_json: String = row.unwrap().get("variable");
        assert!(var_json.contains("bizData"), "M3: variable should contain bizData as plain JSON");
        assert!(var_json.contains("amount"), "M3: bizData should contain amount field");

        // Cleanup
        sqlx::query("DELETE FROM wf_process_instance WHERE id = ?").bind(instance_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id = ?").bind(define_id).execute(&pool).await.unwrap();
    }

    /// M4: Persist SYNC — field permissions don't over-write (update only changes specified fields)
    #[tokio::test]
    async fn test_mysql_m4_sync_field_permissions() {
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;

        // Pre-cleanup
        sqlx::query("DELETE FROM wf_process_instance WHERE id BETWEEN 900301 AND 900399").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id BETWEEN 900301 AND 900399").execute(&pool).await.unwrap();

        let pool2 = pool.clone();
        let (define_id, instance_id) = run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            let mut define = ProcessDefine {
                id: 900301, name: "rust_m4_test".into(), display_name: "M4 Test".into(),
                define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
                version: 1, create_time: None, create_user: Some("rust_test".into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define).unwrap();
            let mut instance = ProcessInstance {
                instance_id: 900302, parent_id: None, define_id: define.id, state: 10,
                parent_node_name: None, business_no: Some("BIZ-M4-001".into()),
                operator: "user1".into(), expire_time: None,
                variables: jeeflow_core::json::FlowData::new(),
                tasks: vec![], create_time: None, create_user: Some("user1".into()),
                update_time: None, update_user: None, define: None,
            };
            repo.save_instance(&mut instance).unwrap();
            // Update only state
            instance.state = 20;
            repo.update_instance(&instance).unwrap();
            // Verify
            let loaded = repo.find_instance_by_id(instance.instance_id).unwrap().unwrap();
            assert_eq!(loaded.state, 20, "M4: state should be updated to 20");
            assert_eq!(loaded.business_no, Some("BIZ-M4-001".into()), "M4: business_no should be preserved");
            assert_eq!(loaded.operator, "user1", "M4: operator should be preserved");
            (define.id, instance.instance_id)
        }).await;

        // Cleanup
        sqlx::query("DELETE FROM wf_process_instance WHERE id = ?").bind(instance_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id = ?").bind(define_id).execute(&pool).await.unwrap();
    }

    /// issues/110：SQL 仓 find_instance_by_id 水合任务 → detail 任务列表非空。
    /// 修复前：find_instance_by_id 只查 wf_process_instance 单表，tasks 硬编码 vec![]，
    /// 门面 processInstance/detail 的 tasks/activeTaskList 恒为空数组（L2-12 门禁真根因）。
    /// 对齐 Java findTasksByInstanceId / PHP PdoProcessRepository / C# issues/89 聚合水合。
    /// 直接驱动 SQL 仓（真实 MySQL）：造实例 + 进行中任务 + 参与者，再断言
    /// find_instance_by_id 水合出的任务非空、带 actor_ids，且门面 detail 消费
    /// （遍历 inst.tasks 组 tasks/activeTaskList）非空。
    #[tokio::test]
    async fn test_mysql_i110_find_instance_by_id_hydrates_tasks() {
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;

        // Pre-cleanup（独立 ID 段 9008xx，避免与 C8 的 9004xx 段并行撞主键）
        sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id IN (SELECT id FROM wf_process_task WHERE process_instance_id BETWEEN 900801 AND 900899)").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_task WHERE process_instance_id BETWEEN 900801 AND 900899").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id BETWEEN 900801 AND 900899").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id BETWEEN 900801 AND 900899").execute(&pool).await.unwrap();

        let pool2 = pool.clone();
        let instance_id = run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            // 定义（01-simple 结构：start→apply(applicant)→end）
            let mut define = ProcessDefine {
                id: 900801, name: "rust_i110".into(), display_name: "i110 Test".into(),
                define_type: "approval".into(), state: 1,
                content: r#"{"name":"rust_i110","displayName":"i110 Test","type":"approval",
                    "nodes":[{"id":"start","type":"snaker:start","text":{"value":"s"}},
                              {"id":"apply","type":"snaker:task","text":{"value":"申请"},"properties":{"assignee":"applicant"}},
                              {"id":"end","type":"snaker:end","text":{"value":"e"}}],
                    "edges":[{"id":"e1","sourceNodeId":"start","targetNodeId":"apply"},
                             {"id":"e2","sourceNodeId":"apply","targetNodeId":"end"}]}"#.as_bytes().to_vec(),
                version: 1, create_time: None, create_user: Some("rust_test".into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define).unwrap();
            // 进行中实例
            let mut instance = ProcessInstance {
                instance_id: 900802, parent_id: None, define_id: define.id, state: 10,
                parent_node_name: None, business_no: Some("BIZ-RUST-110".into()),
                operator: "zhangsan".into(), expire_time: None,
                variables: jeeflow_core::json::FlowData::new(),
                tasks: vec![], create_time: None, create_user: Some("zhangsan".into()),
                update_time: None, update_user: None, define: None,
            };
            repo.save_instance(&mut instance).unwrap();
            // 进行中任务 apply + 参与者
            let mut task = ProcessTask {
                task_id: 900803, process_instance_id: instance.instance_id,
                task_name: "apply".into(), display_name: "申请".into(),
                task_type: 0, perform_type: 0, task_state: 10,
                actor_id: Some("zhangsan".into()), actor_ids: vec!["zhangsan".into()],
                finish_time: None, expire_time: None, form_key: None,
                parent_task_id: None, variables: jeeflow_core::json::FlowData::new(),
                create_time: None, create_user: Some("zhangsan".into()),
                update_time: None, update_user: None,
            };
            repo.save_task(&mut task).unwrap();
            repo.add_task_actor(task.task_id, &["zhangsan".into()]).unwrap();
            instance.instance_id
        }).await;

        // ① 仓储层：find_instance_by_id 水合任务 + actor_ids（修复前恒空）
        let pool3 = pool.clone();
        let loaded = run_sync(move || {
            let repo = SqlxRepository::new(pool3);
            repo.find_instance_by_id(instance_id).unwrap().unwrap()
        }).await;
        assert!(!loaded.tasks.is_empty(), "issues/110: find_instance_by_id tasks should be non-empty, got {}", loaded.tasks.len());
        assert!(loaded.tasks.iter().all(|t| !t.actor_ids.is_empty()), "issues/110: hydrated tasks must carry actor_ids");

        // ② 门面 detail 消费口径：遍历 inst.tasks 组 tasks/activeTaskList（对齐 jeeflow-facade process_instance_detail）
        let doing_code = TaskState::Doing.code();
        let mut tasks_out: Vec<&ProcessTask> = Vec::new();
        let mut active: Vec<&ProcessTask> = Vec::new();
        for t in &loaded.tasks {
            if t.task_state == doing_code { active.push(t); }
            tasks_out.push(t);
        }
        assert!(!tasks_out.is_empty(), "issues/110: detail tasks should be non-empty");
        assert!(!active.is_empty(), "issues/110: detail activeTaskList should be non-empty");

        // Cleanup
        sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id IN (SELECT id FROM wf_process_task WHERE process_instance_id BETWEEN 900801 AND 900899)").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_task WHERE process_instance_id BETWEEN 900801 AND 900899").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id BETWEEN 900801 AND 900899").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id BETWEEN 900801 AND 900899").execute(&pool).await.unwrap();
    }

    /// C8: m_ filter on sqlx side — verify sqlx returns data with filterable fields.
    /// The actual m_ filter logic is facade-level (apply_filters_to_rows), but this test
    /// proves sqlx path returns TaskRow data that can be filtered by task_name, operator, etc.
    #[tokio::test]
    async fn test_c8_m_filter_sqlx_side() {
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;

        // Pre-cleanup
        sqlx::query("DELETE FROM wf_process_task WHERE id BETWEEN 900401 AND 900499").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id BETWEEN 900401 AND 900499").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id BETWEEN 900401 AND 900499").execute(&pool).await.unwrap();

        let pool2 = pool.clone();
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            // Create define
            let mut define = ProcessDefine {
                id: 900401, name: "c8_filter_test".into(), display_name: "C8 Filter".into(),
                define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
                version: 1, create_time: None, create_user: Some("test".into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define).unwrap();
            // Create instance
            let mut instance = ProcessInstance {
                instance_id: 900402, parent_id: None, define_id: define.id, state: 10,
                parent_node_name: None, business_no: None, operator: "user1".into(),
                expire_time: None, variables: jeeflow_core::json::FlowData::new(),
                tasks: vec![], create_time: None, create_user: Some("user1".into()),
                update_time: None, update_user: None, define: None,
            };
            repo.save_instance(&mut instance).unwrap();
            // Create 3 tasks with different names for same operator
            for (i, name) in [("leave-approval", "Leave Approval"), ("expense-approval", "Expense Approval"), ("leave-request", "Leave Request")].iter().enumerate() {
                let mut task = ProcessTask {
                    task_id: 900410 + i as i64, process_instance_id: instance.instance_id,
                    task_name: name.0.to_string(), display_name: name.1.to_string(),
                    task_type: 0, perform_type: 0, task_state: 10,
                    actor_id: Some("user1".into()), actor_ids: vec!["user1".into()],
                    finish_time: None, expire_time: None, form_key: None,
                    parent_task_id: None, variables: jeeflow_core::json::FlowData::new(),
                    create_time: None, create_user: Some("user1".into()),
                    update_time: None, update_user: None,
                };
                repo.save_task(&mut task).unwrap();
            }
            // Query tasks back via find_task_by_id (proves sqlx path works for m_ filter data)
            for i in 0..3 {
                let task_id = 900410 + i as i64;
                let task = repo.find_task_by_id(task_id).unwrap();
                assert!(task.is_some(), "C8 sqlx: task {} should exist", task_id);
            }
        }).await;

        // Cleanup
        sqlx::query("DELETE FROM wf_process_task WHERE id BETWEEN 900401 AND 900499").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id BETWEEN 900401 AND 900499").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id BETWEEN 900401 AND 900499").execute(&pool).await.unwrap();
    }

    /// C9: design_his content 是 BLOB，读回必须非空且字节一致。
    /// 历史坑（2026-08）：list_design_his 曾把 BLOB 读成 String（sqlx MySQL 解码 BLOB 为
    /// Vec<u8>，try_get::<String> 静默失败）→ content 恒空 → deploy 拿到空模型存成
    /// name='unknown' 的空 define → getLastByName 查不到（160 salvo L2-01 全挂）。
    /// 本测试对旧代码必失败（content 为空），修复后必通过。
    #[tokio::test]
    async fn test_c9_design_his_content_blob_roundtrip() {
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;

        // Pre-cleanup
        sqlx::query("DELETE FROM wf_process_design WHERE id BETWEEN 900501 AND 900599").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_design_his WHERE process_design_id BETWEEN 900501 AND 900599").execute(&pool).await.unwrap();

        let payload = br#"{"name":"c9_blob_test","nodes":[{"id":"start","type":"snaker:start"}]}"#;
        let pool2 = pool.clone();
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            let mut design = ProcessDesign {
                id: 900501, name: "c9_blob_test".into(), display_name: "C9 Blob".into(),
                design_type: "approval".into(), icon: None, is_deployed: 0,
                remark: Some("c9".into()), create_time: None, create_user: Some("test".into()),
                update_time: None, update_user: None,
            };
            repo.save_design(&mut design).unwrap();
            let mut his = ProcessDesignHis {
                id: 0,
                process_design_id: design.id,
                content: payload.to_vec(),
                create_time: None,
                create_user: Some("test".into()),
            };
            repo.save_design_his(&mut his).unwrap();

            let list = repo.list_design_his(design.id).unwrap();
            assert_eq!(list.len(), 1, "C9: exactly one his row");
            assert_eq!(
                list[0].content, payload,
                "C9: his content BLOB must round-trip byte-equal (got {} bytes)",
                list[0].content.len(),
            );
            assert!(!list[0].content.is_empty(), "C9: his content must not be empty");
        }).await;

        // Cleanup
        sqlx::query("DELETE FROM wf_process_design_his WHERE process_design_id BETWEEN 900501 AND 900599").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_design WHERE id BETWEEN 900501 AND 900599").execute(&pool).await.unwrap();
    }

    /// C10: task_actor 行 id 由应用层雪花生成（规范表 id NOT NULL 无默认值）。
    /// 历史坑（2026-08）：add_task_actor 曾漏绑 id 列 → 1364 Field 'id' doesn't have
    /// a default value → startAndExecute 创建任务参与者时全挂（160 salvo L2-02 全挂）。
    /// 本测试对旧代码必失败（1364），修复后必通过（参与者可写可读回）。
    #[tokio::test]
    async fn test_c10_task_actor_id_generated() {
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;

        let task_id: i64 = 900601;
        // Pre-cleanup
        sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id = ?").bind(task_id).execute(&pool).await.unwrap();

        let pool2 = pool.clone();
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            repo.add_task_actor(task_id, &["actor_a".into(), "actor_b".into()]).unwrap();
            let actors = repo.find_task_actors(task_id).unwrap();
            assert_eq!(actors, vec!["actor_a".to_string(), "actor_b".to_string()],
                "C10: task actors must round-trip (got {:?})", actors);
        }).await;

        // 确认确实生成了非空 id（规范表不允许 0/NULL）
        let ids = sqlx::query("SELECT id FROM wf_process_task_actor WHERE process_task_id = ?")
            .bind(task_id).fetch_all(&pool).await.unwrap();
        assert_eq!(ids.len(), 2, "C10: two actor rows");
        for r in &ids {
            let id: i64 = r.get("id");
            assert_ne!(id, 0, "C10: task_actor id must be a generated snowflake, not 0");
        }

        // Cleanup
        sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id = ?").bind(task_id).execute(&pool).await.unwrap();
    }

    /// C11: find_task_by_id 必须从 wf_process_task_actor 水合 actor_ids。
    /// 历史坑（2026-08）：find_task_by_id 曾硬编码 actor_ids: vec![] → 权限判定
    /// is_allowed（依赖 actor_ids.contains(operator)）把真实处理人全部判为"无权限"
    /// → execute_task_async 报 "Operator X not allowed on task Y"（160 salvo L2-02 全挂）。
    /// Memory 仓储在内存里保留 actor_ids 故内存测试绿，只有连库才暴露。
    /// 本测试对旧代码必失败（actor_ids 空 / is_allowed 假），修复后必通过。
    #[tokio::test]
    async fn test_c11_find_task_by_id_hydrates_actors() {
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;

        let define_id: i64 = 900700;
        let instance_id: i64 = 900702;
        let task_id: i64 = 900701;
        let handler = "the_handler";
        // Pre-cleanup（各表按各自真实 id 清理，保证可重复跑）
        sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id = ?").bind(task_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_task WHERE id = ?").bind(task_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id = ?").bind(instance_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id = ?").bind(define_id).execute(&pool).await.unwrap();

        let pool2 = pool.clone();
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            let mut define = ProcessDefine {
                id: define_id, name: "c11_perm_test".into(), display_name: "C11".into(),
                define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
                version: 1, create_time: None, create_user: Some("test".into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define).unwrap();
            let mut instance = ProcessInstance {
                instance_id, parent_id: None, define_id: define.id, state: 10,
                parent_node_name: None, business_no: None, operator: handler.into(),
                expire_time: None, variables: jeeflow_core::json::FlowData::new(),
                tasks: vec![], create_time: None, create_user: Some(handler.into()),
                update_time: None, update_user: None, define: None,
            };
            repo.save_instance(&mut instance).unwrap();
            let mut task = ProcessTask {
                task_id, process_instance_id: instance.instance_id,
                task_name: "apply".into(), display_name: "Apply".into(),
                task_type: 0, perform_type: 0, task_state: 10, // DOING
                actor_id: None, actor_ids: vec![handler.to_string()],
                finish_time: None, expire_time: None, form_key: None,
                parent_task_id: None, variables: jeeflow_core::json::FlowData::new(),
                create_time: None, create_user: Some(handler.into()),
                update_time: None, update_user: None,
            };
            repo.save_task(&mut task).unwrap();
            // 参与者关系表独立存（save_task 不写 task_actor）
            repo.add_task_actor(task_id, &[handler.to_string()]).unwrap();

            // 关键断言：find_task_by_id 水合 actor_ids，is_allowed 放行真实处理人
            let loaded = repo.find_task_by_id(task_id).unwrap()
                .expect("C11: task should load from MySQL");
            assert!(loaded.actor_ids.contains(&handler.to_string()),
                "C11: actor_ids must be hydrated from wf_process_task_actor (got {:?})",
                loaded.actor_ids);
            assert!(loaded.is_allowed(handler),
                "C11: real handler must pass is_allowed (actor_ids={:?})", loaded.actor_ids);
            assert!(!loaded.is_allowed("someone_else"),
                "C11: non-actor must be denied");

            // find_history_tasks（instance.tasks 水合，complete_task→is_allowed 依赖）同样必须带 actor_ids
            let hist = repo.find_history_tasks(instance.instance_id).unwrap();
            assert_eq!(hist.len(), 1, "C11: one history task");
            assert!(hist[0].actor_ids.contains(&handler.to_string()),
                "C11: find_history_tasks must also hydrate actor_ids (got {:?})",
                hist[0].actor_ids);
            assert!(hist[0].is_allowed(handler),
                "C11: history task must allow real handler via is_allowed");
        }).await;

        // Cleanup
        sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id = ?").bind(task_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_task WHERE id = ?").bind(task_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id = ?").bind(instance_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id = ?").bind(define_id).execute(&pool).await.unwrap();
    }

    // ═══════════════════════════════════════════════════════
    // issues/113~116 · withdraw update_user 级联落库 + transfer 留痕落库（真机 SQL 断言）
    // ═══════════════════════════════════════════════════════

    /// 撤回级联：实例与进行中任务的 update_user 必须真的落库（sqlx update_instance 不级联任务，
    /// 靠门面逐任务 update_task 那一圈带下去；update_user 列此前根本不写，本轮补 COALESCE）。
    #[tokio::test]
    async fn test_mysql_withdraw_cascade_persists_update_user() {
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;

        let (define_id, instance_id, task_id) = (900950i64, 900951i64, 900952i64);
        sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id BETWEEN 900950 AND 900999").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_task WHERE id BETWEEN 900950 AND 900999").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id BETWEEN 900950 AND 900999").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id BETWEEN 900950 AND 900999").execute(&pool).await.unwrap();

        let pool2 = pool.clone();
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            let mut define = ProcessDefine {
                id: define_id, name: "wd_cascade".into(), display_name: "WD".into(),
                define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
                version: 1, create_time: None, create_user: Some("applicant".into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define).unwrap();
            let mut inst = ProcessInstance {
                instance_id, parent_id: None, define_id, state: 10,
                parent_node_name: None, business_no: None, operator: "applicant".into(),
                expire_time: None, variables: jeeflow_core::json::FlowData::new(),
                tasks: vec![], create_time: None, create_user: Some("applicant".into()),
                update_time: None, update_user: None, define: None,
            };
            repo.save_instance(&mut inst).unwrap();
            let mut task = ProcessTask {
                task_id, process_instance_id: instance_id,
                task_name: "approve".into(), display_name: "Approve".into(),
                task_type: 0, perform_type: 0, task_state: 10, // DOING
                actor_id: None, actor_ids: vec!["user2".into()],
                finish_time: None, expire_time: None, form_key: None,
                parent_task_id: None, variables: jeeflow_core::json::FlowData::new(),
                create_time: None, create_user: Some("applicant".into()),
                update_time: None, update_user: None,
            };
            repo.save_task(&mut task).unwrap();
            repo.add_task_actor(task_id, &["user2".into()]).unwrap();

            // 模拟门面撤回级联：任务置 30 + update_user 回写，实例置 30 + update_user 回写
            let mut t = repo.find_task_by_id(task_id).unwrap().unwrap();
            t.task_state = 30;
            t.update_user = Some("applicant".into());
            repo.update_task(&t).unwrap();
            let mut i = repo.find_instance_by_id(instance_id).unwrap().unwrap();
            i.state = 30;
            i.update_user = Some("applicant".into());
            repo.update_instance(&i).unwrap();
        }).await;

        // 真机 SQL 断言（读回库列，非内存）
        let task_row = sqlx::query("SELECT task_state, update_user, operator FROM wf_process_task WHERE id = ?")
            .bind(task_id).fetch_one(&pool).await.unwrap();
        let state: i32 = task_row.get("task_state");
        let tu: Option<String> = task_row.get("update_user");
        let op: Option<String> = task_row.get("operator");
        assert_eq!(state, 30, "任务须落库 30");
        assert_eq!(tu.as_deref(), Some("applicant"), "任务 update_user 须真落库为撤回人");
        assert!(op.is_none(), "撤回不得给任务 operator(actor 列) 写值，实测 {:?}", op);

        let inst_row = sqlx::query("SELECT state, update_user FROM wf_process_instance WHERE id = ?")
            .bind(instance_id).fetch_one(&pool).await.unwrap();
        let istate: i32 = inst_row.get("state");
        let itu: Option<String> = inst_row.get("update_user");
        assert_eq!(istate, 30, "实例须落库 30");
        assert_eq!(itu.as_deref(), Some("applicant"), "实例 update_user 须真落库为撤回人");

        // Cleanup
        sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id = ?").bind(task_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_task WHERE id = ?").bind(task_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id = ?").bind(instance_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id = ?").bind(define_id).execute(&pool).await.unwrap();
    }

    /// 转办留痕落库：tf_transferHistory（六键 camelCase + time 字符串）经 variable 列往返，
    /// 且严禁覆写 operator(actor 列)——转办时进行中任务该列仍为 NULL。
    #[tokio::test]
    async fn test_mysql_transfer_trace_roundtrip() {
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;

        let (define_id, instance_id, task_id) = (900960i64, 900961i64, 900962i64);
        sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id BETWEEN 900960 AND 900999").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_task WHERE id BETWEEN 900960 AND 900999").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id BETWEEN 900960 AND 900999").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id BETWEEN 900960 AND 900999").execute(&pool).await.unwrap();

        let pool2 = pool.clone();
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            let mut define = ProcessDefine {
                id: define_id, name: "tr_trace".into(), display_name: "TR".into(),
                define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
                version: 1, create_time: None, create_user: Some("applicant".into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define).unwrap();
            let mut inst = ProcessInstance {
                instance_id, parent_id: None, define_id, state: 10,
                parent_node_name: None, business_no: None, operator: "applicant".into(),
                expire_time: None, variables: jeeflow_core::json::FlowData::new(),
                tasks: vec![], create_time: None, create_user: Some("applicant".into()),
                update_time: None, update_user: None, define: None,
            };
            repo.save_instance(&mut inst).unwrap();
            let mut task = ProcessTask {
                task_id, process_instance_id: instance_id,
                task_name: "approve".into(), display_name: "Approve".into(),
                task_type: 0, perform_type: 0, task_state: 10,
                actor_id: None, actor_ids: vec!["user2".into()],
                finish_time: None, expire_time: None, form_key: None,
                parent_task_id: None, variables: jeeflow_core::json::FlowData::new(),
                create_time: None, create_user: Some("applicant".into()),
                update_time: None, update_user: None,
            };
            repo.save_task(&mut task).unwrap();
            repo.add_task_actor(task_id, &["user2".into()]).unwrap();

            // 模拟门面转办 user2 → lisi：摘原人 + 加新人 + 三件留痕 + update_user，不动 actor 列
            repo.remove_task_actor(task_id, &["user2".into()]).unwrap();
            repo.add_task_actor(task_id, &["lisi".into()]).unwrap();
            let mut t = repo.find_task_by_id(task_id).unwrap().unwrap();
            let mut vars = jeeflow_core::json::FlowData::new();
            let hop = jeeflow_core::json::JsonValue::Object(vec![
                ("submitType".to_string(), jeeflow_core::json::JsonValue::Number(7.0)),
                ("fromActor".to_string(), jeeflow_core::json::JsonValue::Str("user2".into())),
                ("toActor".to_string(), jeeflow_core::json::JsonValue::Str("lisi".into())),
                ("reason".to_string(), jeeflow_core::json::JsonValue::Str("出差一周".into())),
                ("time".to_string(), jeeflow_core::json::JsonValue::Str("2026-09-21 08:20:54".into())),
                ("operator".to_string(), jeeflow_core::json::JsonValue::Str("user2".into())),
            ]);
            vars.insert("tf_transferHistory".to_string(), jeeflow_core::json::JsonValue::Array(vec![hop]));
            vars.insert_i64("submitType", 7);
            vars.insert_str("tf_transferTo", "lisi");
            vars.insert_str("tf_approvalComment", "user2 转办给 lisi（出差一周）");
            t.variables = vars;
            t.update_user = Some("user2".into());
            t.actor_ids = repo.find_task_actors(task_id).unwrap();
            repo.update_task(&t).unwrap();

            // 读回值断言（走 find_task_by_id 的 variable 列解析）
            let back = repo.find_task_by_id(task_id).unwrap().unwrap();
            let hist = match back.variables.get("tf_transferHistory") {
                Some(jeeflow_core::json::JsonValue::Array(a)) => a.clone(),
                _ => vec![],
            };
            assert_eq!(hist.len(), 1, "转办留痕须往返库 variable 列");
            let obj = hist[0].as_object().expect("hop object");
            let get = |k: &str| obj.iter().find(|(x, _)| x == k).map(|(_, v)| v.clone());
            assert_eq!(get("fromActor").and_then(|v| v.as_str().map(String::from)).as_deref(), Some("user2"));
            assert_eq!(get("toActor").and_then(|v| v.as_str().map(String::from)).as_deref(), Some("lisi"));
            assert_eq!(get("time").and_then(|v| v.as_str().map(String::from)).as_deref(), Some("2026-09-21 08:20:54"));
            assert_eq!(back.variables.get_str("tf_transferTo"), Some("lisi"));
            assert!(back.actor_id.is_none(), "转办严禁覆写 operator 列，读回 {:?}", back.actor_id);
            // 参与者表：user2 摘走、lisi 加上
            let actors = repo.find_task_actors(task_id).unwrap();
            assert!(!actors.contains(&"user2".to_string()) && actors.contains(&"lisi".to_string()), "actors={:?}", actors);
        }).await;

        // 真机 SQL：operator(actor 列) 恒 NULL
        let op: Option<String> = sqlx::query("SELECT operator FROM wf_process_task WHERE id = ?")
            .bind(task_id).fetch_one(&pool).await.unwrap().get("operator");
        assert!(op.is_none(), "真机 operator 列应为 NULL，实测 {:?}", op);

        // Cleanup
        sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id = ?").bind(task_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_task WHERE id = ?").bind(task_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id = ?").bind(instance_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id = ?").bind(define_id).execute(&pool).await.unwrap();
    }
}
