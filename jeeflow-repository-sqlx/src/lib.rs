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

    /// 委托查询单腿 SQL（规范 06 §4.5 条款 1.4 的正确判序，issues/123）：
    /// **只按 `operator` + 作用域取主键 id 最新的一条**（`ORDER BY id DESC LIMIT 1`），
    /// 不带任何生效判据过滤——enabled / 时间窗 / 自委托一律不参与择优，
    /// 全部交给 `jeeflow_core::surrogate::surrogate_hit` 在**那一条**上裁决
    /// （见 [`Self::get_surrogate`]）。
    ///
    /// ⚠️ 反例（修复前的形状，也是 13 栈 L2-17/L2-18 恒并入的成因）：
    /// SQL 里先写 `AND enabled = 1 AND surrogate <> operator` + 窗口条件，剩下的才排序取最新。
    /// 那等价于"同一授权人历史上只要留过一条窗内且 enabled=1 的记录，之后用户新建的
    /// 窗外 / enabled=0 / 脏值 / 自委托记录全都判不动它"⇒ 代理人被永久并入。
    ///
    /// 保留的判据只有作用域（条款 5 判据①）：
    /// - `process_name` 非空 → 精确腿 `process_name = ?`；
    /// - `process_name` 为空 → 全流程兜底腿 `process_name IS NULL OR = ''`。
    async fn query_newest_surrogate(
        &self,
        operator: &str,
        process_name: &str,
    ) -> JeeflowResult<Option<ProcessSurrogate>> {
        let with_name = !process_name.is_empty();
        let mut sql = String::from(
            "SELECT id, process_name, operator, surrogate, start_time, end_time, enabled, \
                    create_time, create_user, update_time, update_user \
             FROM wf_process_surrogate \
             WHERE operator = ?",
        );
        if with_name {
            sql.push_str(" AND process_name = ?");
        } else {
            sql.push_str(" AND (process_name IS NULL OR process_name = '')");
        }
        sql.push_str(" ORDER BY id DESC LIMIT 1");

        let mut q = sqlx::query(&sql).bind(operator);
        if with_name {
            q = q.bind(process_name);
        }
        let row = q
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
        Ok(row.map(|r| map_surrogate(&r)))
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

/// `page_cc_instances` 的白名单：实例列 ＋ **归属列 `cc.actor_id`**（issues/141 G1）。
///
/// 归属列必须在白名单里——旧形状用 `resolve_instance_col`，`cc.actor_id` 查不到就整条条件
/// 静默丢掉＝"这条不加"，与内存仓同查询返 0 行分叉。java 基准 `CC_INSTANCE_WHITELIST`
/// 同样把 `cc.actor_id` 列在内。
fn resolve_cc_instance_col(alias: &str, column: &str) -> Option<String> {
    if jeeflow_core::model::is_cc_ownership_col(alias, column) {
        return Some("cc.actor_id".to_string());
    }
    resolve_instance_col(alias, column)
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
        // `process_name` 列可空（规范注释"为空=全部流程"），而全流程兜底腿**必然**会读到
        // NULL 行（契约 06 §4.5 条款 5 判据① `process_name IS NULL OR = ''`）：
        // 原先的 `r.get::<String,_>` 遇 NULL 直接 panic（sqlx `get` 不容 NULL）。
        // 统一归一为空串，与内存仓 `ProcessSurrogate.process_name: String` 同形状（条款 6）。
        process_name: get_opt_string(r, "process_name").unwrap_or_default(),
        operator: r.get("operator"),
        surrogate: r.get("surrogate"),
        start_time: get_opt_datetime(r, "start_time"),
        end_time: get_opt_datetime(r, "end_time"),
        // 判据④（条款 5）：`enabled` **只有 1 生效**，NULL/脏值不得折叠成 1（原 `unwrap_or(1)`
        // = "读不出来就当启用"，与内存仓 `enabled != 1 → 不命中` 相反，同栈双仓分叉）。
        enabled: r.try_get::<Option<i32>, _>("enabled").ok().flatten().unwrap_or(0),
        create_time: get_opt_datetime(r, "create_time"),
        create_user: get_opt_string(r, "create_user"),
        update_time: get_opt_datetime(r, "update_time"),
        update_user: get_opt_string(r, "update_user"),
    }
}


/// issues/141 G2 写侧判重的读侧 SQL（`create_cc_instance` 与 `find_cc_actor_ids` 共用一支，
/// 判据只有一份）。逐行返回、不加 DISTINCT。
async fn select_cc_actor_ids(pool: &MySqlPool, instance_id: i64) -> JeeflowResult<Vec<String>> {
    let rows = sqlx::query("SELECT actor_id FROM wf_process_cc_instance WHERE process_instance_id = ?")
        .bind(instance_id)
        .fetch_all(pool)
        .await
        .map_err(|e| JeeflowError::Internal(e.to_string()))?;
    Ok(rows.iter().map(|r| r.get::<String, _>("actor_id")).collect())
}

/// 任务参与者台账的读侧单点（`wf_process_task_actor.actor_id`）。
/// issues/142 B 批：`find_task_actors` 与 `add_task_actor` 的写侧判重共用这一支，
/// 不另抄第二份 SELECT——判重与读回必须看到同一批行。逐行返回，**不加 DISTINCT**
/// （与 [`select_cc_actor_ids`] 同口径：判重只看"这个人在这个任务上有没有行"，
/// 存量脏行原样留着，issues/141 G2 owner 拍板）。
async fn select_task_actor_ids(pool: &MySqlPool, task_id: i64) -> JeeflowResult<Vec<String>> {
    let rows = sqlx::query("SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = ?")
        .bind(task_id)
        .fetch_all(pool)
        .await
        .map_err(|e| JeeflowError::Internal(e.to_string()))?;
    Ok(rows.iter().map(|r| r.get::<String, _>("actor_id")).collect())
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
            // issues/125：update_time 由引擎时钟出口供给并绑参，SQL 文本里不留 NOW()——
            // MySQL 的 NOW() 取 @@session.time_zone 的墙钟，而 create_time 是引擎钟写的裸墙钟，
            // 两把钟会让同一行两列差一个时区偏移（宿主注入东八、库会话 UTC 时立刻发作）。
            // 只取一次，与 save_instance 里 create_time 用的是同一个出口。
            let now = current_time_str();
            // update_user 用 COALESCE：仅当调用方显式回写（如撤回人）时才落库，
            // 其它路径传 None 保持既有值，避免误清（spec/06 withdraw「实例 update_user 回写为撤回人」）。
            sqlx::query("UPDATE wf_process_instance SET state=?, variable=?, update_user=COALESCE(?, update_user), update_time=? WHERE id=?")
                .bind(instance.state)
                .bind(&var_json)
                .bind(&instance.update_user)
                .bind(&now)
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
                // ⚠️ `finish_time` 这一列 **2026-09-30 issues/142 A 批补进来**（spec/02 §6.2 第 1bis 条）：
                // 本语句原先不带它，靠 `update_task` 那条 UPDATE 写——可记录类（`snaker:custom`）那条
                // DONE 行**只走 INSERT 不走 UPDATE**（生来已完成，没有"办理"那一次更新），
                // 于是聚合根赋的 finish_time 在 SQL 仓被静默丢掉，`processTask/doneList`
                // （`task_state<>10 AND operator=?`）按完成时间取数时那格恒 NULL。
                // 内存仓无此问题（整行存），所以只在真库那一路才看得见——典型的"两层冗余藏病灶"。
                // `update_time`/`update_user` 仍不在本语句里：那是办理审计，建单时必为 None，
                // 由 `update_task` 负责（与 java `setTaskParams` 的 17 列全绑不同形，
                // 本轮只补 1bis 点名的那一列，不顺手改形状）。
                "INSERT INTO wf_process_task (id, process_instance_id, task_name, display_name, task_type, perform_type, task_state, operator, finish_time, expire_time, form_key, task_parent_id, variable, create_time, create_user) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(task.task_id)
            .bind(task.process_instance_id)
            .bind(&task.task_name)
            .bind(&task.display_name)
            .bind(task.task_type)
            .bind(task.perform_type)
            .bind(task.task_state)
            .bind(&task.actor_id)
            .bind(&task.finish_time)
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
            // issues/125：同 update_instance——update_time 走引擎时钟出口绑参，SQL 里不留 NOW()。
            // 只取一次，本条语句的 finish_time（来自聚合根，也是引擎钟产的）与它同一基准。
            let now = current_time_str();
            // update_user COALESCE：撤回/转办/办结显式回写操作人时落库，其它路径保持既有值。
            sqlx::query("UPDATE wf_process_task SET task_state=?, operator=?, finish_time=?, variable=?, update_user=COALESCE(?, update_user), update_time=? WHERE id=?")
                .bind(task.task_state)
                .bind(&task.actor_id)
                .bind(&task.finish_time)
                .bind(&var_json)
                .bind(&task.update_user)
                .bind(&now)
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
                // issues/154①（spec/06 §4.6 approvalRecord 口径①）：**必须** `ORDER BY id ASC`——
                // 雪花 id 单调，同秒并发插入时它比 `create_time`/`update_time` 确定；此前整条没有
                // ORDER BY，审批记录的行序就成了"存储顺序的偶然"。只补这一条任务腿，
                // `find_doing_tasks` 等其它腿本轮不动（未在立法范围内）。
                "SELECT id, process_instance_id, task_name, display_name, task_type, perform_type, task_state, operator, finish_time, expire_time, form_key, task_parent_id, variable, create_time, create_user, update_time, update_user FROM wf_process_task WHERE process_instance_id = ? ORDER BY id ASC"
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
        self.block_on(async { select_task_actor_ids(&self.pool, task_id).await })
    }

    fn add_task_actor(&self, task_id: i64, actors: &[String]) -> JeeflowResult<()> {
        self.block_on(async {
            // issues/142 B 批 · spec 06-facade.md §2.11 的**写侧兜底层**，与内存仓
            // `MemoryRepository::add_task_actor` 同一条判据（判据本体＝
            // `jeeflow_core::model::normalize_actors`，与抄送侧 §2.10 同一枚单点）：
            // 逐元素 trim ⇒ 空串/纯空白丢弃 ⇒ 同一次调用内折叠，落库与比较一律取 trim 后的串。
            // 改前本仓是**盲插**（无判空、无 trim、无判重），而同栈内存仓判重 ⇒ 同一串入参
            // 两仓两个答案（issues/117 场景 27 那把尺子点名的形状），绕过门面直连仓储的调用方
            // 能在真库里灌空值/灌重复——空归属值正是 issues/129 那族"空 operator 读全库"的进水口。
            // 反向哨兵（§2.11 硬要求④）：`"0"`／`"00"` 是正常 id，不得被当成空值丢掉。
            let normalized = jeeflow_core::model::normalize_actors(actors);
            // 判重的读侧放在写侧这一层（与 create_cc_instance 同姿势）：先取快照再插，
            // 已有同一人的行 ⇒ 幂等空操作；同一次调用内重复也给同一个人也只落一行。
            let mut existing = select_task_actor_ids(&self.pool, task_id).await?;
            for actor in &normalized {
                if existing.iter().any(|a| a == actor) {
                    continue;
                }
                existing.push(actor.clone());
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
        // issues/137 §3-6（spec 06 §processTask/removeTaskActor 语义 6，owner 2026-10-02 拍
        // 「两形并集」，与内存仓同一枚判据＝`jeeflow_core::model::actor_delete_forms`，
        // issues/117 场景 27 同答案）：空值一律丢弃；非空值以「原值 ∪ trim 值」两形逐个绑
        // DELETE。为什么不能只取原值：「 8601 」删不掉写侧归一后落库的规范行 8601
        // （issues/142 §9.2，静默 no-op 报成功）；为什么也不能只取 trim 形（1.8.36 之前
        // 本仓正是这个形状）：门面按语义 6 交出的是**行上的原值**，修复前落下的未 trim
        // 历史脏行「 9101 」被削成 9101，真库 NO PAD 排序规则下那一行删不掉而门面报成功
        // ——被摘的人待办还在。并集为空 ⇒ 早退，一条 DELETE 都不发（空串入参会批量误删
        // 历史 actor_id='' 脏行，issues/129 的删除位对偶）。
        // 展开在 `block_on` 之前完成（同步 SPI，不引入持锁跨 await），库内逐元素绑定删除。
        let forms = jeeflow_core::model::actor_delete_forms(actors);
        if forms.is_empty() { return Ok(()); }
        self.block_on(async {
            for actor in &forms {
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
            // issues/141 G2 写侧判重＝幂等空操作（spec 06 §4），与内存仓 `create_cc_instance`
            // 同一条判据：同一 `(实例, 被抄送人)` 已有 cc 行 ⇒ 直接跳过——①不新增行、
            // ②不重置未读（state 保持原值）、③不更新原行时间（连 UPDATE 都不发，
            // create_time/update_time 逐字不变）。判重放在**写侧**而不是查询侧：
            // 查询保持现状不引入 DISTINCT，历史重复行也不清理（owner 2026-09-29 拍）。
            // issues/141 G10「空不创建行」（spec 06 §2.10）写侧兜底：与内存仓同一条判据——
            // 空串/纯空白一律丢弃、落库值取 trim 后的串（`" 123 "` 与 `"123"` 判为同一个人，
            // 也才与下面的判重咬合）。绕过引擎漏斗与门面直连仓储的调用方同样建不出 actor_id='' 的行
            // （那正是 issues/129 那族"空 operator 读全库"的病根）。
            let actors = jeeflow_core::model::normalize_cc_actors(actor_ids);
            let mut existing: Vec<String> = select_cc_actor_ids(&self.pool, instance_id).await?;
            for actor in &actors {
                if existing.iter().any(|a| a == actor) {
                    continue;
                }
                // 同一次调用内的重复也算"已存在"，只落一行
                existing.push(actor.clone());
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

    /// issues/141 G2 写侧判重的读侧（覆写 trait default）：逐行返回，**不加 DISTINCT**
    /// ——判重只看"这个人在这条实例上有没有行"，存量重复行原样留着。
    fn find_cc_actor_ids(&self, instance_id: i64) -> JeeflowResult<Vec<String>> {
        self.block_on(async { select_cc_actor_ids(&self.pool, instance_id).await })
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
            // issues/129：空 operator → 空页，且不再下推 `(? IS NULL OR …)` 旁路。
            // 只补门面兜底不够：自定义 SPI 仓储/直连仓储传空时，旁路仍会把全库摊出去。
            let op = match query.operator.as_deref().map(str::trim) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => return Ok(PageResult::new(page_num, page_size, 0, Vec::new())),
            };
            // m_ 过滤下推（issues/106）：白名单条件拼进 COUNT 与 SELECT，bind 顺序 operator → filters → limit
            let (frags, fvals) = jeeflow_core::filter_sql::build_filter_where(&query.filters, resolve_task_col);
            let where_extra = if frags.is_empty() { String::new() } else { format!(" AND {}", frags.join(" AND ")) };
            let count_sql = format!(
                "SELECT COUNT(DISTINCT t.id) AS cnt \
                 FROM wf_process_task t \
                 INNER JOIN wf_process_task_actor ta ON t.id = ta.process_task_id \
                 INNER JOIN wf_process_instance pi ON t.process_instance_id = pi.id \
                 LEFT JOIN wf_process_define pd ON pi.process_define_id = pd.id \
                 WHERE t.task_state = 10 AND ta.actor_id = ?{where_extra}"
            );
            let mut count_q = sqlx::query(&count_sql).bind(op.clone());
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
                 WHERE t.task_state = 10 AND ta.actor_id = ?{where_extra} \
                 ORDER BY t.id DESC LIMIT ? OFFSET ?"
            );
            let mut rows_q = sqlx::query(&select_sql).bind(op.clone());
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
            // 「我已办」三处判据（issues/117，owner 2026-09-21 拍板；内存仓
            // `jeeflow-core/src/memory.rs::page_done_tasks` 同判据，不得"换仓储就换答案"）：
            // ① 状态集合 `t.task_state <> 10`（六栈家族口径）＝"我经手过且不再是我待办"，
            //    含撤回 30 / 终止 40 / 废弃 99。原 `= 20` 会让撤回单从待办与已办两头同时
            //    消失（issues/113 刚把撤回态统一成 30），并掩盖 issues/114 §6.2 那条
            //    "转办覆写 actor_id → 撤回后冒单"在本栈的复现面。
            // ② 归属只按 `t.operator = ?`：原实现多一条 `OR t.create_user = ?`，是契约 §2.5
            //    **点名禁止**的偏宽写法（"我发起但非我办理"被算进我的已办）。
            // ③ 删掉原 `? IS NULL OR …` 空值旁路：operator 为空 → **返回空页**（原来会把
            //    全库已办摊给调用方）。本轮只堵泄漏，不改硬必填（另轮收紧）。
            let op = query.operator.as_deref().map(str::trim).unwrap_or("");
            if op.is_empty() {
                // 与内存仓同一姿势：空值直接空页，不下推 SQL（SQL 里 `= ''` 会漏匹配 NULL、
                // 却可能命中脏空串行，两边答案就分叉了）。
                return Ok(PageResult::new(page_num, page_size, 0, Vec::new()));
            }
            // m_ 过滤下推（issues/106）：白名单条件拼进 COUNT 与 SELECT，bind 顺序 operator → filters → limit
            let (frags, fvals) = jeeflow_core::filter_sql::build_filter_where(&query.filters, resolve_task_col);
            let where_extra = if frags.is_empty() { String::new() } else { format!(" AND {}", frags.join(" AND ")) };
            let count_sql = format!(
                "SELECT COUNT(*) AS cnt \
                 FROM wf_process_task t \
                 INNER JOIN wf_process_instance pi ON t.process_instance_id = pi.id \
                 LEFT JOIN wf_process_define pd ON pi.process_define_id = pd.id \
                 WHERE t.task_state <> 10 AND t.operator = ?{where_extra}"
            );
            let mut count_q = sqlx::query(&count_sql).bind(op);
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
                 WHERE t.task_state <> 10 AND t.operator = ?{where_extra} \
                 ORDER BY t.id DESC LIMIT ? OFFSET ?"
            );
            let mut rows_q = sqlx::query(&select_sql).bind(op);
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
            // issues/129：空 operator → 空页，且不再下推 `(? IS NULL OR …)` 旁路。
            // 只补门面兜底不够：自定义 SPI 仓储/直连仓储传空时，旁路仍会把全库摊出去。
            let op = match query.operator.as_deref().map(str::trim) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => return Ok(PageResult::new(page_num, page_size, 0, Vec::new())),
            };
            // m_ 过滤下推（issues/106）：bind 顺序 operator×2 → filters → limit
            let (frags, fvals) = jeeflow_core::filter_sql::build_filter_where(&query.filters, resolve_instance_col);
            let where_extra = if frags.is_empty() { String::new() } else { format!(" AND {}", frags.join(" AND ")) };
            let count_sql = format!(
                "SELECT COUNT(*) AS cnt \
                 FROM wf_process_instance pi \
                 LEFT JOIN wf_process_define pd ON pi.process_define_id = pd.id \
                 WHERE pi.operator = ?{where_extra}"
            );
            let mut count_q = sqlx::query(&count_sql).bind(op.clone());
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
                 WHERE pi.operator = ?{where_extra} \
                 ORDER BY pi.id DESC LIMIT ? OFFSET ?"
            );
            let mut rows_q = sqlx::query(&select_sql).bind(op.clone());
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

    /// 我的抄送（`processInstance/ccList` 取数腿）。
    ///
    /// **issues/141 G1 归属条件必填**（spec 06 §2.5）：判据走
    /// [`jeeflow_core::model::has_effective_cc_ownership`]，与内存仓同一条——归属列
    /// `cc.actor_id` 没有有效条件（整条没给 / 空值）⇒ **空页**。
    /// 旧形状是 `WHERE cc.actor_id = ?` 只认 `query.operator` 一个通道，`m_cc_actorId_EQ_x`
    /// 那条条件被白名单静默丢掉＝"这条不加"，与内存仓同一查询返 0 行——同一栈两仓储两个答案，
    /// 正是 issues/117 场景 27 立过法的那一类。
    ///
    /// issues/129（空 operator 不得折叠成"看全部"）那一档由同一条判据接住：
    /// 空 operator 且没有别的归属条件 ⇒ 空页，且不再下推 `(? IS NULL OR …)` 旁路。
    fn page_cc_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> {
        self.block_on(async {
            let (page_num, page_size, offset) = page_bounds(query);
            if !jeeflow_core::model::has_effective_cc_ownership(query) {
                return Ok(PageResult::new(page_num, page_size, 0, Vec::new()));
            }
            // 归属谓词的两个通道都落到 `cc.actor_id`：①operator，②打在 cc.actor_id 上的 m_ 条件
            // （下面 resolve_cc_instance_col 把它解析进白名单）。两通道同时给＝AND。
            let mut conds: Vec<String> = Vec::new();
            let mut vals: Vec<String> = Vec::new();
            if let Some(op) = query.operator.as_deref().map(str::trim) {
                if !op.is_empty() {
                    conds.push("cc.actor_id = ?".to_string());
                    vals.push(op.to_string());
                }
            }
            // m_ 过滤下推（issues/106）：加 WHERE 后 DISTINCT 语义不受影响；bind 顺序＝条件顺序 → limit
            let (frags, fvals) =
                jeeflow_core::filter_sql::build_filter_where(&query.filters, resolve_cc_instance_col);
            conds.extend(frags);
            vals.extend(fvals);
            // 双保险：判据已保证至少有一条归属谓词；真为空宁可空页，绝不摊出全库
            if conds.is_empty() {
                return Ok(PageResult::new(page_num, page_size, 0, Vec::new()));
            }
            let where_sql = format!(" AND {}", conds.join(" AND "));

            let count_sql = format!(
                "SELECT COUNT(DISTINCT pi.id) AS cnt \
                 FROM wf_process_cc_instance cc \
                 INNER JOIN wf_process_instance pi ON cc.process_instance_id = pi.id \
                 LEFT JOIN wf_process_define pd ON pi.process_define_id = pd.id \
                 WHERE 1=1{where_sql}"
            );
            let mut count_q = sqlx::query(&count_sql);
            for v in &vals { count_q = count_q.bind(v); }
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
                 WHERE 1=1{where_sql} \
                 ORDER BY pi.id DESC LIMIT ? OFFSET ?"
            );
            let mut rows_q = sqlx::query(&select_sql);
            for v in &vals { rows_q = rows_q.bind(v); }
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
            // issues/152 ②：归属列 operator 空值 ⇒ 空页，不再下推 `(? IS NULL OR operator = ?)` 旁路
            // （形状同 page_instances 的 issues/129 那一句）。原旁路在 `Some("")` 这一档不是"不加条件"，
            // 而是**等值命中历史死行**（rust 门面 save 曾把 operator 落成空串，见 issues/152 ③）——
            // 台账页会把别人的死行摊出来；`None` 那一档则整个不过滤＝全库。两层判据都不能留。
            // 内存仓 MemoryRepository::page_surrogates 同答案（spec 06 §4.5 条款 6）。
            let op = match query.operator.as_deref().map(str::trim) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => return Ok(PageResult::new(page_num, page_size, 0, Vec::new())),
            };
            let count_row = sqlx::query(
                "SELECT COUNT(*) AS cnt FROM wf_process_surrogate WHERE operator = ?"
            )
            .bind(op.clone())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            let total: i64 = count_row.get("cnt");
            let rows = sqlx::query(
                "SELECT id, process_name, operator, surrogate, start_time, end_time, enabled, \
                        create_time, create_user, update_time, update_user \
                 FROM wf_process_surrogate \
                 WHERE operator = ? \
                 ORDER BY id DESC LIMIT ? OFFSET ?"
            )
            .bind(op)
            .bind(page_size)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(PageResult::new(page_num, page_size, total, rows.iter().map(map_surrogate).collect()))
        })
    }

    /// 委托查询（规范 06 §4.5 条款 1.4 + issues/123 的正确判序）：
    /// **每个作用域各自先按主键 id 取最新一条**（[`Self::query_newest_surrogate`]，
    /// SQL 不带任何生效判据过滤），**再由四判据裁决那一条**
    /// （[`jeeflow_core::surrogate::surrogate_hit`]，与内存仓共用同一份判据 ⇒ 条款 6 双仓同答案）。
    ///
    /// 精确作用域那条判否（或该作用域压根没有记录）时**仍要看全流程作用域的最新一条**，
    /// 不得判否即止（Java 参考实现 `JdbcProcessExtRepositoryTest#testSurrogateCrudAndGet`
    /// 钉的正是"精确已过期 → 兜底全流程委托"）。
    fn get_surrogate(&self, operator: &str, process_name: &str, time: &str) -> JeeflowResult<Option<ProcessSurrogate>> {
        self.block_on(async {
            if !process_name.is_empty() {
                if let Some(newest) = self.query_newest_surrogate(operator, process_name).await? {
                    if jeeflow_core::surrogate::surrogate_hit(
                        &newest, operator, process_name, false, time,
                    ) {
                        return Ok(Some(newest));
                    }
                }
            }
            let global = match self.query_newest_surrogate(operator, "").await? {
                Some(g) => g,
                None => return Ok(None),
            };
            if jeeflow_core::surrogate::surrogate_hit(&global, operator, process_name, true, time) {
                Ok(Some(global))
            } else {
                Ok(None)
            }
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

    /// issues/129 T1（真库 MySQL）：SQL 仓三处 page 的空 operator 必须返回空页。
    /// 这条同时承担两件别人替不了的事：
    /// ① 验"删掉 `? IS NULL OR …` 之后 bind 个数与占位符仍对齐"——不对齐会在 execute 期直接炸，
    ///    纯内存仓测试与门面测试都盖不到这条；
    /// ② 验传了人就查得到行（防把"泄漏"修成"失联"这种假修法）。
    #[tokio::test]
    async fn test_mysql_129_empty_operator_no_full_scan() {
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;

        // 独占 id 段 9129xx（共享测试库，必须先清干净，判据才不被脏行污染）
        for sql in [
            "DELETE FROM wf_process_task_actor WHERE process_task_id BETWEEN 912900 AND 912999",
            "DELETE FROM wf_process_cc_instance WHERE process_instance_id BETWEEN 912900 AND 912999",
            "DELETE FROM wf_process_task WHERE id BETWEEN 912900 AND 912999",
            "DELETE FROM wf_process_instance WHERE id BETWEEN 912900 AND 912999",
            "DELETE FROM wf_process_define WHERE id BETWEEN 912900 AND 912999",
        ] {
            sqlx::query(sql).execute(&pool).await.unwrap();
        }

        let pool2 = pool.clone();
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            let mut define = ProcessDefine {
                id: 912901, name: "rust_129_test".into(), display_name: "129 Test".into(),
                define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
                version: 1, create_time: None, create_user: Some("rust_test".into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define).unwrap();

            let mut instance = ProcessInstance {
                instance_id: 912902, parent_id: None, define_id: define.id, state: 10,
                parent_node_name: None, business_no: None, operator: "u129a".into(),
                expire_time: None, variables: jeeflow_core::json::FlowData::new(),
                tasks: vec![], create_time: None, create_user: Some("u129a".into()),
                update_time: None, update_user: None, define: None,
            };
            repo.save_instance(&mut instance).unwrap();

            let mut task = ProcessTask {
                task_id: 912903, process_instance_id: instance.instance_id,
                task_name: "t1".into(), display_name: "T1".into(),
                task_type: 0, perform_type: 0, task_state: 10,
                actor_id: None, actor_ids: vec!["u129a".into()],
                finish_time: None, expire_time: None, form_key: None,
                parent_task_id: None, variables: jeeflow_core::json::FlowData::new(),
                create_time: None, create_user: Some("u129a".into()),
                update_time: None, update_user: None,
            };
            repo.save_task(&mut task).unwrap();
            repo.add_task_actor(task.task_id, &["u129a".to_string()]).unwrap();
            repo.create_cc_instance(instance.instance_id, "u129a", &["u129cc".to_string()]).unwrap();

            // 负向：三处都不许把"没传 operator"折叠成"看全库"
            let empty = PageQuery::new(1, 10);
            assert_eq!(repo.page_instances(&empty).unwrap().record_count, 0,
                "129: page_instances 空 operator 读到了全库");
            assert_eq!(repo.page_todo_tasks(&empty).unwrap().record_count, 0,
                "129: page_todo_tasks 空 operator 读到了全库");
            assert_eq!(repo.page_cc_instances(&empty).unwrap().record_count, 0,
                "129: page_cc_instances 空 operator 读到了全库");

            // 正向：传了人必须查得到（SQL 占位符/绑定也得对，否则这里直接 panic）
            let mut mine = PageQuery::new(1, 10);
            mine.operator = Some("u129a".to_string());
            assert_eq!(repo.page_instances(&mine).unwrap().record_count, 1, "129: u129a 应有 1 条实例");
            assert_eq!(repo.page_todo_tasks(&mine).unwrap().record_count, 1, "129: u129a 应有 1 条待办");
            let mut ccq = PageQuery::new(1, 10);
            ccq.operator = Some("u129cc".to_string());
            assert_eq!(repo.page_cc_instances(&ccq).unwrap().record_count, 1, "129: u129cc 应有 1 条抄送");
        }).await;

        for sql in [
            "DELETE FROM wf_process_task_actor WHERE process_task_id BETWEEN 912900 AND 912999",
            "DELETE FROM wf_process_cc_instance WHERE process_instance_id BETWEEN 912900 AND 912999",
            "DELETE FROM wf_process_task WHERE id BETWEEN 912900 AND 912999",
            "DELETE FROM wf_process_instance WHERE id BETWEEN 912900 AND 912999",
            "DELETE FROM wf_process_define WHERE id BETWEEN 912900 AND 912999",
        ] {
            sqlx::query(sql).execute(&pool).await.unwrap();
        }
    }

    /// issues/152 ② · SQL 仓那一层的归属兜底（真库读数，T1）：`page_surrogates` 的归属通道
    /// （`query.operator`）没给或给的是空值 ⇒ **空页**。
    /// 原形状 `WHERE (? IS NULL OR operator = ?)` 有两格漏：`None` 整个不过滤＝全库台账；
    /// `Some("")` 不是"不加条件"而是**等值捞出 operator='' 的死行**（正是 ③ 门面写侧造出来的形状）。
    /// 内存仓同判据见 `jeeflow-facade::tests::test_i152_surrogate_repo_blank_ownership_returns_empty_page`
    /// （spec 06 §4.5 条款 6：同栈两仓同答案）。SKIP_MYSQL=1 时本条不跑。
    #[test]
    fn test_mysql_i152_surrogate_page_blank_operator_no_full_scan() {
        if skip_mysql() { return; }
        mysql_rt().block_on(async {
            let pool = connect_pool().await;
            setup_schema(&pool).await;
            sqlx::query("DELETE FROM wf_process_surrogate WHERE id BETWEEN 915201 AND 915219")
                .execute(&pool).await.unwrap();
            // 两条真人行 + 一条 operator='' 的死行（③ 的形状，空归属档绝不能把它捞出来）
            seed_surrogate(&pool, 915201, Some("leave"), "u152a", "agent152a",
                Some("2000-01-01 00:00:00"), Some("2999-12-31 23:59:59"), Some(1)).await;
            seed_surrogate(&pool, 915202, Some("leave"), "u152b", "agent152b",
                Some("2000-01-01 00:00:00"), Some("2999-12-31 23:59:59"), Some(1)).await;
            seed_surrogate(&pool, 915203, Some("leave"), "", "agent152dead",
                Some("2000-01-01 00:00:00"), Some("2999-12-31 23:59:59"), Some(1)).await;

            let repo = SqlxRepository::new(pool.clone());
            for blank in [None, Some(""), Some("   "), Some("\t")] {
                let mut q = PageQuery::new(1, 20);
                q.operator = blank.map(str::to_string);
                assert_eq!(repo.page_surrogates(&q).unwrap().record_count, 0,
                    "152: 归属值 [{:?}] 为空不得读全库、也不得捞出 operator='' 的死行", blank);
            }
            // 正向对照：真值必须出行（否则上面那串 0 是"查询恒空"假绿；bind 错位也在这里炸）
            let mut q = PageQuery::new(1, 20);
            q.operator = Some("u152a".to_string());
            let page = repo.page_surrogates(&q).unwrap();
            assert_eq!(page.record_count, 1, "152: u152a 应有 1 条委托");
            assert_eq!(page.rows[0].id, 915201, "152: 归属过滤必须落在自己那一行");

            sqlx::query("DELETE FROM wf_process_surrogate WHERE id BETWEEN 915201 AND 915219")
                .execute(&pool).await.unwrap();
        });
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
        // ⚠️ 预清理段必须**只覆盖本用例自己的 10 个 id**：原写作 `BETWEEN 900950 AND 900999`
        // 与 transfer_trace 用例（900960~900962）重叠，两条用例并行时互相删走对方的行
        // （表现为 find_task_by_id 突然查空，与本仓改动无关的时序假红）。
        sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id BETWEEN 900950 AND 900959").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_task WHERE id BETWEEN 900950 AND 900959").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id BETWEEN 900950 AND 900959").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id BETWEEN 900950 AND 900959").execute(&pool).await.unwrap();

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
        // ⚠️ 同上：本用例独占 900960~900969，不与 withdraw_cascade（900950~900959）互删。
        sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id BETWEEN 900960 AND 900969").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_task WHERE id BETWEEN 900960 AND 900969").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id BETWEEN 900960 AND 900969").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id BETWEEN 900960 AND 900969").execute(&pool).await.unwrap();

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

    // ═══════════════════════════════════════════════════════
    // issues/116 批次 D（委托自动生效 + 四判据双仓对拍）
    // issues/117（「我已办」三判据）· 160 真机 MySQL，库 jeeflow
    //   ID 段：定义/实例/任务 9011xx，委托台账 911101~911199
    //   ⚠️ 必须避开本模块既有用例的**批量 DELETE 段**（900950~900999 被 withdraw/transfer
    //   两条用例按区间预清理，撞上去就是并发时序性假红：任务行被隔壁删走 → 已办查空）；
    //   910xxx 留给 Go 栈的委托用例，911xxx 是本栈自有段。
    // ═══════════════════════════════════════════════════════

    /// 引擎用 multi_thread 运行时（`SqlxRepository` 的同步 SPI 内部是
    /// `block_in_place` + `Handle::block_on`，在 current_thread 运行时上会直接 panic，
    /// 故本组用 `Runtime::new()`（multi-thread）自建，不再套 `run_sync`）。
    fn mysql_rt() -> tokio::runtime::Runtime {
        tokio::runtime::Runtime::new().expect("tokio runtime")
    }

    /// 直插一条委托台账行——**为的是能造出 NULL 列**（门面写入只会给空串），
    /// 而判据①明确要求 `process_name IS NULL OR process_name = ''` 同答案。
    async fn seed_surrogate(
        pool: &MySqlPool,
        id: i64,
        process_name: Option<&str>,
        operator: &str,
        surrogate: &str,
        start: Option<&str>,
        end: Option<&str>,
        enabled: Option<i32>,
    ) {
        sqlx::query(
            "INSERT INTO wf_process_surrogate (id, process_name, operator, surrogate, \
                    start_time, end_time, enabled, create_time, create_user) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(id).bind(process_name).bind(operator).bind(surrogate)
        .bind(start).bind(end).bind(enabled)
        .bind("2026-09-21 00:00:00").bind("rust_test")
        .execute(pool).await.unwrap();
    }

    /// 按定义 id 级联清掉本组用例造的实例/任务/参与者（引擎生成的 id 是雪花，不可预知）。
    async fn clean_by_define(pool: &MySqlPool, define_id: i64) {
        sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id IN \
                     (SELECT id FROM wf_process_task WHERE process_instance_id IN \
                      (SELECT id FROM wf_process_instance WHERE process_define_id = ?))")
            .bind(define_id).execute(pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_task WHERE process_instance_id IN \
                     (SELECT id FROM wf_process_instance WHERE process_define_id = ?)")
            .bind(define_id).execute(pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE process_define_id = ?")
            .bind(define_id).execute(pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id = ?")
            .bind(define_id).execute(pool).await.unwrap();
    }

    /// 读某任务在 `wf_process_task_actor` 里的参与者（排序后对账）。
    async fn actor_rows(pool: &MySqlPool, task_id: i64) -> Vec<String> {
        let rows = sqlx::query("SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = ?")
            .bind(task_id).fetch_all(pool).await.unwrap();
        let mut v: Vec<String> = rows.iter().map(|r| r.get::<String, _>("actor_id")).collect();
        v.sort();
        v
    }

    /// **双仓对拍**（契约 06 §4.5 条款 6）：真机 SQL 仓跑与内存仓
    /// （`jeeflow-core/src/memory.rs::test_get_surrogate_parity_matrix_i116`）**同一张**
    /// 判据矩阵 `jeeflow_core::surrogate::parity`（14 行 × 15 组期望）。
    /// 修复前本栈 SQL 侧缺"空 processName 全流程兜底"与"自委托过滤"两条判据。
    #[test]
    fn test_mysql_i116_surrogate_query_parity() {
        if skip_mysql() { return; }
        use jeeflow_core::surrogate::parity;
        mysql_rt().block_on(async {
            let pool = connect_pool().await;
            setup_schema(&pool).await;
            sqlx::query("DELETE FROM wf_process_surrogate WHERE id BETWEEN 911101 AND 911199")
                .execute(&pool).await.unwrap();
            for row in parity::ROWS {
                seed_surrogate(&pool, row.id, row.process_name, row.operator, row.surrogate,
                    row.start_time, row.end_time, row.enabled).await;
            }

            let repo = SqlxRepository::new(pool.clone());
            for exp in parity::EXPECT {
                let got = repo
                    .get_surrogate(exp.operator, exp.process_name, exp.time)
                    .unwrap()
                    .map(|h| h.id);
                assert_eq!(
                    got, exp.hit_id,
                    "SQL 仓判据矩阵[{}] operator={} process_name={:?} time={:?} → 期望 {:?} 实得 {:?}",
                    exp.note, exp.operator, exp.process_name, exp.time, exp.hit_id, got
                );
            }

            // 直读库列自证兜底腿真的读到了 NULL 行（不是"恰好空串相等"）
            let pn: Option<String> = sqlx::query("SELECT process_name FROM wf_process_surrogate WHERE id = 911104")
                .fetch_one(&pool).await.unwrap().get("process_name");
            assert!(pn.is_none(), "911104 的 process_name 必须真的是库内 NULL（实得 {:?}）", pn);

            sqlx::query("DELETE FROM wf_process_surrogate WHERE id BETWEEN 911101 AND 911199")
                .execute(&pool).await.unwrap();
        });
    }

    /// 条款 2 ⚠️ 真机铁证：引擎建单后**代理人必须出现在 `wf_process_task_actor` 真行里**
    /// （Java 首版把补写挂在 taskId 分配前 → 打在空 id 上静默无效，只看返回码发现不了）。
    /// 同时钉住条款 1.1（取流程模型 name 而非 define.name）与判据① 的库里 NULL 兜底腿。
    #[test]
    fn test_mysql_i116_surrogate_agent_lands_in_task_actor() {
        if skip_mysql() { return; }
        mysql_rt().block_on(async {
            let pool = connect_pool().await;
            setup_schema(&pool).await;
            let define_id = 901101i64;
            clean_by_define(&pool, define_id).await;
            sqlx::query("DELETE FROM wf_process_surrogate WHERE id BETWEEN 911201 AND 911219")
                .execute(&pool).await.unwrap();

            // 流程 JSON 的 name = "rust_i116_model"；定义行故意叫 "rust_i116_decoy_define"
            let mut define = ProcessDefine {
                id: define_id, name: "rust_i116_decoy_define".into(),
                display_name: "i116 委托".into(), define_type: "approval".into(), state: 1,
                content: r#"{"name":"rust_i116_model","displayName":"i116","type":"approval",
                    "nodes":[{"id":"start","type":"snaker:start","text":{"value":"s"}},
                             {"id":"apply","type":"snaker:task","text":{"value":"申请"},"properties":{"assignee":"applicant"}},
                             {"id":"task1","type":"snaker:task","text":{"value":"上级审批"},"properties":{"assignee":"leader"}},
                             {"id":"end","type":"snaker:end","text":{"value":"e"}}],
                    "edges":[{"id":"e1","sourceNodeId":"start","targetNodeId":"apply"},
                             {"id":"e2","sourceNodeId":"apply","targetNodeId":"task1"},
                             {"id":"e3","sourceNodeId":"task1","targetNodeId":"end"}]}"#
                    .as_bytes().to_vec(),
                version: 1, create_time: None, create_user: Some("rust_test".into()),
                update_time: None, update_user: None,
            };
            let repo = Arc::new(SqlxRepository::new(pool.clone()));
            repo.save_define(&mut define).unwrap();
            assert_eq!(define.id, define_id);

            // ① 精确腿（模型名）② 全流程兜底腿（库里 process_name = NULL）
            // ③ define.name 侧诱饵（不得命中）④ 停用
            //
            // ⚠️ issues/123 后 id 序**有语义**：同一 operator+processName 作用域内由
            // **最新一条（id 最大）**交四判据裁决。停用行 ④ 与有效行 ① 同属
            // `leader`@`rust_i116_model`，所以停用行必须落在**更小 id**（911201）上——
            // 它是"旧的一条停用"，被更新的有效设置正确盖住；若把它排成最新一条，按规范本用例
            // 的 ①② 两条主断言必然读成"不并入"（那才是修好后的正确行为，见
            // `test_mysql_i123_newest_invalid_beats_older_valid`）。
            // "停用不生效"本身由判据矩阵（`surrogate::parity` 的 zhouqi@off / n9off 两格）钉。
            seed_surrogate(&pool, 911201, Some("rust_i116_model"), "leader", "agentDisabled",
                None, None, Some(0)).await;
            seed_surrogate(&pool, 911202, None, "applicant", "agentAllFlow",
                Some("2000-01-01 00:00:00"), Some("2999-12-31 23:59:59"), Some(1)).await;
            seed_surrogate(&pool, 911203, Some("rust_i116_decoy_define"), "leader", "agentOnDefineName",
                None, None, Some(1)).await;
            seed_surrogate(&pool, 911204, Some("rust_i116_model"), "leader", "agentOnModelName",
                Some("2000-01-01 00:00:00"), Some("2999-12-31 23:59:59"), Some(1)).await;

            let mk_engine = |auto_on: bool| {
                let ctx = jeeflow_core::context::ServiceContext::new()
                    .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
                    .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
                    .with_id_generator(Arc::new(jeeflow_core::id_gen::DefaultIdGenerator::new(1)))
                    .with_surrogate_auto_apply(auto_on);
                jeeflow_core::engine::JeeflowEngineImpl::new(ctx)
            };

            // ── 默认开启：发起 + 推进两条路径都要落进参与者表 ──
            let engine = mk_engine(true);
            let inst = engine
                .start_async(define_id, "applicant", &jeeflow_core::json::FlowData::new())
                .await
                .expect("建单不得因委托能力被打断");
            let apply_task = repo.find_doing_tasks(inst.instance_id, &[])
                .unwrap().into_iter().find(|t| t.task_name == "apply").expect("应有 apply 任务");
            assert_eq!(
                actor_rows(&pool, apply_task.task_id).await,
                vec!["agentAllFlow".to_string(), "applicant".to_string()],
                "① 发起路径：库里 process_name 为 NULL 的全流程委托必须命中并落进 wf_process_task_actor"
            );
            engine.execute_task_async(apply_task.task_id, "applicant", &jeeflow_core::json::FlowData::new())
                .await.unwrap();
            let task1 = repo.find_doing_tasks(inst.instance_id, &[])
                .unwrap().into_iter().find(|t| t.task_name == "task1").expect("推进应建 task1");
            let actors = actor_rows(&pool, task1.task_id).await;
            assert_eq!(
                actors,
                vec!["agentOnModelName".to_string(), "leader".to_string()],
                "② 推进路径新单同样落库，且取的是**流程模型 name**（命中 define.name 侧即取错列）"
            );
            // 真机列自证：进行中任务的 operator 列仍为 NULL（并入只落在参与者表）
            let op: Option<String> = sqlx::query("SELECT operator FROM wf_process_task WHERE id = ?")
                .bind(task1.task_id).fetch_one(&pool).await.unwrap().get("operator");
            assert!(op.is_none(), "进行中任务 operator 列应恒无值，实得 {:?}", op);

            // ── 显式关闭：回到"仅台账"，参与者表只有原人 ──
            let engine_off = mk_engine(false);
            let inst2 = engine_off
                .start_async(define_id, "applicant", &jeeflow_core::json::FlowData::new())
                .await.unwrap();
            let apply2 = repo.find_doing_tasks(inst2.instance_id, &[])
                .unwrap().into_iter().find(|t| t.task_name == "apply").unwrap();
            assert_eq!(
                actor_rows(&pool, apply2.task_id).await,
                vec!["applicant".to_string()],
                "③ with_surrogate_auto_apply(false) 后不得并入代理人（关闭位真的接进了建单路径）"
            );

            // Cleanup
            clean_by_define(&pool, define_id).await;
            sqlx::query("DELETE FROM wf_process_surrogate WHERE id BETWEEN 911201 AND 911219")
                .execute(&pool).await.unwrap();
        });
    }

    /// issues/123 · 规范 06 §4.5 条款 1.4 的 **sqlx 路**（内存路见
    /// `jeeflow-core/src/memory.rs::test_get_surrogate_i123_*`；共用判据矩阵见
    /// `jeeflow_core::surrogate::parity`，两侧同跑见 `test_mysql_i116_surrogate_query_parity`）。
    ///
    /// A 格：同一 operator+processName 先落"窗内+enabled=1"，再落一条更"新"的无效记录
    /// （窗外已过期 / 窗外未开始 / enabled=0 / enabled 脏值 2 / 自委托 五形）⇒ 建单时代理人
    /// **不**并入，且旧的那条有效记录**不得复活**。
    ///
    /// 旧形状（SQL 里先 `AND enabled = 1 AND surrogate <> operator` + 窗口条件，剩下的才
    /// `ORDER BY id DESC`）在这五格上全部读成"命中旧的那条"⇒ 本用例必红；
    /// 那正是 13 张交付物在 L2-17/L2-18 上恒并入的成因。
    #[test]
    fn test_mysql_i123_newest_invalid_beats_older_valid() {
        if skip_mysql() { return; }
        const SEG: &str = "id BETWEEN 911301 AND 911309";
        const PROC: &str = "rust_i123";
        const OLD_VALID_AGENT: &str = "i123OldValidAgent";
        mysql_rt().block_on(async {
            let pool = connect_pool().await;
            setup_schema(&pool).await;
            sqlx::query(&format!("DELETE FROM wf_process_surrogate WHERE {}", SEG))
                .execute(&pool).await.unwrap();

            // (operator, 新行的 surrogate, start, end, enabled, 说明)
            // 时间基准走引擎钟 `current_time_str()`（issues/120），窗宽 2000~2999 与时区无关。
            let now = jeeflow_core::model::current_time_str();
            let wide = (Some("2000-01-01 00:00:00"), Some("2999-12-31 23:59:59"));
            let cases: Vec<(&str, &str, Option<&str>, Option<&str>, Option<i32>, &str)> = vec![
                ("i123out",     "i123NewExpired", Some("2020-01-01 00:00:00"), Some("2020-12-31 23:59:59"), Some(1), "窗外（已过期）"),
                ("i123future",  "i123NewFuture",  Some("2030-01-01 00:00:00"), Some("2030-12-31 23:59:59"), Some(1), "窗外（未开始）"),
                ("i123off",     "i123NewOff",     wide.0, wide.1, Some(0), "enabled=0"),
                ("i123dirty",   "i123NewDirty",   wide.0, wide.1, Some(2), "enabled 脏值 2（契约：只认 1）"),
                ("i123self",    "i123self",       wide.0, wide.1, Some(1), "自委托（surrogate = operator）"),
            ];
            for (op, agent, start, end, enabled, why) in cases {
                // 旧：窗内 + enabled=1（宽窗，与时区/时钟基准无关）
                seed_surrogate(&pool, 911301, Some(PROC), op, OLD_VALID_AGENT,
                    wide.0, wide.1, Some(1)).await;
                // 新：id 更大 ⇒ 由它裁决
                seed_surrogate(&pool, 911302, Some(PROC), op, agent, start, end, enabled).await;

                let repo = SqlxRepository::new(pool.clone());
                let got = repo.get_surrogate(op, PROC, &now).unwrap().map(|h| h.id);
                assert_eq!(
                    got, None,
                    "SQL 仓：最新一条 {} ⇒ 不得命中，更不得复活旧的窗内有效行（id=911301 agent={}）",
                    why, OLD_VALID_AGENT,
                );

                sqlx::query(&format!("DELETE FROM wf_process_surrogate WHERE {}", SEG))
                    .execute(&pool).await.unwrap();
            }
        });
    }

    /// ↑ 的窗口腿（判据②）单独一条：SQL 侧不判窗时窗内行**必须**命中，
    /// 钉住"最新一条判否"不是因为我把窗口条件整个删掉。
    #[test]
    fn test_mysql_i123_newest_out_of_window_beats_older_valid_with_time() {
        if skip_mysql() { return; }
        const PROC: &str = "rust_i123";
        mysql_rt().block_on(async {
            let pool = connect_pool().await;
            setup_schema(&pool).await;
            sqlx::query("DELETE FROM wf_process_surrogate WHERE id BETWEEN 911311 AND 911319")
                .execute(&pool).await.unwrap();
            // 旧：窗内有效（宽窗）；新：整扇窗在过去
            seed_surrogate(&pool, 911311, Some(PROC), "i123win", "i123OldValidAgent",
                Some("2000-01-01 00:00:00"), Some("2999-12-31 23:59:59"), Some(1)).await;
            seed_surrogate(&pool, 911312, Some(PROC), "i123win", "i123NewExpired",
                Some("2020-01-01 00:00:00"), Some("2020-12-31 23:59:59"), Some(1)).await;

            let repo = SqlxRepository::new(pool.clone());
            let now = jeeflow_core::model::current_time_str();
            assert_eq!(
                repo.get_surrogate("i123win", PROC, &now).unwrap().map(|h| h.id),
                None,
                "SQL 仓：最新一条窗外 ⇒ 判否，旧窗内行不得复活",
            );
            // B 格：作用域内只有一条窗内 enabled=1 ⇒ 必须命中（防修成恒不并）
            sqlx::query("DELETE FROM wf_process_surrogate WHERE id BETWEEN 911312 AND 911312")
                .execute(&pool).await.unwrap();
            assert_eq!(
                repo.get_surrogate("i123win", PROC, &now).unwrap().map(|h| h.id),
                Some(911311),
                "SQL 仓 B 格：唯一一条窗内有效委托必须命中",
            );
            sqlx::query("DELETE FROM wf_process_surrogate WHERE id BETWEEN 911311 AND 911319")
                .execute(&pool).await.unwrap();
        });
    }

    /// issues/123 A/B 格的**建单落库铁证**（真机 `wf_process_task_actor` 行）：
    /// 最新一条无效 ⇒ 参与者表只有原人；只有一条窗内 enabled=1 ⇒ 代理人在参与者表里。
    /// 走 `test_mysql_i116_surrogate_agent_lands_in_task_actor` 同一条引擎收口路径。
    #[test]
    fn test_mysql_i123_newest_invalid_not_merged_into_task_actor() {
        if skip_mysql() { return; }
        const DEFINE_ID: i64 = 901151;
        const PROC: &str = "rust_i123_model";
        mysql_rt().block_on(async {
            let pool = connect_pool().await;
            setup_schema(&pool).await;
            clean_by_define(&pool, DEFINE_ID).await;
            sqlx::query("DELETE FROM wf_process_surrogate WHERE id BETWEEN 911321 AND 911339")
                .execute(&pool).await.unwrap();

            let mut define = ProcessDefine {
                id: DEFINE_ID, name: PROC.into(), display_name: "i123 委托判序".into(),
                define_type: "approval".into(), state: 1,
                content: format!(r#"{{"name":"{}","displayName":"i123","type":"approval",
                    "nodes":[{{"id":"start","type":"snaker:start","text":{{"value":"s"}}}},
                             {{"id":"apply","type":"snaker:task","text":{{"value":"申请"}},"properties":{{"assignee":"applicant"}}}},
                             {{"id":"task1","type":"snaker:task","text":{{"value":"上级审批"}},"properties":{{"assignee":"leader"}}}},
                             {{"id":"end","type":"snaker:end","text":{{"value":"e"}}}}],
                    "edges":[{{"id":"e1","sourceNodeId":"start","targetNodeId":"apply"}},
                             {{"id":"e2","sourceNodeId":"apply","targetNodeId":"task1"}},
                             {{"id":"e3","sourceNodeId":"task1","targetNodeId":"end"}}]}}"#, PROC)
                    .into_bytes(),
                version: 1, create_time: None, create_user: Some("rust_test".into()),
                update_time: None, update_user: None,
            };
            let repo = Arc::new(SqlxRepository::new(pool.clone()));
            repo.save_define(&mut define).unwrap();

            let mk_engine = || {
                let ctx = jeeflow_core::context::ServiceContext::new()
                    .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
                    .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
                    .with_id_generator(Arc::new(jeeflow_core::id_gen::DefaultIdGenerator::new(1)));
                jeeflow_core::engine::JeeflowEngineImpl::new(ctx)
            };

            // 发起 + 办结 apply ⇒ 返回推进建出的 task1（参与者 = leader）
            async fn i123_advance_to_task1(
                engine: &jeeflow_core::engine::JeeflowEngineImpl,
                repo: &Arc<SqlxRepository>,
                define_id: i64,
            ) -> i64 {
                let inst = engine
                    .start_async(define_id, "applicant", &jeeflow_core::json::FlowData::new())
                    .await
                    .expect("建单不得因委托能力被打断");
                let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
                    .into_iter().find(|t| t.task_name == "apply").expect("应有 apply 任务");
                engine.execute_task_async(apply.task_id, "applicant", &jeeflow_core::json::FlowData::new())
                    .await.unwrap();
                repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
                    .into_iter().find(|t| t.task_name == "task1").expect("推进应建 task1").task_id
            }

            // ── A：先窗内有效，再一条更"新"的停用 ⇒ 参与者表只有 leader ──
            seed_surrogate(&pool, 911321, Some(PROC), "leader", "i123OldValidAgent",
                Some("2000-01-01 00:00:00"), Some("2999-12-31 23:59:59"), Some(1)).await;
            seed_surrogate(&pool, 911322, Some(PROC), "leader", "i123NewOff",
                Some("2000-01-01 00:00:00"), Some("2999-12-31 23:59:59"), Some(0)).await;
            let engine_a = mk_engine();
            let task_id = i123_advance_to_task1(&engine_a, &repo, DEFINE_ID).await;
            assert_eq!(
                actor_rows(&pool, task_id).await,
                vec!["leader".to_string()],
                "123 A 格：最新一条 enabled=0 ⇒ 建单不并入，旧的窗内有效行也不得复活",
            );
            clean_by_define(&pool, DEFINE_ID).await;
            sqlx::query("DELETE FROM wf_process_surrogate WHERE id BETWEEN 911321 AND 911339")
                .execute(&pool).await.unwrap();
            // clean_by_define 把定义行也删了 ⇒ B 段重新落一次（同 id 同内容）
            repo.save_define(&mut define).unwrap();

            // ── B：作用域内只有一条窗内 enabled=1 ⇒ 代理人必须真的落进参与者表 ──
            seed_surrogate(&pool, 911331, Some(PROC), "leader", "i123OnlyAgent",
                Some("2000-01-01 00:00:00"), Some("2999-12-31 23:59:59"), Some(1)).await;
            let engine_b = mk_engine();
            let task_id = i123_advance_to_task1(&engine_b, &repo, DEFINE_ID).await;
            assert_eq!(
                actor_rows(&pool, task_id).await,
                vec!["i123OnlyAgent".to_string(), "leader".to_string()],
                "123 B 格：唯一一条窗内 enabled=1 的委托必须并入（防修成恒不并）",
            );

            clean_by_define(&pool, DEFINE_ID).await;
            sqlx::query("DELETE FROM wf_process_surrogate WHERE id BETWEEN 911321 AND 911339")
                .execute(&pool).await.unwrap();
        });
    }

    /// issues/117 三判据的 **sqlx 路**（内存路见 `memory.rs::test_page_done_tasks_predicates_i117`）：
    /// 20/30/40/99 全进、`create_user=我` 但 `operator≠我` 的行不进、operator 传空 → 空页。
    #[test]
    fn test_mysql_i117_done_list_predicates() {
        if skip_mysql() { return; }
        mysql_rt().block_on(async {
            let pool = connect_pool().await;
            setup_schema(&pool).await;
            let (define_id, instance_id) = (901121i64, 901122i64);
            clean_by_define(&pool, define_id).await;

            let repo = SqlxRepository::new(pool.clone());
            let mut define = ProcessDefine {
                id: define_id, name: "rust_i117".into(), display_name: "i117".into(),
                define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
                version: 1, create_time: None, create_user: Some("rust_test".into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define).unwrap();
            let mut inst = ProcessInstance {
                instance_id, parent_id: None, define_id, state: 20, parent_node_name: None,
                business_no: None, operator: "me".into(), expire_time: None,
                variables: jeeflow_core::json::FlowData::new(), tasks: vec![],
                create_time: None, create_user: Some("me".into()),
                update_time: None, update_user: None, define: None,
            };
            repo.save_instance(&mut inst).unwrap();

            let put = |id: i64, state: i32, operator: Option<&str>, create_user: &str| {
                let mut task = ProcessTask {
                    task_id: id, process_instance_id: instance_id,
                    task_name: format!("n{}", id), display_name: "N".into(),
                    task_type: 0, perform_type: 0, task_state: state,
                    actor_id: operator.map(str::to_string),
                    actor_ids: operator.map(str::to_string).into_iter().collect(),
                    finish_time: None, expire_time: None, form_key: None, parent_task_id: None,
                    variables: jeeflow_core::json::FlowData::new(), create_time: None,
                    create_user: Some(create_user.to_string()), update_time: None, update_user: None,
                };
                repo.save_task(&mut task).unwrap();
                task.task_id
            };
            // 我经手过的四种非待办态 + 进行中（不该进）+ 他人办结（诱饵：create_user=me）
            // + 脏空串办理人（诱饵：钉住"空值查询须短路成空页"——少了那道短路，SQL 的
            //   `t.operator = ''` 会把这行摊给一个 operator 传空白的调用方，内存仓则不会）
            for (id, state, op, cu) in [
                (901123i64, TaskState::Finished.code(), Some("me"), "me"),
                (901124, TaskState::Withdraw.code(), Some("me"), "me"),
                (901125, TaskState::Interrupt.code(), Some("me"), "other"),
                (901126, TaskState::Abandon.code(), Some("me"), "me"),
                (901127, TaskState::Doing.code(), Some("me"), "me"),
                (901128, TaskState::Finished.code(), Some("other"), "me"),
                (901129, TaskState::Finished.code(), Some(""), "me"),
            ] {
                put(id, state, op, cu);
            }

            let ids_for = |op: Option<&str>| {
                let mut q = PageQuery::new(1, 50);
                q.operator = op.map(str::to_string);
                let page = repo.page_done_tasks(&q).unwrap();
                assert_eq!(page.record_count as usize, page.rows.len(), "recordCount 与行数须自洽");
                let mut ids: Vec<i64> = page.rows.iter().filter(|r| r.process_instance_id == instance_id)
                    .map(|r| r.id).collect();
                ids.sort();
                ids
            };
            assert_eq!(
                ids_for(Some("me")),
                vec![901123i64, 901124, 901125, 901126],
                "SQL 仓须与内存仓同判据：20/30/40/99 全进，进行中与被他人办理的行不进"
            );
            assert_eq!(ids_for(Some("other")), vec![901128], "办理人口径不受影响");
            assert!(ids_for(None).is_empty(), "operator 缺省不得返回全库已办");
            assert!(ids_for(Some("   ")).is_empty(), "operator 全空白同空值：空页");

            clean_by_define(&pool, define_id).await;
        });
    }

    /// issues/114 §6.2 同尺子回归（sqlx 路）：转办**严禁覆写 operator 列** + 撤回后
    /// 该单不得凭空出现在被摘走人的已办里。本栈 doneList 原为 `=20`，撤回行（30）根本不进
    /// 集合，这条冒单路径显不出来；issues/117 改 `<> 10` 后与另六栈同判据，用例才真的打在它上。
    #[test]
    fn test_mysql_i114_transfer_then_withdraw_not_in_fromactor_done_list() {
        if skip_mysql() { return; }
        mysql_rt().block_on(async {
            let pool = connect_pool().await;
            setup_schema(&pool).await;
            let (define_id, instance_id, task_id, done_task_id) = (901141i64, 901142i64, 901143i64, 901144i64);
            clean_by_define(&pool, define_id).await;

            let repo = SqlxRepository::new(pool.clone());
            let mut define = ProcessDefine {
                id: define_id, name: "rust_i114tr".into(), display_name: "i114tr".into(),
                define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
                version: 1, create_time: None, create_user: Some("rust_test".into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define).unwrap();
            let mut inst = ProcessInstance {
                instance_id, parent_id: None, define_id, state: 10, parent_node_name: None,
                business_no: None, operator: "applicant".into(), expire_time: None,
                variables: jeeflow_core::json::FlowData::new(), tasks: vec![],
                create_time: None, create_user: Some("applicant".into()),
                update_time: None, update_user: None, define: None,
            };
            repo.save_instance(&mut inst).unwrap();
            let mk = |id: i64, name: &str, state: i32, operator: Option<&str>| {
                let mut t = ProcessTask {
                    task_id: id, process_instance_id: instance_id, task_name: name.into(),
                    display_name: name.into(), task_type: 0, perform_type: 0, task_state: state,
                    actor_id: operator.map(str::to_string),
                    actor_ids: operator.map(str::to_string).into_iter().collect(),
                    finish_time: None, expire_time: None, form_key: None, parent_task_id: None,
                    variables: jeeflow_core::json::FlowData::new(), create_time: None,
                    create_user: Some("applicant".into()), update_time: None, update_user: None,
                };
                repo.save_task(&mut t).unwrap();
                t
            };
            let mut doing = mk(task_id, "approve", TaskState::Doing.code(), None);
            repo.add_task_actor(task_id, &["user2".into()]).unwrap();
            // 正向对照行：user2 真办结过的一行（20 + operator=user2）必须在已办里
            mk(done_task_id, "handled", TaskState::Finished.code(), Some("user2"));

            // 转办 user2 → lisi：只动参与者表，绝不写 operator 列
            repo.remove_task_actor(task_id, &["user2".into()]).unwrap();
            repo.add_task_actor(task_id, &["lisi".into()]).unwrap();
            doing.update_user = Some("user2".into());
            repo.update_task(&doing).unwrap();
            // 撤回：Doing→Withdraw(30)，operator 仍空
            let mut t = repo.find_task_by_id(task_id).unwrap().unwrap();
            t.task_state = TaskState::Withdraw.code();
            t.update_user = Some("applicant".into());
            repo.update_task(&t).unwrap();

            let op: Option<String> = sqlx::query("SELECT operator FROM wf_process_task WHERE id = ?")
                .bind(task_id).fetch_one(&pool).await.unwrap().get("operator");
            assert!(op.is_none(), "转办/撤回后 operator 列必须仍为 NULL（冒单的唯一入口），实得 {:?}", op);

            let mut q = PageQuery::new(1, 50);
            q.operator = Some("user2".to_string());
            let page = repo.page_done_tasks(&q).unwrap();
            let ids: Vec<i64> = page.rows.iter().map(|r| r.id).collect();
            assert!(!ids.contains(&task_id),
                "user2 被转办摘走、从没办过的单不得冒进其已办：{:?}", ids);
            assert!(ids.contains(&done_task_id),
                "正向对照：user2 真办结的 {} 应在其已办里（防恒空假绿），实得 {:?}", done_task_id, ids);

            clean_by_define(&pool, define_id).await;
        });
    }

    // ═══════════════════════════════════════════════════════
    // M5（issues/125）：写库的 update_time 必须来自引擎时钟出口
    //
    // 病灶：两条 UPDATE 原先写 `update_time=NOW()`，而 MySQL 的 NOW() 取 @@session.time_zone
    // 的墙钟，同一行的 create_time 却是引擎钟写的裸墙钟 ⇒ 宿主注入东八、库会话 UTC（或反之）
    // 时两列差 8 小时，且 issues/120 辛苦收敛的"壳注入后全栈同基准"在这两列上失效。
    //
    // 判据形状（缺一条就是死格）：
    //  - 牙：注入固定钟后断 `update_time == 注入串`——旧实现会拿 DB 墙钟，必红；
    //  - 探针自证：先断 `NOW() != 注入串`，否则"等于注入串"可能只是两把钟恰好同读数（假绿）；
    //  - 同基准：同一行 create_time 与 update_time 相等（一次 update 的间隔是秒级，不该差 8 小时）；
    //  - 回归：task 路径同款；两条 SQL 的形状由 test_update_sql_has_no_db_clock 常驻盯住。
    //
    // 时钟是进程级 static，故用 ClockScope（内部互斥 + drop 复原），夹具只用 900501–900599 段。
    // ═══════════════════════════════════════════════════════

    const CLOCK125_FIXED: &str = "2026-07-15 08:30:00";

    fn clock125_fixed() -> String {
        CLOCK125_FIXED.to_string()
    }

    #[tokio::test]
    async fn test_mysql_m5_update_time_follows_engine_clock() {
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;

        sqlx::query("DELETE FROM wf_process_task WHERE id BETWEEN 900501 AND 900599").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id BETWEEN 900501 AND 900599").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id BETWEEN 900501 AND 900599").execute(&pool).await.unwrap();

        // 注入必须**先于**任何写库，且横跨 run_sync 的 blocking 线程（进程级 static 全线程可见）
        let _clock = jeeflow_core::clock::ClockScope::injected(clock125_fixed);

        // 探针自证：会话钟与注入钟若相同，下面的等值断言就分辨不出基准来源 ⇒ 无牙，先报红
        let db_now: String = sqlx::query_scalar("SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')")
            .fetch_one(&pool).await.unwrap();
        assert_ne!(db_now, CLOCK125_FIXED,
            "M5: 数据库 NOW() 恰好等于注入钟串 ⇒ 这条判据在此环境分不出两把钟，无牙");

        let pool2 = pool.clone();
        let (instance_id, task_id) = run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            let mut define = ProcessDefine {
                id: 900501, name: "rust_m5_clock".into(), display_name: "M5 Clock".into(),
                define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
                version: 1, create_time: None, create_user: Some("rust_test".into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define).unwrap();

            let mut instance = ProcessInstance {
                instance_id: 900502, parent_id: None, define_id: define.id, state: 10,
                parent_node_name: None, business_no: Some("rust125-instance".into()),
                operator: "user1".into(), expire_time: None,
                variables: jeeflow_core::json::FlowData::new(),
                tasks: vec![], create_time: None, create_user: Some("user1".into()),
                update_time: None, update_user: None, define: None,
            };
            repo.save_instance(&mut instance).unwrap();

            let mut loaded = repo.find_instance_by_id(instance.instance_id).unwrap().unwrap();
            loaded.state = 20;
            loaded.update_user = Some("user1".into());
            repo.update_instance(&loaded).unwrap();

            let mut task = ProcessTask {
                task_id: 900503, process_instance_id: instance.instance_id,
                task_name: "task1".into(), display_name: "M5 Task".into(),
                task_type: 0, perform_type: 0, task_state: 10,
                actor_id: Some("user1".into()), actor_ids: vec!["user1".into()],
                finish_time: None, expire_time: None, form_key: None,
                parent_task_id: None, variables: jeeflow_core::json::FlowData::new(),
                create_time: None, create_user: Some("user1".into()),
                update_time: None, update_user: None,
            };
            repo.save_task(&mut task).unwrap();

            let mut tloaded = repo.find_task_by_id(task.task_id).unwrap().unwrap();
            tloaded.task_state = 20;
            tloaded.finish_time = Some(jeeflow_core::clock::current_time_str());
            repo.update_task(&tloaded).unwrap();

            (instance.instance_id, task.task_id)
        }).await;

        // ── 实例：create_time 与 update_time 都必须是注入串（同一行两列同一把钟）──────────
        let row = sqlx::query("SELECT DATE_FORMAT(create_time, '%Y-%m-%d %H:%i:%s') AS ct, \
             DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s') AS ut FROM wf_process_instance WHERE id = ?")
            .bind(instance_id).fetch_one(&pool).await.unwrap();
        let ct: String = row.get("ct");
        let ut: String = row.get("ut");
        assert_eq!(ct, CLOCK125_FIXED, "M5: 实例 create_time 应为注入钟串");
        assert_eq!(ut, CLOCK125_FIXED,
            "M5: 实例 update_time 不是引擎钟串（实得 {}），说明 SQL 又把钟交给数据库了；DB 墙钟是 {}", ut, db_now);
        assert_eq!(ct, ut, "M5: 同一行两列必须同基准（差 8 小时就是本案签名）");

        // ── 任务：同款 ──────────────────────────────────────────────────────────
        let trow = sqlx::query("SELECT DATE_FORMAT(create_time, '%Y-%m-%d %H:%i:%s') AS ct, \
             DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s') AS ut FROM wf_process_task WHERE id = ?")
            .bind(task_id).fetch_one(&pool).await.unwrap();
        let tct: String = trow.get("ct");
        let tut: String = trow.get("ut");
        assert_eq!(tct, CLOCK125_FIXED, "M5: 任务 create_time 应为注入钟串");
        assert_eq!(tut, CLOCK125_FIXED,
            "M5: 任务 update_time 不是引擎钟串（实得 {}）；DB 墙钟是 {}", tut, db_now);
        assert_eq!(tct, tut, "M5: 任务同一行两列必须同基准");

        // ── 回归：finish_time 也走引擎钟（聚合根产的值经绑参落库，不被会话钟改写）─────────
        let ft: Option<String> = sqlx::query("SELECT DATE_FORMAT(finish_time, '%Y-%m-%d %H:%i:%s') AS ft FROM wf_process_task WHERE id = ?")
            .bind(task_id).fetch_one(&pool).await.unwrap().get("ft");
        assert_eq!(ft.as_deref(), Some(CLOCK125_FIXED), "M5: finish_time 应为引擎钟串");

        sqlx::query("DELETE FROM wf_process_task WHERE id = ?").bind(task_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_instance WHERE id = ?").bind(instance_id).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM wf_process_define WHERE id = 900501").execute(&pool).await.unwrap();

        // drop(_clock) 复原默认基准，别把注入钟留给同批并发的其它用例
    }

    /// 形状门禁（不连库，恒跑）：两处 UPDATE 的 SQL 文本里不许出现 NOW()，必须绑引擎钟。
    /// 静态普查门禁 dbclock_census.py 管八栈，这条管本文件不被后人顺手改回 `update_time=NOW()`。
    #[test]
    fn test_update_sql_has_no_db_clock() {
        // 本文件自读；整行注释里写 NOW() 是在解释缺陷，不算病灶 ⇒ 先剥注释再数
        let src = include_str!("lib.rs");
        for head in ["fn update_instance", "fn update_task"] {
            let i = src.find(head).unwrap_or_else(|| panic!("找不到 {} —— 方法被改名/挪走", head));
            let rest = &src[i..];
            let end = rest.find("\n    fn ").unwrap_or_else(|| panic!("{} 切不出方法边界", head));
            let body = &rest[..end];
            let code: String = body.lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(!code.contains("NOW()"),
                "{} 的代码里仍有 NOW() ⇒ 又把钟交给数据库会话时区了", head);
            assert!(code.contains("update_time=?"),
                "{} 不是绑参形状（没找到 `update_time=?`）", head);
            assert!(code.contains("current_time_str()"),
                "{} 没走引擎时钟出口 current_time_str()", head);
        }
    }

    // ═══════════════════════════════════════════════════════
    // issues/141 G1 ＋ G2 · 抄送分页归属必填／写侧判重＝幂等空操作（sqlx 真库这一支）
    //   ID 段：9141xx（本组独占，避开 900xxx／9011xx／911xxx／9129xx 既有用例段）
    //   基准形状＝jeeflow-java `3d1fc98`（JdbcProcessRepository ＋ JdbcCcOwnershipIdempotentTest）；
    //   "两仓同答案"那一档在同一趟里把内存仓（jeeflow_core::MemoryRepository）也跑一遍对拍。
    // ═══════════════════════════════════════════════════════

    const I141_ACTOR_A: &str = "u141a";
    const I141_ACTOR_B: &str = "u141b";
    const I141_ACTOR_C: &str = "u141c";
    const I141_SENDER: &str = "u141sender";

    /// 本组用例共用 9141xx 段 ＋ 同一批 actor 名 ⇒ 必须串行（共享测试库上并行跑会撞主键、
    /// 也会互相把别人的 cc 行读进来；129/117 那些用例各占独立段，本组按段太碎，直接上锁）。
    static I141_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn i141_serial() -> tokio::sync::MutexGuard<'static, ()> {
        I141_LOCK.lock().await
    }

    async fn clean_i141(pool: &MySqlPool) {
        for sql in [
            "DELETE FROM wf_process_task_actor WHERE process_task_id BETWEEN 914100 AND 914199",
            "DELETE FROM wf_process_cc_instance WHERE process_instance_id BETWEEN 914100 AND 914199",
            "DELETE FROM wf_process_task WHERE id BETWEEN 914100 AND 914199",
            "DELETE FROM wf_process_instance WHERE id BETWEEN 914100 AND 914199",
            "DELETE FROM wf_process_define WHERE id BETWEEN 914100 AND 914199",
        ] {
            sqlx::query(sql).execute(pool).await.unwrap();
        }
    }

    /// 两条实例（发起人各自不同）＋各自一条 cc 行，返回 (实例A, 实例B)。
    /// A 抄给 I141_ACTOR_A、B 抄给 I141_ACTOR_B；business_no 给非空值（NULL 列在 SQL 三值逻辑里
    /// 是"任何条件都不命中"那一档，会让"非归属列空值放行"那一格两仓各说各话）。
    async fn seed_i141(pool: &MySqlPool) -> (i64, i64) {
        clean_i141(pool).await;
        let pool2 = pool.clone();
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            let mut mk = |id: i64, name: &str, operator: &str, actor: &str| {
                let mut define = ProcessDefine {
                    id, name: name.into(), display_name: "141 抄送归属".into(),
                    define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
                    version: 1, create_time: None, create_user: Some(I141_SENDER.into()),
                    update_time: None, update_user: None,
                };
                repo.save_define(&mut define).unwrap();
                let mut inst = ProcessInstance {
                    instance_id: id + 1, parent_id: None, define_id: define.id, state: 10,
                    parent_node_name: None, business_no: Some(format!("biz-{}", id)),
                    operator: operator.into(), expire_time: None,
                    variables: jeeflow_core::json::FlowData::new(),
                    tasks: vec![], create_time: None, create_user: Some(operator.into()),
                    update_time: None, update_user: None, define: None,
                };
                repo.save_instance(&mut inst).unwrap();
                repo.create_cc_instance(inst.instance_id, I141_SENDER, &[actor.to_string()]).unwrap();
                inst.instance_id
            };
            let a = mk(914100, "rust_141_a", "i141_op_a", I141_ACTOR_A);
            let b = mk(914110, "rust_141_b", "i141_op_b", I141_ACTOR_B);
            (a, b)
        }).await
    }

    async fn cc_page(pool: &MySqlPool, q: PageQuery) -> PageResult<InstanceRow> {
        let pool2 = pool.clone();
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            repo.page_cc_instances(&q).unwrap()
        }).await
    }

    fn cc_filter(op: jeeflow_core::model::FilterOp, value: &str) -> QueryFilter {
        QueryFilter { alias: "cc".into(), op, column: "actor_id".into(), value: value.into() }
    }

    /// cc 台账原始行（判重三档①②③的读侧工具，绕开分页的实例聚合）。
    async fn cc_raw_rows(pool: &MySqlPool, instance_id: i64) -> Vec<(i64, String, i32, Option<String>, Option<String>)> {
        let rows = sqlx::query(
            "SELECT id, actor_id, state, \
                    DATE_FORMAT(create_time, '%Y-%m-%d %H:%i:%s.%f') AS ct, \
                    DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s.%f') AS ut \
             FROM wf_process_cc_instance WHERE process_instance_id = ? ORDER BY id"
        ).bind(instance_id).fetch_all(pool).await.unwrap();
        rows.iter().map(|r| (
            r.get::<i64, _>("id"),
            r.get::<String, _>("actor_id"),
            r.get::<i32, _>("state"),
            r.try_get::<Option<String>, _>("ct").ok().flatten(),
            r.try_get::<Option<String>, _>("ut").ok().flatten(),
        )).collect()
    }

    /// G1 正向对照 ＋ 缺条件档：带归属条件照旧只出我的；条件整条没给／空值 ⇒ 空页。
    #[tokio::test]
    async fn test_mysql_i141_g1_ownership_required_on_sqlx_repo() {
        let _serial = i141_serial().await;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        let (a, b) = seed_i141(&pool).await;

        // 正向：带条件 ⇒ 只出自己的那一行
        let mut mine = PageQuery::new(1, 50);
        mine.operator = Some(I141_ACTOR_A.into());
        let page = cc_page(&pool, mine).await;
        assert_eq!(page.record_count, 1, "带归属条件应命中 1 条");
        assert_eq!(page.rows[0].id, a, "命中的应是 A 那条实例");

        // 缺条件（整条没给）⇒ 空页，不得退化成 LEFT JOIN 不过滤那样返回全部实例
        let none = cc_page(&pool, PageQuery::new(1, 50)).await;
        assert_eq!(none.record_count, 0, "缺归属条件必须空页，实得 {:?}", ids_of(&none));
        assert!(none.rows.is_empty(), "空页的 rows 也必须是空集合");

        // 空值三形同档
        for blank in ["", "   ", "\t"] {
            let mut q = PageQuery::new(1, 50);
            q.operator = Some(blank.into());
            assert_eq!(cc_page(&pool, q).await.record_count, 0, "空值归属条件（{blank:?}）必须空页");
        }
        assert_ne!(a, b, "夹具前提：两条实例各一行");
        clean_i141(&pool).await;
    }

    /// G1 · 归属条件的第二通道（m_cc_actorId_EQ_xxx）：sqlx 仓旧形状把它当"这条不加"直接丢掉
    /// ⇒ 白名单外静默失效（内存仓那边同一个查询返 0 行，两仓两个答案）。
    #[tokio::test]
    async fn test_mysql_i141_g1_cc_actor_filter_channel_is_honored() {
        let _serial = i141_serial().await;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        let (a, b) = seed_i141(&pool).await;

        // 只给 m_cc_actorId 这一条有效归属条件 ⇒ 必须命中 A 那一行（旧形状：空页）
        let mut q = PageQuery::new(1, 50);
        q.filters = vec![cc_filter(jeeflow_core::model::FilterOp::Eq, I141_ACTOR_A)];
        let page = cc_page(&pool, q).await;
        assert_eq!(ids_of(&page), vec![a], "m_cc_actorId 单通道也必须命中，实得 {:?}", ids_of(&page));

        // 两通道同一个人 ⇒ AND 同答案（旧形状：过滤条件被丢 ⇒ 这一档侥幸绿）
        let mut both = PageQuery::new(1, 50);
        both.operator = Some(I141_ACTOR_A.into());
        both.filters = vec![cc_filter(jeeflow_core::model::FilterOp::Eq, I141_ACTOR_A)];
        assert_eq!(ids_of(&cc_page(&pool, both).await), vec![a], "两通道同一个人 ⇒ 照常命中");

        // 两通道不同的人 ⇒ AND ⇒ 空页
        let mut conflict = PageQuery::new(1, 50);
        conflict.operator = Some(I141_ACTOR_A.into());
        conflict.filters = vec![cc_filter(jeeflow_core::model::FilterOp::Eq, I141_ACTOR_B)];
        assert_eq!(cc_page(&pool, conflict).await.record_count, 0,
            "两通道不同的人 ⇒ 不得返回任一方的行（改前 sqlx 把过滤条件丢掉 ⇒ 返回 A 那一行）");

        // 归属列上给空值条件 ⇒ 空页，不得当成"这条不加"把 operator 那一档摊出来
        let mut blank = PageQuery::new(1, 50);
        blank.operator = Some(I141_ACTOR_A.into());
        blank.filters = vec![cc_filter(jeeflow_core::model::FilterOp::Eq, "")];
        assert_eq!(cc_page(&pool, blank).await.record_count, 0,
            "空值归属条件不得退化为\"这条不加\"（改前 sqlx 返回 A 那一行）");

        // IN 空集合＝没有人 ⇒ 空页（与 java「集合非空」同一档）
        let mut empty_in = PageQuery::new(1, 50);
        empty_in.filters = vec![cc_filter(jeeflow_core::model::FilterOp::In, " , ")];
        assert_eq!(cc_page(&pool, empty_in).await.record_count, 0, "空集合归属条件 ⇒ 空页");
        assert_eq!(b, 914111, "夹具自检");
        clean_i141(&pool).await;
    }

    /// G1 改动面哨兵：非归属列的空值放行不变（这一档只保证"没被本轮改掉"）。
    #[tokio::test]
    async fn test_mysql_i141_g1_non_ownership_blank_filter_still_ignored() {
        let _serial = i141_serial().await;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        let (a, _b) = seed_i141(&pool).await;

        let mut q = PageQuery::new(1, 50);
        q.operator = Some(I141_ACTOR_A.into());
        q.filters = vec![QueryFilter {
            alias: "t".into(), op: jeeflow_core::model::FilterOp::Like,
            column: "business_no".into(), value: "".into(),
        }];
        let page = cc_page(&pool, q).await;
        assert_eq!(ids_of(&page), vec![a], "空值非归属条件应被放行，归属条件照常生效");
        clean_i141(&pool).await;
    }

    /// G1 · 两仓同答案：同一份数据在内存仓与 sqlx 仓上逐档读数必须一致（117 场景 27 那把尺子）。
    #[tokio::test]
    async fn test_mysql_i141_g1_two_repos_same_answer() {
        let _serial = i141_serial().await;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        let (a, b) = seed_i141(&pool).await;

        // 内存仓造同一份数据：两条实例，A 抄给 ACTOR_A、B 抄给 ACTOR_B
        let mem = jeeflow_core::MemoryRepository::new();
        let mut define = ProcessDefine {
            id: 0, name: "mem_141".into(), display_name: "141".into(),
            define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
            version: 1, create_time: None, create_user: None, update_time: None, update_user: None,
        };
        mem.save_define(&mut define).unwrap();
        let mut mem_ids: Vec<i64> = Vec::new();
        for op in ["i141_op_a", "i141_op_b"] {
            let mut inst = ProcessInstance {
                instance_id: 0, parent_id: None, define_id: define.id, state: 10,
                parent_node_name: None, business_no: Some(format!("biz-{}", op)),
                operator: op.into(), expire_time: None,
                variables: jeeflow_core::json::FlowData::new(),
                tasks: vec![], create_time: None, create_user: Some(op.into()),
                update_time: None, update_user: None, define: None,
            };
            mem.save_instance(&mut inst).unwrap();
            mem_ids.push(inst.instance_id);
        }
        mem.create_cc_instance(mem_ids[0], I141_SENDER, &[I141_ACTOR_A.into()]).unwrap();
        mem.create_cc_instance(mem_ids[1], I141_SENDER, &[I141_ACTOR_B.into()]).unwrap();

        // 每一档给出"命中第几条实例"的符号化标签（0 基索引），两仓各自换算后直比
        let label_sqlx = |rows: &[InstanceRow]| -> Vec<i8> {
            rows.iter().map(|r| if r.id == a { 0 } else if r.id == b { 1 } else { -9 }).collect()
        };
        let label_mem = |rows: &[InstanceRow]| -> Vec<i8> {
            rows.iter().map(|r| if r.id == mem_ids[0] { 0 } else if r.id == mem_ids[1] { 1 } else { -9 }).collect()
        };

        let cases: Vec<(String, PageQuery)> = vec![
            ("条件整条没给".to_string(), PageQuery::new(1, 50)),
            ("operator 空串".to_string(), with_operator("")),
            ("operator 全空白".to_string(), with_operator("   ")),
            ("operator=A".to_string(), with_operator(I141_ACTOR_A)),
            ("operator=B".to_string(), with_operator(I141_ACTOR_B)),
            ("只有 m_cc_actorId=A".to_string(), with_cc_filter(jeeflow_core::model::FilterOp::Eq, I141_ACTOR_A)),
            ("只有 m_cc_actorId=B".to_string(), with_cc_filter(jeeflow_core::model::FilterOp::Eq, I141_ACTOR_B)),
            ("两通道同人".to_string(), with_both(I141_ACTOR_A, I141_ACTOR_A)),
            ("两通道异人".to_string(), with_both(I141_ACTOR_A, I141_ACTOR_B)),
            ("m_cc_actorId 空值".to_string(), with_cc_filter(jeeflow_core::model::FilterOp::Eq, "")),
            ("m_cc_actorId IN A,B".to_string(), with_cc_filter(jeeflow_core::model::FilterOp::In, "u141a,u141b")),
            ("非归属列空值 LIKE".to_string(), with_non_ownership_blank_like()),
        ];
        for (label, q) in cases {
            let sqlx_page = cc_page(&pool, q.clone()).await;
            let mem_page = mem.page_cc_instances(&q).unwrap();
            assert_eq!(sqlx_page.record_count, mem_page.record_count,
                "141 G1 两仓不同答案[{label}]：sqlx={:?} memory={:?}", ids_of(&sqlx_page), ids_of(&mem_page));
            // 比的是**命中哪一批实例**（排序后直比）：行序是另一维度（sqlx `ORDER BY pi.id DESC`
            // vs 内存仓插入序），G1 判据只管"归属条件必填 ⇒ 空页/非空页"，不把行序混进来。
            let mut s = label_sqlx(&sqlx_page.rows); s.sort_unstable();
            let mut m = label_mem(&mem_page.rows); m.sort_unstable();
            assert_eq!(s, m, "141 G1 两仓命中的实例必须同一批[{label}]");
        }
        clean_i141(&pool).await;
    }

    /// G2 · 写侧判重＝幂等空操作（①不新增行 ②不重置未读 ③不更新原行时间）。
    #[tokio::test]
    async fn test_mysql_i141_g2_repeat_cc_is_idempotent_noop() {
        let _serial = i141_serial().await;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        let (a, _b) = seed_i141(&pool).await;

        // ②先置已读，让"重复抄送把 state 抹回未读"这一档照得出来
        let pool2 = pool.clone();
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            repo.update_cc_status(a, I141_ACTOR_A).unwrap();
        }).await;
        let before = cc_raw_rows(&pool, a).await;
        assert_eq!(before.len(), 1, "夹具：A 那条实例应只有一行 cc");
        assert_eq!(before[0].2, 1, "置读后 state 应为 1");
        assert!(before[0].3.is_some(), "cc 行必须带 create_time（③这一档才照得出来）");

        // 隔一秒再重复抄同一人：任何"刷新原行时间"的假修都会在这一档露出来
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let pool3 = pool.clone();
        let actor = I141_ACTOR_A.to_string();
        let created = run_sync(move || {
            let repo = SqlxRepository::new(pool3);
            repo.create_cc_instance_if_absent(a, I141_SENDER, &[actor]).unwrap()
        }).await;

        let after = cc_raw_rows(&pool, a).await;
        assert!(created.is_empty(), "全是已知人 ⇒ 实际新建子集为空（调用点据此不发码 4），实得 {created:?}");
        assert_eq!(after.len(), before.len(), "①重复抄送不得新增第二行");
        assert_eq!(after[0].2, 1, "②重复抄送不得把已读抹回未读");
        assert_eq!(after[0].0, before[0].0, "③原行就是原行（主键不变，也没被删掉重建）");
        assert_eq!(after[0].3, before[0].3, "③重复抄送不得刷新原行 create_time");
        assert_eq!(after[0].4, before[0].4, "③重复抄送不得刷新原行 update_time");

        // 直调旧入口也必须判重（两腿共用判据：判重在仓储写侧，不在调用点）
        let pool4 = pool.clone();
        let actor2 = I141_ACTOR_A.to_string();
        run_sync(move || {
            let repo = SqlxRepository::new(pool4);
            repo.create_cc_instance(a, I141_SENDER, &[actor2]).unwrap()
        }).await;
        assert_eq!(cc_raw_rows(&pool, a).await.len(), 1, "create_cc_instance 自身也必须判重");
        clean_i141(&pool).await;
    }

    /// G2 · 读侧与子集：`find_cc_actor_ids` 逐行返回（不加 DISTINCT），
    /// `create_cc_instance_if_absent` 只返回实际新建的子集，且作用域按实例。
    #[tokio::test]
    async fn test_mysql_i141_g2_subset_and_scope() {
        let _serial = i141_serial().await;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        let (a, b) = seed_i141(&pool).await;

        let pool2 = pool.clone();
        let (first, second, third) = run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            // 同一次调用内重复给同一个人 ⇒ 折叠
            let first = repo.create_cc_instance_if_absent(
                a, I141_SENDER, &[I141_ACTOR_C.into(), I141_ACTOR_C.into()]).unwrap();
            // 已知人 + 新人 ⇒ 子集只含新人
            let second = repo.create_cc_instance_if_absent(
                a, I141_SENDER, &[I141_ACTOR_A.into(), I141_ACTOR_C.into()]).unwrap();
            // 换一个实例，同一个人照样新建（判重按实例作用域）
            let third = repo.create_cc_instance_if_absent(
                b, I141_SENDER, &[I141_ACTOR_A.into()]).unwrap();
            (first, second, third)
        }).await;

        assert_eq!(first, vec![I141_ACTOR_C.to_string()], "同一次调用内的重复只算一次新建");
        assert_eq!(second, Vec::<String>::new(), "已知人不进子集");
        assert_eq!(third, vec![I141_ACTOR_A.to_string()], "同一个人换实例照样新建");
        assert_eq!(cc_raw_rows(&pool, a).await.len(), 2, "A 那条实例＝首抄 A ＋ 新建 C 两行");
        assert_eq!(cc_raw_rows(&pool, b).await.len(), 2, "B 那条实例不受 A 影响");

        let pool3 = pool.clone();
        let actors = run_sync(move || {
            let repo = SqlxRepository::new(pool3);
            repo.find_cc_actor_ids(a).unwrap()
        }).await;
        assert_eq!(actors, vec![I141_ACTOR_A.to_string(), I141_ACTOR_C.to_string()],
            "读侧逐行返回（查询侧不加 DISTINCT）");
        clean_i141(&pool).await;
    }

    /// G2 · 两仓同答案：同样一串写侧调用，内存仓与 sqlx 仓的落库人员集合必须一致。
    #[tokio::test]
    async fn test_mysql_i141_g2_two_repos_same_answer() {
        let _serial = i141_serial().await;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        let (a, _b) = seed_i141(&pool).await;

        let pool2 = pool.clone();
        let sqlx_actors = run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            let iid = repo.find_instance_by_id(a).unwrap().unwrap().instance_id;
            repo.create_cc_instance_if_absent(iid, I141_SENDER, &[I141_ACTOR_A.into(), I141_ACTOR_C.into()]).unwrap();
            repo.create_cc_instance_if_absent(iid, I141_SENDER, &[I141_ACTOR_A.into(), I141_ACTOR_B.into()]).unwrap();
            repo.find_cc_actor_ids(iid).unwrap()
        }).await;

        let mem = jeeflow_core::MemoryRepository::new();
        let iid = {
            let mut define = ProcessDefine {
                id: 0, name: "mem_141_g2".into(), display_name: "141".into(),
                define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
                version: 1, create_time: None, create_user: None, update_time: None, update_user: None,
            };
            mem.save_define(&mut define).unwrap();
            let mut inst = ProcessInstance {
                instance_id: 0, parent_id: None, define_id: define.id, state: 10,
                parent_node_name: None, business_no: Some("biz-mem-141-g2".into()),
                operator: "i141_op_a".into(), expire_time: None,
                variables: jeeflow_core::json::FlowData::new(),
                tasks: vec![], create_time: None, create_user: None,
                update_time: None, update_user: None, define: None,
            };
            mem.save_instance(&mut inst).unwrap();
            inst.instance_id
        };
        mem.create_cc_instance_if_absent(iid, I141_SENDER, &[I141_ACTOR_A.into()]).unwrap();
        mem.create_cc_instance_if_absent(iid, I141_SENDER, &[I141_ACTOR_A.into(), I141_ACTOR_C.into()]).unwrap();
        mem.create_cc_instance_if_absent(iid, I141_SENDER, &[I141_ACTOR_A.into(), I141_ACTOR_B.into()]).unwrap();
        let mem_actors = mem.find_cc_actor_ids(iid).unwrap();

        assert_eq!(sqlx_actors, mem_actors,
            "141 G2 两仓写侧判重必须同答案（sqlx={:?} memory={:?}）", sqlx_actors, mem_actors);
        clean_i141(&pool).await;
    }

    // ═══ issues/141 G10 · 空抄送人不建 cc 行（SQL 真库写侧兜底这一支）═══
    //   基准＝jeeflow-java `5fbd5ac`（JdbcProcessRepository.createCcInstance ＋
    //   JdbcCcOwnershipIdempotentTest 的 G10 六格）；判据与内存仓同一条
    //   （`jeeflow_core::model::normalize_cc_actors`），两仓必须同答案（issues/117 场景 27）。

    /// G10 专用：一条**没有任何 cc 行**的实例（define 9141xx 段，避开 seed_i141 的预置行）。
    async fn new_cc_instance(pool: &MySqlPool, define_id: i64) -> i64 {
        let pool2 = pool.clone();
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            let mut define = ProcessDefine {
                id: define_id, name: format!("rust_141_g10_{define_id}"),
                display_name: "141 空抄送".into(), define_type: "approval".into(),
                state: 1, content: b"{}".to_vec(), version: 1,
                create_time: None, create_user: Some(I141_SENDER.into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define).unwrap();
            let iid = define_id + 1;
            let mut inst = ProcessInstance {
                instance_id: iid, parent_id: None, define_id, state: 10,
                parent_node_name: None, business_no: Some(format!("biz-{iid}")),
                operator: "i141_op_a".into(), expire_time: None,
                variables: jeeflow_core::json::FlowData::new(),
                tasks: vec![], create_time: None, create_user: Some("i141_op_a".into()),
                update_time: None, update_user: None, define: None,
            };
            repo.save_instance(&mut inst).unwrap();
            iid
        }).await
    }

    /// G10 取证读侧：`find_cc_actor_ids` 是同步 SPI，必须经 `run_sync`（spawn_blocking）调用——
    /// 直接在 `#[tokio::test]` 的 current-thread 运行时上调会撞 `block_in_place` 断言。
    async fn cc_actor_ids(pool: &MySqlPool, instance_id: i64) -> Vec<String> {
        let pool2 = pool.clone();
        run_sync(move || SqlxRepository::new(pool2).find_cc_actor_ids(instance_id).unwrap()).await
    }

    /// 写侧兜底（SQL 仓这一层）：空串／纯空白直连仓储也建不出行——
    /// 改前实测 `actor_id=''` 真落库一行（issues/129 那族"空归属值"的病根）。
    #[tokio::test]
    async fn test_mysql_i141_g10_blank_actors_create_no_rows() {
        let _serial = i141_serial().await;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i141(&pool).await;
        let iid = new_cc_instance(&pool, 914120).await;

        let pool2 = pool.clone();
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            repo.create_cc_instance(iid, I141_SENDER,
                &["".into(), "   ".into(), "\t".into()]).unwrap();
            let fresh = repo.create_cc_instance_if_absent(iid, I141_SENDER,
                &["".into(), "  ".into()]).unwrap();
            assert!(fresh.is_empty(), "G10：全空值批次的 if_absent 子集必须为空，实得 {fresh:?}");
        }).await;

        assert!(cc_raw_rows(&pool, iid).await.is_empty(),
            "G10：wf_process_cc_instance 必须零行，实得 {:?}", cc_raw_rows(&pool, iid).await);
        assert_eq!(cc_actor_ids(&pool, iid).await, Vec::<String>::new(),
            "G10：读侧人员集合必须为空");
        clean_i141(&pool).await;
    }

    /// 混给一批：只丢空元素，有效的人照旧落行（顺序随入参）。
    #[tokio::test]
    async fn test_mysql_i141_g10_mixed_batch_keeps_only_valid_actors() {
        let _serial = i141_serial().await;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i141(&pool).await;
        let iid = new_cc_instance(&pool, 914130).await;

        let pool2 = pool.clone();
        let created = run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            repo.create_cc_instance_if_absent(iid, I141_SENDER,
                &["".into(), I141_ACTOR_A.into(), "  ".into(), I141_ACTOR_B.into()]).unwrap()
        }).await;

        assert_eq!(created, vec![I141_ACTOR_A.to_string(), I141_ACTOR_B.to_string()],
            "G10：if_absent 的子集（＝拿去 fire 码 4 的那一批）只含有效的人");
        let rows = cc_raw_rows(&pool, iid).await;
        assert_eq!(rows.len(), 2, "G10：库里只有两行，不得有 actor_id='' 的那一行");
        assert_eq!(rows.iter().map(|r| r.1.clone()).collect::<Vec<_>>(),
            vec![I141_ACTOR_A.to_string(), I141_ACTOR_B.to_string()]);
        clean_i141(&pool).await;
    }

    /// 落库值取 trim 后的串，且与 G2 判重咬合：`" u141a "` 与 `"u141a"` 判为同一个人 ⇒ 仍一行。
    #[tokio::test]
    async fn test_mysql_i141_g10_values_trimmed_and_hit_g2_dedup() {
        let _serial = i141_serial().await;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i141(&pool).await;
        let iid = new_cc_instance(&pool, 914140).await;

        let pool2 = pool.clone();
        let padded = format!("  {I141_ACTOR_A}  ");
        run_sync(move || {
            SqlxRepository::new(pool2).create_cc_instance(iid, I141_SENDER, &[padded]).unwrap();
        }).await;
        assert_eq!(cc_actor_ids(&pool, iid).await, vec![I141_ACTOR_A.to_string()],
            "G10：入库值必须是 trim 后的串");

        let pool3 = pool.clone();
        let fresh = run_sync(move || {
            SqlxRepository::new(pool3)
                .create_cc_instance_if_absent(iid, I141_SENDER, &[I141_ACTOR_A.into()]).unwrap()
        }).await;
        assert!(fresh.is_empty(), "G10＋G2：带空格与不带空格判为同一人 ⇒ 子集为空、不 fire");
        assert_eq!(cc_raw_rows(&pool, iid).await.len(), 1, "G10＋G2：同一人不得落出第二行");
        clean_i141(&pool).await;
    }

    /// 反向哨兵：`"0"` 是正常用户 id，SQL 仓写侧不得把它当成空值丢掉。
    #[tokio::test]
    async fn test_mysql_i141_g10_zero_actor_id_is_not_eaten() {
        let _serial = i141_serial().await;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i141(&pool).await;
        let iid = new_cc_instance(&pool, 914150).await;

        let pool2 = pool.clone();
        let fresh = run_sync(move || {
            SqlxRepository::new(pool2).create_cc_instance_if_absent(iid, I141_SENDER,
                &["0".into(), "".into(), I141_ACTOR_C.into()]).unwrap()
        }).await;

        assert_eq!(fresh, vec!["0".to_string(), I141_ACTOR_C.to_string()],
            "G10 只丢空串/纯空白：'0' 照旧建行照旧进子集");
        assert_eq!(cc_actor_ids(&pool, iid).await,
            vec!["0".to_string(), I141_ACTOR_C.to_string()]);
        clean_i141(&pool).await;
    }

    /// 两仓同答案（issues/117 场景 27 那把尺子）：同一批含空值的入参，内存仓与 SQL 仓
    /// 落库的人员集合必须逐字一致——只修一层就会出现"一个仓建行、一个仓不建"的分叉。
    #[tokio::test]
    async fn test_mysql_i141_g10_two_repos_same_answer_on_blank_actors() {
        let _serial = i141_serial().await;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i141(&pool).await;
        let iid = new_cc_instance(&pool, 914160).await;

        let pool2 = pool.clone();
        let batch = vec!["".to_string(), format!("  {I141_ACTOR_A}  "),
                         I141_ACTOR_B.to_string(), "  ".to_string(), "0".to_string()];
        let sqlx_batch = batch.clone();
        let sqlx_actors = run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            repo.create_cc_instance_if_absent(iid, I141_SENDER, &sqlx_batch).unwrap();
            repo.find_cc_actor_ids(iid).unwrap()
        }).await;

        let mem = jeeflow_core::MemoryRepository::new();
        let mut define = ProcessDefine {
            id: 0, name: "mem_141_g10".into(), display_name: "141".into(),
            define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
            version: 1, create_time: None, create_user: None, update_time: None, update_user: None,
        };
        mem.save_define(&mut define).unwrap();
        let mut inst = ProcessInstance {
            instance_id: 0, parent_id: None, define_id: define.id, state: 10,
            parent_node_name: None, business_no: Some("biz-mem-141-g10".into()),
            operator: "i141_op_a".into(), expire_time: None,
            variables: jeeflow_core::json::FlowData::new(),
            tasks: vec![], create_time: None, create_user: None,
            update_time: None, update_user: None, define: None,
        };
        mem.save_instance(&mut inst).unwrap();
        mem.create_cc_instance_if_absent(inst.instance_id, I141_SENDER, &batch).unwrap();
        let mem_actors = mem.find_cc_actor_ids(inst.instance_id).unwrap();

        assert_eq!(sqlx_actors, mem_actors,
            "141 G10 两仓写侧必须同答案（sqlx={sqlx_actors:?} memory={mem_actors:?}）");
        assert_eq!(sqlx_actors, vec![I141_ACTOR_A.to_string(), I141_ACTOR_B.to_string(), "0".to_string()],
            "G10：两仓都只落有效且 trim 后的人（'0' 是有效 id，保留）");
        clean_i141(&pool).await;
    }

    // ─── 141 用例的 PageQuery 夹具（两仓共用同一支构造，判据才可比）───


    fn ids_of(page: &PageResult<InstanceRow>) -> Vec<i64> {
        page.rows.iter().map(|r| r.id).collect()
    }

    fn with_operator(op: &str) -> PageQuery {
        let mut q = PageQuery::new(1, 50);
        q.operator = Some(op.to_string());
        q
    }

    fn with_cc_filter(op: jeeflow_core::model::FilterOp, value: &str) -> PageQuery {
        let mut q = PageQuery::new(1, 50);
        q.filters = vec![cc_filter(op, value)];
        q
    }

    fn with_both(op: &str, filter_value: &str) -> PageQuery {
        let mut q = with_operator(op);
        q.filters = vec![cc_filter(jeeflow_core::model::FilterOp::Eq, filter_value)];
        q
    }

    fn with_non_ownership_blank_like() -> PageQuery {
        let mut q = with_operator(I141_ACTOR_A);
        q.filters = vec![QueryFilter {
            alias: "t".into(), op: jeeflow_core::model::FilterOp::Like,
            column: "business_no".into(), value: "".into(),
        }];
        q
    }

    /// issues/142 A 批 · spec/02 §6.2 第 1 条的 **sqlx 路**（内存路见
    /// `jeeflow-core/src/engine.rs::custom_node_tests`）：记录类（`snaker:custom`）那条
    /// `task_state=20` 的历史行必须**真进 `wf_process_task`**——条文原话
    /// 「只在聚合内存对象里 append 一条不算做到」，而本栈 HEAD 之前连这条 INSERT 腿都没有
    /// （custom 与 task 合流建 DOING 待办，正是 §6.1 禁止形状①）。
    ///
    /// 一次跑齐四件：① DONE 行查得到（含 `wf_process_task_actor` 里那一条留痕主体）
    /// ② 待办里没有它 ③ 令牌沿出边流到 end（实例 20）④ 到期列 NULL／血缘列照建单不变量。
    #[test]
    fn test_mysql_i142_custom_history_row_lands() {
        if skip_mysql() { return; }
        mysql_rt().block_on(async {
            let pool = connect_pool().await;
            setup_schema(&pool).await;
            let define_id = 901421i64;
            clean_by_define(&pool, define_id).await;

            let mut define = ProcessDefine {
                id: define_id, name: "rust_i142_custom".into(), display_name: "i142 记录类".into(),
                define_type: "approval".into(), state: 1,
                content: r#"{"name":"rust_i142_custom","displayName":"i142","type":"approval",
                    "nodes":[{"id":"start","type":"snaker:start","text":{"value":"s"}},
                             {"id":"apply","type":"snaker:task","text":{"value":"申请"},"properties":{"assignee":"applicant"}},
                             {"id":"custom1","type":"snaker:custom","text":{"value":"通知外部系统"},
                              "properties":{"clazz":"com.mldong.jeeflow.test.TestCustomHandler","val":"customResult","expireTime":"2h"}},
                             {"id":"end","type":"snaker:end","text":{"value":"e"}}],
                    "edges":[{"id":"e1","sourceNodeId":"start","targetNodeId":"apply"},
                             {"id":"e2","sourceNodeId":"apply","targetNodeId":"custom1"},
                             {"id":"e3","sourceNodeId":"custom1","targetNodeId":"end"}]}"#
                    .as_bytes().to_vec(),
                version: 1, create_time: None, create_user: Some("rust_test".into()),
                update_time: None, update_user: None,
            };
            let repo = Arc::new(SqlxRepository::new(pool.clone()));
            repo.save_define(&mut define).unwrap();

            let ctx = jeeflow_core::context::ServiceContext::new()
                .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
                .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
                .with_id_generator(Arc::new(jeeflow_core::id_gen::DefaultIdGenerator::new(1)));
            let engine = jeeflow_core::engine::JeeflowEngineImpl::new(ctx);

            let inst = engine.start_async(define_id, "i142User",
                &jeeflow_core::json::FlowData::new()).await.unwrap();
            let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
                .into_iter().find(|t| t.task_name == "apply").expect("apply 待办应在");
            engine.execute_task_async(apply.task_id, "i142User",
                &jeeflow_core::json::FlowData::new()).await.unwrap();

            // ① 真库里查得到那条 DONE 行（find_done_tasks 走的是 SELECT ... task_state = 20）
            let hist = repo.find_done_tasks(inst.instance_id, &[]).unwrap()
                .into_iter().find(|t| t.task_name == "custom1")
                .expect("§6.2 第 1 条：wf_process_task 里必须查得到记录类那条 DONE 行");
            assert_eq!(hist.task_state, 20, "task_state 必须是 20（已完成）");
            assert_eq!(hist.actor_ids, vec!["i142User".to_string()],
                "参与者＝当前操作人（留痕主体，不是待办）");
            assert_eq!(actor_rows(&pool, hist.task_id).await, vec!["i142User".to_string()],
                "wf_process_task_actor 里那一条也必须落");

            // ② 待办清空：custom1 没有混进 DOING（§6.1 禁止形状①）
            let doing_names: Vec<String> = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
                .into_iter().map(|t| t.task_name).collect();
            assert!(doing_names.is_empty(), "custom1 之后直连 end，待办必须清空，实得 {doing_names:?}");

            // ③ 令牌沿出边继续流转 ⇒ 实例办结
            let state: i32 = sqlx::query("SELECT state FROM wf_process_instance WHERE id = ?")
                .bind(inst.instance_id).fetch_one(&pool).await.unwrap().get("state");
            assert_eq!(state, 20, "记录类不拦路，实例应到终点");

            // ④ 到期列 NULL（判定见内存路同档用例）＋ 血缘列照建单不变量 ＋ 1bis 两列都写
            let row = sqlx::query("SELECT expire_time, task_parent_id, operator, finish_time FROM wf_process_task WHERE id = ?")
                .bind(hist.task_id).fetch_one(&pool).await.unwrap();
            // 两列都是 DATETIME(3)：必须走 `get_opt_datetime`，直接 `row.get::<Option<String>>`
            // 在**有值**时会以 ColumnDecode 崩掉（NULL 才恰好过），把断言变成解码 panic 就不是判据了。
            let exp: Option<String> = get_opt_datetime(&row, "expire_time");
            assert!(exp.is_none(), "记录类行 expire_time 留 NULL（spec/02 §6 无此属性档），实得 {exp:?}");
            let parent: Option<i64> = row.get("task_parent_id");
            assert_eq!(parent, Some(apply.task_id), "task_parent_id 照建单不变量落库");
            let op_col: Option<String> = row.get("operator");
            assert_eq!(op_col.as_deref(), Some("i142User"), "已办结行的 operator 列＝留痕主体");
            // §6.2 第 **1bis** 条在 SQL 仓的那一半：本列曾经**不在 INSERT 语句里**，
            // 聚合根赋的 finish_time 因此到不了库（内存路绿、真库 NULL ⇒ 典型的两层冗余藏病灶）。
            let fin: Option<String> = get_opt_datetime(&row, "finish_time");
            assert!(fin.is_some(),
                "§6.2 1bis：wf_process_task 里那条 DONE 行的 finish_time 必须真落库，实得 NULL");
            assert_eq!(fin.as_deref(), hist.finish_time.as_deref(),
                "真库那格的完成时间要与聚合返回行同值");

            // ⑤ §6.2 第 2 条的 sqlx 路：本引擎**没注册**任何 custom 处理器（clazz 未注册档）
            // ⇒ 不报错、照常落行（上面已断），并且一个流程变量键都不写
            let inst_now = repo.find_instance_by_id(inst.instance_id).unwrap().unwrap();
            assert!(inst_now.variables.get_str("customResult").is_none(),
                "未注册 clazz ⇒ 处理器不执行 ⇒ 不写 val 键");
            assert!(inst_now.variables.get_str("custom_return_val").is_none(),
                "未注册 clazz ⇒ 缺省键也不该出现");

            clean_by_define(&pool, define_id).await;
        });
    }

    // ═══════════════════════════════════════════════════════
    // issues/137 A · A 案的 **sqlx 真库腿**（内存路见 jeeflow-core `engine.rs` 的
    //   `test_i137a_*` 一族五格）。
    //
    // 为什么这一族必须有一条真库格：`wf_process_instance.expire_time` 是 `DATETIME(3)` 列，
    // 而改前搬进这一列的是**定义级表达式的原串**（`"2h"`）。内存仓把它当 String 存 ⇒ 全绿；
    // 真库在 `STRICT_TRANS_TABLES`（160 那台 MySQL **5.7.31**，本轮直连 `SELECT VERSION()` 实测）下**硬拒**——
    // 实测 errno 1292(22007) `Incorrect datetime value: '2h' for column 'expire_time'`
    // （工单案文写的 1366 是字符列那一族的返回码，DATETIME 列这台给的是 1292），
    // 非严格模式则静默存成 `0000-00-00`。"内存绿 ≠ 落库绿"这一族按红线做**两步变异对照**：
    //   ① 摘掉求值只搬原串 ⇒ 内存多格红 ＋ 本格红（INSERT 先炸）；
    //   ② 只摘落库绑定（值算对了但绑参给 NULL）⇒ 内存全绿、**只有本格红**。
    //
    // 判据（一律打在库列的**值**上，不写"非空"空判）：
    //   ① 顶层配 `2h` ⇒ 库列＝注入钟+7200s 那个**具体时刻**（不是原串、不是 now()）；
    //   ③ 顶层没配 ⇒ 库列 `IS NULL`（不赋 now、不赋空串），而同夹具任务行照旧有到期值
    //     ⇒ 证明"实例列没值"不是整条求值没跑（两列各归各的源，issues/126 回归不被带坏）。
    //
    // 时钟按 M5 同款注入固定串（`ClockScope` 持进程级互斥 + drop 复原），故这里能逐值断言
    // 而不是拿真实 now 去凑带宽；并先探一次 DB 会话钟 ≠ 注入钟，否则等值断言无牙。
    // ID 段 901371／901372 独占，跑前跑后各清一次。
    // ═══════════════════════════════════════════════════════

    const I37A_FIXED: &str = "2026-08-08 09:00:00";
    const I37A_WANT: &str = "2026-08-08 11:00:00"; // I37A_FIXED + 2h

    fn i37a_fixed_clock() -> String {
        I37A_FIXED.to_string()
    }

    /// 一条 start → apply → end 的线性流；`top_expire` 给的是**流程定义根上**的
    /// `expireTime` JSON 字面量（`None` ⇒ 整个键都不加）。
    /// 节点 `apply` 上**始终**配着 `expireTime`：这样"实例列为空"只可能是"实例不读节点档"，
    /// 而不是"整条求值没跑"。
    fn i37a_flow_content(name: &str, top_expire: Option<&str>) -> Vec<u8> {
        let top = match top_expire {
            Some(raw) => format!(r#","expireTime":{raw}"#),
            None => String::new(),
        };
        format!(r#"{{"name":"{name}"{top},"displayName":"i137a","type":"approval",
            "nodes":[
                {{"id":"start","type":"snaker:start","text":{{"value":"开始"}},"properties":{{}}}},
                {{"id":"apply","type":"snaker:task","text":{{"value":"申请"}},
                  "properties":{{"assignee":"applicant","expireTime":"2h"}}}},
                {{"id":"end","type":"snaker:end","text":{{"value":"结束"}},"properties":{{}}}}],
            "edges":[
                {{"id":"e1","sourceNodeId":"start","targetNodeId":"apply"}},
                {{"id":"e2","sourceNodeId":"apply","targetNodeId":"end"}}]}}"#)
            .as_bytes().to_vec()
    }

    #[test]
    fn test_mysql_i137a_instance_expire_time_lands_evaluated() {
        if skip_mysql() { return; }
        // 注入必须**先于**任何写库，且横跨 spawn_blocking / block_in_place 的线程
        // （时钟是进程级 static，全线程可见）——同 M5 的姿势
        let _clock = jeeflow_core::clock::ClockScope::injected(i37a_fixed_clock);
        mysql_rt().block_on(async {
            let pool = connect_pool().await;
            setup_schema(&pool).await;
            for id in [901371i64, 901372] {
                clean_by_define(&pool, id).await;
            }

            // 探针自证：DB 会话钟与注入钟若同读数，下面的等值断言分不出基准来源 ⇒ 无牙，先报红
            let db_now: String = sqlx::query("SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s') AS n")
                .fetch_one(&pool).await.unwrap().get("n");
            assert_ne!(db_now, I37A_FIXED,
                "137 A：库钟 NOW() 恰好等于注入钟串 ⇒ 本环境这条判据无牙");

            let mk_engine = |repo: Arc<SqlxRepository>| {
                let ctx = jeeflow_core::context::ServiceContext::new()
                    .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
                    .with_ext_repository(repo as Arc<dyn ProcessExtRepository>)
                    // ⚠️ worker_id 必须与同二进制里其它引擎用例（i142 用 `new(1)`）**不同**：
                    // 共享测试库里跑的是同一个毫秒雪花，两个用例同毫秒各发一号 ⇒ id 完全相同
                    // ⇒ 真库 `Duplicate entry ... for key 'PRIMARY'`（实测 1/1 命中过，
                    // 见本轮收口报告附带发现）。137 段独占 worker，不与既有 1 号 worker 抢。
                    .with_id_generator(Arc::new(jeeflow_core::id_gen::DefaultIdGenerator::new(137)));
                jeeflow_core::engine::JeeflowEngineImpl::new(ctx)
            };

            // ── 格①：顶层配 2h ⇒ 库列落**求值结果**那个具体时刻 ─────────────────
            let mut define = ProcessDefine {
                id: 901371, name: "rust_i37a_on".into(), display_name: "i137a 配了".into(),
                define_type: "approval".into(), state: 1,
                content: i37a_flow_content("rust_i37a_on", Some("\"2h\"")),
                version: 1, create_time: None, create_user: Some("rust_test".into()),
                update_time: None, update_user: None,
            };
            let repo = Arc::new(SqlxRepository::new(pool.clone()));
            repo.save_define(&mut define).unwrap();
            let inst_on = mk_engine(repo.clone()).start_async(define.id, "applicant",
                &jeeflow_core::json::FlowData::new()).await.unwrap();

            let row = sqlx::query("SELECT DATE_FORMAT(expire_time, '%Y-%m-%d %H:%i:%s') AS et, \
                     DATE_FORMAT(create_time, '%Y-%m-%d %H:%i:%s') AS ct \
                     FROM wf_process_instance WHERE id = ?")
                .bind(inst_on.instance_id).fetch_one(&pool).await.unwrap();
            let et: Option<String> = row.get("et");
            let ct: Option<String> = row.get("ct");
            assert_eq!(ct.as_deref(), Some(I37A_FIXED),
                "内部对照：同一行 create_time 取的就是注入钟（两列同基准）");
            assert_eq!(et.as_deref(), Some(I37A_WANT),
                "判据①（真库）：wf_process_instance.expire_time 该是定义级表达式求出的时刻 \
                 {I37A_WANT}；搬原串会把 INSERT 打成实测 1292(22007) Incorrect datetime value，\
                 兜底 now() 则落成库钟 {db_now}+偏移");
            // 聚合读回（走 SELECT + get_opt_datetime 那条路）与库面读数必须同一个值
            let back = repo.find_instance_by_id(inst_on.instance_id).unwrap().unwrap();
            assert_eq!(back.expire_time.as_deref(), Some(I37A_WANT),
                "读回路径与库面对不上：实得 {:?}", back.expire_time);
            // 同一次发起的**任务行**照旧带到期（issues/126 写点①不被本改动带坏）
            let task_exp: Option<String> = sqlx::query(
                "SELECT DATE_FORMAT(expire_time, '%Y-%m-%d %H:%i:%s') AS e FROM wf_process_task \
                 WHERE process_instance_id = ? AND task_name = 'apply'")
                .bind(inst_on.instance_id).fetch_one(&pool).await.unwrap().get("e");
            assert_eq!(task_exp.as_deref(), Some(I37A_WANT),
                "回归：任务行 expire_time 与实例列同值（同一枚尺子、同一份 args）");

            // ── 格③：顶层没配 ⇒ 库列 IS NULL（节点那列照旧有值）─────────────────
            let mut define_off = ProcessDefine {
                id: 901372, name: "rust_i37a_off".into(), display_name: "i137a 没配".into(),
                define_type: "approval".into(), state: 1,
                content: i37a_flow_content("rust_i37a_off", None),
                version: 1, create_time: None, create_user: Some("rust_test".into()),
                update_time: None, update_user: None,
            };
            repo.save_define(&mut define_off).unwrap();
            let inst_off = mk_engine(repo.clone()).start_async(define_off.id, "applicant",
                &jeeflow_core::json::FlowData::new()).await.unwrap();
            let off_row = sqlx::query("SELECT DATE_FORMAT(expire_time, '%Y-%m-%d %H:%i:%s') AS et, \
                     DATE_FORMAT(create_time, '%Y-%m-%d %H:%i:%s') AS ct \
                     FROM wf_process_instance WHERE id = ?")
                .bind(inst_off.instance_id).fetch_one(&pool).await.unwrap();
            let off_exp: Option<String> = off_row.get("et");
            let off_ct: Option<String> = off_row.get("ct");
            assert_eq!(off_ct.as_deref(), Some(I37A_FIXED),
                "内部对照：这行确实落库了（不是\"行没读到\"混成\"值为空\"）");
            assert_eq!(off_exp, None,
                "判据③（真库）：定义没配顶层 expireTime ⇒ 该列必须 IS NULL，\
                 实得 {off_exp:?}（赋 now()/空串/0000-00-00 都在这儿现形）");
            assert_eq!(repo.find_instance_by_id(inst_off.instance_id).unwrap().unwrap().expire_time,
                None, "读回路径同样该是 NULL");
            let off_task_exp: Option<String> = sqlx::query(
                "SELECT DATE_FORMAT(expire_time, '%Y-%m-%d %H:%i:%s') AS e FROM wf_process_task \
                 WHERE process_instance_id = ? AND task_name = 'apply'")
                .bind(inst_off.instance_id).fetch_one(&pool).await.unwrap().get("e");
            assert_eq!(off_task_exp.as_deref(), Some(I37A_WANT),
                "内部对照：节点档照旧生效 ⇒ 上一格的 NULL 是\"实例不读节点档\"，不是求值没跑");

            for id in [901371i64, 901372] {
                clean_by_define(&pool, id).await;
            }
        });
        // drop(_clock) 复原默认基准，别把注入钟留给同批并发的其它用例
    }

    // ═══════════════════════════════════════════════════════
    // issues/142 B 批 · 任务参与者写侧归属值归一（sqlx 真库这一支 · spec 06 §2.11）
    //   普查实读的 rust 形状：**两仓分叉**——本仓 `add_task_actor` 是盲插（无判空、无 trim、
    //   **连判重都没有**），而同栈内存仓判重 ⇒ 同一串入参两仓两个答案，正是 issues/117
    //   场景 27 那把尺子点名的形状；绕过门面直连仓储的调用方能在真库里灌空值/灌重复，
    //   空归属值又是 issues/129 那族"空 operator 读全库"的上游进水口。
    //   判据本体＝`jeeflow_core::model::normalize_actors`（与抄送侧 §2.10 同一枚单点，不抄第二份）。
    //   ID 段：9142xx（本组独占，避开 900xxx／9141xx 既有用例段）；`wf_process_task_actor`
    //   无外键约束（C10 用例同姿势），参与者台账可独立于任务行读写。
    // ═══════════════════════════════════════════════════════

    const I142_ACTOR_A: &str = "u142a";
    const I142_ACTOR_B: &str = "u142b";

    async fn clean_i142_task_actor(pool: &MySqlPool, task_id: i64) {
        sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id = ?")
            .bind(task_id).execute(pool).await.unwrap();
    }

    /// 台账原始行（读的是**库里的列**，按 id 排序＝落库顺序；不走 `find_task_actors` 那条无 ORDER BY 的路径）。
    async fn task_actor_rows(pool: &MySqlPool, task_id: i64) -> Vec<String> {
        sqlx::query("SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = ? ORDER BY id")
            .bind(task_id).fetch_all(pool).await.unwrap()
            .iter().map(|r| r.get::<String, _>("actor_id")).collect()
    }

    /// 写侧兜底（§2.11 硬要求①）：空串／纯空白即使**绕过门面直连仓储**也进不了归属列。
    /// 改前实测（真库）：三行原样落进 `wf_process_task_actor`，`actor_id=''`／`'   '`／制表符各一条。
    #[tokio::test]
    async fn test_mysql_i142_b_add_task_actor_blank_values_write_nothing() {
        let task_id: i64 = 914201;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i142_task_actor(&pool, task_id).await;

        let pool2 = pool.clone();
        run_sync(move || {
            SqlxRepository::new(pool2)
                .add_task_actor(task_id, &["".into(), "   ".into(), "\t".into()]).unwrap();
        }).await;

        assert_eq!(task_actor_rows(&pool, task_id).await, Vec::<String>::new(),
            "§2.11：全空白批次不得落进 actor_id（真库读回）");
        clean_i142_task_actor(&pool, task_id).await;
    }

    /// 落库值取 trim 后的串（§2.11 硬要求②）：`"  u142a  "` 在库里必须是 `"u142a"`。
    #[tokio::test]
    async fn test_mysql_i142_b_add_task_actor_values_are_trimmed() {
        let task_id: i64 = 914202;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i142_task_actor(&pool, task_id).await;

        let pool2 = pool.clone();
        let padded = format!("  {I142_ACTOR_A}  ");
        run_sync(move || {
            SqlxRepository::new(pool2).add_task_actor(task_id, &[padded]).unwrap();
        }).await;

        assert_eq!(task_actor_rows(&pool, task_id).await, vec![I142_ACTOR_A.to_string()],
            "§2.11：入库值＝trim 后的串");
        clean_i142_task_actor(&pool, task_id).await;
    }

    /// 同一次调用内折叠 ＋ 跨调用判重（**本仓改前连判重都没有**，内存仓有 ⇒ 补齐成两仓同形）。
    #[tokio::test]
    async fn test_mysql_i142_b_add_task_actor_folds_within_and_across_calls() {
        let task_id: i64 = 914203;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i142_task_actor(&pool, task_id).await;

        let pool2 = pool.clone();
        let first = vec![format!("  {I142_ACTOR_A}  "), I142_ACTOR_A.to_string(),
                         "".to_string(), I142_ACTOR_B.to_string()];
        run_sync(move || {
            SqlxRepository::new(pool2).add_task_actor(task_id, &first).unwrap();
        }).await;
        assert_eq!(task_actor_rows(&pool, task_id).await,
            vec![I142_ACTOR_A.to_string(), I142_ACTOR_B.to_string()],
            "§2.11：一次调用里 trim 后同值＝同一个人 ⇒ 只落一行，空值丢弃");

        let pool3 = pool.clone();
        let again = format!(" {I142_ACTOR_A} ");
        run_sync(move || {
            SqlxRepository::new(pool3).add_task_actor(task_id, &[again]).unwrap();
        }).await;
        assert_eq!(task_actor_rows(&pool, task_id).await,
            vec![I142_ACTOR_A.to_string(), I142_ACTOR_B.to_string()],
            "§2.11 写侧兜底：跨调用重复给同一人（带空格）⇒ 幂等空操作，不得落第二行");
        clean_i142_task_actor(&pool, task_id).await;
    }

    /// 反向哨兵（§2.11 硬要求④）：`"0"`、`"00"`、`" "`、`"a"` 是**三个人**。
    /// 改前实测（真库）：四条全盲插 ⇒ 台账里躺着 `actor_id=' '` 那一行（判空没 trim）。
    #[tokio::test]
    async fn test_mysql_i142_b_sentinel_four_are_three_people() {
        let task_id: i64 = 914204;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i142_task_actor(&pool, task_id).await;

        let pool2 = pool.clone();
        run_sync(move || {
            SqlxRepository::new(pool2)
                .add_task_actor(task_id, &["0".into(), "00".into(), " ".into(), "a".into()]).unwrap();
        }).await;

        assert_eq!(task_actor_rows(&pool, task_id).await,
            vec!["0".to_string(), "00".to_string(), "a".to_string()],
            "哨兵：'0'/'00'/'a' 都是正常 id，只有 ' ' 是空值；'00' 与 '0' 是两个人，不得被松散判重吞掉");
        clean_i142_task_actor(&pool, task_id).await;
    }

    /// 两仓同答案（issues/117 场景 27 那把尺子）：同一批含空值/带空格的入参，内存仓与 SQL 仓
    /// 落库的人员集合必须逐字一致——只修一层就会出现"一个仓收、一个仓不收"的分叉。
    #[tokio::test]
    async fn test_mysql_i142_b_two_repos_same_answer_on_task_actors() {
        let task_id: i64 = 914205;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i142_task_actor(&pool, task_id).await;

        let batch = vec!["".to_string(), format!("  {I142_ACTOR_A}  "), I142_ACTOR_B.to_string(),
                         "  ".to_string(), "0".to_string(), I142_ACTOR_A.to_string()];
        let want = vec![I142_ACTOR_A.to_string(), I142_ACTOR_B.to_string(), "0".to_string()];

        let pool2 = pool.clone();
        let sqlx_batch = batch.clone();
        let sqlx_actors = run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            repo.add_task_actor(task_id, &sqlx_batch).unwrap();
            repo.find_task_actors(task_id).unwrap()
        }).await;
        assert_eq!(sqlx_actors, want, "SQL 仓写侧：只落有效且 trim 后的人（'0' 保留）");

        let mem = jeeflow_core::MemoryRepository::new();
        mem.add_task_actor(task_id, &batch).unwrap();
        let mem_actors = mem.find_task_actors(task_id).unwrap();
        assert_eq!(mem_actors, want, "内存仓写侧：同一枚判据");

        assert_eq!(sqlx_actors, mem_actors,
            "§2.11：两仓 add_task_actor 必须同答案（sqlx={sqlx_actors:?} memory={mem_actors:?}）");
        clean_i142_task_actor(&pool, task_id).await;
    }

    /// 删除位（issues/142 §9.2 第二批）真库读数：「 8601 」删得掉库里的 8601；
    /// 归一后为空 ⇒ 一条 DELETE 都不发——真库插一条 actor_id='' 的历史脏行，
    /// 空串入参不得批量误删。改前实测（真库）：DELETE 拿未 trim 原值 ⇒ 静默 no-op。
    #[tokio::test]
    async fn test_mysql_i142_b_remove_task_actor_trims_and_blank_is_noop() {
        let task_id: i64 = 914206;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i142_task_actor(&pool, task_id).await;

        let pool2 = pool.clone();
        let padded = format!("  {I142_ACTOR_A}  ");
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            repo.add_task_actor(task_id, &[padded]).unwrap();
            // 带空格的删除必须打得中 trim 后的行
            repo.remove_task_actor(task_id, &[format!("  {I142_ACTOR_A}  ")]).unwrap();
            // 历史脏行（旧版本写进去的 actor_id=''）：空串/全空白入参一条都不许删。
            // run_sync 的闭包是同步的，INSERT 走仓内同一枚 block_on（与 repo 同步方法同口径）。
            repo.block_on(async {
                sqlx::query("INSERT INTO wf_process_task_actor (id, process_task_id, actor_id) VALUES (914206001, ?, '')")
                    .bind(task_id).execute(repo.pool()).await
            }).unwrap();
            repo.remove_task_actor(task_id, &["".to_string(), "   ".to_string()]).unwrap();
            repo.remove_task_actor(task_id, &[]).unwrap();
        }).await;

        assert_eq!(task_actor_rows(&pool, task_id).await, vec!["".to_string()],
            "§9.2 删除位：trim 命中 + 空档零删除，脏行原样保留");
        clean_i142_task_actor(&pool, task_id).await;
    }

    // ═══════════════════════════════════════════════════════
    // issues/137 §3-6 · 参与者删除腿「原值 ∪ trim 值」两形并集（sqlx 真库这一支 · spec 06 语义 6）
    //   判据本体＝`jeeflow_core::model::actor_delete_forms`（与内存仓 `actor_delete_forms_tests`
    //   同一枚，issues/117 场景 27 同答案）。改前本仓删除位过 `normalize_actors`＝只取 trim 形，
    //   真库（NO PAD 排序规则）下面目按语义 6 交出的行原值「 9101 」被削成 9101，脏行删不掉而
    //   门面报成功——本组第一格 N 档在改前正是红的。
    //   种脏行必须**绕开写侧归一**（`add_task_actor` 会 trim＋丢空，正常路径建不出脏行）：直接
    //   INSERT 到 `wf_process_task_actor`（该表无外键，参与者台账可独立于任务行读写）。断言打在
    //   **库里真实存着的列值**上（`task_actor_rows` 读 actor_id 列，不走 find_task_actors）。
    //   ID 段：9137xx（本组独占，避开 900xxx／9141xx／9142xx 既有用例段）。
    // ═══════════════════════════════════════════════════════

    /// 直插参与者行（绕开 add_task_actor 的写侧归一，用来种未 trim 脏行 / 空值脏行）。
    /// 行 id 按 `task_id*100 + idx` **确定性**派生（对齐既有 i142 用例硬编码 914206001 的姿势）：
    /// 每个用例开头 `clean_i142_task_actor` 按 process_task_id 清场，正好覆盖本用例独占的 id 段，
    /// 跨进程重跑（含前一轮断言失败未走到尾部清场的残留）也不会撞主键。
    async fn seed_i137_task_actor(pool: &MySqlPool, task_id: i64, idx: i64, actor_id: &str) {
        let row_id = task_id * 100 + idx;
        sqlx::query("INSERT INTO wf_process_task_actor (id, process_task_id, actor_id, create_time) VALUES (?, ?, ?, ?)")
            .bind(row_id).bind(task_id).bind(actor_id).bind(current_time_str())
            .execute(pool).await.unwrap();
    }

    /// N 档（假成功修复，改前必红）：库里躺着修复前落下的未 trim 历史脏行「 9101 」＋规范行，
    /// 门面按语义 6 交出**行上的原值**去删 ⇒ 脏行必须真消失（真库 NO PAD 下原值形才命中）。
    /// 只取 trim 形的实现（改前 rust）把「 9101 」削成 9101，脏行留在库里、门面报成功。
    #[tokio::test]
    async fn test_mysql_i137_remove_task_actor_deletes_untrimmed_legacy_row_by_raw_form() {
        let task_id: i64 = 913701;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i142_task_actor(&pool, task_id).await;

        seed_i137_task_actor(&pool, task_id, 0, " 9101 ").await; // 未 trim 历史脏行（原值形）
        seed_i137_task_actor(&pool, task_id, 1, "leader").await; // 无关参与人，一行不许动

        let pool2 = pool.clone();
        run_sync(move || {
            SqlxRepository::new(pool2).remove_task_actor(task_id, &[" 9101 ".into()]).unwrap();
        }).await;

        assert_eq!(task_actor_rows(&pool, task_id).await, vec!["leader".to_string()],
            "语义 6：未 trim 历史脏行「 9101 」必须被原值形真删掉（真库读回；否则是假成功）");
        clean_i142_task_actor(&pool, task_id).await;
    }

    /// N 档（142 §9.2 那一路不破）：写侧归一后的规范行 8601，第三方绕过门面直连仓储传「 8601 」
    /// ⇒ 靠 trim 形也必须删得掉。改前改后都要绿。
    #[tokio::test]
    async fn test_mysql_i137_remove_task_actor_still_deletes_normalized_row_by_trimmed_form() {
        let task_id: i64 = 913702;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i142_task_actor(&pool, task_id).await;

        let pool2 = pool.clone();
        run_sync(move || {
            // add_task_actor 写侧归一 ⇒ 落库是 trim 后的规范行 8601
            SqlxRepository::new(pool2).add_task_actor(task_id, &["  8601  ".into(), "leader".into()]).unwrap();
        }).await;
        assert_eq!(task_actor_rows(&pool, task_id).await, vec!["8601".to_string(), "leader".to_string()],
            "前置：写侧落库是规范行 8601");

        let pool3 = pool.clone();
        run_sync(move || {
            SqlxRepository::new(pool3).remove_task_actor(task_id, &[" 8601 ".into()]).unwrap();
        }).await;

        assert_eq!(task_actor_rows(&pool, task_id).await, vec!["leader".to_string()],
            "规范行由 trim 形命中（issues/142 §9.2 既有判据不破）");
        clean_i142_task_actor(&pool, task_id).await;
    }

    /// 脏行与规范行并存 ⇒ 同一个人（§2.11 归一口径）名下两行都摘掉，其余参与人一行不动。
    #[tokio::test]
    async fn test_mysql_i137_remove_task_actor_removes_both_forms_together() {
        let task_id: i64 = 913703;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i142_task_actor(&pool, task_id).await;

        seed_i137_task_actor(&pool, task_id, 0, " 9101 ").await; // 脏行（原值形）
        seed_i137_task_actor(&pool, task_id, 1, "9101").await;   // 规范行（trim 形）
        seed_i137_task_actor(&pool, task_id, 2, "leader").await;
        seed_i137_task_actor(&pool, task_id, 3, "boss").await;

        let pool2 = pool.clone();
        run_sync(move || {
            SqlxRepository::new(pool2).remove_task_actor(task_id, &[" 9101 ".into()]).unwrap();
        }).await;

        let mut left = task_actor_rows(&pool, task_id).await;
        left.sort();
        assert_eq!(left, vec!["boss".to_string(), "leader".to_string()],
            "归一后同一个人 ⇒ 脏行与规范行两行都摘，其余参与人原样保留（语义 1）");
        clean_i142_task_actor(&pool, task_id).await;
    }

    /// P 档（脏行保护）：空值入参不得删掉 actor_id='' 脏行；全空入参（[""]／[]）⇒ 零删除，
    /// 不得清空全部参与者（语义 6 义务③：并集为空则早退，一条 DELETE 都不发）。
    #[tokio::test]
    async fn test_mysql_i137_blank_input_never_deletes_dirty_row_nor_clears_all() {
        let task_id: i64 = 913704;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i142_task_actor(&pool, task_id).await;

        seed_i137_task_actor(&pool, task_id, 0, "").await;     // 历史 actor_id='' 脏行
        seed_i137_task_actor(&pool, task_id, 1, "zhangsan").await;
        seed_i137_task_actor(&pool, task_id, 2, "leader").await;

        let pool2 = pool.clone();
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            repo.remove_task_actor(task_id, &["".to_string(), "   ".to_string(), "\t".to_string()]).unwrap();
            repo.remove_task_actor(task_id, &[]).unwrap();
        }).await;

        let rows = task_actor_rows(&pool, task_id).await;
        assert_eq!(rows.len(), 3, "空值/空列表入参 ⇒ 零删除，三行（含 actor_id='' 脏行）原样保留");
        assert!(rows.iter().any(|a| a == ""), "历史 actor_id='' 脏行不得被空串入参批量误删");
        assert!(rows.contains(&"zhangsan".to_string()) && rows.contains(&"leader".to_string()),
            "不得退化成清空全部参与者");
        clean_i142_task_actor(&pool, task_id).await;
    }

    /// 非参与者静默忽略（语义 7 幂等）＋任务不存在 ⇒ 零操作不 panic（真库这一支）。
    #[tokio::test]
    async fn test_mysql_i137_unknown_actor_ignored_and_unknown_task_noop() {
        let task_id: i64 = 913705;
        if skip_mysql() { return; }
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        clean_i142_task_actor(&pool, task_id).await;

        seed_i137_task_actor(&pool, task_id, 0, "zhangsan").await;
        seed_i137_task_actor(&pool, task_id, 1, "leader").await;

        let pool2 = pool.clone();
        run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            repo.remove_task_actor(task_id, &["stranger".into(), " 9999 ".into()]).unwrap();
            // 任务不存在（无任何参与者行）⇒ 零操作、不 panic
            repo.remove_task_actor(404404, &[" 9101 ".into()]).unwrap();
        }).await;

        assert_eq!(task_actor_rows(&pool, task_id).await,
            vec!["zhangsan".to_string(), "leader".to_string()],
            "非参与者静默忽略，既有参与者一行不动");
        assert!(task_actor_rows(&pool, 404404).await.is_empty(), "不存在任务的参与者台账仍为空");
        clean_i142_task_actor(&pool, task_id).await;
    }

    /// issues/154①（spec/06 §4.6 approvalRecord 口径①）：`find_history_tasks` 必须
    /// `ORDER BY id ASC`——雪花 id 单调，同秒并发插入时它比 `create_time`/`update_time` 确定。
    ///
    /// ⚠ 诚实交代：这条是**回归钉**，不是变异捕捉器。InnoDB 的聚簇扫描与二级索引
    /// （`process_instance_id` + PK）本就按主键升序返回，把 `ORDER BY` 摘掉在真库上大概率
    /// 仍出升序 ⇒ 别拿这条的绿去当"摘掉 ORDER BY 会红"的证据。真正判这一格的是 SQL 文本
    /// 本身（本轮现读已带 `ORDER BY id ASC`）与跨栈门禁的 approvalRecord 行序格。
    #[tokio::test]
    async fn test_mysql_i154_find_history_tasks_is_id_ascending() {
        if skip_mysql() { return; }
        let inst_id: i64 = 915410;
        let pool = connect_pool().await;
        setup_schema(&pool).await;
        sqlx::query("DELETE FROM wf_process_task WHERE process_instance_id = ?")
            .bind(inst_id).execute(&pool).await.unwrap();

        let pool2 = pool.clone();
        let ids = run_sync(move || {
            let repo = SqlxRepository::new(pool2);
            // 插入序刻意与 id 序相反（915413 → 915411）：出口行序只认 id
            for (name, task_id) in [("n3", 915413i64), ("n2", 915412), ("n1", 915411)] {
                let mut task = ProcessTask {
                    task_id,
                    process_instance_id: inst_id,
                    task_name: name.into(),
                    display_name: name.into(),
                    task_type: 0,
                    perform_type: 0,
                    task_state: 20,
                    actor_id: Some("user1".into()),
                    actor_ids: vec!["user1".into()],
                    finish_time: Some("2026-10-09 10:00:00".into()),
                    expire_time: None,
                    form_key: None,
                    parent_task_id: None,
                    variables: jeeflow_core::json::FlowData::new(),
                    create_time: Some("2026-10-09 10:00:00".into()),
                    create_user: Some("user1".into()),
                    update_time: None,
                    update_user: None,
                };
                repo.save_task(&mut task).unwrap();
            }
            repo.find_history_tasks(inst_id)
                .unwrap()
                .iter()
                .map(|t| t.task_id)
                .collect::<Vec<i64>>()
        }).await;

        assert_eq!(
            ids,
            vec![915411, 915412, 915413],
            "find_history_tasks 必须按 id ASC 出口（spec/06 §4.6 approvalRecord 口径①）"
        );
        sqlx::query("DELETE FROM wf_process_task WHERE process_instance_id = ?")
            .bind(inst_id).execute(&pool).await.unwrap();
    }
}
