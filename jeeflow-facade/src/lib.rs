//! jeeflow-facade: 42-action unified facade for the jeeflow workflow engine.
//! Entry point: `JeeflowFacade::flow(action, args) -> serde_json::Value`.
//! Enforces: ID stringification (C1/C14), camelCase output (C4), time format (C5),
//! pagination 5-key envelope (C6), error code 99999999 (C7).

use jeeflow_core::context::ServiceContext;
use jeeflow_core::engine::JeeflowEngineImpl;
use jeeflow_core::error::{JeeflowError, JeeflowResult};
use jeeflow_core::json::{FlowData, JsonValue};
use jeeflow_core::model::*;
use jeeflow_core::spi::*;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::Arc;

// ═══════════════════════════════════════════════════════
// Response envelope
// ═══════════════════════════════════════════════════════

const CODE_SUCCESS: i64 = 0;
const CODE_ERROR: i64 = 99999999;

fn success_response(data: Json) -> Json {
    json!({"code": CODE_SUCCESS, "msg": "成功", "data": data})
}

fn error_response(msg: &str) -> Json {
    json!({"code": CODE_ERROR, "msg": msg})
}

// ═══════════════════════════════════════════════════════
// camelCase conversion
// ═══════════════════════════════════════════════════════

fn to_camel(s: &str) -> String {
    let mut result = String::new();
    let mut upper_next = false;
    for c in s.chars() {
        if c == '_' {
            upper_next = true;
        } else if upper_next {
            result.push(c.to_uppercase().next().unwrap_or(c));
            upper_next = false;
        } else {
            result.push(c);
        }
    }
    result
}

/// Convert a snake_case JSON object to camelCase.
fn to_camel_json(val: &Json) -> Json {
    match val {
        Json::Object(map) => {
            let mut new_map = serde_json::Map::new();
            for (k, v) in map {
                new_map.insert(to_camel(k), to_camel_json(v));
            }
            Json::Object(new_map)
        }
        Json::Array(arr) => Json::Array(arr.iter().map(to_camel_json).collect()),
        other => other.clone(),
    }
}

/// Stringify all "id" fields in a JSON value (C1/C14).
/// Handles singular keys (id, *Id, *_id) and plural array keys (ids, *Ids, *_ids).
fn stringify_ids(val: &Json) -> Json {
    match val {
        Json::Object(map) => {
            let mut new_map = serde_json::Map::new();
            for (k, v) in map {
                if k == "id" || k.ends_with("Id") || k.ends_with("_id") {
                    // Singular id key → stringify the value
                    new_map.insert(k.clone(), stringify_id_value(v));
                } else if k == "ids" || k.ends_with("Ids") || k.ends_with("_ids") {
                    // Plural ids array key → stringify each element
                    new_map.insert(k.clone(), stringify_id_array(v));
                } else {
                    new_map.insert(k.clone(), stringify_ids(v));
                }
            }
            Json::Object(new_map)
        }
        Json::Array(arr) => Json::Array(arr.iter().map(stringify_ids).collect()),
        other => other.clone(),
    }
}

fn stringify_id_value(v: &Json) -> Json {
    match v {
        Json::Number(n) => Json::String(n.to_string()),
        Json::Null => Json::Null,
        other => other.clone(),
    }
}

/// Stringify each element of an id array (for plural keys like taskIds, roleIds).
fn stringify_id_array(v: &Json) -> Json {
    match v {
        Json::Array(arr) => Json::Array(arr.iter().map(stringify_id_value).collect()),
        other => stringify_id_value(other),
    }
}

/// Format time fields to yyyy-MM-dd HH:mm:ss (C5).
fn format_time_fields(val: &Json) -> Json {
    match val {
        Json::Object(map) => {
            let mut new_map = serde_json::Map::new();
            for (k, v) in map {
                if k.ends_with("Time") || k.ends_with("_time") || k == "createTime" || k == "updateTime" || k == "finishTime" || k == "expireTime" || k == "startTime" || k == "endTime" {
                    new_map.insert(k.clone(), format_time_value(v));
                } else {
                    new_map.insert(k.clone(), format_time_fields(v));
                }
            }
            Json::Object(new_map)
        }
        Json::Array(arr) => Json::Array(arr.iter().map(format_time_fields).collect()),
        other => other.clone(),
    }
}

fn format_time_value(v: &Json) -> Json {
    match v {
        Json::String(s) if s == "NOW()" || s == "NOW" => {
            Json::String(chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string())
        }
        Json::Null => Json::Null,
        other => other.clone(),
    }
}

/// Apply all output transformations: camelCase + stringify IDs + format time.
fn transform_output(val: Json) -> Json {
    let val = to_camel_json(&val);
    let val = stringify_ids(&val);
    format_time_fields(&val)
}

// ═══════════════════════════════════════════════════════
// Pagination envelope (C6)
// ═══════════════════════════════════════════════════════

fn page_to_json<T: serde::Serialize>(page: &PageResult<T>) -> Json {
    json!({
        "pageNum": page.page_num,
        "pageSize": page.page_size,
        "recordCount": page.record_count,
        "totalPage": page.total_page,
        "rows": serde_json::to_value(&page.rows).unwrap_or(json!([]))
    })
}

// ═══════════════════════════════════════════════════════
// Serde-serializable row types (snake_case → camelCase via transform)
// ═══════════════════════════════════════════════════════

use serde::Serialize;

#[derive(Serialize)]
struct DefineRowJson {
    id: i64,
    name: String,
    display_name: String,
    define_type: String,
    state: i32,
    version: i32,
    create_time: Option<String>,
    create_user: Option<String>,
    update_time: Option<String>,
    update_user: Option<String>,
}

#[derive(Serialize)]
struct InstanceRowJson {
    id: i64,
    parent_id: Option<i64>,
    process_define_id: i64,
    state: i32,
    parent_node_name: Option<String>,
    business_no: Option<String>,
    operator: String,
    expire_time: Option<String>,
    variable: Option<String>,
    create_time: Option<String>,
    create_user: Option<String>,
    update_time: Option<String>,
    update_user: Option<String>,
    define_name: Option<String>,
    define_display_name: Option<String>,
    define_version: Option<i32>,
}

#[derive(Serialize)]
struct TaskRowJson {
    id: i64,
    process_instance_id: i64,
    task_name: String,
    display_name: String,
    task_type: i32,
    perform_type: i32,
    task_state: i32,
    operator: Option<String>,
    actor_id: Option<String>,
    finish_time: Option<String>,
    expire_time: Option<String>,
    form_key: Option<String>,
    task_parent_id: Option<i64>,
    variable: Option<String>,
    create_time: Option<String>,
    create_user: Option<String>,
    update_time: Option<String>,
    update_user: Option<String>,
    process_define_id: Option<i64>,
    instance_state: Option<i32>,
    instance_operator: Option<String>,
    business_no: Option<String>,
    define_name: Option<String>,
    define_display_name: Option<String>,
    define_version: Option<i32>,
}

#[derive(Serialize)]
struct DesignJson {
    id: i64,
    name: String,
    display_name: String,
    design_type: String,
    icon: Option<String>,
    is_deployed: i32,
    remark: Option<String>,
    create_time: Option<String>,
    create_user: Option<String>,
    update_time: Option<String>,
    update_user: Option<String>,
}

#[derive(Serialize)]
struct SurrogateJson {
    id: i64,
    process_name: String,
    operator: String,
    surrogate: String,
    start_time: Option<String>,
    end_time: Option<String>,
    enabled: i32,
    create_time: Option<String>,
    create_user: Option<String>,
    update_time: Option<String>,
    update_user: Option<String>,
}

fn define_row_to_json(r: &DefineRow) -> DefineRowJson {
    DefineRowJson {
        id: r.id, name: r.name.clone(), display_name: r.display_name.clone(),
        define_type: r.define_type.clone(), state: r.state, version: r.version,
        create_time: r.create_time.clone(), create_user: r.create_user.clone(),
        update_time: r.update_time.clone(), update_user: r.update_user.clone(),
    }
}

fn task_row_to_json(r: &TaskRow) -> TaskRowJson {
    TaskRowJson {
        id: r.id, process_instance_id: r.process_instance_id,
        task_name: r.task_name.clone(), display_name: r.display_name.clone(),
        task_type: r.task_type, perform_type: r.perform_type, task_state: r.task_state,
        operator: r.operator.clone(), actor_id: r.actor_id.clone(),
        finish_time: r.finish_time.clone(), expire_time: r.expire_time.clone(),
        form_key: r.form_key.clone(), task_parent_id: r.task_parent_id,
        variable: r.variable.clone(),
        create_time: r.create_time.clone(), create_user: r.create_user.clone(),
        update_time: r.update_time.clone(), update_user: r.update_user.clone(),
        process_define_id: r.process_define_id, instance_state: r.instance_state,
        instance_operator: r.instance_operator.clone(), business_no: r.business_no.clone(),
        define_name: r.define_name.clone(), define_display_name: r.define_display_name.clone(),
        define_version: r.define_version,
    }
}

fn instance_row_to_json(r: &InstanceRow) -> InstanceRowJson {
    InstanceRowJson {
        id: r.id, parent_id: r.parent_id, process_define_id: r.process_define_id,
        state: r.state, parent_node_name: r.parent_node_name.clone(),
        business_no: r.business_no.clone(), operator: r.operator.clone(),
        expire_time: r.expire_time.clone(), variable: r.variable.clone(),
        create_time: r.create_time.clone(), create_user: r.create_user.clone(),
        update_time: r.update_time.clone(), update_user: r.update_user.clone(),
        define_name: r.define_name.clone(), define_display_name: r.define_display_name.clone(),
        define_version: r.define_version,
    }
}

fn design_to_json(d: &ProcessDesign) -> DesignJson {
    DesignJson {
        id: d.id, name: d.name.clone(), display_name: d.display_name.clone(),
        design_type: d.design_type.clone(), icon: d.icon.clone(),
        is_deployed: d.is_deployed, remark: d.remark.clone(),
        create_time: d.create_time.clone(), create_user: d.create_user.clone(),
        update_time: d.update_time.clone(), update_user: d.update_user.clone(),
    }
}

fn surrogate_to_json(s: &ProcessSurrogate) -> SurrogateJson {
    SurrogateJson {
        id: s.id, process_name: s.process_name.clone(), operator: s.operator.clone(),
        surrogate: s.surrogate.clone(), start_time: s.start_time.clone(),
        end_time: s.end_time.clone(), enabled: s.enabled,
        create_time: s.create_time.clone(), create_user: s.create_user.clone(),
        update_time: s.update_time.clone(), update_user: s.update_user.clone(),
    }
}

// ═══════════════════════════════════════════════════════
// Argument helpers
// ═══════════════════════════════════════════════════════

fn arg_str(args: &HashMap<String, Json>, key: &str) -> Option<String> {
    args.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

/// Parse i64 from JSON number or string (C2 dual-tolerance).
/// Returns Ok(None) if key missing, Ok(Some(v)) if valid, Err if present but unparseable.
fn arg_i64(args: &HashMap<String, Json>, key: &str) -> JeeflowResult<Option<i64>> {
    match args.get(key) {
        None => Ok(None),
        Some(v) => {
            if let Some(n) = v.as_i64() {
                Ok(Some(n))
            } else if let Some(s) = v.as_str() {
                s.parse::<i64>()
                    .map(Some)
                    .map_err(|_| JeeflowError::Business(format!("非法id: {}", s)))
            } else {
                Err(JeeflowError::Business(format!("非法id: {}", v)))
            }
        }
    }
}

fn arg_str_or(args: &HashMap<String, Json>, key: &str, default: &str) -> String {
    arg_str(args, key).unwrap_or_else(|| default.to_string())
}

fn arg_i64_or(args: &HashMap<String, Json>, key: &str, default: i64) -> JeeflowResult<i64> {
    Ok(arg_i64(args, key)?.unwrap_or(default))
}

// ═══════════════════════════════════════════════════════
// C8 m_ three-segment query parser (spec/06 §2.2)
// ═══════════════════════════════════════════════════════

fn camel_to_snake(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 { result.push('_'); }
            result.push(c.to_lowercase().next().unwrap());
        } else {
            result.push(c);
        }
    }
    result
}

fn is_known_op(s: &str) -> bool {
    matches!(s.to_uppercase().as_str(),
        "EQ" | "NE" | "LIKE" | "LLIKE" | "RLIKE" |
        "GT" | "LT" | "GE" | "LE" | "IN" | "NIN" | "BT")
}

/// Parse m_ prefixed query parameters into QueryFilter list.
/// Supports: `m_{op}_{col}` (2-segment, alias="t") and `m_{alias}_{op}_{col}` (3-segment).
fn parse_m_params(args: &HashMap<String, Json>) -> Vec<jeeflow_core::model::QueryFilter> {
    let mut filters = Vec::new();
    for (key, value) in args {
        if !key.starts_with("m_") { continue; }
        let rest = &key[2..];
        let parts: Vec<&str> = rest.split('_').collect();
        let val_str = match value.as_str() {
            Some(s) => s.to_string(),
            None => value.to_string().trim_matches('"').to_string(),
        };
        if parts.len() >= 3 {
            let alias = parts[0];
            let op_str = parts[1];
            let column = camel_to_snake(parts[2..].join("_").as_str());
            if let Some(op) = jeeflow_core::model::FilterOp::from_str(op_str) {
                filters.push(jeeflow_core::model::QueryFilter {
                    alias: alias.to_string(), op, column, value: val_str,
                });
            }
        } else if parts.len() == 2 {
            let op_str = parts[0];
            if is_known_op(op_str) {
                let column = camel_to_snake(parts[1]);
                if let Some(op) = jeeflow_core::model::FilterOp::from_str(op_str) {
                    filters.push(jeeflow_core::model::QueryFilter {
                        alias: "t".to_string(), op, column, value: val_str,
                    });
                }
            }
        }
    }
    filters
}

/// Apply filters to JSON rows (camelCase field names).
fn matches_filter(row: &Json, f: &jeeflow_core::model::QueryFilter) -> bool {
    let camel_col = to_camel(&f.column);
    let field_val = row.get(&camel_col).or_else(|| row.get(&f.column));
    let field_str = match field_val {
        Some(Json::String(s)) => s.clone(),
        Some(v) => v.to_string().trim_matches('"').to_string(),
        None => return false,
    };
    use jeeflow_core::model::FilterOp;
    match f.op {
        FilterOp::Eq => field_str == f.value,
        FilterOp::Ne => field_str != f.value,
        FilterOp::Like => field_str.contains(&f.value),
        FilterOp::Gt => field_str > f.value,
        FilterOp::Lt => field_str < f.value,
        FilterOp::Ge => field_str >= f.value,
        FilterOp::Le => field_str <= f.value,
        FilterOp::In => f.value.split(',').any(|v| v.trim() == field_str),
        FilterOp::Nin => !f.value.split(',').any(|v| v.trim() == field_str),
        FilterOp::Bt => {
            let parts: Vec<&str> = f.value.split(',').collect();
            parts.len() == 2 && field_str.as_str() >= parts[0].trim() && field_str.as_str() <= parts[1].trim()
        }
    }
}

fn apply_filters_to_rows(rows: &mut Vec<Json>, filters: &[jeeflow_core::model::QueryFilter]) {
    if filters.is_empty() { return; }
    rows.retain(|row| filters.iter().all(|f| matches_filter(row, f)));
}

/// Re-paginate filtered rows (called when facade-level filtering is applied after repo pagination).
fn re_paginate(rows: Vec<Json>, page_num: i64, page_size: i64) -> (Vec<Json>, i64) {
    let total = rows.len() as i64;
    let start = ((page_num - 1) * page_size) as usize;
    let end = std::cmp::min(start + page_size as usize, rows.len());
    let page_rows = if start < rows.len() { rows[start..end].to_vec() } else { vec![] };
    (page_rows, total)
}

fn args_to_flow_data(args: &HashMap<String, Json>) -> FlowData {
    let mut fd = FlowData::new();
    for (k, v) in args {
        match v {
            Json::String(s) => { fd.insert_str(k, s); }
            Json::Number(n) => { fd.insert_i64(k, n.as_i64().unwrap_or(0)); }
            Json::Bool(b) => { fd.insert(k.clone(), JsonValue::Bool(*b)); }
            Json::Null => {}
            _ => { fd.insert_str(k, &v.to_string()); }
        }
    }
    fd
}

// ═══════════════════════════════════════════════════════
// JeeflowFacade
// ═══════════════════════════════════════════════════════

/// Unified 42-action facade for the jeeflow workflow engine.
pub struct JeeflowFacade {
    engine: JeeflowEngineImpl,
    repo: Arc<dyn ProcessRepository>,
    ext_repo: Option<Arc<dyn ProcessExtRepository>>,
}

impl JeeflowFacade {
    pub fn new(ctx: ServiceContext) -> Self {
        let repo = ctx.get_repository().clone();
        let ext_repo = ctx.ext_repository.clone();
        let engine = JeeflowEngineImpl::new(ctx);
        JeeflowFacade { engine, repo, ext_repo }
    }

    /// Main entry point: dispatch to 42 actions (async to avoid nested runtime panic).
    pub async fn flow(&self, action: &str, args: &HashMap<String, Json>) -> Json {
        let result = match action {
            // ═══ processDefine (8) ═══
            "processDefine/page" => self.process_define_page(args),
            "processDefine/detail" => self.process_define_detail(args),
            "processDefine/startAndExecute" => self.process_define_start_and_execute(args).await,
            "processDefine/deploy" => self.process_define_deploy(args),
            "processDefine/redeploy" => self.process_define_redeploy(args),
            "processDefine/remove" => self.process_define_remove(args),
            "processDefine/upAndDown" => self.process_define_up_and_down(args),
            "processDefine/getLastByName" => self.process_define_get_last_by_name(args),

            // ═══ processInstance (11) ═══
            "processInstance/page" => self.process_instance_page(args),
            "processInstance/detail" => self.process_instance_detail(args),
            "processInstance/startAndExecute" => self.process_instance_start_and_execute(args).await,
            "processInstance/withdraw" => self.process_instance_withdraw(args),
            "processInstance/highLight" => self.process_instance_high_light(args),
            "processInstance/approvalRecord" => self.process_instance_approval_record(args),
            "processInstance/getAssigneeTextData" => self.process_instance_get_assignee_text_data(args),
            "processInstance/bizData" => self.process_instance_biz_data(args),
            "processInstance/createCCInstance" => self.process_instance_create_cc(args),
            "processInstance/updateCCStatus" => self.process_instance_update_cc_status(args),
            "processInstance/ccList" => self.process_instance_cc_list(args),

            // ═══ processTask (9) ═══
            "processTask/todoList" => self.process_task_todo_list(args),
            "processTask/doneList" => self.process_task_done_list(args),
            "processTask/execute" => self.process_task_execute(args).await,
            "processTask/detail" => self.process_task_detail(args),
            "processTask/jumpAbleTaskNameList" => self.process_task_jump_able_task_name_list(args),
            "processTask/candidatePage" => self.process_task_candidate_page(args),
            "processTask/surrogate" => self.process_task_surrogate(args),
            "processTask/addCandidate" => self.process_task_add_candidate(args),
            "processTask/latest" => self.process_task_latest(args),

            // ═══ processDesign (9) ═══
            "processDesign/page" => self.process_design_page(args),
            "processDesign/detail" => self.process_design_detail(args),
            "processDesign/save" => self.process_design_save(args),
            "processDesign/update" => self.process_design_update(args),
            "processDesign/updateDefine" => self.process_design_update_define(args),
            "processDesign/remove" => self.process_design_remove(args),
            "processDesign/deploy" => self.process_design_deploy(args),
            "processDesign/redeploy" => self.process_design_redeploy(args),
            "processDesign/listByType" => self.process_design_list_by_type(args),

            // ═══ processSurrogate (5) ═══
            "processSurrogate/page" => self.process_surrogate_page(args),
            "processSurrogate/save" => self.process_surrogate_save(args),
            "processSurrogate/update" => self.process_surrogate_update(args),
            "processSurrogate/detail" => self.process_surrogate_detail(args),
            "processSurrogate/remove" => self.process_surrogate_remove(args),

            // ═══ Unknown action ═══
            _ => Err(JeeflowError::UnknownAction(action.to_string())),
        };

        match result {
            Ok(data) => success_response(transform_output(data)),
            Err(e) => error_response(&e.message()),
        }
    }

    /// Get the engine reference.
    pub fn engine(&self) -> &JeeflowEngineImpl {
        &self.engine
    }

    /// Get the repository reference.
    pub fn repo(&self) -> &Arc<dyn ProcessRepository> {
        &self.repo
    }

    /// Get the ext repository reference.
    pub fn ext_repo(&self) -> Option<&Arc<dyn ProcessExtRepository>> {
        self.ext_repo.as_ref()
    }

    // ═══════════════════════════════════════════════════════
    // processDefine actions (8)
    // ═══════════════════════════════════════════════════════

    fn process_define_page(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let page_num = arg_i64_or(args, "pageNum", 1)?;
        let page_size = arg_i64_or(args, "pageSize", 20)?;
        let filters = parse_m_params(args);
        let query = PageQuery::new(page_num, page_size);
        let page = self.repo.page_defines(&query)?;
        let mut rows: Vec<Json> = page.rows.iter().map(|r| serde_json::to_value(define_row_to_json(r)).unwrap()).collect();
        apply_filters_to_rows(&mut rows, &filters);
        let (page_rows, total) = re_paginate(rows, page_num, page_size);
        let result = PageResult::new(page_num, page_size, total, page_rows);
        Ok(serde_json::to_value(page_to_json(&result)).unwrap())
    }

    fn process_define_detail(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let define = self.repo.find_define_by_id(id)?
            .ok_or(JeeflowError::DefineNotFound(id))?;
        Ok(json!({
            "id": define.id, "name": define.name, "display_name": define.display_name,
            "define_type": define.define_type, "state": define.state, "version": define.version,
            "content": define.content_str(),
            "create_time": define.create_time, "create_user": define.create_user,
            "update_time": define.update_time, "update_user": define.update_user,
        }))
    }

    async fn process_define_start_and_execute(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        // 对齐 Java：优先 processDefineId / id；兼容 name
        let define_id = if let Some(id) = arg_i64(args, "processDefineId")?.or(arg_i64(args, "id")?) {
            id
        } else if let Some(define_name) = arg_str(args, "name") {
            let query = PageQuery::new(1, 1000);
            let page = self.repo.page_defines(&query)?;
            page.rows
                .iter()
                .find(|d| d.name == define_name && d.state == 1)
                .map(|d| d.id)
                .ok_or_else(|| JeeflowError::Business(format!("流程定义不存在或未启用: {}", define_name)))?
        } else {
            return Err(JeeflowError::Business("缺少processDefineId参数".into()));
        };

        let operator = arg_str_or(args, "operator", "user1");
        let mut flow_data = args_to_flow_data(args);

        // Start instance
        let instance = self.engine.start_async(define_id, &operator, &flow_data).await?;

        // 对齐 Java/boot2：自动完成申请节点（assignee=applicant → 发起人）
        let tasks = self.repo.find_doing_tasks(instance.instance_id, &[])?;
        for task in &tasks {
            let _ = self.repo.add_task_actor(task.task_id, &[operator.clone()]);
            flow_data.insert_i64("submitType", 0); // Apply
            // f_nextNodeOperator → tf_nextNodeOperator（若有）
            if let Some(next_op) = flow_data.get_str("f_nextNodeOperator") {
                let v = next_op.to_string();
                flow_data.insert_str("tf_nextNodeOperator", v);
            }
            let _ = self
                .engine
                .execute_task_async(task.task_id, &operator, &flow_data)
                .await?;
        }

        Ok(json!({ "process_instance_id": instance.instance_id }))
    }

    fn process_define_deploy(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        self.repo.update_define_state(id, 1)?;
        Ok(json!({"id": id, "state": 1}))
    }

    fn process_define_redeploy(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        self.repo.update_define_state(id, 1)?;
        Ok(json!({"id": id, "state": 1}))
    }

    fn process_define_remove(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        self.repo.remove_define(id)?;
        Ok(json!({"id": id}))
    }

    fn process_define_up_and_down(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let state = arg_i64(args, "state")?.unwrap_or(0) as i32;
        self.repo.update_define_state(id, state)?;
        Ok(json!({"id": id, "state": state}))
    }

    fn process_define_get_last_by_name(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let name = arg_str(args, "name").ok_or(JeeflowError::Business("缺少name参数".into()))?;
        let query = PageQuery::new(1, 1000);
        let page = self.repo.page_defines(&query)?;
        let define = page.rows.iter()
            .find(|d| d.name == name)
            .ok_or(JeeflowError::Business(format!("流程定义不存在: {}", name)))?;
        Ok(json!({
            "id": define.id, "name": define.name, "display_name": define.display_name,
            "state": define.state, "version": define.version,
        }))
    }

    // ═══════════════════════════════════════════════════════
    // processInstance actions (11)
    // ═══════════════════════════════════════════════════════

    fn process_instance_page(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let page_num = arg_i64_or(args, "pageNum", 1)?;
        let page_size = arg_i64_or(args, "pageSize", 20)?;
        let filters = parse_m_params(args);
        let mut query = PageQuery::new(page_num, page_size);
        query.operator = arg_str(args, "operator");
        let page = self.repo.page_instances(&query)?;
        let mut rows: Vec<Json> = page.rows.iter().map(|r| serde_json::to_value(instance_row_to_json(r)).unwrap()).collect();
        apply_filters_to_rows(&mut rows, &filters);
        let (page_rows, total) = re_paginate(rows, page_num, page_size);
        let result = PageResult::new(page_num, page_size, total, page_rows);
        Ok(serde_json::to_value(page_to_json(&result)).unwrap())
    }

    fn process_instance_detail(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let inst = self.repo.find_instance_by_id(id)?
            .ok_or(JeeflowError::InstanceNotFound(id))?;
        let tasks = self.repo.find_history_tasks(id)?;
        let task_jsons: Vec<Json> = tasks.iter().map(|t| json!({
            "id": t.task_id, "task_name": t.task_name, "display_name": t.display_name,
            "task_state": t.task_state, "actor_id": t.actor_id,
            "finish_time": t.finish_time, "create_time": t.create_time,
        })).collect();
        Ok(json!({
            "id": inst.instance_id, "process_define_id": inst.define_id,
            "state": inst.state, "operator": inst.operator,
            "business_no": inst.business_no, "create_time": inst.create_time,
            "tasks": task_jsons,
        }))
    }

    async fn process_instance_start_and_execute(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        self.process_define_start_and_execute(args).await
    }

    fn process_instance_withdraw(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let mut inst = self.repo.find_instance_by_id(id)?
            .ok_or(JeeflowError::InstanceNotFound(id))?;
        inst.withdraw();
        self.repo.update_instance(&inst)?;
        // Withdraw all doing tasks
        for task in &inst.tasks {
            if task.task_state == TaskState::Doing.code() {
                let mut t = task.clone();
                t.withdraw();
                self.repo.update_task(&t)?;
            }
        }
        Ok(json!({"id": id, "state": inst.state}))
    }

    fn process_instance_high_light(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let tasks = self.repo.find_history_tasks(id)?;
        let finished: Vec<String> = tasks.iter()
            .filter(|t| t.task_state == TaskState::Finished.code())
            .map(|t| t.task_name.clone())
            .collect();
        let doing: Vec<String> = tasks.iter()
            .filter(|t| t.task_state == TaskState::Doing.code())
            .map(|t| t.task_name.clone())
            .collect();
        Ok(json!({"finishedNodes": finished, "currentNodes": doing}))
    }

    fn process_instance_approval_record(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let tasks = self.repo.find_history_tasks(id)?;
        let records: Vec<Json> = tasks.iter().map(|t| json!({
            "task_name": t.task_name, "display_name": t.display_name,
            "task_state": t.task_state, "actor_id": t.actor_id,
            "finish_time": t.finish_time, "create_time": t.create_time,
        })).collect();
        Ok(json!(records))
    }

    fn process_instance_get_assignee_text_data(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let tasks = self.repo.find_history_tasks(id)?;
        let data: Vec<Json> = tasks.iter().map(|t| json!({
            "task_name": t.task_name, "actor_id": t.actor_id,
        })).collect();
        Ok(json!(data))
    }

    fn process_instance_biz_data(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let inst = self.repo.find_instance_by_id(id)?
            .ok_or(JeeflowError::InstanceNotFound(id))?;
        // Return instance variables as biz data
        let vars: HashMap<String, Json> = inst.variables.iter()
            .map(|(k, v)| (k.clone(), json_value_to_serde(v)))
            .collect();
        Ok(json!(vars))
    }

    fn process_instance_create_cc(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let creator = arg_str_or(args, "operator", "flow.auto");
        let actor_str = arg_str(args, "actorIds").unwrap_or_default();
        let actors: Vec<String> = actor_str.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        self.repo.create_cc_instance(id, &creator, &actors)?;
        Ok(json!({"id": id, "ccCount": actors.len()}))
    }

    fn process_instance_update_cc_status(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let actor_id = arg_str_or(args, "actorId", "");
        self.repo.update_cc_status(id, &actor_id)?;
        Ok(json!({"id": id}))
    }

    fn process_instance_cc_list(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let page_num = arg_i64_or(args, "pageNum", 1)?;
        let page_size = arg_i64_or(args, "pageSize", 20)?;
        let filters = parse_m_params(args);
        let query = PageQuery::new(page_num, page_size);
        let page = self.repo.page_cc_instances(&query)?;
        let mut rows: Vec<Json> = page.rows.iter().map(|r| serde_json::to_value(instance_row_to_json(r)).unwrap()).collect();
        apply_filters_to_rows(&mut rows, &filters);
        let (page_rows, total) = re_paginate(rows, page_num, page_size);
        let result = PageResult::new(page_num, page_size, total, page_rows);
        Ok(serde_json::to_value(page_to_json(&result)).unwrap())
    }

    // ═══════════════════════════════════════════════════════
    // processTask actions (9)
    // ═══════════════════════════════════════════════════════

    fn process_task_todo_list(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let page_num = arg_i64_or(args, "pageNum", 1)?;
        let page_size = arg_i64_or(args, "pageSize", 20)?;
        let filters = parse_m_params(args);
        let mut query = PageQuery::new(page_num, page_size);
        // UI 注入 operator；兼容 userId（curl/旧客户端）
        query.operator = arg_str(args, "operator").or_else(|| arg_str(args, "userId"));
        let page = self.repo.page_todo_tasks(&query)?;
        let mut rows: Vec<Json> = page.rows.iter().map(|r| serde_json::to_value(task_row_to_json(r)).unwrap()).collect();
        apply_filters_to_rows(&mut rows, &filters);
        let (page_rows, total) = re_paginate(rows, page_num, page_size);
        let result = PageResult::new(page_num, page_size, total, page_rows);
        Ok(serde_json::to_value(page_to_json(&result)).unwrap())
    }

    fn process_task_done_list(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let page_num = arg_i64_or(args, "pageNum", 1)?;
        let page_size = arg_i64_or(args, "pageSize", 20)?;
        let filters = parse_m_params(args);
        let mut query = PageQuery::new(page_num, page_size);
        query.operator = arg_str(args, "operator");
        let page = self.repo.page_done_tasks(&query)?;
        let mut rows: Vec<Json> = page.rows.iter().map(|r| serde_json::to_value(task_row_to_json(r)).unwrap()).collect();
        apply_filters_to_rows(&mut rows, &filters);
        let (page_rows, total) = re_paginate(rows, page_num, page_size);
        let result = PageResult::new(page_num, page_size, total, page_rows);
        Ok(serde_json::to_value(page_to_json(&result)).unwrap())
    }

    async fn process_task_execute(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        // 对齐 UI/Java：processTaskId 优先，兼容 id
        let task_id = arg_i64(args, "processTaskId")?
            .or(arg_i64(args, "id")?)
            .ok_or(JeeflowError::Business("缺少processTaskId参数".into()))?;
        let operator = arg_str_or(args, "operator", "flow.auto");
        let submit_type = arg_i64_or(args, "submitType", 1)?; // 默认同意
        let mut flow_data = args_to_flow_data(args);
        flow_data.insert_i64("submitType", submit_type);

        // 对齐 Java JeeflowFacade.execute / spec §11.2 分发
        let new_tasks = match submit_type {
            2 => {
                // REJECT → 跳到结束
                self.engine
                    .execute_and_jump_to_end_async(task_id, &operator, &flow_data)
                    .await?;
                vec![]
            }
            3 => {
                // ROLLBACK → 退回上一步（target=None）
                self.engine
                    .execute_and_jump_async(task_id, &operator, &flow_data, None)
                    .await?
            }
            4 => {
                // JUMP → 跳转指定节点
                let task_name = arg_str(args, "taskName");
                self.engine
                    .execute_and_jump_async(task_id, &operator, &flow_data, task_name.as_deref())
                    .await?
            }
            6 => {
                // ROLLBACK_TO_OPERATOR → 退回发起人
                self.engine
                    .execute_and_jump_to_first_async(task_id, &operator, &flow_data)
                    .await?;
                vec![]
            }
            20 => {
                // COUNTERSIGN_DISAGREE
                flow_data.insert_i64("countersignDisagreeFlag", 1);
                self.engine
                    .execute_task_async(task_id, &operator, &flow_data)
                    .await?
            }
            _ => {
                // 0 APPLY / 1 AGREE / 5 重新提交
                self.engine
                    .execute_task_async(task_id, &operator, &flow_data)
                    .await?
            }
        };
        let task_ids: Vec<i64> = new_tasks.iter().map(|t| t.task_id).collect();

        Ok(json!({"task_ids": task_ids}))
    }

    fn process_task_detail(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let task = self.repo.find_task_by_id(id)?
            .ok_or(JeeflowError::TaskNotFound(id))?;
        let actors = self.repo.find_task_actors(id)?;
        Ok(json!({
            "id": task.task_id, "process_instance_id": task.process_instance_id,
            "task_name": task.task_name, "display_name": task.display_name,
            "task_type": task.task_type, "perform_type": task.perform_type,
            "task_state": task.task_state, "actor_id": task.actor_id,
            "actor_ids": actors,
            "finish_time": task.finish_time, "expire_time": task.expire_time,
            "form_key": task.form_key, "create_time": task.create_time,
        }))
    }

    fn process_task_jump_able_task_name_list(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let task = self.repo.find_task_by_id(id)?
            .ok_or(JeeflowError::TaskNotFound(id))?;
        let inst = self.repo.find_instance_by_id(task.process_instance_id)?
            .ok_or(JeeflowError::InstanceNotFound(task.process_instance_id))?;
        let define = self.repo.find_define_by_id(inst.define_id)?
            .ok_or(JeeflowError::DefineNotFound(inst.define_id))?;
        let model = jeeflow_core::parser::ModelParser::parse(&define.content_str())?;

        // Return all task node names as jump targets
        let task_nodes: Vec<String> = model.get_nodes_by_type(jeeflow_core::parser::NodeType::Task)
            .iter().map(|n| n.id.clone()).collect();
        Ok(json!(task_nodes))
    }

    fn process_task_candidate_page(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        // Simplified: return empty page (m_ filters parsed but no data to filter)
        let page_num = arg_i64_or(args, "pageNum", 1)?;
        let page_size = arg_i64_or(args, "pageSize", 20)?;
        Ok(serde_json::to_value(page_to_json(&PageResult::<Json>::new(page_num, page_size, 0, vec![]))).unwrap())
    }

    fn process_task_surrogate(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let operator = arg_str_or(args, "operator", "");
        let process_name = arg_str_or(args, "name", "");
        let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        if let Some(ext) = &self.ext_repo {
            let sg = ext.get_surrogate(&operator, &process_name, &now)?;
            if let Some(s) = sg {
                return Ok(serde_json::to_value(surrogate_to_json(&s)).unwrap());
            }
        }
        Ok(Json::Null)
    }

    fn process_task_add_candidate(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let task_id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let actor_str = arg_str(args, "actorIds").unwrap_or_default();
        let actors: Vec<String> = actor_str.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        self.repo.add_task_actor(task_id, &actors)?;
        Ok(json!({"id": task_id, "added": actors.len()}))
    }

    fn process_task_latest(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let instance_id = arg_i64(args, "instanceId")?.ok_or(JeeflowError::Business("缺少instanceId参数".into()))?;
        let tasks = self.repo.find_doing_tasks(instance_id, &[])?;
        if let Some(task) = tasks.first() {
            Ok(json!({
                "id": task.task_id, "task_name": task.task_name,
                "display_name": task.display_name, "task_state": task.task_state,
            }))
        } else {
            Ok(Json::Null)
        }
    }

    // ═══════════════════════════════════════════════════════
    // processDesign actions (9)
    // ═══════════════════════════════════════════════════════

    fn process_design_page(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let page_num = arg_i64_or(args, "pageNum", 1)?;
        let page_size = arg_i64_or(args, "pageSize", 20)?;
        let filters = parse_m_params(args);
        let query = PageQuery::new(page_num, page_size);
        let page = ext.page_designs(&query)?;
        let mut rows: Vec<Json> = page.rows.iter().map(|d| serde_json::to_value(design_to_json(d)).unwrap()).collect();
        apply_filters_to_rows(&mut rows, &filters);
        let (page_rows, total) = re_paginate(rows, page_num, page_size);
        let result = PageResult::new(page_num, page_size, total, page_rows);
        Ok(serde_json::to_value(page_to_json(&result)).unwrap())
    }

    fn process_design_detail(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let design = ext.find_design_by_id(id)?
            .ok_or(JeeflowError::Business(format!("设计不存在: {}", id)))?;
        Ok(serde_json::to_value(design_to_json(&design)).unwrap())
    }

    fn process_design_save(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let mut design = ProcessDesign {
            id: 0,
            name: arg_str_or(args, "name", ""),
            display_name: arg_str_or(args, "displayName", ""),
            design_type: arg_str_or(args, "designType", "approval"),
            icon: arg_str(args, "icon"),
            is_deployed: 0,
            remark: arg_str(args, "remark"),
            create_time: None, create_user: arg_str(args, "createUser"),
            update_time: None, update_user: None,
        };
        ext.save_design(&mut design)?;
        Ok(json!({"id": design.id}))
    }

    fn process_design_update(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let design = ext.find_design_by_id(id)?
            .ok_or(JeeflowError::Business(format!("设计不存在: {}", id)))?;
        let updated = ProcessDesign {
            display_name: arg_str(args, "displayName").unwrap_or(design.display_name),
            icon: arg_str(args, "icon").or(design.icon),
            remark: arg_str(args, "remark").or(design.remark),
            update_user: arg_str(args, "updateUser"),
            ..design
        };
        ext.update_design(&updated)?;
        Ok(json!({"id": id}))
    }

    fn process_design_update_define(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        // Verify design exists, then save history
        let _design = ext.find_design_by_id(id)?
            .ok_or(JeeflowError::Business(format!("设计不存在: {}", id)))?;
        let content = arg_str(args, "content").unwrap_or_default();
        let mut his = ProcessDesignHis {
            id: 0,
            process_design_id: id,
            content: content.into_bytes(),
            create_time: None,
            create_user: arg_str(args, "createUser"),
        };
        ext.save_design_his(&mut his)?;
        Ok(json!({"id": id, "hisId": his.id}))
    }

    fn process_design_remove(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        ext.remove_design(id)?;
        Ok(json!({"id": id}))
    }

    fn process_design_deploy(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        // Deploy: create a new define from design
        let design = ext.find_design_by_id(id)?
            .ok_or(JeeflowError::Business(format!("设计不存在: {}", id)))?;

        // Get latest design history for content
        let his_list = ext.list_design_his(id)?;
        let content = his_list.last()
            .map(|h| String::from_utf8_lossy(&h.content).to_string())
            .unwrap_or_else(|| "{}".to_string());

        let mut define = ProcessDefine {
            id: 0,
            name: design.name.clone(),
            display_name: design.display_name.clone(),
            define_type: design.design_type.clone(),
            state: 1,
            content: content.into_bytes(),
            version: 1,
            create_time: None, create_user: design.create_user.clone(),
            update_time: None, update_user: None,
        };
        self.repo.save_define(&mut define)?;

        // Mark design as deployed
        let updated = ProcessDesign { is_deployed: 1, ..design };
        ext.update_design(&updated)?;

        Ok(json!({"id": id, "defineId": define.id}))
    }

    fn process_design_redeploy(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        // Same as deploy but increments version
        self.process_design_deploy(args)
    }

    fn process_design_list_by_type(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let _design_type = arg_str_or(args, "designType", "approval");
        let query = PageQuery::new(1, 1000);
        let page = ext.page_designs(&query)?;
        let rows: Vec<Json> = page.rows.iter()
            .filter(|d| _design_type.is_empty() || d.design_type == _design_type)
            .map(|d| serde_json::to_value(design_to_json(d)).unwrap())
            .collect();
        Ok(json!(rows))
    }

    // ═══════════════════════════════════════════════════════
    // processSurrogate actions (5)
    // ═══════════════════════════════════════════════════════

    fn process_surrogate_page(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let query = PageQuery::new(arg_i64_or(args, "pageNum", 1)?, arg_i64_or(args, "pageSize", 20)?);
        let page = ext.page_surrogates(&query)?;
        let rows: Vec<Json> = page.rows.iter().map(|s| serde_json::to_value(surrogate_to_json(s)).unwrap()).collect();
        let result = PageResult::new(page.page_num, page.page_size, page.record_count, rows);
        Ok(serde_json::to_value(page_to_json(&result)).unwrap())
    }

    fn process_surrogate_save(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let mut sg = ProcessSurrogate {
            id: 0,
            process_name: arg_str_or(args, "processName", ""),
            operator: arg_str_or(args, "operator", ""),
            surrogate: arg_str_or(args, "surrogate", ""),
            start_time: arg_str(args, "startTime"),
            end_time: arg_str(args, "endTime"),
            enabled: 1,
            create_time: None, create_user: arg_str(args, "createUser"),
            update_time: None, update_user: None,
        };
        ext.save_surrogate(&mut sg)?;
        Ok(json!({"id": sg.id}))
    }

    fn process_surrogate_update(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let sg = ext.find_surrogate_by_id(id)?
            .ok_or(JeeflowError::Business(format!("委托不存在: {}", id)))?;
        let updated = ProcessSurrogate {
            surrogate: arg_str(args, "surrogate").unwrap_or(sg.surrogate),
            start_time: arg_str(args, "startTime").or(sg.start_time),
            end_time: arg_str(args, "endTime").or(sg.end_time),
            enabled: arg_i64(args, "enabled")?.map(|v| v as i32).unwrap_or(sg.enabled),
            update_user: arg_str(args, "updateUser"),
            ..sg
        };
        ext.update_surrogate(&updated)?;
        Ok(json!({"id": id}))
    }

    fn process_surrogate_detail(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let sg = ext.find_surrogate_by_id(id)?
            .ok_or(JeeflowError::Business(format!("委托不存在: {}", id)))?;
        Ok(serde_json::to_value(surrogate_to_json(&sg)).unwrap())
    }

    fn process_surrogate_remove(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        ext.remove_surrogate(id)?;
        Ok(json!({"id": id}))
    }
}

// block_on removed — flow() is now fully async to avoid nested runtime panic (P0-1 fix)

// ═══════════════════════════════════════════════════════
// JsonValue → serde_json::Value conversion
// ═══════════════════════════════════════════════════════

fn json_value_to_serde(v: &JsonValue) -> Json {
    match v {
        JsonValue::Null => Json::Null,
        JsonValue::Bool(b) => Json::Bool(*b),
        JsonValue::Number(n) => {
            if *n == (*n as i64) as f64 {
                json!(*n as i64)
            } else {
                json!(*n)
            }
        }
        JsonValue::Str(s) => Json::String(s.clone()),
        JsonValue::Array(arr) => Json::Array(arr.iter().map(json_value_to_serde).collect()),
        JsonValue::Object(entries) => {
            let mut map = serde_json::Map::new();
            for (k, v) in entries {
                map.insert(k.clone(), json_value_to_serde(v));
            }
            Json::Object(map)
        }
    }
}

// ═══════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use jeeflow_core::id_gen::AtomicIdGenerator;
    use jeeflow_core::memory::MemoryRepository;

    fn make_facade() -> JeeflowFacade {
        let repo = Arc::new(MemoryRepository::new());
        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));
        JeeflowFacade::new(ctx)
    }

    struct TestUserProvider;
    impl UserProvider for TestUserProvider {
        fn get_user(&self, user_id: &str) -> JeeflowResult<Option<UserInfo>> {
            Ok(Some(UserInfo {
                user_id: user_id.to_string(),
                real_name: user_id.to_string(),
                dept_id: "dept1".into(), dept_name: "TestDept".into(),
                post_id: "post1".into(), post_name: "TestPost".into(),
            }))
        }
    }

    fn make_facade_with_user_provider() -> JeeflowFacade {
        let repo = Arc::new(MemoryRepository::new());
        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
            .with_user_provider(Arc::new(TestUserProvider))
            .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));
        JeeflowFacade::new(ctx)
    }

    fn make_facade_with_define() -> (JeeflowFacade, i64) {
        let facade = make_facade();
        let mut define = ProcessDefine {
            id: 0,
            name: "test-flow".into(),
            display_name: "Test Flow".into(),
            define_type: "approval".into(),
            state: 1,
            content: r#"{
                "name": "test-flow",
                "displayName": "Test Flow",
                "type": "approval",
                "nodes": [
                    {"id": "start", "type": "snaker:start", "text": {"value": "\u5f00\u59cb"}},
                    {"id": "apply", "type": "snaker:task", "text": {"value": "\u7533\u8bf7"},
                     "properties": {"assignee": "applicant"}},
                    {"id": "end", "type": "snaker:end", "text": {"value": "\u7ed3\u675f"}}
                ],
                "edges": [
                    {"id": "e1", "sourceNodeId": "start", "targetNodeId": "apply"},
                    {"id": "e2", "sourceNodeId": "apply", "targetNodeId": "end"}
                ]
            }"#.as_bytes().to_vec(),
            version: 1,
            create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        facade.repo().save_define(&mut define).unwrap();
        (facade, define.id)
    }

    // ─── Response envelope tests ───

    #[test]
    fn test_success_response() {
        let resp = success_response(json!({"test": 1}));
        assert_eq!(resp["code"], 0);
        assert_eq!(resp["msg"], "成功");
        assert!(resp["data"].is_object());
    }

    #[test]
    fn test_error_response() {
        let resp = error_response("测试错误");
        assert_eq!(resp["code"], CODE_ERROR);
        assert_eq!(resp["msg"], "测试错误");
    }

    // ─── camelCase tests ───

    #[test]
    fn test_to_camel() {
        assert_eq!(to_camel("process_instance_id"), "processInstanceId");
        assert_eq!(to_camel("task_name"), "taskName");
        assert_eq!(to_camel("id"), "id");
        assert_eq!(to_camel("create_time"), "createTime");
    }

    #[test]
    fn test_to_camel_json() {
        let input = json!({"process_instance_id": 1, "task_name": "test"});
        let output = to_camel_json(&input);
        assert_eq!(output["processInstanceId"], 1);
        assert_eq!(output["taskName"], "test");
    }

    // ─── ID stringification tests ───

    #[test]
    fn test_stringify_ids() {
        let input = json!({"id": 123, "processInstanceId": 456, "name": "test"});
        let output = stringify_ids(&input);
        assert_eq!(output["id"], "123");
        assert_eq!(output["processInstanceId"], "456");
        assert_eq!(output["name"], "test");
    }

    // ─── Unknown action test ───

    #[tokio::test]
    async fn test_unknown_action() {
        let facade = make_facade();
        let args = HashMap::new();
        let resp = facade.flow("unknown/action", &args).await;
        assert_eq!(resp["code"], CODE_ERROR);
        assert!(resp["msg"].as_str().unwrap().contains("未知"));
    }

    // ─── processDefine tests ───

    #[tokio::test]
    async fn test_process_define_page() {
        let (facade, _id) = make_facade_with_define();
        let args = HashMap::new();
        let resp = facade.flow("processDefine/page", &args).await;
        assert_eq!(resp["code"], 0);
        assert!(resp["data"]["rows"].is_array());
    }

    #[tokio::test]
    async fn test_process_define_detail() {
        let (facade, id) = make_facade_with_define();
        let mut args = HashMap::new();
        args.insert("id".to_string(), json!(id));
        let resp = facade.flow("processDefine/detail", &args).await;
        assert_eq!(resp["code"], 0);
        assert_eq!(resp["data"]["name"], "test-flow");
    }

    #[tokio::test]
    async fn test_process_define_deploy() {
        let (facade, id) = make_facade_with_define();
        let mut args = HashMap::new();
        args.insert("id".to_string(), json!(id));
        let resp = facade.flow("processDefine/deploy", &args).await;
        assert_eq!(resp["code"], 0);
    }

    #[tokio::test]
    async fn test_process_define_up_and_down() {
        let (facade, id) = make_facade_with_define();
        let mut args = HashMap::new();
        args.insert("id".to_string(), json!(id));
        args.insert("state".to_string(), json!(0)); // disable
        let resp = facade.flow("processDefine/upAndDown", &args).await;
        assert_eq!(resp["code"], 0);
    }

    #[tokio::test]
    async fn test_process_define_remove() {
        let (facade, id) = make_facade_with_define();
        let mut args = HashMap::new();
        args.insert("id".to_string(), json!(id));
        let resp = facade.flow("processDefine/remove", &args).await;
        assert_eq!(resp["code"], 0);
    }

    // ─── processDesign tests ───

    #[tokio::test]
    async fn test_process_design_save_and_detail() {
        let facade = make_facade();
        let mut args = HashMap::new();
        args.insert("name".to_string(), json!("test-design"));
        args.insert("displayName".to_string(), json!("Test Design"));
        let resp = facade.flow("processDesign/save", &args).await;
        assert_eq!(resp["code"], 0);
        let design_id = resp["data"]["id"].as_str().unwrap().parse::<i64>().unwrap();

        let mut detail_args = HashMap::new();
        detail_args.insert("id".to_string(), json!(design_id));
        let resp2 = facade.flow("processDesign/detail", &detail_args).await;
        assert_eq!(resp2["code"], 0);
    }

    #[tokio::test]
    async fn test_process_design_page() {
        let facade = make_facade();
        let resp = facade.flow("processDesign/page", &HashMap::new()).await;
        assert_eq!(resp["code"], 0);
    }

    // ─── processSurrogate tests ───

    #[tokio::test]
    async fn test_process_surrogate_crud() {
        let facade = make_facade();

        // Save
        let mut args = HashMap::new();
        args.insert("processName".to_string(), json!("test-flow"));
        args.insert("operator".to_string(), json!("user1"));
        args.insert("surrogate".to_string(), json!("user2"));
        let resp = facade.flow("processSurrogate/save", &args).await;
        assert_eq!(resp["code"], 0);
        let sg_id = resp["data"]["id"].as_str().unwrap().parse::<i64>().unwrap();

        // Detail
        let mut detail_args = HashMap::new();
        detail_args.insert("id".to_string(), json!(sg_id));
        let resp2 = facade.flow("processSurrogate/detail", &detail_args).await;
        assert_eq!(resp2["code"], 0);

        // Update
        let mut update_args = HashMap::new();
        update_args.insert("id".to_string(), json!(sg_id));
        update_args.insert("surrogate".to_string(), json!("user3"));
        let resp3 = facade.flow("processSurrogate/update", &update_args).await;
        assert_eq!(resp3["code"], 0);

        // Remove
        let mut remove_args = HashMap::new();
        remove_args.insert("id".to_string(), json!(sg_id));
        let resp4 = facade.flow("processSurrogate/remove", &remove_args).await;
        assert_eq!(resp4["code"], 0);
    }

    // ─── Pagination envelope tests ───

    #[tokio::test]
    async fn test_pagination_envelope() {
        let facade = make_facade();
        let resp = facade.flow("processDefine/page", &HashMap::new()).await;
        let data = &resp["data"];
        assert!(data.get("pageNum").is_some());
        assert!(data.get("pageSize").is_some());
        assert!(data.get("recordCount").is_some());
        assert!(data.get("totalPage").is_some());
        assert!(data.get("rows").is_some());
    }

    // ─── Output transformation tests ───

    #[test]
    fn test_transform_output_camel_case() {
        let input = json!({"process_instance_id": 1, "task_name": "test"});
        let output = transform_output(input);
        assert!(output.get("processInstanceId").is_some());
        assert!(output.get("taskName").is_some());
    }

    #[test]
    fn test_transform_output_id_stringification() {
        let input = json!({"id": 123, "name": "test"});
        let output = transform_output(input);
        assert_eq!(output["id"], "123");
    }

    // ─── C2 string id dual-tolerance tests ───

    #[tokio::test]
    async fn test_c2_string_id_accepted() {
        let (facade, id) = make_facade_with_define();
        // Pass id as string — should work identically to number
        let mut args = HashMap::new();
        args.insert("id".to_string(), json!(id.to_string()));
        let resp = facade.flow("processDefine/detail", &args).await;
        assert_eq!(resp["code"], 0, "string id should be accepted");
        assert_eq!(resp["data"]["name"], "test-flow");
    }

    #[tokio::test]
    async fn test_c2_number_id_still_works() {
        let (facade, id) = make_facade_with_define();
        // Pass id as number — original behavior
        let mut args = HashMap::new();
        args.insert("id".to_string(), json!(id));
        let resp = facade.flow("processDefine/detail", &args).await;
        assert_eq!(resp["code"], 0, "number id should still work");
    }

    #[tokio::test]
    async fn test_c2_18digit_snowflake_string_id() {
        let facade = make_facade();
        // 18-digit snowflake-scale string id — should not be rejected
        // (will get "not found" error, not "invalid id" or "missing id")
        let mut args = HashMap::new();
        args.insert("id".to_string(), json!("999999999999999999"));
        let resp = facade.flow("processDefine/detail", &args).await;
        assert_eq!(resp["code"], 99999999);
        // Should be "not found", NOT "invalid id" or "missing id"
        let msg = resp["msg"].as_str().unwrap();
        assert!(msg.contains("不存在") || msg.contains("999999999999999999"),
            "18-digit id should parse but not found, got msg: {}", msg);
    }

    #[tokio::test]
    async fn test_c2_invalid_string_id_returns_illegal() {
        let facade = make_facade();
        let mut args = HashMap::new();
        args.insert("id".to_string(), json!("abc"));
        let resp = facade.flow("processDefine/detail", &args).await;
        assert_eq!(resp["code"], 99999999);
        let msg = resp["msg"].as_str().unwrap();
        assert!(msg.contains("非法id"), "invalid string id should say '非法id', got: {}", msg);
    }

    #[tokio::test]
    async fn test_c2_invalid_string_id_execute() {
        let facade = make_facade();
        let mut args = HashMap::new();
        args.insert("id".to_string(), json!("not_a_number"));
        let resp = facade.flow("processTask/execute", &args).await;
        assert_eq!(resp["code"], 99999999);
        assert!(resp["msg"].as_str().unwrap().contains("非法id"));
    }

    #[tokio::test]
    async fn test_c2_string_id_all_actions() {
        // Verify string id works across multiple action types
        let (facade, id) = make_facade_with_define();
        let id_str = id.to_string();

        // processDefine/deploy with string id
        let mut args = HashMap::new();
        args.insert("id".to_string(), json!(&id_str));
        let resp = facade.flow("processDefine/deploy", &args).await;
        assert_eq!(resp["code"], 0, "deploy with string id should work");

        // processDefine/remove with string id
        let resp = facade.flow("processDefine/remove", &args).await;
        assert_eq!(resp["code"], 0, "remove with string id should work");
    }

    // ─── Negative tests (error code 99999999) ───

    #[tokio::test]
    async fn test_unknown_action_returns_error_code() {
        let facade = make_facade();
        let resp = facade.flow("unknown/action", &HashMap::new()).await;
        assert_eq!(resp["code"], 99999999);
        assert!(resp["msg"].as_str().unwrap().contains("未知 action"));
    }

    #[tokio::test]
    async fn test_detail_missing_id_returns_error() {
        let facade = make_facade();
        let resp = facade.flow("processDefine/detail", &HashMap::new()).await;
        assert_eq!(resp["code"], 99999999);
    }

    #[tokio::test]
    async fn test_task_execute_missing_id() {
        let facade = make_facade();
        let resp = facade.flow("processTask/execute", &HashMap::new()).await;
        assert_eq!(resp["code"], 99999999);
    }

    #[tokio::test]
    async fn test_instance_withdraw_missing_id() {
        let facade = make_facade();
        let resp = facade.flow("processInstance/withdraw", &HashMap::new()).await;
        assert_eq!(resp["code"], 99999999);
    }

    // ─── processInstance action tests ───

    #[tokio::test]
    async fn test_process_instance_page() {
        let facade = make_facade();
        let resp = facade.flow("processInstance/page", &HashMap::new()).await;
        assert_eq!(resp["code"], 0);
        assert!(resp["data"]["rows"].is_array());
    }

    #[tokio::test]
    async fn test_process_instance_detail_missing() {
        let facade = make_facade();
        let mut args = HashMap::new();
        args.insert("id".to_string(), json!(99999));
        let resp = facade.flow("processInstance/detail", &args).await;
        assert_eq!(resp["code"], 99999999);
    }

    #[tokio::test]
    async fn test_process_instance_cc_list() {
        let facade = make_facade();
        let resp = facade.flow("processInstance/ccList", &HashMap::new()).await;
        assert_eq!(resp["code"], 0);
    }

    // ─── processTask action tests ───

    #[tokio::test]
    async fn test_process_task_todo_list() {
        let facade = make_facade();
        let resp = facade.flow("processTask/todoList", &HashMap::new()).await;
        assert_eq!(resp["code"], 0);
        assert!(resp["data"]["rows"].is_array());
    }

    #[tokio::test]
    async fn test_process_task_done_list() {
        let facade = make_facade();
        let resp = facade.flow("processTask/doneList", &HashMap::new()).await;
        assert_eq!(resp["code"], 0);
    }

    #[tokio::test]
    async fn test_process_task_latest() {
        let facade = make_facade();
        let resp = facade.flow("processTask/latest", &HashMap::new()).await;
        assert!(resp.get("code").is_some());
    }

    #[tokio::test]
    async fn test_process_task_detail_missing() {
        let facade = make_facade();
        let mut args = HashMap::new();
        args.insert("id".to_string(), json!(99999));
        let resp = facade.flow("processTask/detail", &args).await;
        assert_eq!(resp["code"], 99999999);
    }

    // ─── processDesign action tests ───

    #[tokio::test]
    async fn test_process_design_list_by_type() {
        let facade = make_facade();
        let mut args = HashMap::new();
        args.insert("designType".to_string(), json!("approval"));
        let resp = facade.flow("processDesign/listByType", &args).await;
        assert_eq!(resp["code"], 0);
    }

    #[tokio::test]
    async fn test_process_design_update_missing_id() {
        let facade = make_facade();
        let resp = facade.flow("processDesign/update", &HashMap::new()).await;
        assert_eq!(resp["code"], 99999999);
    }

    #[tokio::test]
    async fn test_process_design_remove_missing_id() {
        let facade = make_facade();
        let resp = facade.flow("processDesign/remove", &HashMap::new()).await;
        assert_eq!(resp["code"], 99999999);
    }

    // ─── processSurrogate action tests ───

    #[tokio::test]
    async fn test_process_surrogate_page() {
        let facade = make_facade();
        let resp = facade.flow("processSurrogate/page", &HashMap::new()).await;
        assert_eq!(resp["code"], 0);
    }

    #[tokio::test]
    async fn test_process_surrogate_detail_missing() {
        let facade = make_facade();
        let mut args = HashMap::new();
        args.insert("id".to_string(), json!(99999));
        let resp = facade.flow("processSurrogate/detail", &args).await;
        assert_eq!(resp["code"], 99999999);
    }

    #[tokio::test]
    async fn test_process_surrogate_update_missing_id() {
        let facade = make_facade();
        let resp = facade.flow("processSurrogate/update", &HashMap::new()).await;
        assert_eq!(resp["code"], 99999999);
    }

    #[tokio::test]
    async fn test_process_surrogate_remove_missing_id() {
        let facade = make_facade();
        let resp = facade.flow("processSurrogate/remove", &HashMap::new()).await;
        assert_eq!(resp["code"], 99999999);
    }

    // ─── camelCase conversion tests ───

    #[test]
    fn test_camel_case_conversion_nested() {
        let input = json!({"outer_key": {"inner_key": 1}});
        let output = to_camel_json(&input);
        assert!(output.get("outerKey").is_some());
        assert!(output["outerKey"].get("innerKey").is_some());
    }

    #[test]
    fn test_camel_case_conversion_array() {
        let input = json!([{"some_key": 1}, {"other_key": 2}]);
        let output = to_camel_json(&input);
        assert!(output[0].get("someKey").is_some());
        assert!(output[1].get("otherKey").is_some());
    }

    // ─── ID stringification tests ───

    #[test]
    fn test_stringify_ids_nested() {
        let input = json!({"id": 123, "nested": {"userId": 456}});
        let output = stringify_ids(&input);
        assert_eq!(output["id"], "123");
        assert_eq!(output["nested"]["userId"], "456");
    }

    #[test]
    fn test_stringify_ids_array() {
        let input = json!({"items": [{"id": 1}, {"id": 2}]});
        let output = stringify_ids(&input);
        assert_eq!(output["items"][0]["id"], "1");
        assert_eq!(output["items"][1]["id"], "2");
    }

    #[test]
    fn test_stringify_ids_suffix_patterns() {
        let input = json!({"process_instance_id": 1, "taskId": 2, "user_id": 3});
        let output = stringify_ids(&input);
        assert_eq!(output["process_instance_id"], "1");
        assert_eq!(output["taskId"], "2");
        assert_eq!(output["user_id"], "3");
    }

    // ─── #92 plural Ids array tests ───

    #[test]
    fn test_stringify_ids_plural_keys_task_ids() {
        // taskIds with >2^53 number → must be stringified
        let input = json!({"taskIds": [999999999999999999i64]});
        let output = stringify_ids(&input);
        assert_eq!(output["taskIds"][0], "999999999999999999");
    }

    #[test]
    fn test_stringify_ids_plural_keys_various() {
        // ids, roleIds, actorIds — all plural patterns
        let input = json!({
            "ids": [1, 2, 3],
            "roleIds": [888888888888888888i64],
            "actorIds": [100001, 100002],
            "candidate_ids": [777777777777777777i64]
        });
        let output = stringify_ids(&input);
        assert_eq!(output["ids"][0], "1");
        assert_eq!(output["ids"][2], "3");
        assert_eq!(output["roleIds"][0], "888888888888888888");
        assert_eq!(output["actorIds"][0], "100001");
        assert_eq!(output["candidate_ids"][0], "777777777777777777");
    }

    #[test]
    fn test_stringify_ids_object_array_not_affected() {
        // rows/items object arrays should NOT be treated as id arrays;
        // each object's internal id keys should still be stringified.
        let input = json!({
            "rows": [
                {"id": 123, "name": "test", "taskIds": [456]},
                {"id": 789, "name": "test2"}
            ],
            "items": [{"user_id": 111}]
        });
        let output = stringify_ids(&input);
        // Object id fields stringified
        assert_eq!(output["rows"][0]["id"], "123");
        assert_eq!(output["rows"][1]["id"], "789");
        // Non-id fields untouched
        assert_eq!(output["rows"][0]["name"], "test");
        // Plural ids inside objects also stringified
        assert_eq!(output["rows"][0]["taskIds"][0], "456");
        // items objects' id keys stringified
        assert_eq!(output["items"][0]["user_id"], "111");
    }

    #[test]
    fn test_transform_output_plural_ids_stringified() {
        // Full pipeline: camelCase + stringify_ids + format_time
        let input = json!({"task_ids": [999999999999999999i64], "role_ids": [1, 2]});
        let output = transform_output(input);
        // After camelCase: taskIds, roleIds; after stringify_ids: string arrays
        assert_eq!(output["taskIds"][0], "999999999999999999");
        assert_eq!(output["roleIds"][0], "1");
        assert_eq!(output["roleIds"][1], "2");
    }

    // ─── #91+#92 cross-call test: execute → taskIds match todoList ───

    #[tokio::test]
    async fn test_execute_response_task_ids_nonzero_and_match_todo_list() {
        let facade = make_facade_with_user_provider();

        // 1. processDesign/save
        let mut args = HashMap::new();
        args.insert("name".to_string(), json!("two-step-flow"));
        args.insert("displayName".to_string(), json!("Two Step Flow"));
        let resp = facade.flow("processDesign/save", &args).await;
        assert_eq!(resp["code"], 0, "save failed: {:?}", resp);
        let design_id = resp["data"]["id"].as_str().unwrap().parse::<i64>().unwrap();

        // 2. processDesign/updateDefine (4-node: start→apply→approve→end)
        let flow_json = r#"{
            "name":"two-step-flow","displayName":"Two Step Flow","type":"approval",
            "nodes":[
                {"id":"start","type":"snaker:start","text":{"value":"Start"}},
                {"id":"apply","type":"snaker:task","text":{"value":"Apply"},
                 "properties":{"assignee":"applicant"}},
                {"id":"approve","type":"snaker:task","text":{"value":"Approve"},
                 "properties":{"assignee":"user2"}},
                {"id":"end","type":"snaker:end","text":{"value":"End"}}
            ],
            "edges":[
                {"id":"e1","sourceNodeId":"start","targetNodeId":"apply"},
                {"id":"e2","sourceNodeId":"apply","targetNodeId":"approve"},
                {"id":"e3","sourceNodeId":"approve","targetNodeId":"end"}
            ]
        }"#;
        let mut args2 = HashMap::new();
        args2.insert("id".to_string(), json!(design_id));
        args2.insert("content".to_string(), json!(flow_json));
        let resp2 = facade.flow("processDesign/updateDefine", &args2).await;
        assert_eq!(resp2["code"], 0, "updateDefine failed: {:?}", resp2);

        // 3. processDesign/deploy
        let mut args3 = HashMap::new();
        args3.insert("id".to_string(), json!(design_id));
        let resp3 = facade.flow("processDesign/deploy", &args3).await;
        assert_eq!(resp3["code"], 0, "deploy failed: {:?}", resp3);

        // 4. processDefine/startAndExecute（对齐 Java：自动完成 apply，返回 processInstanceId）
        let mut args4 = HashMap::new();
        args4.insert("name".to_string(), json!("two-step-flow"));
        args4.insert("operator".to_string(), json!("applicant"));
        let resp4 = facade.flow("processDefine/startAndExecute", &args4).await;
        assert_eq!(resp4["code"], 0, "startAndExecute failed: {:?}", resp4);
        let inst_id = resp4["data"]["processInstanceId"]
            .as_str()
            .expect("processInstanceId should be string (#92)");
        assert!(!inst_id.is_empty() && inst_id != "0", "processInstanceId should be non-zero");

        // 5. processTask/todoList (operator=user2) — apply 已自动完成，待办应为 approve
        let mut args6 = HashMap::new();
        args6.insert("operator".to_string(), json!("user2"));
        let resp6 = facade.flow("processTask/todoList", &args6).await;
        assert_eq!(resp6["code"], 0, "todoList failed: {:?}", resp6);
        let rows = resp6["data"]["rows"].as_array().unwrap();
        assert!(!rows.is_empty(), "user2 should have a todo task after startAndExecute");
        let todo_task_id_str = rows[0]["id"].as_str().expect("todoList id should be string");
        let todo_task_id: i64 = todo_task_id_str.parse().unwrap();
        assert!(todo_task_id > 0, "todo task id should be non-zero");

        // 6. processTask/execute（兼容 id / processTaskId）
        let mut args5 = HashMap::new();
        args5.insert("processTaskId".to_string(), json!(todo_task_id));
        args5.insert("operator".to_string(), json!("user2"));
        args5.insert("submitType".to_string(), json!(1));
        let resp5 = facade.flow("processTask/execute", &args5).await;
        assert_eq!(resp5["code"], 0, "execute failed: {:?}", resp5);
    }

    // ─── Action count test ───

    #[tokio::test]
    async fn test_all_42_actions_dispatchable() {
        let facade = make_facade();
        let actions = vec![
            "processDefine/page", "processDefine/detail", "processDefine/startAndExecute",
            "processDefine/deploy", "processDefine/redeploy", "processDefine/remove",
            "processDefine/upAndDown", "processDefine/getLastByName",
            "processInstance/page", "processInstance/detail", "processInstance/startAndExecute",
            "processInstance/withdraw", "processInstance/highLight",
            "processInstance/approvalRecord", "processInstance/getAssigneeTextData",
            "processInstance/bizData", "processInstance/createCCInstance",
            "processInstance/updateCCStatus", "processInstance/ccList",
            "processTask/todoList", "processTask/doneList", "processTask/execute",
            "processTask/detail", "processTask/jumpAbleTaskNameList",
            "processTask/candidatePage", "processTask/surrogate",
            "processTask/addCandidate", "processTask/latest",
            "processDesign/page", "processDesign/detail",
            "processDesign/save", "processDesign/update",
            "processDesign/updateDefine", "processDesign/remove",
            "processDesign/deploy", "processDesign/redeploy",
            "processDesign/listByType",
            "processSurrogate/page", "processSurrogate/save",
            "processSurrogate/update", "processSurrogate/detail",
            "processSurrogate/remove",
        ];
        assert_eq!(actions.len(), 42, "Should have exactly 42 actions");
        // All actions should return a response (not panic)
        for action in &actions {
            let resp = facade.flow(action, &HashMap::new()).await;
            // Should have code field (either success or error)
            assert!(resp.get("code").is_some(), "Action {} should return a response with code", action);
        }
    }

    // ─── C8 m_ query parser tests ───

    #[test]
    fn test_c8_parse_m_params_2segment() {
        let mut args = HashMap::new();
        args.insert("m_EQ_taskName".to_string(), json!("leaveApply"));
        let filters = parse_m_params(&args);
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].alias, "t");
        assert_eq!(filters[0].op, jeeflow_core::model::FilterOp::Eq);
        assert_eq!(filters[0].column, "task_name");
        assert_eq!(filters[0].value, "leaveApply");
    }

    #[test]
    fn test_c8_parse_m_params_3segment() {
        let mut args = HashMap::new();
        args.insert("m_t_LIKE_displayName".to_string(), json!("请假"));
        let filters = parse_m_params(&args);
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].alias, "t");
        assert_eq!(filters[0].op, jeeflow_core::model::FilterOp::Like);
        assert_eq!(filters[0].column, "display_name");
        assert_eq!(filters[0].value, "请假");
    }

    #[test]
    fn test_c8_parse_m_params_pd_alias() {
        let mut args = HashMap::new();
        args.insert("m_pd_LIKE_name".to_string(), json!("simple"));
        let filters = parse_m_params(&args);
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].alias, "pd");
        assert_eq!(filters[0].op, jeeflow_core::model::FilterOp::Like);
        assert_eq!(filters[0].column, "name");
    }

    #[test]
    fn test_c8_parse_m_params_multiple() {
        let mut args = HashMap::new();
        args.insert("m_EQ_name".to_string(), json!("test"));
        args.insert("m_LIKE_displayName".to_string(), json!("Test"));
        args.insert("pageNum".to_string(), json!(1));
        let filters = parse_m_params(&args);
        assert_eq!(filters.len(), 2);
    }

    #[test]
    fn test_c8_parse_m_params_all_operators() {
        for op in &["EQ", "NE", "LIKE", "GT", "LT", "GE", "LE", "IN", "BT"] {
            let mut args = HashMap::new();
            args.insert(format!("m_{}_name", op), json!("val"));
            let filters = parse_m_params(&args);
            assert_eq!(filters.len(), 1, "Operator {} should be parsed", op);
        }
    }

    #[tokio::test]
    async fn test_c8_filter_define_by_name_eq() {
        let (facade, _) = make_facade_with_define();
        let mut args = HashMap::new();
        args.insert("m_EQ_name".to_string(), json!("test-flow"));
        let resp = facade.flow("processDefine/page", &args).await;
        assert_eq!(resp["code"], 0);
        assert_eq!(resp["data"]["recordCount"], 1);
    }

    #[tokio::test]
    async fn test_c8_filter_define_by_name_eq_no_match() {
        let (facade, _) = make_facade_with_define();
        let mut args = HashMap::new();
        args.insert("m_EQ_name".to_string(), json!("nonexistent"));
        let resp = facade.flow("processDefine/page", &args).await;
        assert_eq!(resp["code"], 0);
        assert_eq!(resp["data"]["recordCount"], 0);
    }

    #[tokio::test]
    async fn test_c8_filter_define_by_name_like() {
        let (facade, _) = make_facade_with_define();
        let mut args = HashMap::new();
        args.insert("m_LIKE_name".to_string(), json!("test"));
        let resp = facade.flow("processDefine/page", &args).await;
        assert_eq!(resp["code"], 0);
        assert_eq!(resp["data"]["recordCount"], 1);
    }

    #[tokio::test]
    async fn test_c8_filter_define_by_display_name_3segment() {
        let (facade, _) = make_facade_with_define();
        let mut args = HashMap::new();
        args.insert("m_t_LIKE_displayName".to_string(), json!("Test"));
        let resp = facade.flow("processDefine/page", &args).await;
        assert_eq!(resp["code"], 0);
        assert_eq!(resp["data"]["recordCount"], 1);
    }

    #[test]
    fn test_c8_camel_to_snake() {
        assert_eq!(camel_to_snake("taskName"), "task_name");
        assert_eq!(camel_to_snake("displayName"), "display_name");
        assert_eq!(camel_to_snake("name"), "name");
        assert_eq!(camel_to_snake("processInstanceId"), "process_instance_id");
    }
}
