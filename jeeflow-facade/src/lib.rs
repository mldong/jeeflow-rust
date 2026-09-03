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
use chrono::{Datelike, Timelike};

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

/// Keys whose values are free-form maps / LogicFlow graphs — keep inner keys as-is
/// (对齐 Java/Go：VO 顶层 camelCase，ext/variable/jsonObject 内保留 u_realName、PERMISSION_* 等原键)。
fn is_opaque_value_key(k: &str) -> bool {
    matches!(
        k,
        "jsonObject"
            | "json_object"
            | "ext"
            | "instanceExt"
            | "instance_ext"
            | "variable"
            | "taskFormData"
            | "task_form_data"
            | "formData"
            | "form_data"
            | "nodeProgress"
            | "node_progress"
    )
}

/// Convert snake_case VO keys to camelCase; do not rewrite opaque nested maps.
fn to_camel_json(val: &Json) -> Json {
    to_camel_json_inner(val, true)
}

fn to_camel_json_inner(val: &Json, convert_keys: bool) -> Json {
    match val {
        Json::Object(map) => {
            let mut new_map = serde_json::Map::new();
            for (k, v) in map {
                let new_key = if convert_keys {
                    to_camel(k)
                } else {
                    k.clone()
                };
                let child_convert =
                    convert_keys && !is_opaque_value_key(k) && !is_opaque_value_key(&new_key);
                new_map.insert(new_key, to_camel_json_inner(v, child_convert));
            }
            Json::Object(new_map)
        }
        Json::Array(arr) => Json::Array(
            arr.iter()
                .map(|v| to_camel_json_inner(v, convert_keys))
                .collect(),
        ),
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
        Json::String(s) => {
            // ISO-like → yyyy-MM-dd HH:mm:ss when easily parseable
            let t = s.trim();
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S") {
                return Json::String(dt.format("%Y-%m-%d %H:%M:%S").to_string());
            }
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S%.f") {
                return Json::String(dt.format("%Y-%m-%d %H:%M:%S").to_string());
            }
            if t.len() >= 19 && t.as_bytes().get(10) == Some(&b'T') {
                let approx = format!("{} {}", &t[0..10], &t[11..19]);
                if chrono::NaiveDateTime::parse_from_str(&approx, "%Y-%m-%d %H:%M:%S").is_ok() {
                    return Json::String(approx);
                }
            }
            Json::String(s.clone())
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
// Row VO（对齐 Java JeeflowFacade *RowToMap / issues/05）
// 字段名用 snake_case，由 transform_output → camelCase；
// 例外：契约键 `type` 直接输出（不能变成 defineType）。
// ═══════════════════════════════════════════════════════

const FORM_DATA_PREFIX: &str = "f_";
const TASK_FORM_DATA_PREFIX: &str = "tf_";

fn parse_graph(content: &str) -> Option<Json> {
    let t = content.trim();
    if t.is_empty() {
        return None;
    }
    serde_json::from_str(t).ok()
}

fn parse_json_map(s: Option<&str>) -> serde_json::Map<String, Json> {
    match s {
        Some(raw) if !raw.trim().is_empty() => {
            serde_json::from_str::<Json>(raw)
                .ok()
                .and_then(|v| v.as_object().cloned())
                .unwrap_or_default()
        }
        _ => serde_json::Map::new(),
    }
}

fn form_data_of_map(vars: &serde_json::Map<String, Json>, prefix: &str) -> Json {
    let mut out = serde_json::Map::new();
    for (k, v) in vars {
        if k.starts_with(prefix) {
            out.insert(k.clone(), v.clone());
            out.insert(k[prefix.len()..].to_string(), v.clone());
        }
    }
    Json::Object(out)
}

fn form_data_of_flow(vars: &FlowData, prefix: &str) -> Json {
    let mut out = serde_json::Map::new();
    for (k, v) in vars.inner() {
        if k.starts_with(prefix) {
            let sv = json_value_to_serde(v);
            out.insert(k.clone(), sv.clone());
            out.insert(k[prefix.len()..].to_string(), sv);
        }
    }
    Json::Object(out)
}

fn flow_data_to_object(vars: &FlowData) -> Json {
    let mut map = serde_json::Map::new();
    for (k, v) in vars.inner() {
        map.insert(k.clone(), json_value_to_serde(v));
    }
    Json::Object(map)
}

fn first_task_node_id(json_object: &Json) -> Option<String> {
    let nodes = json_object.get("nodes")?.as_array()?;
    for n in nodes {
        if n.get("type").and_then(|t| t.as_str()) == Some("snaker:task") {
            return n.get("id").and_then(|id| id.as_str()).map(|s| s.to_string());
        }
    }
    None
}

fn define_row_to_json(r: &DefineRow) -> Json {
    json!({
        "id": r.id,
        "name": r.name,
        "display_name": r.display_name,
        "type": r.define_type,
        "state": r.state,
        "version": r.version,
        "create_time": r.create_time,
        "create_user": r.create_user,
        "update_time": r.update_time,
        "update_user": r.update_user,
    })
}

fn task_row_to_json(r: &TaskRow) -> Json {
    let instance_ext = parse_json_map(r.instance_variable.as_deref());
    let mut ext = parse_json_map(r.variable.as_deref());
    if ext.is_empty() {
        ext = instance_ext.clone();
    }
    json!({
        "id": r.id,
        "process_instance_id": r.process_instance_id,
        "task_name": r.task_name,
        "display_name": r.display_name,
        "task_type": r.task_type,
        "perform_type": r.perform_type,
        "task_state": r.task_state,
        "operator": r.operator,
        "finish_time": r.finish_time,
        "expire_time": r.expire_time,
        "form_key": r.form_key,
        "task_parent_id": r.task_parent_id,
        "variable": r.variable,
        "create_time": r.create_time,
        "create_user": r.create_user,
        "update_time": r.update_time,
        "update_user": r.update_user,
        "process_define_name": r.define_name,
        "process_define_display_name": r.define_display_name,
        "ext": Json::Object(ext),
        "instance_ext": Json::Object(instance_ext),
        "instance_create_time": r.instance_create_time,
        "version": r.define_version,
        "task_form_data": form_data_of_map(&parse_json_map(r.variable.as_deref()), TASK_FORM_DATA_PREFIX),
    })
}

fn instance_row_to_json(r: &InstanceRow) -> Json {
    json!({
        "id": r.id,
        "parent_id": r.parent_id,
        "process_define_id": r.process_define_id,
        "state": r.state,
        "parent_node_name": r.parent_node_name,
        "business_no": r.business_no,
        "operator": r.operator,
        "expire_time": r.expire_time,
        "variable": r.variable,
        "create_time": r.create_time,
        "create_user": r.create_user,
        "update_time": r.update_time,
        "update_user": r.update_user,
        "process_define_name": r.define_name,
        "process_define_display_name": r.define_display_name,
        "process_define_version": r.define_version,
        "ext": Json::Object(parse_json_map(r.variable.as_deref())),
        "display_name": r.define_display_name,
        "version": r.define_version,
    })
}

fn design_to_json(d: &ProcessDesign) -> Json {
    json!({
        "id": d.id,
        "name": d.name,
        "display_name": d.display_name,
        "type": d.design_type,
        "icon": d.icon,
        "is_deployed": d.is_deployed,
        "remark": d.remark,
        "create_time": d.create_time,
        "create_user": d.create_user,
        "update_time": d.update_time,
        "update_user": d.update_user,
    })
}

fn surrogate_to_json(s: &ProcessSurrogate) -> Json {
    json!({
        "id": s.id,
        "process_name": s.process_name,
        "operator": s.operator,
        "surrogate": s.surrogate,
        "start_time": s.start_time,
        "end_time": s.end_time,
        "enabled": s.enabled,
        "create_time": s.create_time,
        "create_user": s.create_user,
        "update_time": s.update_time,
        "update_user": s.update_user,
    })
}

/// 对齐 Java taskVo（detail / instance.tasks）
fn task_vo(t: &ProcessTask) -> Json {
    json!({
        "id": t.task_id,
        "process_instance_id": t.process_instance_id,
        "task_name": t.task_name,
        "display_name": t.display_name,
        "task_type": t.task_type,
        "perform_type": t.perform_type,
        "task_state": t.task_state,
        "operator": t.actor_id,
        "form_key": t.form_key,
        "task_parent_id": t.parent_task_id,
        "task_actor_id_list": t.actor_ids,
        "task_form_data": form_data_of_flow(&t.variables, TASK_FORM_DATA_PREFIX),
        "finish_time": t.finish_time,
        "create_time": t.create_time,
    })
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

/// First present i64 among preferred Java/UI keys.
fn arg_id(args: &HashMap<String, Json>, keys: &[&str]) -> JeeflowResult<Option<i64>> {
    for k in keys {
        if let Some(v) = arg_i64(args, k)? {
            return Ok(Some(v));
        }
    }
    Ok(None)
}

/// Accept JSON array or comma-separated string → Vec<String>.
fn arg_actor_ids(args: &HashMap<String, Json>) -> Vec<String> {
    match args.get("actorIds") {
        Some(Json::Array(arr)) => arr
            .iter()
            .filter_map(|v| {
                if let Some(s) = v.as_str() {
                    Some(s.to_string())
                } else if let Some(n) = v.as_i64() {
                    Some(n.to_string())
                } else if !v.is_null() {
                    Some(v.to_string().trim_matches('"').to_string())
                } else {
                    None
                }
            })
            .filter(|s| !s.is_empty())
            .collect(),
        Some(Json::String(s)) => s
            .split(',')
            .map(|x| x.trim().to_string())
            .filter(|x| !x.is_empty())
            .collect(),
        _ => Vec::new(),
    }
}

fn arg_ids(args: &HashMap<String, Json>) -> JeeflowResult<Vec<i64>> {
    if let Some(Json::Array(arr)) = args.get("ids") {
        let mut out = Vec::with_capacity(arr.len());
        for v in arr {
            if let Some(n) = v.as_i64() {
                out.push(n);
            } else if let Some(s) = v.as_str() {
                out.push(
                    s.parse::<i64>()
                        .map_err(|_| JeeflowError::Business(format!("非法id: {}", s)))?,
                );
            } else {
                return Err(JeeflowError::Business(format!("非法id: {}", v)));
            }
        }
        return Ok(out);
    }
    if let Some(id) = arg_i64(args, "id")? {
        return Ok(vec![id]);
    }
    Ok(Vec::new())
}

/// Parse `content` (string or object→string); boot3-compatible top-level fallback.
fn content_bytes(args: &HashMap<String, Json>) -> Option<Vec<u8>> {
    match args.get("content") {
        Some(Json::String(s)) => Some(s.as_bytes().to_vec()),
        Some(Json::Object(_) | Json::Array(_)) => {
            Some(serde_json::to_vec(args.get("content").unwrap()).unwrap_or_default())
        }
        Some(other) if !other.is_null() => {
            Some(other.to_string().trim_matches('"').as_bytes().to_vec())
        }
        _ => {
            let mut copy = serde_json::Map::new();
            for (k, v) in args {
                if matches!(
                    k.as_str(),
                    "processDesignId"
                        | "processDefineId"
                        | "operator"
                        | "id"
                        | "pageNum"
                        | "pageSize"
                ) {
                    continue;
                }
                copy.insert(k.clone(), v.clone());
            }
            if copy.is_empty() {
                None
            } else {
                Some(Json::Object(copy).to_string().into_bytes())
            }
        }
    }
}

fn resolve_rel_table_name(content: &str) -> Option<String> {
    let meta: Json = serde_json::from_str(content).ok()?;
    let obj = meta.as_object()?;
    obj.get("relTableName")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            obj.get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
}

fn design_his_to_json(h: &ProcessDesignHis) -> Json {
    json!({
        "id": h.id,
        "process_design_id": h.process_design_id,
        "content": String::from_utf8_lossy(&h.content).to_string(),
        "create_time": h.create_time,
        "create_user": h.create_user,
    })
}

fn candidate_row(actor_id: &str, real_name: &str, extra: Option<&Json>) -> Json {
    let mut row = json!({
        "id": actor_id,
        "realName": if real_name.is_empty() { actor_id } else { real_name },
    });
    if let Some(Json::Object(src)) = extra {
        if let Some(obj) = row.as_object_mut() {
            if let Some(v) = src.get("userId").or_else(|| src.get("user_id")) {
                obj.insert("userId".into(), v.clone());
            }
            if let Some(v) = src.get("userName").or_else(|| src.get("user_name")) {
                obj.insert("userName".into(), v.clone());
            }
            if let Some(v) = src.get("deptName").or_else(|| src.get("dept_name")) {
                obj.insert("deptName".into(), v.clone());
            }
        }
    }
    row
}

fn collect_static_candidate_actors(
    model: &jeeflow_core::parser::ProcessModel,
    task_name: &str,
) -> Vec<String> {
    let mut actors = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for edge in model.get_output_edges(task_name) {
        let Some(node) = model.get_node(&edge.target_node_id) else {
            continue;
        };
        if node.node_type != jeeflow_core::parser::NodeType::Task {
            continue;
        }
        if let Some(a) = node.assignee() {
            let t = a.trim().to_string();
            if !t.is_empty() && t != "applicant" && seen.insert(t.clone()) {
                actors.push(t);
            }
        }
        if let Some(cu) = node.candidate_users() {
            for u in cu.split(',') {
                let t = u.trim().to_string();
                if !t.is_empty() && seen.insert(t.clone()) {
                    actors.push(t);
                }
            }
        }
    }
    actors
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
            // 数组/对象原样透传（对齐 Go map[string]interface{}）：
            // vben 多选 ApiSelect 提交 JSON 数组（如发起抄送 f_ccActors），
            // 旧版落兜底分支被 v.to_string() 字符串化成 `"[...]"`，
            // 引擎抄送人变成字面量 `["<id>"]`（L3 S6）。
            Json::Array(items) => {
                fd.insert(k.clone(), JsonValue::Array(items.iter().map(json_to_value).collect()));
            }
            Json::Object(obj) => {
                fd.insert(
                    k.clone(),
                    JsonValue::Object(
                        obj.iter().map(|(a, b)| (a.clone(), json_to_value(b))).collect(),
                    ),
                );
            }
        }
    }
    fd
}

/// serde_json::Value → 引擎 JsonValue（递归，保留嵌套结构）。
fn json_to_value(v: &Json) -> JsonValue {
    match v {
        Json::String(s) => JsonValue::Str(s.clone()),
        Json::Number(n) => JsonValue::Number(n.as_f64().unwrap_or(0.0)),
        Json::Bool(b) => JsonValue::Bool(*b),
        Json::Null => JsonValue::Null,
        Json::Array(items) => JsonValue::Array(items.iter().map(json_to_value).collect()),
        Json::Object(obj) => JsonValue::Object(
            obj.iter().map(|(a, b)| (a.clone(), json_to_value(b))).collect(),
        ),
    }
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

            // ═══ processInstance (14) ═══
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
            "processInstance/stats/overview" => self.stats_overview(args),
            "processInstance/stats/trend" => self.stats_trend(args),
            "processInstance/stats/group" => self.stats_group(args),

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

    /// deploy 版本管理（对齐 Java）：按 name 取最新 version，存在则 +1，否则从 0。
    fn save_deployed_define(
        &self,
        model: &jeeflow_core::parser::ProcessModel,
        bytes: &[u8],
        operator: &str,
    ) -> JeeflowResult<i64> {
        let page = self.repo.page_defines(&PageQuery::new(1, i64::MAX / 4))?;
        let version = page
            .rows
            .iter()
            .filter(|r| r.name == model.name)
            .map(|r| r.version)
            .max()
            .map(|v| v + 1)
            .unwrap_or(0);
        let mut define = ProcessDefine {
            id: 0,
            name: model.name.clone(),
            display_name: model.display_name.clone(),
            define_type: model.model_type.clone(),
            state: 1,
            content: bytes.to_vec(),
            version,
            create_time: None,
            create_user: Some(operator.to_string()),
            update_time: None,
            update_user: Some(operator.to_string()),
        };
        self.repo.save_define(&mut define)?;
        Ok(define.id)
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
        let mut rows: Vec<Json> = page.rows.iter().map(define_row_to_json).collect();
        apply_filters_to_rows(&mut rows, &filters);
        let (page_rows, total) = re_paginate(rows, page_num, page_size);
        let result = PageResult::new(page_num, page_size, total, page_rows);
        Ok(serde_json::to_value(page_to_json(&result)).unwrap())
    }

    fn process_define_detail(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_i64(args, "id")?.ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let define = self.repo.find_define_by_id(id)?
            .ok_or(JeeflowError::DefineNotFound(id))?;
        // 对齐 Java defineDetail：type + jsonObject（非 defineType/content）
        Ok(json!({
            "id": define.id,
            "name": define.name,
            "display_name": define.display_name,
            "type": define.define_type,
            "state": define.state,
            "version": define.version,
            "json_object": parse_graph(&define.content_str()),
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
        let bytes = content_bytes(args).ok_or(JeeflowError::Business("content 缺失".into()))?;
        let content = String::from_utf8_lossy(&bytes).to_string();
        let model = jeeflow_core::parser::ModelParser::parse(&content)?;
        let operator = arg_str_or(args, "operator", "system");
        let define_id = self.save_deployed_define(&model, &bytes, &operator)?;
        Ok(json!({"process_define_id": define_id}))
    }

    fn process_define_redeploy(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let define_id = arg_id(args, &["processDefineId", "id"])?
            .ok_or(JeeflowError::Business("缺少processDefineId参数".into()))?;
        let bytes = content_bytes(args).ok_or(JeeflowError::Business("content 缺失".into()))?;
        let content = String::from_utf8_lossy(&bytes).to_string();
        let model = jeeflow_core::parser::ModelParser::parse(&content)?;
        let operator = arg_str_or(args, "operator", "system");
        let def = ProcessDefine {
            id: define_id,
            name: model.name.clone(),
            display_name: model.display_name.clone(),
            define_type: model.model_type.clone(),
            state: 1,
            content: bytes,
            version: 0, // updateDefine 不改 version（仓储侧保留原值）
            create_time: None,
            create_user: None,
            update_time: None,
            update_user: Some(operator),
        };
        // 保留原 version：先读再写
        if let Some(old) = self.repo.find_define_by_id(define_id)? {
            let mut updated = def;
            updated.version = old.version;
            updated.state = old.state;
            updated.create_time = old.create_time;
            updated.create_user = old.create_user;
            self.repo.update_define(&updated)?;
        } else {
            self.repo.update_define(&def)?;
        }
        Ok(json!({}))
    }

    fn process_define_remove(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ids = arg_ids(args)?;
        if ids.is_empty() {
            return Err(JeeflowError::Business("缺少id参数".into()));
        }
        for id in ids {
            self.repo.remove_define(id)?;
        }
        Ok(json!({}))
    }

    fn process_define_up_and_down(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let state = arg_i64(args, "opType")?
            .or(arg_i64(args, "state")?)
            .ok_or(JeeflowError::Business("缺少state/opType参数".into()))? as i32;
        let ids = arg_ids(args)?;
        if ids.is_empty() {
            return Err(JeeflowError::Business("缺少id参数".into()));
        }
        for id in ids {
            self.repo.update_define_state(id, state)?;
        }
        Ok(json!({}))
    }

    fn process_define_get_last_by_name(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let name = arg_str(args, "processDefineName")
            .or_else(|| arg_str(args, "name"))
            .ok_or(JeeflowError::Business("缺少processDefineName参数".into()))?;
        let page = self.repo.page_defines(&PageQuery::new(1, i64::MAX / 4))?;
        let define = page
            .rows
            .iter()
            .filter(|d| d.name == name)
            .max_by_key(|d| d.version)
            .ok_or(JeeflowError::Business(format!("流程定义不存在: {}", name)))?;
        Ok(json!({
            "id": define.id,
            "name": define.name,
            "display_name": define.display_name,
            "type": define.define_type,
            "state": define.state,
            "version": define.version,
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
        let mut rows: Vec<Json> = page.rows.iter().map(instance_row_to_json).collect();
        apply_filters_to_rows(&mut rows, &filters);
        let (page_rows, total) = re_paginate(rows, page_num, page_size);
        let result = PageResult::new(page_num, page_size, total, page_rows);
        Ok(serde_json::to_value(page_to_json(&result)).unwrap())
    }

    fn process_instance_detail(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_id(args, &["processInstanceId", "id"])?
            .ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let inst = self.repo.find_instance_by_id(id)?
            .ok_or(JeeflowError::InstanceNotFound(id))?;
        let def0 = self.repo.find_define_by_id(inst.define_id)?;
        let json_object = def0.as_ref().and_then(|d| parse_graph(&d.content_str()));
        let first_node = json_object.as_ref().and_then(first_task_node_id);

        let mut tasks_out: Vec<Json> = Vec::new();
        let mut active: Vec<Json> = Vec::new();
        for t in &inst.tasks {
            let mut vo = task_vo(t);
            let mut ext = flow_data_to_object(&t.variables);
            let doing = t.task_state == TaskState::Doing.code();
            let is_first = doing
                && first_node
                    .as_ref()
                    .map(|n| n == &t.task_name)
                    .unwrap_or(false);
            if let Some(obj) = ext.as_object_mut() {
                obj.insert("isFirstTaskNode".into(), Json::Bool(is_first));
            }
            if let Some(obj) = vo.as_object_mut() {
                obj.insert("ext".into(), ext);
            }
            if doing {
                active.push(vo.clone());
            }
            tasks_out.push(vo);
        }

        Ok(json!({
            "id": inst.instance_id,
            "parent_id": inst.parent_id,
            "process_define_id": inst.define_id,
            "state": inst.state,
            "parent_node_name": inst.parent_node_name,
            "business_no": inst.business_no,
            "operator": inst.operator,
            "variables": flow_data_to_object(&inst.variables),
            "form_data": form_data_of_flow(&inst.variables, FORM_DATA_PREFIX),
            "create_time": inst.create_time,
            "create_user": inst.create_user,
            "display_name": def0.as_ref().map(|d| d.display_name.clone()),
            "name": def0.as_ref().map(|d| d.name.clone()),
            "version": def0.as_ref().map(|d| d.version),
            "json_object": json_object,
            "tasks": tasks_out,
            "active_task_list": active,
        }))
    }

    async fn process_instance_start_and_execute(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        self.process_define_start_and_execute(args).await
    }

    fn process_instance_withdraw(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_id(args, &["processInstanceId", "id"])?
            .ok_or(JeeflowError::Business("缺少id参数".into()))?;
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
        let id = arg_id(args, &["processInstanceId", "id"])?
            .ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let inst = self
            .repo
            .find_instance_by_id(id)?
            .ok_or(JeeflowError::InstanceNotFound(id))?;

        // 活跃节点 = 进行中任务
        let doing = self.repo.find_doing_tasks(id, &[])?;
        let mut active: Vec<String> = Vec::new();
        for t in &doing {
            if !active.contains(&t.task_name) {
                active.push(t.task_name.clone());
            }
        }

        // 历史节点 = 已产生任务（排除活跃）+ 模型路径补全
        let history_tasks = self.repo.find_history_tasks(id)?;
        let mut history: Vec<String> = Vec::new();
        for t in &history_tasks {
            if !active.contains(&t.task_name) && !history.contains(&t.task_name) {
                history.push(t.task_name.clone());
            }
        }

        let mut edges: Vec<String> = Vec::new();
        let mut node_progress = json!({});
        if let Some(def) = self.repo.find_define_by_id(inst.define_id)? {
            if let Ok(model) = jeeflow_core::parser::ModelParser::parse(&def.content_str()) {
                node_progress = build_node_progress(&model, &history_tasks);
                if let Some(start) = model.get_start() {
                    let mut visited = std::collections::HashSet::new();
                    collect_high_light_path(
                        &model,
                        &start.id,
                        &active,
                        &mut history,
                        &mut edges,
                        &mut visited,
                    );
                }
            }
        }

        // 已是 camelCase 契约键（transform 幂等）
        Ok(json!({
            "activeNodeNames": active,
            "historyNodeNames": history,
            "historyEdgeNames": edges,
            "nodeProgress": node_progress,
        }))
    }

    fn process_instance_approval_record(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_id(args, &["processInstanceId", "id"])?
            .ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let inst = self
            .repo
            .find_instance_by_id(id)?
            .ok_or(JeeflowError::InstanceNotFound(id))?;
        let instance_vars = flow_data_to_object(&inst.variables);
        let tasks = self.repo.find_history_tasks(id)?;
        let records: Vec<Json> = tasks
            .iter()
            .map(|t| {
                let task_vars = flow_data_to_object(&t.variables);
                // 对齐 Go taskRowToMap：任务变量空时回退实例变量（UI 读 ext.u_realName）
                let ext = if task_vars.as_object().map(|m| m.is_empty()).unwrap_or(true) {
                    instance_vars.clone()
                } else {
                    task_vars.clone()
                };
                json!({
                    "task_name": t.task_name,
                    "display_name": t.display_name,
                    "task_type": t.task_type,
                    "perform_type": t.perform_type,
                    "task_state": t.task_state,
                    "operator": t.actor_id,
                    "finish_time": t.finish_time,
                    "variable": task_vars,
                    "ext": ext,
                })
            })
            .collect();
        Ok(json!(records))
    }

    fn process_instance_get_assignee_text_data(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_id(args, &["processInstanceId", "id"])?
            .ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let include_node_name = args
            .get("includeNodeName")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let doing = self.repo.find_doing_tasks(id, &[])?;
        let mut rows: Vec<Json> = Vec::new();
        for t in &doing {
            let actors = self.repo.find_task_actors(t.task_id)?;
            for actor in actors {
                let label = if include_node_name {
                    format!("{}:{}", t.display_name, actor)
                } else {
                    actor.clone()
                };
                rows.push(json!({"value": actor, "label": label}));
            }
        }
        Ok(json!(rows))
    }

    fn process_instance_biz_data(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_id(args, &["processInstanceId", "id"])?
            .ok_or(JeeflowError::Business("processInstanceId 缺失".into()))?;
        let inst = self
            .repo
            .find_instance_by_id(id)?
            .ok_or(JeeflowError::InstanceNotFound(id))?;
        let define = self
            .repo
            .find_define_by_id(inst.define_id)?
            .ok_or(JeeflowError::DefineNotFound(inst.define_id))?;
        let table_name = resolve_rel_table_name(&define.content_str())
            .ok_or(JeeflowError::Business("流程定义未配置 relTableName".into()))?;
        // BizDataReader 由集成方注册（issues/30）；未注册明确报错，禁止把 vars 当成功返回
        let reader = self
            .engine
            .context()
            .biz_data_reader
            .as_ref()
            .ok_or(JeeflowError::Business(
                "业务数据读取器未注册（ServiceContext.with_biz_data_reader(...)，需集成层注入 SqlxBizDataReader）"
                    .into(),
            ))?;
        match reader.read_by_process_instance(&table_name, id)? {
            Some(row) => {
                let mut map = serde_json::Map::new();
                for (k, v) in row {
                    map.insert(k, json_value_to_serde(&v));
                }
                Ok(Json::Object(map))
            }
            None => Ok(Json::Null),
        }
    }

    fn process_instance_create_cc(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_id(args, &["processInstanceId", "id"])?
            .ok_or(JeeflowError::Business("缺少processInstanceId参数".into()))?;
        let operator = arg_str_or(args, "operator", "user1");
        let actors = arg_actor_ids(args);
        if actors.is_empty() {
            return Err(JeeflowError::Business("actorIds 缺失".into()));
        }
        self.repo.create_cc_instance(id, &operator, &actors)?;
        Ok(json!({}))
    }

    fn process_instance_update_cc_status(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let id = arg_id(args, &["processInstanceId", "id"])?
            .ok_or(JeeflowError::Business("缺少processInstanceId参数".into()))?;
        let operator = arg_str_or(args, "operator", "user1");
        self.repo.update_cc_status(id, &operator)?;
        Ok(json!({}))
    }

    fn process_instance_cc_list(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let page_num = arg_i64_or(args, "pageNum", 1)?;
        let page_size = arg_i64_or(args, "pageSize", 20)?;
        let filters = parse_m_params(args);
        let mut query = PageQuery::new(page_num, page_size);
        query.operator = arg_str(args, "operator");
        let page = self.repo.page_cc_instances(&query)?;
        let mut rows: Vec<Json> = page.rows.iter().map(instance_row_to_json).collect();
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
        let mut rows: Vec<Json> = page.rows.iter().map(task_row_to_json).collect();
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
        let mut rows: Vec<Json> = page.rows.iter().map(task_row_to_json).collect();
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
        let operator = arg_str_or(args, "operator", "user1");
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
        let id = arg_id(args, &["processTaskId", "id"])?
            .ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let operator = arg_str_or(args, "operator", "user1");
        let task = self.repo.find_task_by_id(id)?
            .ok_or(JeeflowError::TaskNotFound(id))?;
        let actors = self.repo.find_task_actors(id)?;
        let mut vo = task_vo(&task);
        if let Some(obj) = vo.as_object_mut() {
            // 仓储侧 actors 为准（与 Java findTaskActors 一致）
            obj.insert("task_actor_id_list".into(), json!(actors));
            obj.insert("executable".into(), Json::Bool(task.is_allowed(&operator)));
        }

        let doing = task.task_state == TaskState::Doing.code();
        let mut t_ext = flow_data_to_object(&task.variables);
        if let Some(obj) = t_ext.as_object_mut() {
            obj.insert("isFirstTaskNode".into(), Json::Bool(false));
        }

        if let Some(inst) = self.repo.find_instance_by_id(task.process_instance_id)? {
            if let Some(def) = self.repo.find_define_by_id(inst.define_id)? {
                let json_object = parse_graph(&def.content_str());
                if let Some(obj) = vo.as_object_mut() {
                    obj.insert("json_object".into(), json_object.clone().unwrap_or(Json::Null));
                }
                let is_first = doing
                    && json_object
                        .as_ref()
                        .and_then(first_task_node_id)
                        .map(|n| n == task.task_name)
                        .unwrap_or(false);
                if let Some(obj) = t_ext.as_object_mut() {
                    obj.insert("isFirstTaskNode".into(), Json::Bool(is_first));
                }
                // taskModel：对齐 Java（name/displayName/type/form/ext）
                if let Ok(model) = jeeflow_core::parser::ModelParser::parse(&def.content_str()) {
                    for node in model.get_nodes_by_type(jeeflow_core::parser::NodeType::Task) {
                        if node.id == task.task_name {
                            let ext_prop = node
                                .properties
                                .get("ext")
                                .map(json_value_to_serde)
                                .unwrap_or(Json::Null);
                            let tm = json!({
                                "name": node.id,
                                "display_name": node.display_name,
                                "type": "task",
                                "form": node.form_key(),
                                "ext": ext_prop,
                            });
                            if let Some(obj) = vo.as_object_mut() {
                                obj.insert("task_model".into(), tm);
                            }
                            break;
                        }
                    }
                }
            }
        }
        if let Some(obj) = vo.as_object_mut() {
            obj.insert("ext".into(), t_ext);
        }
        Ok(vo)
    }

    fn process_task_jump_able_task_name_list(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let instance_id = arg_id(args, &["processInstanceId", "id"])?
            .ok_or(JeeflowError::Business("缺少processInstanceId参数".into()))?;
        let done = self.repo.find_done_tasks(instance_id, &[])?;
        let mut rows: Vec<Json> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for t in &done {
            // 跳过会签 perform_type==1
            if t.perform_type == 1 {
                continue;
            }
            if seen.insert(t.task_name.clone()) {
                rows.push(json!({
                    "label": t.display_name,
                    "value": t.task_name,
                }));
            }
        }
        Ok(json!(rows))
    }

    fn process_task_candidate_page(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let task_id = arg_id(args, &["processTaskId", "id"])?
            .ok_or(JeeflowError::Business("processTaskId 缺失".into()))?;
        let task = self
            .repo
            .find_task_by_id(task_id)?
            .ok_or(JeeflowError::TaskNotFound(task_id))?;
        let inst = self
            .repo
            .find_instance_by_id(task.process_instance_id)?
            .ok_or(JeeflowError::InstanceNotFound(task.process_instance_id))?;
        let def = self
            .repo
            .find_define_by_id(inst.define_id)?
            .ok_or(JeeflowError::DefineNotFound(inst.define_id))?;

        let page_num = arg_i64_or(args, "pageNum", 1)?;
        let page_size = arg_i64_or(args, "pageSize", 10)?;

        let mut static_actors: Vec<String> = Vec::new();
        if let Ok(model) = jeeflow_core::parser::ModelParser::parse(&def.content_str()) {
            static_actors = collect_static_candidate_actors(&model, &task.task_name);
        }

        if !static_actors.is_empty() {
            let ctx = self.engine.context();
            let mut rows: Vec<Json> = Vec::new();
            for actor in &static_actors {
                let mut real_name = actor.clone();
                let mut extra: Option<Json> = None;
                if let Some(usp) = &ctx.user_search_provider {
                    if let Ok(Some(u)) = usp.find_by_id(actor) {
                        let mut map = serde_json::Map::new();
                        for (k, v) in &u {
                            map.insert(k.clone(), json_value_to_serde(v));
                        }
                        if let Some(rn) = map.get("realName").or_else(|| map.get("real_name")) {
                            if let Some(s) = rn.as_str() {
                                real_name = s.to_string();
                            }
                        }
                        extra = Some(Json::Object(map));
                    }
                }
                if extra.is_none() {
                    if let Some(up) = &ctx.user_provider {
                        if let Ok(Some(info)) = up.get_user(actor) {
                            real_name = info.real_name.clone();
                            extra = Some(json!({
                                "userId": info.user_id,
                                "realName": info.real_name,
                                "deptName": info.dept_name,
                            }));
                        }
                    }
                }
                rows.push(candidate_row(actor, &real_name, extra.as_ref()));
            }
            let result = PageResult::new(1, 10, rows.len() as i64, rows);
            return Ok(serde_json::to_value(page_to_json(&result)).unwrap());
        }

        // 无模型候选 → 用户分页搜索
        let Some(usp) = &self.engine.context().user_search_provider else {
            return Err(JeeflowError::Business(
                "未配置 IUserSearchProvider（用户搜索钩子）".into(),
            ));
        };
        let mut query = PageQuery::new(page_num, page_size);
        query.operator = arg_str(args, "operator");
        let page = usp.page(&query)?;
        let rows: Vec<Json> = page
            .rows
            .iter()
            .map(|u| {
                let mut map = serde_json::Map::new();
                for (k, v) in u {
                    map.insert(k.clone(), json_value_to_serde(v));
                }
                let id = map
                    .get("id")
                    .or_else(|| map.get("userId"))
                    .or_else(|| map.get("user_id"))
                    .and_then(|v| v.as_str().map(|s| s.to_string()).or_else(|| {
                        v.as_i64().map(|n| n.to_string())
                    }))
                    .unwrap_or_default();
                let real = map
                    .get("realName")
                    .or_else(|| map.get("real_name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or(id.as_str())
                    .to_string();
                candidate_row(&id, &real, Some(&Json::Object(map)))
            })
            .collect();
        let result = PageResult::new(page.page_num, page.page_size, page.record_count, rows);
        Ok(serde_json::to_value(page_to_json(&result)).unwrap())
    }

    fn process_task_surrogate(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        // Java: surrogate = addTaskActor(processTaskId, actorIds) — NOT lookup surrogate table
        let task_id = arg_id(args, &["processTaskId", "id"])?
            .ok_or(JeeflowError::Business("processTaskId/actorIds 缺失".into()))?;
        let actors = arg_actor_ids(args);
        if actors.is_empty() {
            return Err(JeeflowError::Business("processTaskId/actorIds 缺失".into()));
        }
        self.repo.add_task_actor(task_id, &actors)?;
        Ok(json!({}))
    }

    fn process_task_add_candidate(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        self.process_task_surrogate(args)
    }

    fn process_task_latest(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let instance_id = arg_id(args, &["processInstanceId", "instanceId", "id"])?
            .ok_or(JeeflowError::Business("缺少processInstanceId参数".into()))?;
        let tasks = self.repo.find_doing_tasks(instance_id, &[])?;
        if let Some(task) = tasks.first() {
            Ok(task_vo(task))
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
        let mut rows: Vec<Json> = page.rows.iter().map(design_to_json).collect();
        apply_filters_to_rows(&mut rows, &filters);
        let (page_rows, total) = re_paginate(rows, page_num, page_size);
        let result = PageResult::new(page_num, page_size, total, page_rows);
        Ok(serde_json::to_value(page_to_json(&result)).unwrap())
    }

    fn process_design_detail(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let id = arg_id(args, &["processDesignId", "id"])?
            .ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let design = ext
            .find_design_by_id(id)?
            .ok_or(JeeflowError::Business("流程设计不存在".into()))?;
        let his_list = ext.list_design_his(id)?;
        let mut json_object = his_list
            .first()
            .and_then(|h| parse_graph(&String::from_utf8_lossy(&h.content)))
            .unwrap_or_else(|| json!({}));
        if let Some(obj) = json_object.as_object_mut() {
            obj.entry("name".to_string())
                .or_insert(Json::String(design.name.clone()));
            obj.entry("displayName".to_string())
                .or_insert(Json::String(design.display_name.clone()));
            obj.entry("type".to_string())
                .or_insert(Json::String(design.design_type.clone()));
            obj.entry("processDesignId".to_string())
                .or_insert(json!(design.id));
        }
        let mut data = design_to_json(&design);
        if let Some(obj) = data.as_object_mut() {
            obj.insert("json_object".into(), json_object);
            obj.insert(
                "his".into(),
                Json::Array(his_list.iter().map(design_his_to_json).collect()),
            );
        }
        Ok(data)
    }

    fn process_design_save(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let operator = arg_str_or(args, "operator", "user1");
        let id_opt = arg_id(args, &["processDesignId", "id"])?;
        let design_type = arg_str(args, "type")
            .or_else(|| arg_str(args, "designType"))
            .unwrap_or_else(|| "approval".to_string());

        let design = if let Some(id) = id_opt {
            let mut design = ext
                .find_design_by_id(id)?
                .ok_or(JeeflowError::Business("流程设计不存在".into()))?;
            if let Some(v) = arg_str(args, "displayName") {
                design.display_name = v;
            }
            if args.contains_key("type") || args.contains_key("designType") {
                design.design_type = design_type;
            }
            if let Some(v) = arg_str(args, "icon") {
                design.icon = Some(v);
            }
            if let Some(v) = arg_str(args, "remark") {
                design.remark = Some(v);
            }
            design.update_user = Some(operator.clone());
            if content_bytes(args).is_some() {
                design.is_deployed = 0;
            }
            ext.update_design(&design)?;
            design
        } else {
            let mut design = ProcessDesign {
                id: 0,
                name: arg_str_or(args, "name", ""),
                display_name: arg_str_or(args, "displayName", ""),
                design_type,
                icon: arg_str(args, "icon"),
                is_deployed: 0,
                remark: arg_str(args, "remark"),
                create_time: None,
                create_user: Some(operator.clone()),
                update_time: None,
                update_user: Some(operator.clone()),
            };
            ext.save_design(&mut design)?;
            design
        };

        if let Some(bytes) = content_bytes(args) {
            let mut his = ProcessDesignHis {
                id: 0,
                process_design_id: design.id,
                content: bytes,
                create_time: None,
                create_user: Some(operator),
            };
            ext.save_design_his(&mut his)?;
        }
        Ok(json!({"id": design.id}))
    }

    fn process_design_update(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let id = arg_id(args, &["processDesignId", "id"])?
            .ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let mut design = ext
            .find_design_by_id(id)?
            .ok_or(JeeflowError::Business("流程设计不存在".into()))?;
        if let Some(v) = arg_str(args, "name") {
            design.name = v;
        }
        if let Some(v) = arg_str(args, "displayName") {
            design.display_name = v;
        }
        if let Some(v) = arg_str(args, "type").or_else(|| arg_str(args, "designType")) {
            design.design_type = v;
        }
        if let Some(v) = arg_str(args, "icon") {
            design.icon = Some(v);
        }
        if let Some(v) = arg_str(args, "remark") {
            design.remark = Some(v);
        }
        design.update_user = Some(arg_str_or(args, "operator", "system"));
        ext.update_design(&design)?;
        Ok(json!({}))
    }

    fn process_design_update_define(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let design_id = arg_id(args, &["processDesignId", "id"])?
            .ok_or(JeeflowError::Business("缺少processDesignId参数".into()))?;
        let mut design = ext
            .find_design_by_id(design_id)?
            .ok_or(JeeflowError::Business("流程设计不存在".into()))?;
        let bytes = content_bytes(args).ok_or(JeeflowError::Business("content 缺失".into()))?;
        let his_list = ext.list_design_his(design_id)?;
        let same = his_list
            .first()
            .map(|h| h.content == bytes)
            .unwrap_or(false);
        if !same {
            let mut his = ProcessDesignHis {
                id: 0,
                process_design_id: design_id,
                content: bytes.clone(),
                create_time: None,
                create_user: Some(arg_str_or(args, "operator", "system")),
            };
            ext.save_design_his(&mut his)?;
        }
        if let Ok(model) = jeeflow_core::parser::ModelParser::parse(&String::from_utf8_lossy(&bytes))
        {
            design.name = model.name;
            design.display_name = model.display_name;
            design.design_type = model.model_type;
        }
        design.is_deployed = 0;
        design.update_user = Some(arg_str_or(args, "operator", "system"));
        ext.update_design(&design)?;
        Ok(json!({}))
    }

    fn process_design_remove(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let ids = arg_ids(args)?;
        let ids = if ids.is_empty() {
            if let Some(id) = arg_id(args, &["processDesignId", "id"])? {
                vec![id]
            } else {
                return Err(JeeflowError::Business("缺少id参数".into()));
            }
        } else {
            ids
        };
        for id in ids {
            ext.remove_design(id)?;
        }
        Ok(json!({}))
    }

    fn process_design_deploy(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let id = arg_id(args, &["processDesignId", "id"])?
            .ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let design = ext
            .find_design_by_id(id)?
            .ok_or(JeeflowError::Business("流程设计不存在".into()))?;
        let his_list = ext.list_design_his(id)?;
        if his_list.is_empty() {
            return Err(JeeflowError::Business("流程设计没有内容，无法发布".into()));
        }
        let bytes = his_list[0].content.clone(); // newest first
        let content = String::from_utf8_lossy(&bytes).to_string();
        let model = jeeflow_core::parser::ModelParser::parse(&content)?;
        let operator = arg_str_or(args, "operator", "system");
        let define_id = self.save_deployed_define(&model, &bytes, &operator)?;
        let updated = ProcessDesign {
            is_deployed: 1,
            update_user: Some(operator),
            ..design
        };
        ext.update_design(&updated)?;
        Ok(json!({"process_define_id": define_id}))
    }

    fn process_design_redeploy(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let id = arg_id(args, &["processDesignId", "id"])?
            .ok_or(JeeflowError::Business("缺少id参数".into()))?;
        let design = ext
            .find_design_by_id(id)?
            .ok_or(JeeflowError::Business("流程设计不存在".into()))?;
        let his_list = ext.list_design_his(id)?;
        if his_list.is_empty() {
            return Err(JeeflowError::Business("流程设计没有内容，无法发布".into()));
        }
        let bytes = his_list[0].content.clone();
        let content = String::from_utf8_lossy(&bytes).to_string();
        let model = jeeflow_core::parser::ModelParser::parse(&content)?;
        let operator = arg_str_or(args, "operator", "system");

        let page = self.repo.page_defines(&PageQuery::new(1, i64::MAX / 4))?;
        let last = page
            .rows
            .iter()
            .filter(|r| r.name == model.name)
            .max_by_key(|r| r.version);
        let define_id = if let Some(last) = last {
            let old = self.repo.find_define_by_id(last.id)?.unwrap_or_else(|| ProcessDefine {
                id: last.id,
                name: last.name.clone(),
                display_name: last.display_name.clone(),
                define_type: last.define_type.clone(),
                state: last.state,
                content: bytes.clone(),
                version: last.version,
                create_time: last.create_time.clone(),
                create_user: last.create_user.clone(),
                update_time: None,
                update_user: None,
            });
            let updated = ProcessDefine {
                name: model.name.clone(),
                display_name: model.display_name.clone(),
                define_type: model.model_type.clone(),
                content: bytes,
                update_user: Some(operator.clone()),
                ..old
            };
            self.repo.update_define(&updated)?;
            last.id
        } else {
            self.save_deployed_define(&model, &bytes, &operator)?
        };

        let updated = ProcessDesign {
            is_deployed: 1,
            update_user: Some(operator),
            ..design
        };
        ext.update_design(&updated)?;
        Ok(json!({"process_define_id": define_id}))
    }

    fn process_design_list_by_type(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        // 不默认过滤 approval；仅当 UI 显式传 type/designType 时过滤
        let type_filter = arg_str(args, "type").or_else(|| arg_str(args, "designType"));
        let page = ext.page_designs(&PageQuery::new(1, i64::MAX / 4))?;
        let def_page = self.repo.page_defines(&PageQuery::new(1, i64::MAX / 4))?;
        let mut latest_by_name: HashMap<String, &DefineRow> = HashMap::new();
        for row in &def_page.rows {
            match latest_by_name.get(&row.name) {
                Some(prev) if prev.version >= row.version => {}
                _ => {
                    latest_by_name.insert(row.name.clone(), row);
                }
            }
        }

        let mut groups: serde_json::Map<String, Json> = serde_json::Map::new();
        for d in &page.rows {
            if let Some(ref tf) = type_filter {
                if &d.design_type != tf {
                    continue;
                }
            }
            let type_key = d.design_type.clone();
            let latest = latest_by_name.get(&d.name);
            let his = ext.list_design_his(d.id)?;
            let json_object = his
                .first()
                .and_then(|h| parse_graph(&String::from_utf8_lossy(&h.content)));
            let item = json!({
                "process_design_id": d.id,
                "name": d.name,
                "display_name": d.display_name,
                "icon": d.icon,
                "remark": d.remark,
                "process_define_id": latest.map(|r| r.id),
                "process_define_state": latest.map(|r| r.state),
                "json_object": json_object,
            });
            let entry = groups.entry(type_key).or_insert_with(|| Json::Array(vec![]));
            if let Some(arr) = entry.as_array_mut() {
                arr.push(item);
            }
        }
        Ok(Json::Object(groups))
    }

    // ═══════════════════════════════════════════════════════
    // processSurrogate actions (5)
    // ═══════════════════════════════════════════════════════

    fn process_surrogate_page(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        let query = PageQuery::new(arg_i64_or(args, "pageNum", 1)?, arg_i64_or(args, "pageSize", 20)?);
        let page = ext.page_surrogates(&query)?;
        let rows: Vec<Json> = page.rows.iter().map(surrogate_to_json).collect();
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
        Ok(surrogate_to_json(&sg))
    }

    fn process_surrogate_remove(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let ext = self.ext_repo.as_ref().ok_or(JeeflowError::Internal("ExtRepository not registered".into()))?;
        // issues/95：前端「我的委托」行内/批量删除统一发 {ids}，与 define/design remove 同惯例
        let ids = arg_ids(args)?;
        if ids.is_empty() {
            return Err(JeeflowError::Business("缺少id参数".into()));
        }
        for id in ids {
            ext.remove_surrogate(id)?;
        }
        Ok(json!({}))
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

/// 对齐 Java collectPath：沿输出边补全 history / edges；遇活跃节点仍收集边，但停止深入。
fn collect_high_light_path(
    model: &jeeflow_core::parser::ProcessModel,
    node_id: &str,
    active: &[String],
    history: &mut Vec<String>,
    edges: &mut Vec<String>,
    visited: &mut std::collections::HashSet<String>,
) {
    if visited.contains(node_id) {
        return;
    }
    visited.insert(node_id.to_string());
    for edge in model.get_output_edges(node_id) {
        // 决策边带 expr 时：无表达式引擎则保守跳过（对齐 Java evaluator==null → false）
        let src = model.nodes.iter().find(|n| n.id == node_id);
        if src.map(|n| n.node_type == jeeflow_core::parser::NodeType::Decision).unwrap_or(false) {
            if let Some(expr) = edge.expr() {
                if !expr.is_empty() {
                    continue;
                }
            }
        }
        if !edge.id.is_empty() && !edges.contains(&edge.id) {
            edges.push(edge.id.clone());
        }
        let tid = &edge.target_node_id;
        if tid.is_empty() {
            continue;
        }
        if !active.contains(tid) && !history.contains(tid) {
            history.push(tid.clone());
        }
        if active.contains(tid) {
            continue;
        }
        collect_high_light_path(model, tid, active, history, edges, visited);
    }
}

/// 对齐 Java/Go buildNodeProgress（会签成员进度；动态参与人无成员则跳过）
fn build_node_progress(
    model: &jeeflow_core::parser::ProcessModel,
    tasks: &[ProcessTask],
) -> Json {
    let mut progress = serde_json::Map::new();
    let mut seen = std::collections::HashSet::new();
    let mut names: Vec<String> = Vec::new();
    for t in tasks {
        if seen.insert(t.task_name.clone()) {
            names.push(t.task_name.clone());
        }
    }
    for name in names {
        let ts: Vec<&ProcessTask> = tasks.iter().filter(|t| t.task_name == name).collect();
        if ts.is_empty() {
            continue;
        }
        // operatorList_{node} 优先，否则 actor_ids 并集
        let mut members: Vec<String> = Vec::new();
        if let Some(list) = ts[0].variables.get_str(&format!("operatorList_{}", name)) {
            members = list
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
        if members.is_empty() {
            let mut set = std::collections::HashSet::new();
            for t in &ts {
                for a in &t.actor_ids {
                    if set.insert(a.clone()) {
                        members.push(a.clone());
                    }
                }
            }
        }
        if members.is_empty() {
            continue;
        }
        let mut done_set = std::collections::HashSet::new();
        for t in &ts {
            if t.task_state == TaskState::Finished.code() {
                for a in &t.actor_ids {
                    done_set.insert(a.clone());
                }
            }
        }
        let active_actor = ts
            .iter()
            .find(|t| t.task_state == TaskState::Doing.code() && !t.actor_ids.is_empty())
            .and_then(|t| t.actor_ids.first().cloned());

        let node = model.nodes.iter().find(|n| n.id == name);
        let cs_type = node.and_then(|n| n.prop_str("countersignType"));
        let is_cs = cs_type.is_some()
            || node
                .map(|n| {
                    let p = n.perform_type();
                    p == 1
                })
                .unwrap_or(false);

        let members_out: Vec<Json> = members
            .iter()
            .map(|uid| {
                let mut m = json!({"id": uid, "name": ""});
                if let Some(obj) = m.as_object_mut() {
                    if done_set.contains(uid) {
                        obj.insert("done".into(), Json::Bool(true));
                    } else if active_actor.as_ref() == Some(uid) {
                        obj.insert("active".into(), Json::Bool(true));
                    }
                }
                m
            })
            .collect();

        let mut item = json!({"members": members_out});
        if is_cs {
            if let Some(obj) = item.as_object_mut() {
                obj.insert(
                    "type".into(),
                    Json::String(cs_type.unwrap_or_else(|| "PARALLEL".into())),
                );
            }
        }
        progress.insert(name, item);
    }
    Json::Object(progress)
}

// ═══════════════════════════════════════════════════════
// Stats 3 actions (issues/103)
// ═══════════════════════════════════════════════════════

const DEFAULT_STATE_IN: &[i32] = &[10, 20, 30, 40, 45, 50];
const DEFAULT_STATS_LIMIT: usize = 10;
const VALID_GRANULARITY: &[&str] = &["hour", "day", "week", "month"];
const VALID_DIMENSION: &[&str] = &[
    "state", "define", "category", "approver", "applicant",
    "node", "stuckNode", "stuckApprover", "durationBucket",
];

fn stats_parse_time(s: Option<&str>) -> Option<chrono::NaiveDateTime> {
    s.and_then(|v| chrono::NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S").ok())
}

fn stats_round4(v: f64) -> f64 {
    (v * 10000.0).round() / 10000.0
}

fn stats_filter_instances(
    instances: &[ProcessInstance],
    state_in: Option<&[i32]>,
    start: Option<chrono::NaiveDateTime>,
    end: Option<chrono::NaiveDateTime>,
) -> Vec<ProcessInstance> {
    instances.iter().filter(|inst| {
        // state_in 为 None = 无 state 过滤（对齐内置线：仅 overview 六计数用 stateIn）
        if let Some(states) = state_in {
            if !states.contains(&inst.state) {
                return false;
            }
        }
        if let (Some(s), Some(ct)) = (start, inst.create_time.as_ref()) {
            if let Ok(t) = chrono::NaiveDateTime::parse_from_str(ct, "%Y-%m-%d %H:%M:%S") {
                if t < s { return false; }
            } else { return false; }
        }
        if let (Some(e), Some(ct)) = (end, inst.create_time.as_ref()) {
            if let Ok(t) = chrono::NaiveDateTime::parse_from_str(ct, "%Y-%m-%d %H:%M:%S") {
                if t >= e { return false; }
            } else { return false; }
        }
        true
    }).cloned().collect()
}

fn stats_filter_finished_tasks(
    tasks: &[ProcessTask],
    start: Option<chrono::NaiveDateTime>,
    end: Option<chrono::NaiveDateTime>,
) -> Vec<ProcessTask> {
    tasks.iter().filter(|t| {
        if t.task_state != TaskState::Finished.code() { return false; }
        if let Some(ft_str) = &t.finish_time {
            if let Ok(ft) = chrono::NaiveDateTime::parse_from_str(ft_str, "%Y-%m-%d %H:%M:%S") {
                if let Some(s) = start { if ft < s { return false; } }
                if let Some(e) = end { if ft >= e { return false; } }
                true
            } else { false }
        } else { false }
    }).cloned().collect()
}

fn stats_enumerate_buckets(
    start: Option<chrono::NaiveDateTime>,
    end: Option<chrono::NaiveDateTime>,
    granularity: &str,
) -> Vec<String> {
    let now = chrono::Local::now().naive_local();
    let s = start.unwrap_or_else(|| now - chrono::Duration::days(30));
    let e = end.unwrap_or(now);
    let mut buckets = Vec::new();
    match granularity {
        "hour" => {
            let mut cursor = s.date().and_hms_opt(s.hour(), 0, 0).unwrap();
            while cursor <= e {
                buckets.push(cursor.format("%Y-%m-%d %H:00").to_string());
                cursor += chrono::Duration::hours(1);
            }
        }
        "day" => {
            let mut cursor = s.date().and_hms_opt(0, 0, 0).unwrap();
            let end_day = e.date().and_hms_opt(0, 0, 0).unwrap();
            while cursor <= end_day {
                buckets.push(cursor.format("%Y-%m-%d").to_string());
                cursor += chrono::Duration::days(1);
            }
        }
        "week" => {
            let mut cursor = s.date().and_hms_opt(0, 0, 0).unwrap();
            let weekday = cursor.date().weekday();
            let offset = weekday.num_days_from_monday() as i64;
            cursor -= chrono::Duration::days(offset);
            let end_day = e.date().and_hms_opt(0, 0, 0).unwrap();
            while cursor <= end_day {
                let iso_year = cursor.date().iso_week().year();
                let iso_week = cursor.date().iso_week().week();
                buckets.push(format!("{}-W{:02}", iso_year, iso_week));
                cursor += chrono::Duration::days(7);
            }
        }
        "month" => {
            let mut year = s.year();
            let mut month = s.month() as i32;
            let end_year = e.year();
            let end_month = e.month() as i32;
            while year < end_year || (year == end_year && month <= end_month) {
                buckets.push(format!("{:04}-{:02}", year, month));
                month += 1;
                if month > 12 { month = 1; year += 1; }
            }
        }
        _ => {}
    }
    buckets
}

fn stats_bucket_key(dt: chrono::NaiveDateTime, granularity: &str) -> String {
    match granularity {
        "hour" => dt.format("%Y-%m-%d %H:00").to_string(),
        "day" => dt.format("%Y-%m-%d").to_string(),
        "week" => {
            let iso_year = dt.date().iso_week().year();
            let iso_week = dt.date().iso_week().week();
            format!("{}-W{:02}", iso_year, iso_week)
        }
        "month" => dt.format("%Y-%m").to_string(),
        _ => String::new(),
    }
}

impl JeeflowFacade {
    /// stats/overview — 13-field flat overview
    pub fn stats_overview(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let start = stats_parse_time(args.get("start").and_then(|v| v.as_str()));
        let end = stats_parse_time(args.get("end").and_then(|v| v.as_str()));

        // B：stateIn 入参（缺省 DEFAULT_STATE_IN），作用于六个状态计数
        let state_in_arg: Option<Vec<i32>> = args.get("stateIn").and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|x| x.as_i64().map(|n| n as i32)).collect());
        let state_in: Vec<i32> = match state_in_arg {
            Some(v) if !v.is_empty() => v,
            _ => DEFAULT_STATE_IN.to_vec(),
        };

        let all_instances = self.repo.get_all_instances()?;
        let filtered = stats_filter_instances(&all_instances, Some(&state_in), start, end);

        let mut by_state: HashMap<i32, i32> = HashMap::new();
        for inst in &filtered {
            *by_state.entry(inst.state).or_insert(0) += 1;
        }
        let total = filtered.len() as i32;
        let in_progress = by_state.get(&10).copied().unwrap_or(0);
        let completed = by_state.get(&20).copied().unwrap_or(0);
        let rejected = by_state.get(&45).copied().unwrap_or(0);
        let withdrawn = by_state.get(&30).copied().unwrap_or(0);
        let suspended = by_state.get(&50).copied().unwrap_or(0);

        // todayNew — server today, ignores start/end
        let now = chrono::Local::now().naive_local();
        let today_start = now.date().and_hms_opt(0, 0, 0).unwrap();
        let today_end = today_start + chrono::Duration::days(1);
        // E：todayNew 恒按服务器当日、不过滤 state / 不受 stateIn 影响（对齐内置线 countTodayNew）
        let today_filtered = stats_filter_instances(&all_instances, None, Some(today_start), Some(today_end));
        let today_new = today_filtered.len() as i32;

        // pendingTaskCount + overdueTaskCount — all tasks, not filtered by stateIn
        let all_tasks = self.repo.get_all_tasks()?;
        let now_str = now.format("%Y-%m-%d %H:%M:%S").to_string();
        let mut pending_count = 0i32;
        let mut overdue_count = 0i32;
        for t in &all_tasks {
            if t.task_state == TaskState::Doing.code() {
                pending_count += 1;
                if let Some(exp) = &t.expire_time {
                    if exp.as_str() < now_str.as_str() {
                        overdue_count += 1;
                    }
                }
            }
        }

        // countersignRate + onTimeRate — from finished tasks
        let finished_tasks: Vec<&ProcessTask> = all_tasks.iter()
            .filter(|t| t.task_state == TaskState::Finished.code())
            .collect();
        let task_total = finished_tasks.len();
        let countersign = finished_tasks.iter()
            .filter(|t| t.perform_type == PerformType::Countersign.code())
            .count();
        let mut on_time = 0i32;
        let mut on_time_denom = 0i32;
        for t in &finished_tasks {
            if let (Some(ft_str), Some(exp_str)) = (&t.finish_time, &t.expire_time) {
                on_time_denom += 1;
                if ft_str.as_str() <= exp_str.as_str() {
                    on_time += 1;
                }
            }
        }
        let countersign_rate = if task_total > 0 {
            stats_round4(countersign as f64 / task_total as f64)
        } else { 0.0 };
        let on_time_rate = if on_time_denom > 0 {
            stats_round4(on_time as f64 / on_time_denom as f64)
        } else { 0.0 };

        // avgDurationSeconds — state=20 完成实例平均时长，不受 stateIn 影响（对齐内置线 avgCompletedInstanceDurationSeconds）
        let avg_base = stats_filter_instances(&all_instances, None, start, end);
        let completed_instances: Vec<&ProcessInstance> = avg_base.iter()
            .filter(|inst| inst.state == InstanceState::Finished.code())
            .collect();
        let mut total_dur: i64 = 0;
        let mut dur_count = 0i64;
        for inst in &completed_instances {
            if let Some(ct_str) = &inst.create_time {
                if let Some(ct) = stats_parse_time(Some(ct_str.as_str())) {
                    let mut max_ft: Option<chrono::NaiveDateTime> = None;
                    for t in &all_tasks {
                        if t.process_instance_id == inst.instance_id {
                            if let Some(ft_str) = &t.finish_time {
                                if let Some(ft) = stats_parse_time(Some(ft_str.as_str())) {
                                    max_ft = Some(match max_ft {
                                        Some(prev) if prev > ft => prev,
                                        _ => ft,
                                    });
                                }
                            }
                        }
                    }
                    if let Some(ft) = max_ft {
                        total_dur += (ft - ct).num_seconds();
                        dur_count += 1;
                    }
                }
            }
        }
        let avg_duration = if dur_count > 0 {
            ((total_dur as f64) / (dur_count as f64)).round() as i64
        } else { 0 };

        let reject_rate = stats_round4(rejected as f64 / std::cmp::max(1, completed + rejected) as f64);

        Ok(json!({
            "total": total,
            "inProgress": in_progress,
            "completed": completed,
            "rejected": rejected,
            "withdrawn": withdrawn,
            "suspended": suspended,
            "todayNew": today_new,
            "avgDurationSeconds": avg_duration,
            "rejectRate": reject_rate,
            "pendingTaskCount": pending_count,
            "overdueTaskCount": overdue_count,
            "countersignRate": countersign_rate,
            "onTimeRate": on_time_rate,
        }))
    }

    /// stats/trend — continuous time buckets
    pub fn stats_trend(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let start = stats_parse_time(args.get("start").and_then(|v| v.as_str()));
        let end = stats_parse_time(args.get("end").and_then(|v| v.as_str()));
        let granularity = args.get("granularity")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        // C：start/end/granularity 均必填（对齐内置线 20010012 缺参语义）
        if granularity.is_empty() || start.is_none() || end.is_none() {
            return Err(JeeflowError::Business("trend 缺少必填参数：start/end/granularity".into()));
        }
        if !VALID_GRANULARITY.contains(&granularity) {
            return Err(JeeflowError::Business("granularity 参数非法，允许值：hour/day/week/month".into()));
        }

        // 实例侧无 state 过滤（对齐内置线 countInstanceStartedByBucket）
        let all_instances = self.repo.get_all_instances()?;
        let filtered = stats_filter_instances(&all_instances, None, start, end);

        let all_tasks = self.repo.get_all_tasks()?;
        let finished_tasks = stats_filter_finished_tasks(&all_tasks, start, end);

        let buckets = stats_enumerate_buckets(start, end, granularity);
        let mut bucket_map: HashMap<String, (i32, i32)> = HashMap::new();
        for b in &buckets {
            bucket_map.insert(b.clone(), (0, 0));
        }

        for inst in &filtered {
            if let Some(ct_str) = &inst.create_time {
                if let Some(ct) = stats_parse_time(Some(ct_str.as_str())) {
                    let bk = stats_bucket_key(ct, granularity);
                    if let Some(entry) = bucket_map.get_mut(&bk) {
                        entry.0 += 1;
                    }
                }
            }
        }

        for task in &finished_tasks {
            if let Some(ft_str) = &task.finish_time {
                if let Some(ft) = stats_parse_time(Some(ft_str.as_str())) {
                    let bk = stats_bucket_key(ft, granularity);
                    if let Some(entry) = bucket_map.get_mut(&bk) {
                        entry.1 += 1;
                    }
                }
            }
        }

        let series: Vec<Json> = buckets.iter().map(|b| {
            let (started, finished) = bucket_map.get(b).copied().unwrap_or((0, 0));
            json!({"bucket": b, "started": started, "finished": finished})
        }).collect();

        // A：data 本体为裸数组（去掉 {granularity, series} 包装，对齐契约 spec 06 §4.2 / 内置线）
        Ok(json!(series))
    }

    /// stats/group — dimension-based grouping
    pub fn stats_group(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let start = stats_parse_time(args.get("start").and_then(|v| v.as_str()));
        let end = stats_parse_time(args.get("end").and_then(|v| v.as_str()));
        let dimension = args.get("dimension")
            .and_then(|v| v.as_str())
            .unwrap_or("define");
        let limit = args.get("limit")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(DEFAULT_STATS_LIMIT);

        if !VALID_DIMENSION.contains(&dimension) {
            return Err(JeeflowError::Business("dimension 参数非法，允许值：state/define/category/approver/applicant/node/stuckNode/stuckApprover/durationBucket".into()));
        }

        let all_instances = self.repo.get_all_instances()?;
        let all_tasks = self.repo.get_all_tasks()?;
        // 无 state 过滤（对齐内置线 groupByDimension：仅按时间限定，契约 group 无 stateIn 入参）
        let filtered = stats_filter_instances(&all_instances, None, start, end);

        let rows: Vec<Json> = match dimension {
            "state" => {
                let mut grouped: HashMap<String, i32> = HashMap::new();
                for inst in &filtered {
                    *grouped.entry(inst.state.to_string()).or_insert(0) += 1;
                }
                let mut entries: Vec<(String, i32)> = grouped.into_iter().collect();
                entries.sort_by(|a, b| b.1.cmp(&a.1));
                entries.truncate(limit);
                entries.iter().map(|(k, c)| json!({
                    "key": k, "label": Json::Null, "count": c, "avgDurationSeconds": Json::Null,
                })).collect()
            }

            "define" => {
                let ext = self.ext_repo.as_ref()
                    .ok_or_else(|| JeeflowError::Internal("ExtRepository not registered".into()))?;
                let mut by_define: HashMap<i64, Vec<&ProcessInstance>> = HashMap::new();
                for inst in &filtered {
                    by_define.entry(inst.define_id).or_default().push(inst);
                }
                // max finish_time per instance from all_tasks
                let mut inst_max_ft: HashMap<i64, Option<chrono::NaiveDateTime>> = HashMap::new();
                for t in &all_tasks {
                    if t.task_state == TaskState::Finished.code() {
                        if let Some(ft_str) = &t.finish_time {
                            if let Some(ft) = stats_parse_time(Some(ft_str.as_str())) {
                                let entry = inst_max_ft.entry(t.process_instance_id).or_insert(None);
                                *entry = Some(match entry {
                                    Some(prev) if *prev > ft => *prev,
                                    _ => ft,
                                });
                            }
                        }
                    }
                }
                let mut entries: Vec<(String, Option<String>, usize, Option<i64>)> = Vec::new();
                for (define_id, insts) in &by_define {
                    let design = ext.find_design_by_id(*define_id)?;
                    let (key, label) = match &design {
                        Some(d) => (d.name.clone(), Some(d.display_name.clone())),
                        None => (define_id.to_string(), None),
                    };
                    let mut total_dur: i64 = 0;
                    let mut dur_count: i64 = 0;
                    for inst in insts {
                        if inst.state == InstanceState::Finished.code() {
                            if let Some(ct_str) = &inst.create_time {
                                if let Some(ct) = stats_parse_time(Some(ct_str.as_str())) {
                                    if let Some(Some(ft)) = inst_max_ft.get(&inst.instance_id) {
                                        total_dur += (*ft - ct).num_seconds();
                                        dur_count += 1;
                                    }
                                }
                            }
                        }
                    }
                    let avg = if dur_count > 0 {
                        Some((total_dur as f64 / dur_count as f64).round() as i64)
                    } else { None };
                    entries.push((key, label, insts.len(), avg));
                }
                entries.sort_by(|a, b| b.2.cmp(&a.2));
                entries.truncate(limit);
                entries.iter().map(|(k, l, c, avg)| json!({
                    "key": k, "label": l, "count": c, "avgDurationSeconds": avg,
                })).collect()
            }

            "category" => {
                let ext = self.ext_repo.as_ref()
                    .ok_or_else(|| JeeflowError::Internal("ExtRepository not registered".into()))?;
                let mut define_types: HashMap<i64, String> = HashMap::new();
                for inst in &filtered {
                    if !define_types.contains_key(&inst.define_id) {
                        let design = ext.find_design_by_id(inst.define_id)?;
                        let tp = design.map(|d| d.design_type.clone()).unwrap_or_default();
                        define_types.insert(inst.define_id, tp);
                    }
                }
                let mut grouped: HashMap<String, i32> = HashMap::new();
                for inst in &filtered {
                    let tp = define_types.get(&inst.define_id).cloned().unwrap_or_default();
                    *grouped.entry(tp).or_insert(0) += 1;
                }
                let mut entries: Vec<(String, i32)> = grouped.into_iter().collect();
                entries.sort_by(|a, b| b.1.cmp(&a.1));
                entries.truncate(limit);
                entries.iter().map(|(k, c)| json!({
                    "key": k, "label": Json::Null, "count": c, "avgDurationSeconds": Json::Null,
                })).collect()
            }

            "approver" => {
                let finished = stats_filter_finished_tasks(&all_tasks, start, end);
                let mut grouped: HashMap<String, i32> = HashMap::new();
                for t in &finished {
                    if let Some(aid) = &t.actor_id {
                        if !aid.is_empty() {
                            *grouped.entry(aid.clone()).or_insert(0) += 1;
                        }
                    }
                }
                let mut entries: Vec<(String, i32)> = grouped.into_iter().collect();
                entries.sort_by(|a, b| b.1.cmp(&a.1));
                entries.truncate(limit);
                entries.iter().map(|(k, c)| json!({
                    "key": k, "label": Json::Null, "count": c, "avgDurationSeconds": Json::Null,
                })).collect()
            }

            "applicant" => {
                let mut grouped: HashMap<String, i32> = HashMap::new();
                for inst in &filtered {
                    if !inst.operator.is_empty() {
                        *grouped.entry(inst.operator.clone()).or_insert(0) += 1;
                    }
                }
                let mut entries: Vec<(String, i32)> = grouped.into_iter().collect();
                entries.sort_by(|a, b| b.1.cmp(&a.1));
                entries.truncate(limit);
                entries.iter().map(|(k, c)| json!({
                    "key": k, "label": Json::Null, "count": c, "avgDurationSeconds": Json::Null,
                })).collect()
            }

            "node" => {
                let finished = stats_filter_finished_tasks(&all_tasks, start, end);
                struct NodeAgg { count: i32, total_dur: i64 }
                let mut grouped: HashMap<String, NodeAgg> = HashMap::new();
                for t in &finished {
                    if t.display_name.is_empty() { continue; }
                    let dur = if let (Some(ft_str), Some(ct_str)) = (&t.finish_time, &t.create_time) {
                        if let (Some(ft), Some(ct)) = (stats_parse_time(Some(ft_str.as_str())), stats_parse_time(Some(ct_str.as_str()))) {
                            (ft - ct).num_seconds()
                        } else { 0 }
                    } else { 0 };
                    let agg = grouped.entry(t.display_name.clone()).or_insert(NodeAgg { count: 0, total_dur: 0 });
                    agg.count += 1;
                    agg.total_dur += dur;
                }
                let mut entries: Vec<(String, NodeAgg)> = grouped.into_iter().collect();
                entries.sort_by(|a, b| b.1.count.cmp(&a.1.count));
                entries.truncate(limit);
                entries.iter().map(|(k, agg)| {
                    let avg: Option<i64> = if agg.count > 0 {
                        Some((agg.total_dur as f64 / agg.count as f64).round() as i64)
                    } else { None };
                    json!({"key": k, "label": Json::Null, "count": agg.count, "avgDurationSeconds": avg})
                }).collect()
            }

            "stuckNode" => {
                let stuck: Vec<&ProcessTask> = all_tasks.iter()
                    .filter(|t| t.task_state == TaskState::Doing.code())
                    .collect();
                let mut grouped: HashMap<String, i32> = HashMap::new();
                for t in &stuck {
                    if !t.display_name.is_empty() {
                        *grouped.entry(t.display_name.clone()).or_insert(0) += 1;
                    }
                }
                let mut entries: Vec<(String, i32)> = grouped.into_iter().collect();
                entries.sort_by(|a, b| b.1.cmp(&a.1));
                entries.truncate(limit);
                entries.iter().map(|(k, c)| json!({
                    "key": k, "label": Json::Null, "count": c, "avgDurationSeconds": Json::Null,
                })).collect()
            }

            "stuckApprover" => {
                let stuck: Vec<&ProcessTask> = all_tasks.iter()
                    .filter(|t| t.task_state == TaskState::Doing.code())
                    .collect();
                let mut grouped: HashMap<String, i32> = HashMap::new();
                for t in &stuck {
                    for aid in &t.actor_ids {
                        if !aid.is_empty() {
                            *grouped.entry(aid.clone()).or_insert(0) += 1;
                        }
                    }
                }
                let mut entries: Vec<(String, i32)> = grouped.into_iter().collect();
                entries.sort_by(|a, b| b.1.cmp(&a.1));
                entries.truncate(limit);
                entries.iter().map(|(k, c)| json!({
                    "key": k, "label": Json::Null, "count": c, "avgDurationSeconds": Json::Null,
                })).collect()
            }

            "durationBucket" => {
                let completed: Vec<&ProcessInstance> = filtered.iter()
                    .filter(|inst| inst.state == InstanceState::Finished.code())
                    .collect();
                let mut inst_max_ft: HashMap<i64, Option<chrono::NaiveDateTime>> = HashMap::new();
                for t in &all_tasks {
                    if t.task_state == TaskState::Finished.code() {
                        if let Some(ft_str) = &t.finish_time {
                            if let Some(ft) = stats_parse_time(Some(ft_str.as_str())) {
                                let entry = inst_max_ft.entry(t.process_instance_id).or_insert(None);
                                *entry = Some(match entry {
                                    Some(prev) if *prev > ft => *prev,
                                    _ => ft,
                                });
                            }
                        }
                    }
                }
                let mut same_day = 0i32;
                let mut d1to3 = 0i32;
                let mut d3to7 = 0i32;
                let mut over7d = 0i32;
                for inst in &completed {
                    if let Some(ct_str) = &inst.create_time {
                        if let Some(ct) = stats_parse_time(Some(ct_str.as_str())) {
                            if let Some(Some(ft)) = inst_max_ft.get(&inst.instance_id) {
                                let dur = (*ft - ct).num_seconds();
                                if dur < 86400 { same_day += 1; }
                                else if dur < 259200 { d1to3 += 1; }
                                else if dur < 604800 { d3to7 += 1; }
                                else { over7d += 1; }
                            }
                        }
                    }
                }
                let keys = ["sameDay", "1to3d", "3to7d", "over7d"];
                let counts = [same_day, d1to3, d3to7, over7d];
                keys.iter().zip(counts.iter()).map(|(k, c)| json!({
                    "key": k, "label": Json::Null, "count": c, "avgDurationSeconds": Json::Null,
                })).collect()
            }

            _ => unreachable!(),
        };

        // A：data 本体为裸数组（去掉 {dimension, rows} 包装，对齐契约 spec 06 §4.2 / 内置线）
        Ok(json!(rows))
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

    #[test]
    fn test_to_camel_json_preserves_opaque_maps() {
        let input = json!({
            "process_instance_id": 1,
            "ext": {"u_realName": "张三", "tf_approvalComment": "ok"},
            "json_object": {
                "nodes": [{
                    "properties": {
                        "field": {"PERMISSION_f_leaveType": 1, "PERMISSION_days": 2}
                    }
                }]
            }
        });
        let output = to_camel_json(&input);
        assert_eq!(output["processInstanceId"], 1);
        assert_eq!(output["ext"]["u_realName"], "张三");
        assert_eq!(output["ext"]["tf_approvalComment"], "ok");
        assert_eq!(
            output["jsonObject"]["nodes"][0]["properties"]["field"]["PERMISSION_f_leaveType"],
            1
        );
        assert_eq!(
            output["jsonObject"]["nodes"][0]["properties"]["field"]["PERMISSION_days"],
            2
        );
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
        assert!(resp["data"]["type"].is_string(), "契约字段 type，不能是 defineType");
        assert!(resp["data"].get("defineType").is_none());
        assert!(resp["data"]["jsonObject"].is_object(), "契约字段 jsonObject（解析后的图）");
        assert!(resp["data"].get("content").is_none());
    }

    #[tokio::test]
    async fn test_process_define_deploy() {
        let facade = make_facade();
        let mut args = HashMap::new();
        args.insert(
            "content".to_string(),
            json!(r#"{"name":"deploy-flow","displayName":"DF","type":"approval","nodes":[{"id":"start","type":"snaker:start","text":{"value":"S"}},{"id":"apply","type":"snaker:task","text":{"value":"A"},"properties":{"assignee":"applicant"}},{"id":"end","type":"snaker:end","text":{"value":"E"}}],"edges":[{"id":"e1","sourceNodeId":"start","targetNodeId":"apply"},{"id":"e2","sourceNodeId":"apply","targetNodeId":"end"}]}"#),
        );
        let resp = facade.flow("processDefine/deploy", &args).await;
        assert_eq!(resp["code"], 0, "{:?}", resp);
        assert!(
            resp["data"]["processDefineId"].is_string(),
            "deploy must return processDefineId: {:?}",
            resp
        );
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

    /// issues/95：前端「我的委托」行内与批量删除统一发 {ids}（行内 = 长度 1 的数组），
    /// 此前门面只读单数 {id} → 该页删除整体不可用；单 {id} 形态保留兼容（移动端发这个）。
    #[tokio::test]
    async fn test_process_surrogate_remove_batch_ids() {
        let facade = make_facade();
        macro_rules! save_sg {
            ($op:expr, $agent:expr, $name:expr) => {{
                let mut a = HashMap::new();
                a.insert("operator".to_string(), json!($op));
                a.insert("surrogate".to_string(), json!($agent));
                a.insert("processName".to_string(), json!($name));
                let r = facade.flow("processSurrogate/save", &a).await;
                assert_eq!(r["code"], 0, "save {} 应成功: {}", $name, r);
                r["data"]["id"].as_str().unwrap().parse::<i64>().unwrap()
            }};
        }
        macro_rules! assert_gone {
            ($id:expr, $label:expr) => {{
                let mut d = HashMap::new();
                d.insert("id".to_string(), json!($id));
                assert_eq!(
                    facade.flow("processSurrogate/detail", &d).await["code"],
                    99999999,
                    "{} 应已删除",
                    $label
                );
            }};
        }

        let a = save_sg!("zhangsan", "lisiA", "leaveA");
        let b = save_sg!("zhangsan", "lisiB", "leaveB");
        let mut ids_args = HashMap::new();
        ids_args.insert("ids".to_string(), json!([a, b]));
        let resp = facade.flow("processSurrogate/remove", &ids_args).await;
        assert_eq!(resp["code"], 0, "批量 {{ids}} 删除应成功: {}", resp);
        assert_gone!(a, "批量 a");
        assert_gone!(b, "批量 b");

        // 行内删除：前端同样走 {ids}，长度 1
        let c = save_sg!("lisiC", "lisiD", "leaveC");
        let mut one = HashMap::new();
        one.insert("ids".to_string(), json!([c]));
        assert_eq!(facade.flow("processSurrogate/remove", &one).await["code"], 0);
        assert_gone!(c, "行内 c");

        // 单 {id} 兼容形态回归
        let d = save_sg!("zhangsan", "lisiE", "leaveD");
        let mut single = HashMap::new();
        single.insert("id".to_string(), json!(d));
        assert_eq!(facade.flow("processSurrogate/remove", &single).await["code"], 0);
        assert_gone!(d, "单 id d");
    }

    /// issues/95 §5②：{ids}/{id} 缺失或空数组一律报错，禁止静默成功。
    #[tokio::test]
    async fn test_remove_empty_ids_rejected() {
        let facade = make_facade();
        let cases: Vec<(&str, Vec<(&str, Json)>)> = vec![
            ("processSurrogate/remove", vec![("ids", json!([]))]),
            ("processSurrogate/remove", vec![("surrogate", json!("lisi"))]),
            ("processSurrogate/remove", vec![("ids", json!([123, null]))]),
            ("processDefine/remove", vec![("ids", json!([]))]),
            ("processDesign/remove", vec![("ids", json!([]))]),
            ("processDefine/upAndDown", vec![("ids", json!([])), ("opType", json!(0))]),
        ];
        for (action, pairs) in cases {
            let args: HashMap<String, Json> =
                pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
            let resp = facade.flow(action, &args).await;
            assert_eq!(resp["code"], 99999999, "{} {:?} 应报错而非静默成功", action, args);
        }
    }

    // ─── issues/96 §4B 入口批量参数形态矩阵（arg_ids 助手四态 + 4 action × 4 态）───

    /// 门面入口 args 构造助手（`&str` 键不匹配 `String`，必须 to_string 后再 collect）。
    fn args_of(pairs: Vec<(&str, Json)>) -> HashMap<String, Json> {
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    /// 走仓储落一条测试用流程定义（state=1），返回 id —— 与 make_facade_with_define 同源写法。
    fn save_define(facade: &JeeflowFacade, name: &str) -> i64 {
        let mut define = ProcessDefine {
            id: 0,
            name: name.into(),
            display_name: name.into(),
            define_type: "approval".into(),
            state: 1,
            content: br#"{"name":"shape-matrix","nodes":[],"edges":[]}"#.to_vec(),
            version: 1,
            create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        facade.repo().save_define(&mut define).unwrap();
        define.id
    }

    /// 走门面落一条流程设计，返回 id。
    async fn save_design(facade: &JeeflowFacade, name: &str) -> i64 {
        let args = args_of(vec![("name", json!(name)), ("displayName", json!(name))]);
        let resp = facade.flow("processDesign/save", &args).await;
        assert_eq!(resp["code"], 0, "processDesign/save {} 应成功: {}", name, resp);
        resp["data"]["id"].as_str().unwrap().parse::<i64>().unwrap()
    }

    /// 走门面落一条委托，返回 id。
    async fn save_surrogate(facade: &JeeflowFacade, operator: &str, agent: &str) -> i64 {
        let args = args_of(vec![
            ("operator", json!(operator)),
            ("surrogate", json!(agent)),
            ("processName", json!("shape-matrix-flow")),
        ]);
        let resp = facade.flow("processSurrogate/save", &args).await;
        assert_eq!(resp["code"], 0, "processSurrogate/save {}→{} 应成功: {}", operator, agent, resp);
        resp["data"]["id"].as_str().unwrap().parse::<i64>().unwrap()
    }

    async fn assert_surrogate_gone(facade: &JeeflowFacade, id: i64, label: &str) {
        let args = args_of(vec![("id", json!(id))]);
        assert_eq!(
            facade.flow("processSurrogate/detail", &args).await["code"],
            99999999,
            "{}(surrogate id={}) 应已删除",
            label,
            id
        );
    }

    async fn assert_design_gone(facade: &JeeflowFacade, id: i64, label: &str) {
        let args = args_of(vec![("id", json!(id))]);
        assert_eq!(
            facade.flow("processDesign/detail", &args).await["code"],
            99999999,
            "{}(design id={}) 应已删除",
            label,
            id
        );
    }

    async fn assert_define_gone(facade: &JeeflowFacade, id: i64, label: &str) {
        let args = args_of(vec![("id", json!(id))]);
        assert_eq!(
            facade.flow("processDefine/detail", &args).await["code"],
            99999999,
            "{}(define id={}) 应已删除",
            label,
            id
        );
    }

    /// upAndDown 不删数据，"取不到"的等价断言是 state 已变更。
    async fn assert_define_state(facade: &JeeflowFacade, id: i64, want: i64, label: &str) {
        let args = args_of(vec![("id", json!(id))]);
        let resp = facade.flow("processDefine/detail", &args).await;
        assert_eq!(resp["code"], 0, "{}(define id={}) 应仍可查", label, id);
        assert_eq!(
            resp["data"]["state"].as_i64(),
            Some(want),
            "{}(define id={}) 的 state 应已变为 {}",
            label,
            id,
            want
        );
    }

    /// issues/96 §4B：把 `arg_ids()` 助手直接单元化（此前零测试）。四态 = 正常数组 / 单值 id 回落 /
    /// 空数组 / 含非法值。
    /// ⚠️ 助手对「空数组」与「ids、id 皆缺」的语义是 `Ok(空 Vec)`——那是"没拿到 id"的信号，
    /// **报错责任在调用方**的 `ids.is_empty()` 守卫（四个 action 的红由下方矩阵用例钉住）；
    /// 只有含非法值（空串 / null / 非数字串）才由助手本身 Err。
    #[test]
    fn test_arg_ids_helper_four_states() {
        // ① 正常数组（前端 Long 会序列化成字符串，故数字/字符串/混合都要收）
        let parsed = arg_ids(&args_of(vec![("ids", json!([1, "2", 3]))])).unwrap();
        assert_eq!(parsed, vec![1i64, 2, 3], "{{ids:[1,\"2\",3]}} 应解析成 3 个 i64");
        let both = arg_ids(&args_of(vec![("ids", json!([7])), ("id", json!(9))])).unwrap();
        assert_eq!(both, vec![7], "ids 与 id 同时在时应取 ids");

        // ② 单值 id 回落（移动端旧形态）
        assert_eq!(arg_ids(&args_of(vec![("id", json!(5))])).unwrap(), vec![5]);
        assert_eq!(
            arg_ids(&args_of(vec![("id", json!("6"))])).unwrap(),
            vec![6],
            "字符串形式的单值 id 也应回落成功"
        );

        // ③ 空数组 / 两者皆缺 → Ok(空集)，由 action 守卫报错（禁止静默成功）
        assert_eq!(
            arg_ids(&args_of(vec![("ids", json!([]))])).unwrap(),
            Vec::<i64>::new(),
            "{{ids:[]}} 助手应交出空集（非 Err），action 必须据此报错"
        );
        assert_eq!(
            arg_ids(&args_of(vec![("surrogate", json!("lisi"))])).unwrap(),
            Vec::<i64>::new(),
            "ids/id 皆缺时同样交出空集"
        );

        // ④ 含非法值 → 助手本身必须 Err
        for bad in [json!([""]), json!([123, null]), json!(["abc"]), json!([null])] {
            assert!(
                arg_ids(&args_of(vec![("ids", bad.clone())])).is_err(),
                "{{ids:{}}} 应 Err",
                bad
            );
        }
    }

    /// issues/96 §4B：processSurrogate/remove 的 4 种入口形态。
    /// 负向只断 code —— Rust 文案（「缺少id参数」/「非法id: …」）与其余五语言
    /// （「id 缺失或非法」）的 drift 是 issues/95 §偏差 1 + issues/77 已挂号残留，本轮不改文案。
    #[tokio::test]
    async fn test_process_surrogate_remove_ids_shape_matrix() {
        let facade = make_facade();

        // ① {ids:[a,b]} → 成功且事后回查两条都取不到
        let a = save_surrogate(&facade, "zhangsan", "lisiA").await;
        let b = save_surrogate(&facade, "zhangsan", "lisiB").await;
        let args = args_of(vec![("ids", json!([a, b]))]);
        let resp = facade.flow("processSurrogate/remove", &args).await;
        assert_eq!(resp["code"], 0, "{{ids:[a,b]}} 删除应成功: {}", resp);
        assert_surrogate_gone(&facade, a, "批量 a").await;
        assert_surrogate_gone(&facade, b, "批量 b").await;

        // ② {id:c} → 旧形态不被改坏
        let c = save_surrogate(&facade, "lisiC", "lisiD").await;
        let args = args_of(vec![("id", json!(c))]);
        let resp = facade.flow("processSurrogate/remove", &args).await;
        assert_eq!(resp["code"], 0, "{{id}} 旧形态应仍可用: {}", resp);
        assert_surrogate_gone(&facade, c, "单 id c").await;

        // ③ {ids:[]} → 必须非成功（禁止静默成功）
        let args = args_of(vec![("ids", json!([]))]);
        assert_eq!(
            facade.flow("processSurrogate/remove", &args).await["code"],
            99999999,
            "{{ids:[]}} 必须报错"
        );

        // ④ {ids:[""]} / 含 null → 必须报错
        for bad in [json!([""]), json!([1, null])] {
            let args = args_of(vec![("ids", bad.clone())]);
            assert_eq!(
                facade.flow("processSurrogate/remove", &args).await["code"],
                99999999,
                "{{ids:{}}} 必须报错",
                bad
            );
        }
    }

    /// issues/96 §4B：processDesign/remove 的 4 种入口形态。
    #[tokio::test]
    async fn test_process_design_remove_ids_shape_matrix() {
        let facade = make_facade();

        // ① {ids:[a,b]}
        let a = save_design(&facade, "design-matrix-a").await;
        let b = save_design(&facade, "design-matrix-b").await;
        let args = args_of(vec![("ids", json!([a, b]))]);
        let resp = facade.flow("processDesign/remove", &args).await;
        assert_eq!(resp["code"], 0, "{{ids:[a,b]}} 删除应成功: {}", resp);
        assert_design_gone(&facade, a, "批量 a").await;
        assert_design_gone(&facade, b, "批量 b").await;

        // ② {id:c}
        let c = save_design(&facade, "design-matrix-c").await;
        let args = args_of(vec![("id", json!(c))]);
        let resp = facade.flow("processDesign/remove", &args).await;
        assert_eq!(resp["code"], 0, "{{id}} 旧形态应仍可用: {}", resp);
        assert_design_gone(&facade, c, "单 id c").await;

        // ③ {ids:[]}
        let args = args_of(vec![("ids", json!([]))]);
        assert_eq!(
            facade.flow("processDesign/remove", &args).await["code"],
            99999999,
            "{{ids:[]}} 必须报错"
        );

        // ④ {ids:[""]} / 含 null
        for bad in [json!([""]), json!([1, null])] {
            let args = args_of(vec![("ids", bad.clone())]);
            assert_eq!(
                facade.flow("processDesign/remove", &args).await["code"],
                99999999,
                "{{ids:{}}} 必须报错",
                bad
            );
        }
    }

    /// issues/96 §4B：processDefine/remove 的 4 种入口形态。
    #[tokio::test]
    async fn test_process_define_remove_ids_shape_matrix() {
        let facade = make_facade();

        // ① {ids:[a,b]}
        let a = save_define(&facade, "define-matrix-a");
        let b = save_define(&facade, "define-matrix-b");
        let args = args_of(vec![("ids", json!([a, b]))]);
        let resp = facade.flow("processDefine/remove", &args).await;
        assert_eq!(resp["code"], 0, "{{ids:[a,b]}} 删除应成功: {}", resp);
        assert_define_gone(&facade, a, "批量 a").await;
        assert_define_gone(&facade, b, "批量 b").await;

        // ② {id:c}
        let c = save_define(&facade, "define-matrix-c");
        let args = args_of(vec![("id", json!(c))]);
        let resp = facade.flow("processDefine/remove", &args).await;
        assert_eq!(resp["code"], 0, "{{id}} 旧形态应仍可用: {}", resp);
        assert_define_gone(&facade, c, "单 id c").await;

        // ③ {ids:[]}
        let args = args_of(vec![("ids", json!([]))]);
        assert_eq!(
            facade.flow("processDefine/remove", &args).await["code"],
            99999999,
            "{{ids:[]}} 必须报错"
        );

        // ④ {ids:[""]} / 含 null
        for bad in [json!([""]), json!([1, null])] {
            let args = args_of(vec![("ids", bad.clone())]);
            assert_eq!(
                facade.flow("processDefine/remove", &args).await["code"],
                99999999,
                "{{ids:{}}} 必须报错",
                bad
            );
        }
    }

    /// issues/96 §4B：processDefine/upAndDown 的 4 种入口形态。
    /// ⚠️ 每条载荷都带 opType：该 action 先校验 state/opType，不带就先撞 state 报错，
    /// ③④ 的"非成功"断言会恒真（失去意义）。state 别名回落已由 test_process_define_up_and_down 覆盖。
    #[tokio::test]
    async fn test_process_define_up_and_down_ids_shape_matrix() {
        let facade = make_facade();

        // ① {ids:[a,b]} + opType → 成功且两条 state 都已变更（upAndDown 不删数据）
        let a = save_define(&facade, "updown-matrix-a");
        let b = save_define(&facade, "updown-matrix-b");
        let args = args_of(vec![("ids", json!([a, b])), ("opType", json!(0))]);
        let resp = facade.flow("processDefine/upAndDown", &args).await;
        assert_eq!(resp["code"], 0, "{{ids:[a,b]}} 停用应成功: {}", resp);
        assert_define_state(&facade, a, 0, "批量 a").await;
        assert_define_state(&facade, b, 0, "批量 b").await;

        // ② {id:c} + opType
        let c = save_define(&facade, "updown-matrix-c");
        let args = args_of(vec![("id", json!(c)), ("opType", json!(0))]);
        let resp = facade.flow("processDefine/upAndDown", &args).await;
        assert_eq!(resp["code"], 0, "{{id}} 旧形态应仍可用: {}", resp);
        assert_define_state(&facade, c, 0, "单 id c").await;

        // ③ {ids:[]} + opType
        let args = args_of(vec![("ids", json!([])), ("opType", json!(0))]);
        assert_eq!(
            facade.flow("processDefine/upAndDown", &args).await["code"],
            99999999,
            "{{ids:[]}} 必须报错"
        );

        // ④ {ids:[""]} / 含 null + opType
        for bad in [json!([""]), json!([c, null])] {
            let args = args_of(vec![("ids", bad.clone()), ("opType", json!(0))]);
            assert_eq!(
                facade.flow("processDefine/upAndDown", &args).await["code"],
                99999999,
                "{{ids:{}}} 必须报错",
                bad
            );
        }
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

        // processDefine/upAndDown with string id
        let mut args = HashMap::new();
        args.insert("id".to_string(), json!(&id_str));
        args.insert("state".to_string(), json!(0));
        let resp = facade.flow("processDefine/upAndDown", &args).await;
        assert_eq!(resp["code"], 0, "upAndDown with string id should work");

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
        // missing processInstanceId → business error
        assert_eq!(resp["code"], 99999999);
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
        // save one design so groups is non-empty object map
        let mut save = HashMap::new();
        save.insert("name".to_string(), json!("list-type-flow"));
        save.insert("displayName".to_string(), json!("LT"));
        save.insert("type".to_string(), json!("approval"));
        let saved = facade.flow("processDesign/save", &save).await;
        assert_eq!(saved["code"], 0);

        let resp = facade.flow("processDesign/listByType", &HashMap::new()).await;
        assert_eq!(resp["code"], 0, "{:?}", resp);
        assert!(resp["data"].is_object(), "listByType must return grouped map, got {:?}", resp["data"]);
        assert!(
            resp["data"].get("approval").is_some(),
            "expected approval group: {:?}",
            resp["data"]
        );
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

    #[tokio::test]
    async fn test_high_light_contract_fields() {
        let facade = make_facade_with_user_provider();
        let mut define = ProcessDefine {
            id: 0,
            name: "hl-flow".into(),
            display_name: "HL".into(),
            define_type: "approval".into(),
            state: 1,
            content: r#"{
                "name":"hl-flow","displayName":"HL","type":"approval",
                "nodes":[
                    {"id":"start","type":"snaker:start","text":{"value":"S"}},
                    {"id":"apply","type":"snaker:task","text":{"value":"A"},
                     "properties":{"assignee":"applicant"}},
                    {"id":"approve","type":"snaker:task","text":{"value":"B"},
                     "properties":{"assignee":"user2"}},
                    {"id":"end","type":"snaker:end","text":{"value":"E"}}
                ],
                "edges":[
                    {"id":"e1","sourceNodeId":"start","targetNodeId":"apply"},
                    {"id":"e2","sourceNodeId":"apply","targetNodeId":"approve"},
                    {"id":"e3","sourceNodeId":"approve","targetNodeId":"end"}
                ]
            }"#
            .as_bytes()
            .to_vec(),
            version: 1,
            create_time: None,
            create_user: None,
            update_time: None,
            update_user: None,
        };
        facade.repo().save_define(&mut define).unwrap();

        let mut args = HashMap::new();
        args.insert("processDefineId".to_string(), json!(define.id));
        args.insert("operator".to_string(), json!("user1"));
        let start = facade.flow("processDefine/startAndExecute", &args).await;
        assert_eq!(start["code"], 0, "{:?}", start);
        let iid = start["data"]["processInstanceId"].as_str().unwrap();

        let mut hl_args = HashMap::new();
        hl_args.insert("id".to_string(), json!(iid));
        let hl = facade.flow("processInstance/highLight", &hl_args).await;
        assert_eq!(hl["code"], 0, "{:?}", hl);
        let data = &hl["data"];
        assert!(data.get("finishedNodes").is_none());
        assert!(data.get("currentNodes").is_none());
        let active = data["activeNodeNames"].as_array().expect("activeNodeNames");
        let history = data["historyNodeNames"].as_array().expect("historyNodeNames");
        let edges = data["historyEdgeNames"].as_array().expect("historyEdgeNames");
        assert!(data["nodeProgress"].is_object());
        assert!(
            active.iter().any(|v| v.as_str() == Some("approve")),
            "active should contain approve: {:?}",
            active
        );
        assert!(
            history.iter().any(|v| v.as_str() == Some("apply"))
                || history.iter().any(|v| v.as_str() == Some("start")),
            "history should contain apply/start: {:?}",
            history
        );
        assert!(!edges.is_empty(), "historyEdgeNames should not be empty: {:?}", edges);
    }

    #[tokio::test]
    async fn test_get_assignee_text_data_shape() {
        let facade = make_facade_with_user_provider();
        let mut define = ProcessDefine {
            id: 0,
            name: "assignee-flow".into(),
            display_name: "AF".into(),
            define_type: "approval".into(),
            state: 1,
            content: r#"{
                "name":"assignee-flow","displayName":"AF","type":"approval",
                "nodes":[
                    {"id":"start","type":"snaker:start","text":{"value":"S"}},
                    {"id":"apply","type":"snaker:task","text":{"value":"申请"},
                     "properties":{"assignee":"applicant"}},
                    {"id":"approve","type":"snaker:task","text":{"value":"审批"},
                     "properties":{"assignee":"user2"}},
                    {"id":"end","type":"snaker:end","text":{"value":"E"}}
                ],
                "edges":[
                    {"id":"e1","sourceNodeId":"start","targetNodeId":"apply"},
                    {"id":"e2","sourceNodeId":"apply","targetNodeId":"approve"},
                    {"id":"e3","sourceNodeId":"approve","targetNodeId":"end"}
                ]
            }"#
            .as_bytes()
            .to_vec(),
            version: 1,
            create_time: None,
            create_user: None,
            update_time: None,
            update_user: None,
        };
        facade.repo().save_define(&mut define).unwrap();
        let mut args = HashMap::new();
        args.insert("processDefineId".to_string(), json!(define.id));
        args.insert("operator".to_string(), json!("user1"));
        let start = facade.flow("processDefine/startAndExecute", &args).await;
        assert_eq!(start["code"], 0, "{:?}", start);
        let iid = start["data"]["processInstanceId"].as_str().unwrap();

        let mut a_args = HashMap::new();
        a_args.insert("processInstanceId".to_string(), json!(iid));
        let resp = facade.flow("processInstance/getAssigneeTextData", &a_args).await;
        assert_eq!(resp["code"], 0, "{:?}", resp);
        let rows = resp["data"].as_array().expect("array");
        assert!(!rows.is_empty());
        assert!(rows[0].get("value").is_some());
        assert!(rows[0].get("label").is_some());
        let label = rows[0]["label"].as_str().unwrap();
        assert!(label.contains(':'), "includeNodeName default true → displayName:actor, got {}", label);
    }

    #[tokio::test]
    async fn test_jump_able_task_name_list_shape() {
        let facade = make_facade_with_user_provider();
        let mut define = ProcessDefine {
            id: 0,
            name: "jump-flow".into(),
            display_name: "JF".into(),
            define_type: "approval".into(),
            state: 1,
            content: r#"{
                "name":"jump-flow","displayName":"JF","type":"approval",
                "nodes":[
                    {"id":"start","type":"snaker:start","text":{"value":"S"}},
                    {"id":"apply","type":"snaker:task","text":{"value":"申请"},
                     "properties":{"assignee":"applicant"}},
                    {"id":"approve","type":"snaker:task","text":{"value":"审批"},
                     "properties":{"assignee":"user2"}},
                    {"id":"end","type":"snaker:end","text":{"value":"E"}}
                ],
                "edges":[
                    {"id":"e1","sourceNodeId":"start","targetNodeId":"apply"},
                    {"id":"e2","sourceNodeId":"apply","targetNodeId":"approve"},
                    {"id":"e3","sourceNodeId":"approve","targetNodeId":"end"}
                ]
            }"#
            .as_bytes()
            .to_vec(),
            version: 1,
            create_time: None,
            create_user: None,
            update_time: None,
            update_user: None,
        };
        facade.repo().save_define(&mut define).unwrap();
        let mut args = HashMap::new();
        args.insert("processDefineId".to_string(), json!(define.id));
        args.insert("operator".to_string(), json!("user1"));
        let start = facade.flow("processDefine/startAndExecute", &args).await;
        assert_eq!(start["code"], 0, "{:?}", start);
        let iid = start["data"]["processInstanceId"].as_str().unwrap();

        let mut j_args = HashMap::new();
        j_args.insert("processInstanceId".to_string(), json!(iid));
        let resp = facade.flow("processTask/jumpAbleTaskNameList", &j_args).await;
        assert_eq!(resp["code"], 0, "{:?}", resp);
        let rows = resp["data"].as_array().expect("array of {label,value}");
        assert!(!rows.is_empty());
        assert!(rows[0].get("label").is_some());
        assert!(rows[0].get("value").is_some());
    }

    #[tokio::test]
    async fn test_task_surrogate_adds_actors() {
        let facade = make_facade_with_user_provider();
        let mut define = ProcessDefine {
            id: 0,
            name: "sg-flow".into(),
            display_name: "SG".into(),
            define_type: "approval".into(),
            state: 1,
            content: r#"{
                "name":"sg-flow","displayName":"SG","type":"approval",
                "nodes":[
                    {"id":"start","type":"snaker:start","text":{"value":"S"}},
                    {"id":"apply","type":"snaker:task","text":{"value":"申请"},
                     "properties":{"assignee":"applicant"}},
                    {"id":"approve","type":"snaker:task","text":{"value":"审批"},
                     "properties":{"assignee":"user2"}},
                    {"id":"end","type":"snaker:end","text":{"value":"E"}}
                ],
                "edges":[
                    {"id":"e1","sourceNodeId":"start","targetNodeId":"apply"},
                    {"id":"e2","sourceNodeId":"apply","targetNodeId":"approve"},
                    {"id":"e3","sourceNodeId":"approve","targetNodeId":"end"}
                ]
            }"#
            .as_bytes()
            .to_vec(),
            version: 1,
            create_time: None,
            create_user: None,
            update_time: None,
            update_user: None,
        };
        facade.repo().save_define(&mut define).unwrap();
        let mut args = HashMap::new();
        args.insert("processDefineId".to_string(), json!(define.id));
        args.insert("operator".to_string(), json!("user1"));
        let start = facade.flow("processDefine/startAndExecute", &args).await;
        assert_eq!(start["code"], 0, "{:?}", start);
        let iid: i64 = start["data"]["processInstanceId"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let doing = facade.repo().find_doing_tasks(iid, &[]).unwrap();
        assert!(!doing.is_empty());
        let task_id = doing[0].task_id;

        let mut s_args = HashMap::new();
        s_args.insert("processTaskId".to_string(), json!(task_id));
        s_args.insert("actorIds".to_string(), json!(["user9", "user8"]));
        let resp = facade.flow("processTask/surrogate", &s_args).await;
        assert_eq!(resp["code"], 0, "{:?}", resp);
        let actors = facade.repo().find_task_actors(task_id).unwrap();
        assert!(actors.contains(&"user9".to_string()), "{:?}", actors);
        assert!(actors.contains(&"user8".to_string()), "{:?}", actors);
    }

    #[tokio::test]
    async fn test_biz_data_requires_meta_table_reader() {
        let (facade, _) = make_facade_with_define();
        let mut args = HashMap::new();
        args.insert("processInstanceId".to_string(), json!(1));
        let resp = facade.flow("processInstance/bizData", &args).await;
        assert_eq!(resp["code"], 99999999);
        // either instance not found or reader not registered — must NOT succeed with vars dump
        assert!(resp.get("data").is_none() || resp["data"].is_null());
    }

    // ─── Action count test ───

    #[tokio::test]
    async fn test_all_45_actions_dispatchable() {
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
            "processInstance/stats/overview", "processInstance/stats/trend",
            "processInstance/stats/group",
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
        assert_eq!(actions.len(), 45, "Should have exactly 45 actions");
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

    /// c9: args_to_flow_data 数组/对象原样透传（L3 S6 回归）。
    /// vben 多选 ApiSelect 提交 JSON 数组 f_ccActors=["<id>"]；旧版兜底分支
    /// 用 v.to_string() 字符串化成 `"[...]"`，抄送人变成字面量。
    #[test]
    fn test_c9_args_array_passthrough() {
        let mut args = HashMap::new();
        args.insert(
            "f_ccActors".to_string(),
            json!(["1711661958608584706", "1686404946814533633"]),
        );
        args.insert("nested".to_string(), json!({"a": 1, "b": "x"}));
        args.insert("reason".to_string(), json!("hello"));
        let fd = args_to_flow_data(&args);
        // 数组 → JsonValue::Array（不是字符串 `"[...]"`）
        match fd.inner().get("f_ccActors") {
            Some(JsonValue::Array(items)) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0].as_str(), Some("1711661958608584706"));
                assert_eq!(items[1].as_str(), Some("1686404946814533633"));
            }
            other => panic!("c9: f_ccActors should be Array, got {:?}", other.is_some()),
        }
        // 对象 → JsonValue::Object
        assert!(matches!(fd.inner().get("nested"), Some(JsonValue::Object(_))));
        // 标量不受影响
        assert_eq!(fd.get_str("reason"), Some("hello"));
    }

    // ─── Stats tests (issues/103) ───

    fn seed_stats_facade() -> JeeflowFacade {
        let repo = Arc::new(MemoryRepository::new());

        let mut design = ProcessDesign {
            id: 1001, name: "leave".into(), display_name: "请假审批".into(),
            design_type: "leave".into(), icon: None, is_deployed: 1,
            remark: None, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_design(&mut design).unwrap();

        let mut inst1 = ProcessInstance {
            instance_id: 0, parent_id: None, define_id: 1001, state: 20,
            parent_node_name: None, business_no: None,
            operator: "user1".into(), expire_time: None,
            variables: FlowData::new(), tasks: vec![],
            create_time: Some("2025-01-10 10:00:00".into()),
            create_user: Some("user1".into()),
            update_time: None, update_user: None,
            define: None,
        };
        repo.save_instance(&mut inst1).unwrap();

        let mut inst2 = ProcessInstance {
            instance_id: 0, parent_id: None, define_id: 1001, state: 10,
            parent_node_name: None, business_no: None,
            operator: "user2".into(), expire_time: None,
            variables: FlowData::new(), tasks: vec![],
            create_time: Some("2025-01-11 10:00:00".into()),
            create_user: Some("user2".into()),
            update_time: None, update_user: None,
            define: None,
        };
        repo.save_instance(&mut inst2).unwrap();

        let mut inst3 = ProcessInstance {
            instance_id: 0, parent_id: None, define_id: 1001, state: 45,
            parent_node_name: None, business_no: None,
            operator: "user3".into(), expire_time: None,
            variables: FlowData::new(), tasks: vec![],
            create_time: Some("2025-01-12 10:00:00".into()),
            create_user: Some("user3".into()),
            update_time: None, update_user: None,
            define: None,
        };
        repo.save_instance(&mut inst3).unwrap();

        let mut task1 = ProcessTask {
            task_id: 0, process_instance_id: inst1.instance_id,
            task_name: "managerApproval".into(), display_name: "经理审批".into(),
            task_type: 0, perform_type: 1, task_state: 20,
            actor_id: Some("approver1".into()), actor_ids: vec![],
            finish_time: Some("2025-01-10 11:00:00".into()),
            expire_time: Some("2025-01-10 18:00:00".into()),
            form_key: None, parent_task_id: None,
            variables: FlowData::new(),
            create_time: Some("2025-01-10 10:00:00".into()),
            create_user: None, update_time: None, update_user: None,
        };
        repo.save_task(&mut task1).unwrap();

        let mut task2 = ProcessTask {
            task_id: 0, process_instance_id: inst1.instance_id,
            task_name: "directorApproval".into(), display_name: "总监审批".into(),
            task_type: 0, perform_type: 0, task_state: 20,
            actor_id: Some("approver1".into()), actor_ids: vec![],
            finish_time: Some("2025-01-10 10:30:00".into()),
            expire_time: None,
            form_key: None, parent_task_id: None,
            variables: FlowData::new(),
            create_time: Some("2025-01-10 10:00:00".into()),
            create_user: None, update_time: None, update_user: None,
        };
        repo.save_task(&mut task2).unwrap();

        let mut task3 = ProcessTask {
            task_id: 0, process_instance_id: inst2.instance_id,
            task_name: "deptApproval".into(), display_name: "部门审批".into(),
            task_type: 0, perform_type: 0, task_state: 10,
            actor_id: None, actor_ids: vec!["approver2".into(), "approver3".into()],
            finish_time: None, expire_time: None,
            form_key: None, parent_task_id: None,
            variables: FlowData::new(),
            create_time: Some("2025-01-11 10:00:00".into()),
            create_user: None, update_time: None, update_user: None,
        };
        repo.save_task(&mut task3).unwrap();

        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));
        JeeflowFacade::new(ctx)
    }

    #[tokio::test]
    async fn test_stats_dispatch_via_flow() {
        let facade = seed_stats_facade();
        for action in &["processInstance/stats/overview", "processInstance/stats/trend", "processInstance/stats/group"] {
            let mut args = HashMap::new();
            if action.ends_with("/trend") {
                // C：trend 的 start/end/granularity 必填
                args.insert("start".to_string(), json!("2025-01-10 00:00:00"));
                args.insert("end".to_string(), json!("2025-01-12 00:00:00"));
                args.insert("granularity".to_string(), json!("day"));
            }
            let resp = facade.flow(action, &args).await;
            assert_eq!(resp["code"], 0, "Action {} should succeed, got: {}", action, resp);
        }
    }

    #[test]
    fn test_stats_overview_empty() {
        let facade = make_facade();
        let resp = facade.stats_overview(&HashMap::new()).unwrap();
        assert_eq!(resp["total"], 0);
        assert_eq!(resp["inProgress"], 0);
        assert_eq!(resp["completed"], 0);
        assert_eq!(resp["rejected"], 0);
        assert_eq!(resp["withdrawn"], 0);
        assert_eq!(resp["suspended"], 0);
        assert_eq!(resp["todayNew"], 0);
        assert_eq!(resp["avgDurationSeconds"], 0);
        assert_eq!(resp["rejectRate"], 0.0);
        assert_eq!(resp["pendingTaskCount"], 0);
        assert_eq!(resp["overdueTaskCount"], 0);
        assert_eq!(resp["countersignRate"], 0.0);
        assert_eq!(resp["onTimeRate"], 0.0);
    }

    #[test]
    fn test_stats_overview_with_data() {
        let facade = seed_stats_facade();
        let resp = facade.stats_overview(&HashMap::new()).unwrap();
        assert_eq!(resp["total"], 3);
        assert_eq!(resp["inProgress"], 1);
        assert_eq!(resp["completed"], 1);
        assert_eq!(resp["rejected"], 1);
        assert_eq!(resp["withdrawn"], 0);
        assert_eq!(resp["suspended"], 0);
        assert_eq!(resp["todayNew"], 0);
        assert_eq!(resp["avgDurationSeconds"], 3600);
        assert_eq!(resp["rejectRate"], 0.5);
        assert_eq!(resp["pendingTaskCount"], 1);
        assert_eq!(resp["overdueTaskCount"], 0);
        assert_eq!(resp["countersignRate"], 0.5);
        assert_eq!(resp["onTimeRate"], 1.0);
    }

    #[test]
    fn test_stats_overview_null_expire() {
        let repo = Arc::new(MemoryRepository::new());
        let mut inst = ProcessInstance {
            instance_id: 0, parent_id: None, define_id: 1, state: 20,
            parent_node_name: None, business_no: None,
            operator: "u1".into(), expire_time: None,
            variables: FlowData::new(), tasks: vec![],
            create_time: Some("2025-01-10 10:00:00".into()),
            create_user: None, update_time: None, update_user: None,
            define: None,
        };
        repo.save_instance(&mut inst).unwrap();
        let mut task = ProcessTask {
            task_id: 0, process_instance_id: inst.instance_id,
            task_name: "t".into(), display_name: "T".into(),
            task_type: 0, perform_type: 0, task_state: 20,
            actor_id: Some("a1".into()), actor_ids: vec![],
            finish_time: Some("2025-01-10 11:00:00".into()),
            expire_time: None,
            form_key: None, parent_task_id: None,
            variables: FlowData::new(),
            create_time: Some("2025-01-10 10:00:00".into()),
            create_user: None, update_time: None, update_user: None,
        };
        repo.save_task(&mut task).unwrap();
        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));
        let facade = JeeflowFacade::new(ctx);
        let resp = facade.stats_overview(&HashMap::new()).unwrap();
        assert_eq!(resp["overdueTaskCount"], 0);
        assert_eq!(resp["onTimeRate"], 0.0);
    }

    #[tokio::test]
    async fn test_stats_trend_invalid_granularity() {
        let facade = seed_stats_facade();
        let mut args = HashMap::new();
        args.insert("granularity".to_string(), json!("abc"));
        let resp = facade.flow("processInstance/stats/trend", &args).await;
        assert_ne!(resp["code"], 0);
    }

    #[tokio::test]
    async fn test_stats_trend_missing_required_params() {
        // C 自证：缺 start / 缺 end → code!=0，不静默回退不限时间
        let facade = seed_stats_facade();
        let mut args = HashMap::new();
        args.insert("granularity".to_string(), json!("day"));
        args.insert("end".to_string(), json!("2025-01-12 00:00:00"));
        let resp = facade.flow("processInstance/stats/trend", &args).await;
        assert_ne!(resp["code"], 0, "missing start should fail");

        let mut args = HashMap::new();
        args.insert("granularity".to_string(), json!("day"));
        args.insert("start".to_string(), json!("2025-01-10 00:00:00"));
        let resp = facade.flow("processInstance/stats/trend", &args).await;
        assert_ne!(resp["code"], 0, "missing end should fail");
    }

    #[test]
    fn test_stats_overview_state_in_respected() {
        // B 自证：非缺省 stateIn 六个计数随动
        let facade = seed_stats_facade();
        let mut args = HashMap::new();
        args.insert("stateIn".to_string(), json!([10]));
        let resp = facade.stats_overview(&args).unwrap();
        assert_eq!(resp["total"], 1);
        assert_eq!(resp["inProgress"], 1);
        assert_eq!(resp["completed"], 0);
        // 种子日期 2025-01-x，非当日 → todayNew 恒 0
        assert_eq!(resp["todayNew"], 0);
    }

    #[test]
    fn test_stats_trend_day_empty() {
        let facade = make_facade();
        let mut args = HashMap::new();
        args.insert("start".to_string(), json!("2025-01-10 00:00:00"));
        args.insert("end".to_string(), json!("2025-01-12 00:00:00"));
        args.insert("granularity".to_string(), json!("day"));
        let resp = facade.stats_trend(&args).unwrap();
        // A：data 本体为裸数组
        let series = resp.as_array().unwrap();
        assert_eq!(series.len(), 3);
        assert_eq!(series[0]["bucket"], "2025-01-10");
        assert_eq!(series[0]["started"], 0);
        assert_eq!(series[0]["finished"], 0);
        assert_eq!(series[1]["bucket"], "2025-01-11");
        assert_eq!(series[2]["bucket"], "2025-01-12");
    }

    #[test]
    fn test_stats_trend_day_with_data() {
        let facade = seed_stats_facade();
        let mut args = HashMap::new();
        args.insert("start".to_string(), json!("2025-01-10 00:00:00"));
        args.insert("end".to_string(), json!("2025-01-12 00:00:00"));
        args.insert("granularity".to_string(), json!("day"));
        let resp = facade.stats_trend(&args).unwrap();
        let series = resp.as_array().unwrap();
        assert_eq!(series.len(), 3);
        assert_eq!(series[0]["bucket"], "2025-01-10");
        assert_eq!(series[0]["started"], 1);
        assert_eq!(series[0]["finished"], 2);
        assert_eq!(series[1]["bucket"], "2025-01-11");
        assert_eq!(series[1]["started"], 1);
        assert_eq!(series[1]["finished"], 0);
        assert_eq!(series[2]["bucket"], "2025-01-12");
        assert_eq!(series[2]["started"], 0);
        assert_eq!(series[2]["finished"], 0);
    }

    #[tokio::test]
    async fn test_stats_group_invalid_dimension() {
        let facade = seed_stats_facade();
        let mut args = HashMap::new();
        args.insert("dimension".to_string(), json!("bogus"));
        let resp = facade.flow("processInstance/stats/group", &args).await;
        assert_ne!(resp["code"], 0);
    }

    #[test]
    fn test_stats_group_empty_state() {
        let facade = make_facade();
        let mut args = HashMap::new();
        args.insert("dimension".to_string(), json!("state"));
        let resp = facade.stats_group(&args).unwrap();
        let rows = resp.as_array().unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_stats_group_all_9_dimensions() {
        let facade = seed_stats_facade();
        for dim in &["state", "define", "category", "approver", "applicant",
                     "node", "stuckNode", "stuckApprover", "durationBucket"] {
            let mut args = HashMap::new();
            args.insert("dimension".to_string(), json!(dim));
            let resp = facade.stats_group(&args).unwrap();
            // A：data 本体为裸数组
            let rows = resp.as_array().unwrap();
            assert!(rows.len() > 0, "Dimension {} should have rows", dim);
            for row in rows {
                assert!(row.get("key").is_some(), "Dimension {} row missing key", dim);
                assert!(row.get("count").is_some(), "Dimension {} row missing count", dim);
            }
        }
    }

    #[test]
    fn test_stats_group_duration_bucket_fixed_order() {
        let facade = seed_stats_facade();
        let mut args = HashMap::new();
        args.insert("dimension".to_string(), json!("durationBucket"));
        let resp = facade.stats_group(&args).unwrap();
        let rows = resp.as_array().unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0]["key"], "sameDay");
        assert_eq!(rows[1]["key"], "1to3d");
        assert_eq!(rows[2]["key"], "3to7d");
        assert_eq!(rows[3]["key"], "over7d");
        assert_eq!(rows[0]["count"], 1);
        assert_eq!(rows[1]["count"], 0);
        assert_eq!(rows[2]["count"], 0);
        assert_eq!(rows[3]["count"], 0);
    }

    #[test]
    fn test_stats_group_define_with_label() {
        let facade = seed_stats_facade();
        let mut args = HashMap::new();
        args.insert("dimension".to_string(), json!("define"));
        let resp = facade.stats_group(&args).unwrap();
        let rows = resp.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["key"], "leave");
        assert_eq!(rows[0]["label"], "请假审批");
        assert_eq!(rows[0]["count"], 3);
        assert_eq!(rows[0]["avgDurationSeconds"], 3600);
    }

    #[test]
    fn test_stats_group_node_with_avg() {
        let facade = seed_stats_facade();
        let mut args = HashMap::new();
        args.insert("dimension".to_string(), json!("node"));
        let resp = facade.stats_group(&args).unwrap();
        let rows = resp.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        let mgr = rows.iter().find(|r| r["key"] == "经理审批").unwrap();
        assert_eq!(mgr["count"], 1);
        assert_eq!(mgr["avgDurationSeconds"], 3600);
        let dir = rows.iter().find(|r| r["key"] == "总监审批").unwrap();
        assert_eq!(dir["count"], 1);
        assert_eq!(dir["avgDurationSeconds"], 1800);
    }
}
