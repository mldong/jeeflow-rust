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
            // 与写库审计列同一时钟出口（issues/120）——此前此处直接吃 chrono::Local，
            // 而同一次响应里的 create_time 走 UTC，同栈两个基准。
            Json::String(current_time_str())
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
    // issues/121 P1：引擎建单必写的控制键不算「任务变量非空」，否则新建任务的 ext
    // 永远不再回退实例变量（issues/82-3 既有契约）。
    if ext.is_empty() || (ext.len() == 1 && ext.contains_key("isFirstTaskNode")) {
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

/// `operator` 取参 + 缺省兜底（issues/129）：对齐 Java 参考实现
/// `toStr(args.get("operator"), "user1")`——门面不感知登录态，缺省走 demo 风格 `user1`
/// （spec 06 §2.4）。空串/全空白同样按缺省处理：否则"没传"会在仓储层被折叠成"不过滤"，
/// 一条不带 operator 的 `processInstance/page` 就能读到别人的实例（线上实测 4 → 25）。
fn operator_arg(args: &HashMap<String, Json>, keys: &[&str]) -> String {
    for k in keys {
        if let Some(v) = arg_str(args, k) {
            let t = v.trim();
            if !t.is_empty() {
                return t.to_string();
            }
        }
    }
    "user1".to_string()
}

/// 硬必填字符串参数：缺失或 trim 后为空串 → 返回携带统一 msg 的 Business 错误。
/// 撤回/转办的 operator、fromActor、toActor 一律走它，严禁缺省回落固定账号（issues/114/115）。
fn require_non_empty(
    args: &HashMap<String, Json>,
    key: &str,
    msg: &str,
) -> JeeflowResult<String> {
    match arg_str(args, key) {
        Some(s) if !s.trim().is_empty() => Ok(s),
        _ => Err(JeeflowError::Business(msg.to_string())),
    }
}

/// 系统代执行（flow.auto）/ 超级管理员（flow.admin）放行——isAllowed 既有约定，撤回/转办共用。
fn is_privileged_operator(operator: &str) -> bool {
    operator.eq_ignore_ascii_case("flow.auto") || operator.eq_ignore_ascii_case("flow.admin")
}

fn arg_i64_or(args: &HashMap<String, Json>, key: &str, default: i64) -> JeeflowResult<i64> {
    Ok(arg_i64(args, key)?.unwrap_or(default))
}

/// 委托 `enabled` 写入侧归一（issues/116 批次 D，判据④的写侧那一半）：
/// - **未传** → `1`（契约 06 §4.5「enabled 否 int 1 启用/0 停用，默认 1」）；
/// - 布尔 `true`/`false` → `1`/`0`（前端开关组件偶发传布尔，不落脏值分支）；
/// - 可解析为整数（`1` / `"1"` / `0` / `"0"` / `2`）→ 原值；
/// - 传了但不可解析（`null` / `"abc"` / `{}` / `[]`）→ **`0` 停用**。
///
/// 修复前两处病：`processSurrogate/save` **根本不读 enabled**（恒落 1，"停用的委托"存进去
/// 就成启用）；`update` 走 `arg_i64` 把脏值判成 `非法id: abc` 直接报错。默认方向按契约
/// canonical 取"脏值停用"（Go `parseSurrogateEnabled` / Java `03474fe` 同侧；
/// C# 回落 1 是相反侧，见 issues/116 §8.4）。读侧判据见
/// `jeeflow_core::surrogate::surrogate_hit`——写侧若把脏值落成 1，读侧再严也白搭。
fn parse_surrogate_enabled(args: &HashMap<String, Json>) -> i32 {
    match args.get("enabled") {
        None | Some(Json::Null) => 1,
        Some(Json::Bool(b)) => i32::from(*b),
        Some(Json::String(s)) => s.trim().parse::<i32>().unwrap_or(0),
        Some(v) => v.as_i64().map(|n| n as i32).unwrap_or(0),
    }
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
///
/// issues/142 B 批 · spec 06-facade.md §2.11「两形同判据」：两条腿只负责**拆形**，
/// 拆完的原始串集合统一交给 [`normalize_actors`]（与抄送侧 §2.10、引擎消费腿
/// `parse_actor_ids`、两仓 `add_task_actor` 写侧同一枚判据，**严禁第二份**）。
///
/// 改前形状（普查实读）：数组腿只做 `filter(|s| !s.is_empty())` ⇒ **不 trim**，
/// `"  "`／`"\t"` 这类纯空白元素存活并真落进 `wf_process_task_actor.actor_id`，
/// 同一次调用里的重复也不折叠；只有逗号串腿才 trim＋丢空——两形两个答案。
/// 现在：逐元素 trim、空串/纯空白丢弃、同次调用折叠、数字元素收成字符串后**照样 trim**；
/// `null` 元素丢弃（不串化成 `"null"`）；反向哨兵——`"0"` 是正常 id，不得被当成空值丢掉。
fn arg_actor_ids(args: &HashMap<String, Json>) -> Vec<String> {
    let raw: Vec<String> = match args.get("actorIds") {
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
            .collect(),
        Some(Json::String(s)) => s.split(',').map(|x| x.to_string()).collect(),
        _ => Vec::new(),
    };
    normalize_actors(&raw)
}

/// 主键类参数**另判一档**（spec 06 §2.11 末段）：`processTaskId` 没给／给了 `0` 或负数
/// ⇒ 响亮报错，不得拿 `''`/`0` 当 id 往下落库（归属值可有可无，主键没给就是调用方写错了）。
/// 错误信封沿用本仓既有的"缺参数"文案（§2.11 硬要求③：不新造错误码/文案）；
/// 空串形态在 [`arg_i64`] 里已经报「非法id: 」，这一支管"整条没给"和"给了 0/负数"两档。
fn require_task_id(args: &HashMap<String, Json>, missing_msg: &str) -> JeeflowResult<i64> {
    let task_id = arg_id(args, &["processTaskId", "id"])?
        .ok_or_else(|| JeeflowError::Business(missing_msg.to_string()))?;
    if task_id <= 0 {
        return Err(JeeflowError::Business(missing_msg.to_string()));
    }
    Ok(task_id)
}

/// 单值归属参数归一后再用（spec 06 §2.11 表第二行：`transfer` 的 `fromActor`/`toActor`
/// 现在各栈只判必填、存的是未 trim 的原值）。必填校验仍走 [`require_non_empty`]
/// （既有"缺参数"信封逐字不动），随后把值交给 §2.10/§2.11 那一枚单点取 trim 后的串 ⇒
/// 落库与归属比较（`operator != from_actor`、`find_task_actors` 命中判定）用的是同一个尺度。
/// 判空一律 `trim().is_empty()`：`"0"` 是正常 id，不得被当成空值丢掉。
fn require_normalized_actor(
    args: &HashMap<String, Json>,
    key: &str,
    msg: &str,
) -> JeeflowResult<String> {
    let raw = require_non_empty(args, key, msg)?;
    normalize_actors(&[raw])
        .into_iter()
        .next()
        .ok_or_else(|| JeeflowError::Business(msg.to_string()))
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
        // 候选源只取 candidateUsers（对齐 Java/Python/Go 参考实现，salvo s15 修复）：
        // assignee 是节点默认处理人（引擎运行时落 task.actor_id），不属于候选池成员——
        // 若把它收进静态候选，candidatePage 会短路 user_search 全量搜索，使「指定
        // 下一节点处理人」弹窗只剩默认处理人、选不到他人（e2e S15 红）。
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

// issues/106：m_ 过滤与分页已整体下推仓储（memory/sqlx 消费 PageQuery.filters），
// 旧的 facade 内存过滤 matches_filter/apply_filters_to_rows 与二次切片 re_paginate 已删除。

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

            // ═══ processTask (10) ═══
            "processTask/todoList" => self.process_task_todo_list(args),
            "processTask/doneList" => self.process_task_done_list(args),
            "processTask/execute" => self.process_task_execute(args).await,
            "processTask/detail" => self.process_task_detail(args),
            "processTask/jumpAbleTaskNameList" => self.process_task_jump_able_task_name_list(args),
            "processTask/candidatePage" => self.process_task_candidate_page(args),
            "processTask/surrogate" => self.process_task_surrogate(args),
            "processTask/addCandidate" => self.process_task_add_candidate(args),
            "processTask/transfer" => self.process_task_transfer(args),
            "processTask/removeTaskActor" => self.process_task_remove_actor(args),
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
            Err(e) => {
                // issues/137 §3-1（spec/06 §2.12）：判别式 `is_foreign_detail`（error.rs 纯函数）
                // 判定 Internal 档＝内部实现细节（驱动／运行时／集成方 provider 原文）⇒ 原文只进
                // 日志（下面这一支），出口 msg 由 `message()` 收敛为固定文案「流程处理失败」；
                // 引擎自己写的契约文案不记内部异常日志、照旧逐字透出（对齐 java 顶层 catch：
                // isForeignDetail ⇒ log SEVERE + INTERNAL_FAILURE_MSG，否则 e.getMessage()）。
                if jeeflow_core::error::is_foreign_detail(&e) {
                    eprintln!(
                        "[jeeflow] action 执行失败: action={} detail={}",
                        action,
                        e.detail()
                    );
                }
                error_response(&e.message())
            }
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
        let mut query = PageQuery::new(page_num, page_size);
        query.filters = parse_m_params(args); // m_ 过滤下推仓储（issues/106）
        let page = self.repo.page_defines(&query)?;
        let rows: Vec<Json> = page.rows.iter().map(define_row_to_json).collect();
        let result = PageResult::new(page.page_num, page.page_size, page.record_count, rows);
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
            // f_nextNodeOperator → tf_nextNodeOperator（若有）。
            // issues/142 B 批 · spec 06 §2.11 表第三行：这一支**原样透传值**（数组形态也要转出去），
            // 拆形与判据收在引擎消费腿 `parse_actor_ids` 那一枚单点里。旧写法 `get_str` 只认字符串
            // ⇒ 前端 UserSelect(multiple) 提交的数组在这里整条被静默丢弃（与普查点名的
            // "消费腿用 get_str＝数组形态整条失效"同一形状，只是长在发起腿上）。
            if let Some(next_op) = flow_data.get("f_nextNodeOperator").cloned() {
                flow_data.insert("tf_nextNodeOperator".to_string(), next_op);
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
        let mut query = PageQuery::new(page_num, page_size);
        query.operator = Some(operator_arg(args, &["operator"]));
        query.filters = parse_m_params(args); // m_ 过滤下推仓储（issues/106）
        let page = self.repo.page_instances(&query)?;
        let rows: Vec<Json> = page.rows.iter().map(instance_row_to_json).collect();
        let result = PageResult::new(page.page_num, page.page_size, page.record_count, rows);
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
            // issues/121 P1：ext.isFirstTaskNode **行上值优先**（引擎建单时写入，历史行同样有效），
            // 缺键（存量行）才回退现算。回退那条带"仅进行中"判定 ⇒ 只够展示，不能当引擎判据。
            let row_first = t.variables.get("isFirstTaskNode").and_then(|v| v.as_bool());
            let is_first = row_first.unwrap_or_else(|| doing
                && first_node
                    .as_ref()
                    .map(|n| n == &t.task_name)
                    .unwrap_or(false));
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
            "ext": flow_data_to_object(&inst.variables), // issues/124：变量唯一对外出口（ext 豁免 camel，键保持下划线）
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
        // issues/114：operator 硬必填——严禁缺省回落 user1（撤回人被静默记成别人，
        // update_user 与审计链一起失真且不报错）。缺失/空串统一 msg「operator 必填」。
        let operator = arg_str(args, "operator")
            .filter(|s| !s.trim().is_empty())
            .ok_or(JeeflowError::Business("operator 必填".into()))?;
        let mut inst = self.repo.find_instance_by_id(id)?
            .ok_or(JeeflowError::InstanceNotFound(id))?;

        // 归属判据（命中任一放行）：发起人 / 任一进行中任务参与者 / flow.auto|admin。
        if !self.can_withdraw(&inst, &operator) {
            return Err(JeeflowError::Business("无权限撤回该流程实例".into()));
        }

        // issues/134 案 A：实例状态守卫在聚合根 `ProcessInstance::withdraw` 内
        // （state≠10 ⇒ 内部码 20010009，出口 99999999 ＋ 固定文案「流程实例非进行中，无法撤回」），
        // 本调用排在**一切改写与落库之前**——被拒时实例 state、任务行、update_user 一行都不动，
        // 下方两次 update 也走不到（对齐 Java `JeeflowFacade.withdraw` 的 canWithdraw → inst.withdraw
        // → updateInstance 序；鉴权两判据仍排在它之前，顺序未动）。
        // 先快照"改写前哪些行是进行中"，让成功路径的 update_user 回写判据与改前逐字一致。
        let doing_ids: Vec<i64> = inst.tasks.iter()
            .filter(|t| t.task_state == TaskState::Doing.code())
            .map(|t| t.task_id)
            .collect();
        inst.withdraw()?;

        // 实例 update_user 回写为撤回人。
        inst.update_user = Some(operator.clone());
        // 进行中任务的 update_user 同样回写（withdraw 只翻 Doing→Withdraw，
        // 已完成(20)/已终止(40) 的行不受影响，其 update_user 保持不动）。
        for task in &mut inst.tasks {
            if doing_ids.contains(&task.task_id) {
                task.update_user = Some(operator.clone());
            }
        }
        // 级联落库判据取 Withdraw(30)：inst.withdraw() 已在内存里把进行中任务翻成 30，
        // 此处若仍判 Doing 则该循环永不命中，任务会留在库里 10（继续出现在待办）。
        for task in &inst.tasks {
            if task.task_state == TaskState::Withdraw.code() {
                self.repo.update_task(task)?;
            }
        }
        self.repo.update_instance(&inst)?;
        // 8 TASK_WITHDRAW（规范 11 §11.3 码 8／08 场景 34）：被撤任务行更新完成 **且** 实例
        // state=30 落库之后 fire，**每轮撤回只 fire 一次**（不逐任务）。
        // 被 issues/134 的状态守卫拒掉时上面 `inst.withdraw()?` 已提前 return，这支不会发。
        self.engine.notify_task_withdraw(inst.instance_id, &operator);
        Ok(Json::Null)
    }

    /// 撤回归属判据（issues/114，命中任一即放行）：
    /// 1. operator = 实例发起人（wf_process_instance.operator）；
    /// 2. operator 是该实例任一「进行中」任务的参与者（wf_process_task_actor.actor_id）；
    /// 3. operator ∈ {flow.auto, flow.admin}。
    /// ⚠️ 判据 1 不可复用 `is_allowed`：引擎 is_allowed 只判「在不在该任务 actorIds」+
    /// auto/admin 放行，从不查发起人，撤回路径必须显式补这一支。
    fn can_withdraw(&self, inst: &ProcessInstance, operator: &str) -> bool {
        if is_privileged_operator(operator) {
            return true;
        }
        if operator == inst.operator {
            return true;
        }
        // 以参与者表为准（聚合副本可能滞后于加签/转办的增量写入）
        if let Ok(doing) = self.repo.find_doing_tasks(inst.instance_id, &[]) {
            for t in &doing {
                if let Ok(actors) = self.repo.find_task_actors(t.task_id) {
                    if actors.iter().any(|a| a == operator) {
                        return true;
                    }
                }
            }
        }
        false
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
                let ctx = self.engine.context();
                node_progress = build_node_progress(
                    &model,
                    &history_tasks,
                    ctx.user_provider.as_ref(),
                );
                if let Some(start) = model.get_start() {
                    let mut visited = std::collections::HashSet::new();
                    // 决策边的求值原料与两侧参考实现同形：实例变量（go/java 的 `vars`）
                    // ＋ 历史任务（前置任务变量在那一层并进去），SPI 从 ServiceContext 取。
                    collect_high_light_path(
                        &model,
                        &start.id,
                        &active,
                        &mut history,
                        &mut edges,
                        &mut visited,
                        inst.variables.inner(),
                        &history_tasks,
                        ctx.expression_evaluator.as_ref(),
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
        // issues/154②（spec/06 §4.6 approvalRecord 口径②）：视图端点**不因实例 id 不存在报错**——
        // 这里不再查实例（旧的 `InstanceNotFound` 早退与那份实例变量一起删掉），实例不存在时
        // `find_history_tasks` 自然返回零行 ⇒ 出口空数组。基准＝java `approvalRecord`
        // （它压根没有 findInstanceById 这一步）。
        let tasks = self.repo.find_history_tasks(id)?;
        let records: Vec<Json> = tasks
            .iter()
            .map(|t| {
                // issues/154③（spec 口径③）：任务变量为空 ⇒ `ext` 出**空对象**，不回落实例变量。
                // 旧注释写的「对齐 Go taskRowToMap」是谎——go 的 `approvalRecord` 直接取
                // `t.Variables`，没有回退那一支；回退会让"任务变量"与"实例变量"两种语义共用一个键，
                // 前端分辨不出办理人来源。
                let ext = flow_data_to_object(&t.variables);
                json!({
                    // issues/154①④：行主键在引擎自己的出口就字符串化，**不得**指望门面那层
                    // `stringify_ids` 兜底（transform_output 是门面级兜底，宿主改动即失效；19 位
                    // 雪花出 number 会被 JS 截精度，同 issues/75/92 那族坑）。
                    // java 同形状＝`vo.put("id", String.valueOf(t.getTaskId()))` 作首列。
                    "id": Json::String(t.task_id.to_string()),
                    "task_name": t.task_name,
                    "display_name": t.display_name,
                    "task_type": t.task_type,
                    "perform_type": t.perform_type,
                    "task_state": t.task_state,
                    "operator": t.actor_id,
                    "finish_time": t.finish_time,
                    "ext": ext, // issues/124：variable 原串出口下线
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
        // issues/141 G10「空不创建行」（spec 06 §2.10）：手动腿与引擎两条腿走同一个归一判据
        // （`jeeflow_core::model::normalize_cc_actors`，与 `parse_cc_actors` 同一条腿）——
        // 逗号串与数组两形的空串/纯空白/空元素一律丢弃，落库与比较值取 trim 后的串。
        // 丢完为空 ⇒ 不建行、不 fire，并且**与上面那条"空集合＝actorIds 缺失"同档**
        // （spec §2.10 实现要求③：沿用既有文案，不新造错误码/错误语义）。
        let actors = normalize_cc_actors(&arg_actor_ids(args));
        if actors.is_empty() {
            return Err(JeeflowError::Business("actorIds 缺失".into()));
        }
        // issues/141 G2 写侧判重＝幂等空操作（spec 06 §4）：手动腿与引擎两条腿同一条判据
        // （spec §11.7「三条入口共用一支」）——已有 cc 行的 (实例, 人) 跳过，不新增行、
        // 不重置未读、不更新原行时间；只有**实际新建的子集**拿去 fire。
        let created = self.repo.create_cc_instance_if_absent(id, &operator, &actors)?;
        // CC_CREATE（4）——**手动支与引擎支归一**（规范 11 §11.2 原则 1／§11.7，issues/132 §4.5
        // rust 条目）：本路径与引擎的发起 `f_ccActors`、办理 `tf_ccActors` 两条腿共用
        // `JeeflowEngineImpl::notify_cc_create` 这唯一收口，逐抄送人在 **cc 行落库之后** fire。
        // "新增了一条抄送记录"这个事实成立就发，路径不进事件名（Java 旧状"手动不 fire"是缺不是基准）。
        // 入参＝实际新建子集（issues/141 G2）：重复抄送没发生"创建"⇒ 不发码 4，子集为空整支不 fire。
        if !created.is_empty() {
            self.engine.notify_cc_create(id, &created);
        }
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
        // issues/154④（spec/06 §4.6 ccList 参数表）：分页默认值统一 `pageNum=1 / pageSize=10`，
        // 与其它分页 action 同口径——rust 旧默认 20 已判分叉。只改这一条 action，
        // 其余 action 的 20 不在本轮立法范围内。
        let page_num = arg_i64_or(args, "pageNum", 1)?;
        let page_size = arg_i64_or(args, "pageSize", 10)?;
        let mut query = PageQuery::new(page_num, page_size);
        query.operator = Some(operator_arg(args, &["operator"]));
        query.filters = parse_m_params(args); // m_ 过滤下推仓储（issues/106）
        let page = self.repo.page_cc_instances(&query)?;
        let rows: Vec<Json> = page.rows.iter().map(instance_row_to_json).collect();
        let result = PageResult::new(page.page_num, page.page_size, page.record_count, rows);
        Ok(serde_json::to_value(page_to_json(&result)).unwrap())
    }

    // ═══════════════════════════════════════════════════════
    // processTask actions (10)
    // ═══════════════════════════════════════════════════════

    fn process_task_todo_list(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let page_num = arg_i64_or(args, "pageNum", 1)?;
        let page_size = arg_i64_or(args, "pageSize", 20)?;
        let mut query = PageQuery::new(page_num, page_size);
        // UI 注入 operator；兼容 userId（curl/旧客户端）
        query.operator = Some(operator_arg(args, &["operator", "userId"]));
        query.filters = parse_m_params(args); // m_ 过滤下推仓储（issues/106）
        let page = self.repo.page_todo_tasks(&query)?;
        let rows: Vec<Json> = page.rows.iter().map(task_row_to_json).collect();
        let result = PageResult::new(page.page_num, page.page_size, page.record_count, rows);
        Ok(serde_json::to_value(page_to_json(&result)).unwrap())
    }

    fn process_task_done_list(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let page_num = arg_i64_or(args, "pageNum", 1)?;
        let page_size = arg_i64_or(args, "pageSize", 20)?;
        let mut query = PageQuery::new(page_num, page_size);
        query.operator = Some(operator_arg(args, &["operator"]));
        query.filters = parse_m_params(args); // m_ 过滤下推仓储（issues/106）
        let page = self.repo.page_done_tasks(&query)?;
        let rows: Vec<Json> = page.rows.iter().map(task_row_to_json).collect();
        let result = PageResult::new(page.page_num, page.page_size, page.record_count, rows);
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
        // 同上：行上值优先、缺键才回退现算（先取键再被出口覆写，否则会丢掉"缺键"这一事实）
        let t_row_first = task.variables.get("isFirstTaskNode").and_then(|v| v.as_bool());
        let mut t_ext = flow_data_to_object(&task.variables);
        if let Some(obj) = t_ext.as_object_mut() {
            obj.insert("isFirstTaskNode".into(), Json::Bool(t_row_first.unwrap_or(false)));
        }

        if let Some(inst) = self.repo.find_instance_by_id(task.process_instance_id)? {
            if let Some(def) = self.repo.find_define_by_id(inst.define_id)? {
                let json_object = parse_graph(&def.content_str());
                if let Some(obj) = vo.as_object_mut() {
                    obj.insert("json_object".into(), json_object.clone().unwrap_or(Json::Null));
                }
                if t_row_first.is_none() {
                    // 存量行没有落库标记 ⇒ 回退现算（仅进行中口径）
                    let is_first = doing
                        && json_object
                            .as_ref()
                            .and_then(first_task_node_id)
                            .map(|n| n == task.task_name)
                            .unwrap_or(false);
                    if let Some(obj) = t_ext.as_object_mut() {
                        obj.insert("isFirstTaskNode".into(), Json::Bool(is_first));
                    }
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
        // 主键另判一档（§2.11 末段）＋ actorIds 两形同判据（§2.11 表第一行，判据＝arg_actor_ids
        // 里的 normalize_actors）；丢完为空与本仓既有的"缺参数"档逐字同判（硬要求③）。
        let task_id = require_task_id(args, "processTaskId/actorIds 缺失")?;
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

    /// 转办（issues/115/116）：摘原参与人 + 换新人，沿用同一 taskId，三件留痕。
    /// 与 surrogate（加签=只追加）语义相反——本 action 会摘走 fromActor 那一行。
    fn process_task_transfer(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        let task_id = require_task_id(args, "缺少processTaskId参数")?;
        // operator 硬必填（缺失/空串统一 msg），fromActor/toActor 同口径。
        // issues/142 B 批 · spec 06 §2.11 表第二行：三个归属值**归一后再用**——必填档的既有
        // "缺参数"信封逐字不动，值取 trim 后的串，于是下面的权限判定（operator==fromActor）、
        // 参与者命中判定（find_task_actors）与 tf_transferHistory 留痕用的是同一个尺度；
        // 落库侧再由两仓 `add_task_actor` 兜第二层（§2.11 硬要求①「两层都挡」）。
        let operator = require_normalized_actor(args, "operator", "operator 必填")?;
        let from_actor = require_normalized_actor(args, "fromActor", "fromActor 必填")?;
        let to_actor = require_normalized_actor(args, "toActor", "toActor 必填")?;
        let reason = arg_str(args, "reason").unwrap_or_default();

        let mut task = self
            .repo
            .find_task_by_id(task_id)?
            .ok_or(JeeflowError::Business("任务不存在".into()))?;

        // 归属判据：只能转自己那一条待办（operator==fromActor），flow.auto|admin 例外。
        if !is_privileged_operator(&operator) && operator != from_actor {
            return Err(JeeflowError::Business("无权限转办该任务".into()));
        }
        // 前置态：仅进行中（DOING=10）任务可转办。
        if task.task_state != TaskState::Doing.code() {
            return Err(JeeflowError::Business("任务非进行中，不可转办".into()));
        }
        // 以参与者表为判据（聚合副本可能滞后于加签/转办的增量写入）。
        let actors = self.repo.find_task_actors(task_id)?;
        if !actors.iter().any(|a| a == &from_actor) {
            return Err(JeeflowError::Business("原办理人不是该任务参与人".into()));
        }
        if actors.iter().any(|a| a == &to_actor) {
            return Err(JeeflowError::Business("目标人已是该任务参与人".into()));
        }

        // 摘原人（仅 fromActor 一行）+ 加新人（同一 taskId，不新建任务）。
        self.repo.remove_task_actor(task_id, &[from_actor.clone()])?;
        self.repo.add_task_actor(task_id, &[to_actor.clone()])?;

        // 留痕三件（契约第 4 条，缺一不可）——严禁覆写 actor_id/operator 列（进行中任务该列恒无值）。
        let mut vars = task.variables.clone();
        // ① 追加式跨跳账本 tf_transferHistory（六键固定 camelCase，只追加不覆盖）。
        let mut history: Vec<JsonValue> = match vars.get("tf_transferHistory") {
            Some(JsonValue::Array(arr)) => arr.clone(),
            _ => Vec::new(),
        };
        let time_str = current_time_str();
        let hop = JsonValue::Object(vec![
            ("submitType".to_string(), JsonValue::Number(7.0)),
            ("fromActor".to_string(), JsonValue::Str(from_actor.clone())),
            ("toActor".to_string(), JsonValue::Str(to_actor.clone())),
            ("reason".to_string(), JsonValue::Str(reason.clone())),
            ("time".to_string(), JsonValue::Str(time_str.clone())),
            ("operator".to_string(), JsonValue::Str(operator.clone())),
        ]);
        history.push(hop);
        vars.insert("tf_transferHistory".to_string(), JsonValue::Array(history));
        // ② 当前槽位 submitType=7（B 办结时由 args 覆盖，属预期）+ 单跳便捷键。
        vars.insert_i64("submitType", 7);
        vars.insert_str("tf_transferTo", to_actor.clone());
        vars.insert_str("tf_transferReason", reason.clone());
        // ③ 末跳可读文案写 tf_approvalComment（前端既有读取位）。
        let transfer_text = if reason.is_empty() {
            format!("{} 转办给 {}", from_actor, to_actor)
        } else {
            format!("{} 转办给 {}（{}）", from_actor, to_actor, reason)
        };
        vars.insert_str("tf_approvalComment", transfer_text);

        task.variables = vars;
        task.update_user = Some(operator.clone());
        // 同步为摘/加之后的最新参与者集合：内存仓 page_todo 按副本 actor_ids 过滤，
        // 不同步则待办不会真正挪到 B（sqlx update_task 不写 actor 表，无副作用）。
        task.actor_ids = self.repo.find_task_actors(task_id)?;
        self.repo.update_task(&task)?;
        // 7 TASK_TRANSFER（规范 11 §11.3 码 7／08 场景 34）：参与者被替换（remove+add）
        // 且留痕行落库之后 fire，sourceId＝taskId，载荷五键
        // instanceId / taskId / fromActor / toActor / operator。
        self.engine.notify_task_transfer(
            task.process_instance_id, task_id, &from_actor, &to_actor, &operator);
        Ok(Json::Null)
    }

    /// 摘除参与人（issues/115 残留 · 门面第 **47** 个 action，spec 06-facade.md
    /// §processTask/removeTaskActor）。SPI 侧 [`ProcessRepository::remove_task_actor`] 从第一天起
    /// 就是必选方法、两仓都实现，只是没上门面——本 action 补的就是这一段（摘人过去只能靠
    /// `transfer`，而它是"摘 A **并**加 B"）。
    ///
    /// 三个兄弟 action 的分工（写清楚，免得后来人把三条混用）：
    /// - `processTask/surrogate`／`addCandidate` ＝ **只加**（原人保留可办）；
    /// - `processTask/transfer` ＝ **换人**（摘 A 加 B，submitType=7 ＋ tf_transferHistory 三件留痕
    ///   ＋ fire 码 7）；
    /// - 本 action ＝ **只摘不加、零留痕、不 fire 事件**：删掉 `actorIds` 在本任务的参与者行，
    ///   不新建任务、不写任何任务变量、不覆写任务 `actor_id`/`operator` 列。
    ///   「不发事件」是定稿判据（issues/132 §11.3 事件集里**没有**"摘除参与人"这一码，
    ///   码 7 `TASK_TRANSFER` 的语义是"参与者被**替换**"，只摘不加套它就是凭空造出一条
    ///   根本没发生的转办事实；要立法先开 issue）。
    ///
    /// 守卫次序（逐栈一致，spec 同节钉死，门禁按 msg 断言，不接受本栈自行排序）：
    /// `operator 必填` → `processTaskId/actorIds 缺失` → `任务不存在` → `无权限摘除该任务参与人`
    /// → `任务非进行中，不可摘除参与人` → `至少需保留一名参与人` → 落库。
    fn process_task_remove_actor(&self, args: &HashMap<String, Json>) -> JeeflowResult<Json> {
        // ① operator **硬必填且先判**（issues/114/115 同 transfer 口径，严禁缺省回落 user1）：
        //    参数全缺时若先报主键缺失，会把鉴权缺口藏进"缺参数"报错里。归一（trim）在入口就做，
        //    于是下面的归属比较与被摘集合是同一个尺度（§2.11，本栈单点 require_normalized_actor）。
        let operator = require_normalized_actor(args, "operator", "operator 必填")?;
        // ② 主键档与归属值档分得很清楚（§2.11「主键类参数另判一档」）：`require_task_id` ＋
        //    `arg_actor_ids` 都是 surrogate/addCandidate 已在用的那两枚腿，"缺参数"文案与它们
        //    逐字同一条（同族同文案，不另造）。两条都不落库，空串元素也绝不会被喂进 DELETE
        //    （归一单点丢空 ⇒ 历史 actor_id='' 脏行因此安全）。
        // 主键**空串/纯空白/JSON null** 归「缺参数」档（spec 语义 8：缺键/空串/纯空白/0/负数
        // 同一句逐字文案；java `toLong("")` 得 null、go 的 taskIDArg、php 的 normalizeActor 本来
        // 都落这一档）。本栈 `arg_i64` 的既有形状是把空串一并折进「非法id: 」（兄弟 action 同款），
        // 故在调用 `require_task_id` **之前**先把空值档截出来判。
        // ⚠️ 只截空值：给了但不是数字（"abc"）仍走本栈既有「非法id」——spec 明文不作跨栈统一
        // 那一档，伪装成"参数没传"会让调用方看不出自己传错了类型。
        let pk_blank = match args.get("processTaskId") {
            Some(Json::String(s)) => s.trim().is_empty(),
            Some(Json::Null) => true,
            _ => false,
        };
        if pk_blank {
            return Err(JeeflowError::Business("processTaskId/actorIds 缺失".into()));
        }
        let task_id = require_task_id(args, "processTaskId/actorIds 缺失")?;
        let actors = arg_actor_ids(args);
        if actors.is_empty() {
            return Err(JeeflowError::Business("processTaskId/actorIds 缺失".into()));
        }
        let task = self
            .repo
            .find_task_by_id(task_id)?
            .ok_or(JeeflowError::Business("任务不存在".into()))?;
        // ③ 归属判据同 transfer：被摘集合必须含操作人本人（**按归一值比**），flow.auto|flow.admin
        //    例外。transfer 能"摘 A 加 B"是因为 A 就是操作人本人，本 action 同理不得成为
        //    借道摘他人的口子。哨兵生效边界同 transfer 那条注（真实超管不命中，超管可操作性
        //    归集成层权限码）。
        if !is_privileged_operator(&operator) && !actors.iter().any(|a| a == &operator) {
            return Err(JeeflowError::Business("无权限摘除该任务参与人".into()));
        }
        // ④ 前置态：仅进行中（DOING=10）任务可摘。已办结/撤回/废弃任务的历史参与人行是
        //    approvalRecord 的取证依据（它读全状态任务行），摘它等于改写审批历史。
        if task.task_state != TaskState::Doing.code() {
            return Err(JeeflowError::Business("任务非进行中，不可摘除参与人".into()));
        }
        // 以参与者表为判据（聚合副本 `task.actor_ids` 可能滞后于加签/转办的增量写入，与 transfer 同源）。
        let current = self.repo.find_task_actors(task_id)?;
        // 语义 6「匹配取归一值、DELETE 取行上的原值」（§2.11 硬要求②的**删除腿**）：
        // 库里的行可能是修复前落下的未 trim 原值 `" leader "`，入参 `leader` 必须**判成同一个人
        // 并真删掉它**——所以命中用归一形、`to_delete` 收的是**那一行的原值**。反面形状＝拿归一值
        // 去 DELETE：判成同一人却一条没删，门面报成功而被摘的人待办还在（**假成功**，go 栈实测到）。
        // 语义 5「不得摘空」的下限按**能办单的人数**算：`remaining` 只数"归一后非空"的行——
        // 历史 `actor_id=''`/纯空白脏行谁也办不了，拿它撑起下限等于让"摘空"伪装成成功，
        // 而摘空会造出**无人可办又无法撤回重派的死单**，比"配错表达式落 NULL"更难恢复。
        let mut to_delete: Vec<String> = Vec::new();
        let mut remaining: usize = 0;
        for row in &current {
            // 归一仍复用本栈那一枚单点（§2.11「严禁第二份」）：空串/纯空白 ⇒ 归一后无元素，
            // 既不匹配也不计人（脏行不算一个人）。
            let normalized = normalize_actors(std::slice::from_ref(row));
            let Some(name) = normalized.first() else {
                continue;
            };
            if actors.contains(name) {
                to_delete.push(row.clone());
            } else {
                remaining += 1;
            }
        }
        // 判据是**集合差**（当前参与者 − 归一后的 actorIds），不是"入参条数"——
        // actorIds 里混进非参与者 id 也绕不过这一条（spec 语义 5 第二句）。
        if !to_delete.is_empty() && remaining == 0 {
            return Err(JeeflowError::Business("至少需保留一名参与人".into()));
        }
        // 语义 7「幂等」：非参与者静默忽略；一个都没命中 ⇒ 空操作、照样成功信封（前端双点、
        // 集成层重放第二次不再报错）。要"人不在任务里就报错"请用 transfer。
        if !to_delete.is_empty() {
            self.repo.remove_task_actor(task_id, &to_delete)?;
        }
        // 语义 2：不置 submitType、不写 tf_* 变量、不覆写 actor_id/operator 列、不 fire 事件
        // ——本函数从头到尾没有 `update_task`／`notify_*` 这两条腿，"零留痕零事件"由构造成立。
        // data → null（spec 同节：前端消费面 surrogate/addCandidate 的调用点也不读 data）。
        Ok(Json::Null)
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
        let mut query = PageQuery::new(page_num, page_size);
        query.filters = parse_m_params(args); // m_ 过滤下推仓储（issues/106）
        let page = ext.page_designs(&query)?;
        let rows: Vec<Json> = page.rows.iter().map(design_to_json).collect();
        let result = PageResult::new(page.page_num, page.page_size, page.record_count, rows);
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
        let mut query = PageQuery::new(arg_i64_or(args, "pageNum", 1)?, arg_i64_or(args, "pageSize", 20)?);
        // issues/152 ②：`operator` 在本栈是「我的委托」的归属通道（与 process_instance_page :1128 逐字同形），
        // 走 §2.5 归一（缺键／空串／全空白 ⇒ user1）。修前这里既不注入也不下推 ⇒ "只看自己授出的委托"
        // 全靠集成壳注入 operator，换宿主／直调 SPI 就退化成全库台账（spec 06 §2.5 表 + §4.5 归属不变式）。
        query.operator = Some(operator_arg(args, &["operator"]));
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
            // issues/152 ③：改前是 `arg_str_or(args, "operator", "")`——缺省/空白落**空串**，
            // 而空串行是「死行」：get_surrogate 的 `WHERE operator = ?` 永不命中它，
            // 台账里看得见、待办永远不并人（比报错更难查）。本栈早有 [`operator_arg`]（§2.5 归一：
            // 缺键／空串／全空白 ⇒ user1），只是这条腿漏用了。spec 06 §4.5「新建时授权人默认取操作人」。
            operator: operator_arg(args, &["operator"]),
            surrogate: arg_str_or(args, "surrogate", ""),
            start_time: arg_str(args, "startTime"),
            end_time: arg_str(args, "endTime"),
            enabled: parse_surrogate_enabled(args),
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
            // issues/152 ③ 的 update 腿（对齐 java `applySurrogateFields`）：授权人只有**非空显式值**才覆盖，
            // 缺键／空串／全空白一律保留原授权人——空白档若覆写进去同样造出「死行」（§2.5 空串＝缺键同档）。
            operator: match arg_str(args, "operator") {
                Some(raw) => {
                    let t = raw.trim();
                    if t.is_empty() { sg.operator.clone() } else { t.to_string() }
                }
                None => sg.operator.clone(),
            },
            start_time: arg_str(args, "startTime").or(sg.start_time),
            end_time: arg_str(args, "endTime").or(sg.end_time),
            // enabled：未传保持原值，传了走写侧归一（脏值 → 0 停用，见 parse_surrogate_enabled）。
            // 原先用 arg_i64，脏值（"abc"）会被判成「非法id: abc」整条 update 报错。
            enabled: if args.contains_key("enabled") {
                parse_surrogate_enabled(args)
            } else {
                sg.enabled
            },
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
///
/// 决策节点的带表达式出边（spec/06 §4.6 highLight 义务 2／issues/153）：**先求值再过滤**——
/// 表达式为 false 的那条分支未实际执行，边名与目标节点都不收。此前实现是「带 expr 的出边整条
/// `continue` 丢弃」（等于恒 false），那是分叉不是基准；「不求值全量收集」同样违反本条。
/// 逐字对照 java `collectPath` 的
/// `node instanceof DecisionModel && isNotEmpty(tm.getExpr()) && !evalDecisionExpr(...) → continue`。
#[allow(clippy::too_many_arguments)]
fn collect_high_light_path(
    model: &jeeflow_core::parser::ProcessModel,
    node_id: &str,
    active: &[String],
    history: &mut Vec<String>,
    edges: &mut Vec<String>,
    visited: &mut std::collections::HashSet<String>,
    instance_vars: &HashMap<String, JsonValue>,
    history_tasks: &[ProcessTask],
    evaluator: Option<&Arc<dyn ExpressionEvaluator>>,
) {
    if visited.contains(node_id) {
        return;
    }
    visited.insert(node_id.to_string());
    let is_decision = model
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .map(|n| n.node_type == jeeflow_core::parser::NodeType::Decision)
        .unwrap_or(false);
    for edge in model.get_output_edges(node_id) {
        // 决策档：空表达式不过滤（java `StringUtils.isNotEmpty(tm.getExpr())` 同一判据）
        if is_decision {
            if let Some(expr) = edge.expr() {
                if !expr.is_empty()
                    && !eval_decision_expr(
                        model,
                        node_id,
                        &expr,
                        instance_vars,
                        history_tasks,
                        evaluator,
                    )
                {
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
        collect_high_light_path(
            model,
            tid,
            active,
            history,
            edges,
            visited,
            instance_vars,
            history_tasks,
            evaluator,
        );
    }
}

/// 决策出边表达式求值（spec/06 §4.6 highLight 义务 2；基准＝java `evalDecisionExpr`
/// ＋ go `evalDecisionExpr`）：args ＝ 实例变量 ∪ 决策节点**前置任务**（输入边第一个源节点）
/// 的任务变量——与引擎运行时 `DecisionModel.exec` 同一份原料。
///
/// ⚠ **降级档**：`IExpressionEvaluator` **未注册**时整档判 false（java 的
/// `if (evaluator == null) return false;` 那一支）。spec 只允许"未注册"这一种判 false 的缺省，
/// 它是缺省保护而不是常态——注册了 SPI 就必须真求值，旧注释「无表达式引擎则保守跳过（对齐 Java
/// evaluator==null → false）」把降级档当成了唯一路径，掩盖了"注册了也不求值"的分叉。
///
/// 判 true 的形状也与两侧逐字一致：只有求值结果**恰为布尔 true** 才算走过（java
/// `Boolean.TRUE.equals(...)`／go `b, _ := result.(bool)`），求值报错、返回数字或字符串
/// 一律判 false（不套用引擎运行时 `evaluate_expression` 的数字/字符串宽松折算——那两条腿不同档）。
fn eval_decision_expr(
    model: &jeeflow_core::parser::ProcessModel,
    decision_id: &str,
    expr: &str,
    instance_vars: &HashMap<String, JsonValue>,
    history_tasks: &[ProcessTask],
    evaluator: Option<&Arc<dyn ExpressionEvaluator>>,
) -> bool {
    let Some(evaluator) = evaluator else {
        return false; // 降级档：SPI 未注册（见上方注释），整档判 false
    };
    let mut args: HashMap<String, JsonValue> = instance_vars.clone();
    if let Some(input) = model.get_input_edges(decision_id).first() {
        if !input.source_node_id.is_empty() {
            if let Some(t) = history_tasks
                .iter()
                .find(|t| t.task_name == input.source_node_id)
            {
                for (k, v) in t.variables.inner() {
                    args.insert(k.clone(), v.clone());
                }
            }
        }
    }
    matches!(evaluator.eval(expr, &args), Ok(JsonValue::Bool(true)))
}

/// 对齐 Java/Go buildNodeProgress（会签成员进度；动态参与人无成员则跳过）
fn build_node_progress(
    model: &jeeflow_core::parser::ProcessModel,
    tasks: &[ProcessTask],
    user_provider: Option<&Arc<dyn UserProvider>>,
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
        // issues/131：名册现在是任务变量里的**数组**（java ProcessInstance.java:257 也是 List）。
        // 逗号串那一支有意不兜——存量兼容不做（owner 2026-09-28）。
        members = ts[0].variables
            .get(&format!("operatorList_{}", name))
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();
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
                // spec/06 §4.6 highLight 义务 3（issues/153）：`name` 必须经 IUserProvider 解析
                // realName，**只有查不到才允许空串**。此前是硬编码 `""`——SPI 注册了但这一路没接，
                // 正是义务 3 点名的那种违反（前端拿不到姓名，只剩降级显示 id）。
                let mut m = json!({"id": uid, "name": resolve_user_name(user_provider, uid)});
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

/// 成员姓名解析（spec/06 §4.6 highLight 义务 3；基准＝java `resolveUserName`）：
/// 走 `IUserProvider::get_user` 取 realName。返回空串的**只有**这四档，与 java 逐字同判：
/// SPI 未注册 / 查无此人 / realName 为空 / provider 报错——前端对这些降级显示 id。
/// 调用姿势照本栈既有那一支（`process_task_candidate_page` 里 `ctx.user_provider`），不另开一套。
fn resolve_user_name(user_provider: Option<&Arc<dyn UserProvider>>, user_id: &str) -> String {
    let Some(up) = user_provider else {
        return String::new();
    };
    match up.get_user(user_id) {
        Ok(Some(info)) if !info.real_name.is_empty() => info.real_name,
        _ => String::new(),
    }
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
                // end 含端（对齐内置线 create_time < date_add(end, interval 1 second)）
                if t > e { return false; }
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
                // end 含端（对齐内置线 finish_time < date_add(end, interval 1 second)）
                if let Some(e) = end { if ft > e { return false; } }
                true
            } else { false }
        } else { false }
    }).cloned().collect()
}

/// 统计口径的"现在"——与引擎时间串**同一出口**（issues/120）。
///
/// 此前这两处各取一次 `chrono::Local::now()`，而任务/实例的时间列走
/// `jeeflow_core::clock::current_time_str()`：宿主注入非本地钟（或没注入、引擎回落 UTC）时，
/// "最近 30 天"的范围与"今日新增"的当日边界会跟数据基准错开一个时区 —— 表现就是
/// 统计图/今日新增漏掉凌晨或深夜的单。解析失败才回落 Local，保证不因此 panic。
fn stats_now() -> chrono::NaiveDateTime {
    chrono::NaiveDateTime::parse_from_str(&jeeflow_core::clock::current_time_str(), "%Y-%m-%d %H:%M:%S")
        .unwrap_or_else(|_| chrono::Local::now().naive_local())
}

fn stats_enumerate_buckets(
    start: Option<chrono::NaiveDateTime>,
    end: Option<chrono::NaiveDateTime>,
    granularity: &str,
) -> Vec<String> {
    let now = stats_now();
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
        let now = stats_now();
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

    /// 固定返回 3 人的 user_search 桩（candidatePage 全量搜索判据用）
    struct TestUserSearchProvider;
    impl UserSearchProvider for TestUserSearchProvider {
        fn page(&self, query: &PageQuery) -> JeeflowResult<PageResult<HashMap<String, JsonValue>>> {
            let mut all = Vec::new();
            for id in ["user1", "user2", "user3"] {
                let mut m = HashMap::new();
                m.insert("id".into(), JsonValue::Str(id.into()));
                m.insert("userId".into(), JsonValue::Str(id.into()));
                m.insert("realName".into(), JsonValue::Str(id.into()));
                all.push(m);
            }
            let pn = query.page_num.max(1);
            let ps = query.page_size.max(1);
            let start = ((pn - 1) * ps) as usize;
            let end = (start + ps as usize).min(all.len());
            let rows = if start < all.len() { all[start..end].to_vec() } else { Vec::new() };
            Ok(PageResult::new(pn, ps, all.len() as i64, rows))
        }
        fn find_by_id(&self, user_id: &str) -> JeeflowResult<Option<HashMap<String, JsonValue>>> {
            let mut m = HashMap::new();
            m.insert("id".into(), JsonValue::Str(user_id.into()));
            m.insert("userId".into(), JsonValue::Str(user_id.into()));
            m.insert("realName".into(), JsonValue::Str(user_id.into()));
            Ok(Some(m))
        }
    }

    fn make_facade_with_user_search() -> JeeflowFacade {
        let repo = Arc::new(MemoryRepository::new());
        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
            .with_user_provider(Arc::new(TestUserProvider))
            .with_user_search_provider(Arc::new(TestUserSearchProvider))
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

    /// issues/113 正向：撤回后原进行中任务必须**落库**为 30（WITHDRAW）。
    /// 改前级联循环判的是 Doing，而 inst.withdraw() 已在内存把任务翻成 30 → 循环永不命中，
    /// 库里任务停在 10，撤回的单子继续留在办理人待办里。
    #[tokio::test]
    async fn test_instance_withdraw_persists_task_state_30() {
        let facade = make_facade();

        let mut a1 = HashMap::new();
        a1.insert("name".to_string(), json!("withdraw-persist-flow"));
        a1.insert("displayName".to_string(), json!("Withdraw Persist Flow"));
        let r1 = facade.flow("processDesign/save", &a1).await;
        assert_eq!(r1["code"], 0, "save failed: {:?}", r1);
        let design_id = r1["data"]["id"].as_str().unwrap().parse::<i64>().unwrap();

        let mut a2 = HashMap::new();
        a2.insert("id".to_string(), json!(design_id));
        a2.insert(
            "content".to_string(),
            json!(r#"{
                "name":"withdraw-persist-flow","displayName":"Withdraw Persist Flow","type":"approval",
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
            }"#),
        );
        assert_eq!(facade.flow("processDesign/updateDefine", &a2).await["code"], 0);
        let mut a3 = HashMap::new();
        a3.insert("id".to_string(), json!(design_id));
        assert_eq!(facade.flow("processDesign/deploy", &a3).await["code"], 0);

        let mut a4 = HashMap::new();
        a4.insert("name".to_string(), json!("withdraw-persist-flow"));
        a4.insert("operator".to_string(), json!("applicant"));
        let r4 = facade.flow("processDefine/startAndExecute", &a4).await;
        assert_eq!(r4["code"], 0, "startAndExecute failed: {:?}", r4);
        let inst_id: i64 = r4["data"]["processInstanceId"].as_str().unwrap().parse().unwrap();

        let doing_before = facade.repo().find_doing_tasks(inst_id, &[]).unwrap();
        assert_eq!(doing_before.len(), 1, "approve 应为唯一进行中任务");
        let task_id = doing_before[0].task_id;
        assert_eq!(doing_before[0].task_state, TaskState::Doing.code());

        let mut a5 = HashMap::new();
        a5.insert("id".to_string(), json!(inst_id));
        // issues/114：operator 硬必填，撤回人=发起人 applicant（命中归属判据 1）。
        a5.insert("operator".to_string(), json!("applicant"));
        let r5 = facade.flow("processInstance/withdraw", &a5).await;
        assert_eq!(r5["code"], 0, "withdraw failed: {:?}", r5);

        let stored = facade.repo().find_task_by_id(task_id).unwrap().expect("task exists");
        assert_eq!(
            stored.task_state,
            TaskState::Withdraw.code(),
            "issues/113：撤回后任务须落库 30，实测库里仍是 {}",
            stored.task_state
        );
        assert!(
            facade.repo().find_doing_tasks(inst_id, &[]).unwrap().is_empty(),
            "撤回后不应再有进行中任务"
        );
        assert_eq!(
            facade.repo().find_instance_by_id(inst_id).unwrap().unwrap().state,
            InstanceState::Withdraw.code()
        );

        // 待办列表按人复查：撤回的单子不再出现在 user2 待办里
        let mut a6 = HashMap::new();
        a6.insert("operator".to_string(), json!("user2"));
        let r6 = facade.flow("processTask/todoList", &a6).await;
        assert_eq!(
            r6["data"]["rows"].as_array().unwrap().iter()
                .filter(|t| t["processInstanceId"].as_str() == Some(&inst_id.to_string()))
                .count(),
            0
        );
    }

    // ─── processInstance action tests ───

    #[tokio::test]
    async fn test_process_instance_page() {
        let facade = make_facade();
        let resp = facade.flow("processInstance/page", &HashMap::new()).await;
        assert_eq!(resp["code"], 0);
        assert!(resp["data"]["rows"].is_array());
    }

    /// issues/129 夹具：user1 与 user2 **各**一条实例 + 各自的待办/已办 + 抄送。
    /// 必须存在"别人的行"——空仓上"不传 operator == 传 user1"恒真，那种夹具等于没测。
    fn seed_operator_facade() -> (JeeflowFacade, Arc<MemoryRepository>) {
        let repo = Arc::new(MemoryRepository::new());
        let mut design = ProcessDesign {
            id: 2001, name: "op129".into(), display_name: "operator 兜底夹具".into(),
            design_type: "op129".into(), icon: None, is_deployed: 1, remark: None,
            create_time: None, create_user: None, update_time: None, update_user: None,
        };
        repo.save_design(&mut design).unwrap();
        let mut ids = Vec::new();
        for who in ["user1", "user2"] {
            let mut inst = ProcessInstance {
                instance_id: 0, parent_id: None, define_id: 2001, state: 10,
                parent_node_name: None, business_no: None,
                operator: who.into(), expire_time: None,
                variables: FlowData::new(), tasks: vec![],
                create_time: Some("2026-01-10 10:00:00".into()),
                create_user: Some(who.into()),
                update_time: None, update_user: None,
                define: None,
            };
            repo.save_instance(&mut inst).unwrap();
            ids.push(inst.instance_id);
            // 待办：actor_ids 含本人；已办：actor_id = 本人且 state=20
            let mut todo = ProcessTask {
                task_id: 0, process_instance_id: inst.instance_id,
                task_name: "a".into(), display_name: "待办".into(),
                task_type: 0, perform_type: 0, task_state: 10,
                actor_id: Some(who.into()), actor_ids: vec![who.into()],
                finish_time: None, expire_time: None,
                form_key: None, parent_task_id: None,
                variables: FlowData::new(),
                create_time: Some("2026-01-10 10:00:00".into()),
                create_user: None, update_time: None, update_user: None,
            };
            repo.save_task(&mut todo).unwrap();
            let mut done = ProcessTask {
                task_id: 0, process_instance_id: inst.instance_id,
                task_name: "b".into(), display_name: "已办".into(),
                task_type: 0, perform_type: 0, task_state: 20,
                actor_id: Some(who.into()), actor_ids: vec![],
                finish_time: Some("2026-01-10 11:00:00".into()), expire_time: None,
                form_key: None, parent_task_id: None,
                variables: FlowData::new(),
                create_time: Some("2026-01-10 10:00:00".into()),
                create_user: None, update_time: None, update_user: None,
            };
            repo.save_task(&mut done).unwrap();
            // 抄送也给每人一条：否则 ccList 在空 cc 表上"不传==传 user1==0"恒真，等于没测
            repo.create_cc_instance(inst.instance_id, "user1", &[who.to_string()]).unwrap();
        }
        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(200000)));
        (JeeflowFacade::new(ctx), repo)
    }

    async fn count_of(facade: &JeeflowFacade, action: &str, operator: Option<&str>) -> i64 {
        let mut args: HashMap<String, Json> = HashMap::new();
        if let Some(o) = operator {
            args.insert("operator".to_string(), json!(o));
        }
        let resp = facade.flow(action, &args).await;
        assert_eq!(resp["code"], 0, "{} => {}", action, resp);
        resp["data"]["recordCount"].as_i64().unwrap_or(-1)
    }

    /// 正向：不传 operator 与传 `user1` 同答案（缺省兜底，对齐 Java）；
    /// 负向：不存在的用户必须 0 行；**回归红线**：不传绝不能等于"全库"（修复前 page 是 2、todo 是 2）。
    #[tokio::test]
    async fn test_operator_absent_does_not_scan_all() {
        for action in [
            "processInstance/page",
            "processTask/todoList",
            "processTask/doneList",
            "processInstance/ccList",
        ] {
            let (facade, _repo) = seed_operator_facade();
            let none = count_of(&facade, action, None).await;
            let mine = count_of(&facade, action, Some("user1")).await;
            let ghost = count_of(&facade, action, Some("__nobody__")).await;
            assert_eq!(none, mine, "{}: 不传 operator 与传 user1 不同 ⇒ 缺省没兜住", action);
            assert_eq!(ghost, 0, "{}: 不存在的用户仍拿到行 ⇒ operator 过滤未生效", action);
            assert!(none <= 1, "{}: 不传 operator 拿到 {} 行 ⇒ 空值被折叠成读全库（期望 ≤1）", action, none);
        }
    }

    /// 绕开门面直连仓储：`operator=None` 必须空页（自定义 SPI 仓储不走门面兜底，
    /// 只补门面不补仓储的话，这条会红——两处都得堵）。内存仓与 SQL 仓同判据。
    #[test]
    fn test_repository_empty_operator_returns_empty_page() {
        let (_facade, repo) = seed_operator_facade();
        let q = PageQuery::new(1, 20); // operator 字段默认 None
        assert_eq!(repo.page_instances(&q).unwrap().record_count, 0, "page_instances 空 operator 不得读全库");
        assert_eq!(repo.page_todo_tasks(&q).unwrap().record_count, 0, "page_todo_tasks 空 operator 不得读全库");
        assert_eq!(repo.page_cc_instances(&q).unwrap().record_count, 0, "page_cc_instances 空 operator 不得读全库");
        // 同一夹具下传了人就必须有行（防"永远返回空页"这种把泄漏改成失联的假修法）
        let mut q2 = PageQuery::new(1, 20);
        q2.operator = Some("user1".to_string());
        assert_eq!(repo.page_instances(&q2).unwrap().record_count, 1, "user1 应有 1 条实例");
        assert_eq!(repo.page_todo_tasks(&q2).unwrap().record_count, 1, "user1 应有 1 条待办");
        assert_eq!(repo.page_cc_instances(&q2).unwrap().record_count, 1, "user1 应有 1 条抄送");
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

    // ─── s15 回归：candidatePage 静态候选源不含 assignee（对齐 Java/Python/Go）───
    // 后继节点仅配置 assignee（默认处理人）而无 candidateUsers 时，candidatePage 的
    // 静态候选必须为空——否则短路 user_search 全量搜索，「指定下一节点处理人」弹窗
    // 只剩默认处理人、选不到他人（e2e S15 红）。此处以 user_search 全量返回非空
    // 作为"已落到用户搜索"的判据：修复前会短路返回 [assignee]（rows 仅 1 行 user2）。
    #[tokio::test]
    async fn test_candidate_page_assignee_only_falls_back_to_user_search() {
        let facade = make_facade_with_user_search();

        let mut args = HashMap::new();
        args.insert("name".to_string(), json!("s15-flow"));
        args.insert("displayName".to_string(), json!("S15 Flow"));
        let resp = facade.flow("processDesign/save", &args).await;
        assert_eq!(resp["code"], 0, "save failed: {:?}", resp);
        let design_id = resp["data"]["id"].as_str().unwrap().parse::<i64>().unwrap();

        // t1→t2 两个 task 节点；t2 仅 assignee 无 candidateUsers（复刻 e2e L3请假申请
        // leave_approve→gm_approve：lina 办 leave_approve，后继 gm_approve 只有默认处理人）
        let flow_json = r#"{
            "name":"s15-flow","displayName":"S15 Flow","type":"approval",
            "nodes":[
                {"id":"start","type":"snaker:start","text":{"value":"Start"}},
                {"id":"apply","type":"snaker:task","text":{"value":"Apply"},
                 "properties":{"assignee":"applicant"}},
                {"id":"t1","type":"snaker:task","text":{"value":"T1"},
                 "properties":{"assignee":"user1"}},
                {"id":"t2","type":"snaker:task","text":{"value":"T2"},
                 "properties":{"assignee":"user2"}},
                {"id":"end","type":"snaker:end","text":{"value":"End"}}
            ],
            "edges":[
                {"id":"e1","sourceNodeId":"start","targetNodeId":"apply"},
                {"id":"e2","sourceNodeId":"apply","targetNodeId":"t1"},
                {"id":"e3","sourceNodeId":"t1","targetNodeId":"t2"},
                {"id":"e4","sourceNodeId":"t2","targetNodeId":"end"}
            ]
        }"#;
        let mut args2 = HashMap::new();
        args2.insert("id".to_string(), json!(design_id));
        args2.insert("content".to_string(), json!(flow_json));
        assert_eq!(facade.flow("processDesign/updateDefine", &args2).await["code"], 0);

        let mut args3 = HashMap::new();
        args3.insert("id".to_string(), json!(design_id));
        assert_eq!(facade.flow("processDesign/deploy", &args3).await["code"], 0);

        let mut args4 = HashMap::new();
        args4.insert("name".to_string(), json!("s15-flow"));
        args4.insert("operator".to_string(), json!("applicant"));
        let resp4 = facade.flow("processDefine/startAndExecute", &args4).await;
        assert_eq!(resp4["code"], 0, "startAndExecute failed: {:?}", resp4);

        // apply 自动完成 → 当前 DOING=t1（user1）。查 user1 待办里的 t1
        let mut args5 = HashMap::new();
        args5.insert("operator".to_string(), json!("user1"));
        let resp5 = facade.flow("processTask/todoList", &args5).await;
        let rows5 = resp5["data"]["rows"].as_array().unwrap();
        assert_eq!(rows5.len(), 1, "user1 待办应为 t1: {:?}", rows5);
        let task_id: i64 = rows5[0]["id"].as_str().unwrap().parse().unwrap();

        // t1 的后继 t2 仅 assignee 无 candidateUsers：
        //   修复前 → 静态候选=[user2] 短路，rows 仅 1 行（bug）
        //   修复后 → 静态候选空 → 落 user_search 全量 3 人（对齐 Java/Python/Go）
        let mut args6 = HashMap::new();
        args6.insert("processTaskId".to_string(), json!(task_id));
        let resp6 = facade.flow("processTask/candidatePage", &args6).await;
        assert_eq!(resp6["code"], 0, "candidatePage failed: {:?}", resp6);
        let rows = resp6["data"]["rows"].as_array().unwrap();
        assert_eq!(
            rows.len(),
            3,
            "assignee-only 后继节点应落到 user_search 全量搜索（非短路 [assignee]）: {:?}",
            rows
        );
        let ids: Vec<&str> = rows.iter().filter_map(|r| r["id"].as_str()).collect();
        assert!(ids.contains(&"user2") && ids.len() > 1, "应含 assignee 之外的候选: {:?}", ids);
    }

    // ─── s15 回归②：tf_nextNodeOperator 数组形态必须生效（对齐 Python _resolve_actors）───
    // 前端「指定下一节点处理人」UserSelect(multiple) 提交的是**数组**，经 args_to_flow_data
    // 存成 JsonValue::Array。旧 resolve_assignee Priority 1 只用 get_str（只认字符串）→
    // 数组取不到 → 落到 assignee 字面量（默认处理人）→ 指定不生效（e2e S15 红：指定刘洋后
    // gm_approve 仍是 chenhong）。本测试用数组形态断言下一节点落到指定人而非默认 assignee。
    #[tokio::test]
    async fn test_execute_next_node_operator_array_applies_to_next_task() {
        let facade = make_facade_with_user_provider();

        let mut args = HashMap::new();
        args.insert("name".to_string(), json!("s15b-flow"));
        args.insert("displayName".to_string(), json!("S15b Flow"));
        let resp = facade.flow("processDesign/save", &args).await;
        assert_eq!(resp["code"], 0, "save failed: {:?}", resp);
        let design_id = resp["data"]["id"].as_str().unwrap().parse::<i64>().unwrap();

        // t1(assignee=user1) → t2(assignee=user2)：execute t1 时用数组指定 t2 给 user3
        let flow_json = r#"{
            "name":"s15b-flow","displayName":"S15b Flow","type":"approval",
            "nodes":[
                {"id":"start","type":"snaker:start","text":{"value":"Start"}},
                {"id":"apply","type":"snaker:task","text":{"value":"Apply"},
                 "properties":{"assignee":"applicant"}},
                {"id":"t1","type":"snaker:task","text":{"value":"T1"},
                 "properties":{"assignee":"user1"}},
                {"id":"t2","type":"snaker:task","text":{"value":"T2"},
                 "properties":{"assignee":"user2"}},
                {"id":"end","type":"snaker:end","text":{"value":"End"}}
            ],
            "edges":[
                {"id":"e1","sourceNodeId":"start","targetNodeId":"apply"},
                {"id":"e2","sourceNodeId":"apply","targetNodeId":"t1"},
                {"id":"e3","sourceNodeId":"t1","targetNodeId":"t2"},
                {"id":"e4","sourceNodeId":"t2","targetNodeId":"end"}
            ]
        }"#;
        let mut a2 = HashMap::new();
        a2.insert("id".to_string(), json!(design_id));
        a2.insert("content".to_string(), json!(flow_json));
        assert_eq!(facade.flow("processDesign/updateDefine", &a2).await["code"], 0);
        let mut a3 = HashMap::new();
        a3.insert("id".to_string(), json!(design_id));
        assert_eq!(facade.flow("processDesign/deploy", &a3).await["code"], 0);
        let mut a4 = HashMap::new();
        a4.insert("name".to_string(), json!("s15b-flow"));
        a4.insert("operator".to_string(), json!("applicant"));
        assert_eq!(facade.flow("processDefine/startAndExecute", &a4).await["code"], 0);

        // user1 办 t1，指定下一节点处理人 = 数组 ["user3"]（模拟 UserSelect multiple 提交）
        let mut a5 = HashMap::new();
        a5.insert("operator".to_string(), json!("user1"));
        let r5 = facade.flow("processTask/todoList", &a5).await;
        let t1_id: i64 = r5["data"]["rows"][0]["id"].as_str().unwrap().parse().unwrap();
        let mut a6 = HashMap::new();
        a6.insert("processTaskId".to_string(), json!(t1_id));
        a6.insert("operator".to_string(), json!("user1"));
        a6.insert("submitType".to_string(), json!(1));
        a6.insert("tf_nextNodeOperator".to_string(), json!(["user3"]));
        assert_eq!(facade.flow("processTask/execute", &a6).await["code"], 0);

        // t2 应落到 user3（指定人）而非默认 assignee user2
        let mut a7 = HashMap::new();
        a7.insert("operator".to_string(), json!("user3"));
        let r7 = facade.flow("processTask/todoList", &a7).await;
        let user3_rows = r7["data"]["rows"].as_array().unwrap();
        assert_eq!(user3_rows.len(), 1, "t2 应落到指定人 user3 处: {:?}", user3_rows);
        // 默认处理人 user2 不应再持有 t2
        let mut a8 = HashMap::new();
        a8.insert("operator".to_string(), json!("user2"));
        let r8 = facade.flow("processTask/todoList", &a8).await;
        assert!(r8["data"]["rows"].as_array().unwrap().is_empty(),
            "t2 不应仍是默认处理人 user2（数组 tf_nextNodeOperator 未生效）");
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

    // ─── issues/153·154（spec/06 §4.6）：highLight 三条义务 + approvalRecord 四条口径 ───

    /// 极简数值求值桩（形状照 java 测试桩 `TestExpressionEvaluator.evalSimple` 的数值档）：
    /// 先把上下文里的数值变量替换进表达式，再按 `>= <= != == > <` 比一次，答案出**布尔**。
    /// 注册它 ⇒ `IExpressionEvaluator` 处在"已注册"档，highLight 必须走真求值，
    /// 不得再落进"未注册 ⇒ 整档判 false"的降级档（spec/06 §4.6 义务 2）。
    struct TestExprEvaluator;

    impl ExpressionEvaluator for TestExprEvaluator {
        fn eval(
            &self,
            expression: &str,
            context: &HashMap<String, JsonValue>,
        ) -> JeeflowResult<JsonValue> {
            Ok(JsonValue::Bool(eval_numeric(expression, context)))
        }
    }

    fn eval_numeric(expr: &str, ctx: &HashMap<String, JsonValue>) -> bool {
        let mut s = expr.trim().to_string();
        // 键排序后替换：避免 HashMap 随机序让"短键吃掉长键"的结果在一次跑一次不跑之间漂
        let mut keys: Vec<&String> = ctx.keys().collect();
        keys.sort();
        for k in keys {
            if let Some(JsonValue::Number(n)) = ctx.get(k) {
                let v = if *n == (*n as i64) as f64 {
                    format!("{}", *n as i64)
                } else {
                    n.to_string()
                };
                s = s.replace(k.as_str(), &v);
            }
        }
        // 两字符档必须排在一字符档前面，否则 ">=" 会被 ">" 先截走
        for (op, ord) in [(">=", 0), ("<=", 1), ("!=", 2), ("==", 3), (">", 4), ("<", 5)] {
            if let Some(pos) = s.find(op) {
                let left = s[..pos].trim().parse::<f64>();
                let right = s[pos + op.len()..].trim().parse::<f64>();
                return match (left, right) {
                    (Ok(a), Ok(b)) => match ord {
                        0 => a >= b,
                        1 => a <= b,
                        2 => a != b,
                        3 => a == b,
                        4 => a > b,
                        _ => a < b,
                    },
                    _ => false,
                };
            }
        }
        false
    }

    /// 只认 leader 一人的 IUserProvider：照出义务 3 的两档——接上了出真名，**查不到**才允许空串。
    struct NameProvider;

    impl UserProvider for NameProvider {
        fn get_user(&self, user_id: &str) -> JeeflowResult<Option<UserInfo>> {
            if user_id == "leader" {
                return Ok(Some(UserInfo {
                    user_id: user_id.into(),
                    real_name: "组长张三".into(),
                    dept_id: "dept1".into(),
                    dept_name: "研发部".into(),
                    post_id: "post1".into(),
                    post_name: "组长".into(),
                }));
            }
            Ok(None)
        }
    }

    fn make_facade_with_expr_and_names() -> JeeflowFacade {
        let repo = Arc::new(MemoryRepository::new());
        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
            .with_user_provider(Arc::new(NameProvider))
            .with_expression_evaluator(Arc::new(TestExprEvaluator))
            .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));
        JeeflowFacade::new(ctx)
    }

    /// 读仓内共享夹具（八语言同一份，编辑源在 jeeflow-java，本仓 flows/ 是镜像副本）——
    /// 只读不新增：新增流程 JSON 会被发版漂移门禁判红。
    /// 走 manifest 相对路径而不借 `jeeflow_core::flowsdir`：那个模块在 core 那边是
    /// `#[cfg(any(test, feature = "dev-flows"))]` 门控的，门面 crate 不开该 feature
    /// （为一条测试去改 crate 依赖面不划算），而镜像刷新由 core 自己的用例与发版门禁保证。
    fn load_shared_flow(name: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../flows")
            .join(format!("{name}.json"));
        std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("读取共享夹具失败 {}: {e}", path.display()))
    }

    /// 决策流程的 historyEdgeNames ＝ **恰为**求值为 true 的边集合（spec/06 §4.6 义务 2）。
    /// 用例形状照 java `JeeflowFacadeTest.testHighLightFiltersDecisionBranch`（:1178-1215）：
    /// amount=500 ⇒ e4（amount<=1000 → task3）走过，e3（amount>1000 → task2）未走。
    #[tokio::test]
    async fn test_i153_high_light_edges_are_exactly_the_true_decision_branches() {
        let facade = make_facade_with_expr_and_names();
        let content = load_shared_flow("03-decision-expr");
        let mut define = ProcessDefine {
            id: 0,
            name: "decision-expr".into(),
            display_name: "决策表达式流程".into(),
            define_type: "approval".into(),
            state: 1,
            content: content.as_bytes().to_vec(),
            version: 1,
            create_time: None,
            create_user: Some("applicant".into()),
            update_time: None,
            update_user: None,
        };
        facade.repo().save_define(&mut define).unwrap();

        // amount=500 发起（startAndExecute 自动办完 apply）
        let mut start_args = HashMap::new();
        start_args.insert("processDefineId".to_string(), json!(define.id));
        start_args.insert("operator".to_string(), json!("applicant"));
        start_args.insert("amount".to_string(), json!(500));
        let started = facade
            .flow("processInstance/startAndExecute", &start_args)
            .await;
        assert_eq!(started["code"], 0, "发起失败: {:?}", started);
        let iid = started["data"]["processInstanceId"]
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap();

        // 推进：task1(leader) → decision1 求值 → task3(director) → end
        for (node, actor) in [("task1", "leader"), ("task3", "director")] {
            let doing = facade.repo().find_doing_tasks(iid, &[]).unwrap();
            let task = doing.iter().find(|t| t.task_name == node).unwrap_or_else(|| {
                panic!(
                    "{node} 应为进行中任务，实得 {:?}",
                    doing.iter().map(|t| &t.task_name).collect::<Vec<_>>()
                )
            });
            let mut exec_args = HashMap::new();
            exec_args.insert("processTaskId".to_string(), json!(task.task_id));
            exec_args.insert("operator".to_string(), json!(actor));
            exec_args.insert("submitType".to_string(), json!(1));
            let resp = facade.flow("processTask/execute", &exec_args).await;
            assert_eq!(resp["code"], 0, "办理 {node} 失败: {:?}", resp);
        }

        let mut hl_args = HashMap::new();
        hl_args.insert("id".to_string(), json!(iid));
        let hl = facade.flow("processInstance/highLight", &hl_args).await;
        assert_eq!(hl["code"], 0, "{:?}", hl);

        let edges: Vec<&str> = hl["data"]["historyEdgeNames"]
            .as_array()
            .expect("historyEdgeNames")
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        let mut sorted_edges = edges.clone();
        sorted_edges.sort();
        // 走过的边＝主干 e0/e_apply_1/e2 ＋求值为 true 的 e4 ＋ e4 之后的 e6；
        // 恒 true（不求值全收）会多出 e3/e5，恒 false（旧"整条丢弃"）会缺 e4/e6 ⇒ 两侧都判红
        assert_eq!(
            sorted_edges,
            vec!["e0", "e2", "e4", "e6", "e_apply_1"],
            "historyEdgeNames 必须恰为求值为 true 的边集合（spec/06 §4.6 义务 2），实得 {:?}",
            edges
        );

        let nodes: Vec<&str> = hl["data"]["historyNodeNames"]
            .as_array()
            .expect("historyNodeNames")
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert!(nodes.contains(&"task3"), "走过的 task3 必须高亮: {:?}", nodes);
        assert!(!nodes.contains(&"task2"), "未走分支的 task2 不得高亮: {:?}", nodes);
    }

    /// nodeProgress 成员 `name` 经 IUserProvider 解析（spec/06 §4.6 义务 3）：
    /// 查得到 ⇒ 出 realName；**只有查不到**才允许空串（前端降级显示 id）。
    #[tokio::test]
    async fn test_i153_high_light_node_progress_resolves_member_name() {
        let facade = make_facade_with_expr_and_names();
        let mut define = ProcessDefine {
            id: 0,
            name: "np-flow".into(),
            display_name: "NP".into(),
            define_type: "approval".into(),
            state: 1,
            content: r#"{
                "name":"np-flow","displayName":"NP","type":"approval",
                "nodes":[
                    {"id":"start","type":"snaker:start","text":{"value":"S"}},
                    {"id":"apply","type":"snaker:task","text":{"value":"A"},
                     "properties":{"assignee":"applicant"}},
                    {"id":"approve","type":"snaker:task","text":{"value":"B"},
                     "properties":{"assignee":"leader","performType":1}},
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

        let mut inst = ProcessInstance {
            instance_id: 0,
            parent_id: None,
            define_id: define.id,
            state: 10,
            parent_node_name: None,
            business_no: None,
            operator: "applicant".into(),
            expire_time: None,
            variables: FlowData::new(),
            tasks: vec![],
            create_time: Some("2026-10-09 10:00:00".into()),
            create_user: Some("applicant".into()),
            update_time: None,
            update_user: None,
            define: None,
        };
        facade.repo().save_instance(&mut inst).unwrap();

        // approve 节点两行：leader 已完成（IUserProvider 查得到）＋ ghost 进行中（查无此人）。
        // 行序不进判据（内存仓 HashMap 无序），成员一律按 id 取。
        for (actor, state, finish) in
            [("leader", 20, Some("2026-10-09 11:00:00")), ("ghost", 10, None)]
        {
            let mut task = ProcessTask {
                task_id: 0,
                process_instance_id: inst.instance_id,
                task_name: "approve".into(),
                display_name: "审批".into(),
                task_type: 0,
                perform_type: 1,
                task_state: state,
                actor_id: Some(actor.into()),
                actor_ids: vec![actor.into()],
                finish_time: finish.map(|s| s.to_string()),
                expire_time: None,
                form_key: None,
                parent_task_id: None,
                variables: FlowData::new(),
                create_time: Some("2026-10-09 10:00:00".into()),
                create_user: Some(actor.into()),
                update_time: None,
                update_user: None,
            };
            facade.repo().save_task(&mut task).unwrap();
        }

        let mut hl_args = HashMap::new();
        hl_args.insert("id".to_string(), json!(inst.instance_id));
        let hl = facade.flow("processInstance/highLight", &hl_args).await;
        assert_eq!(hl["code"], 0, "{:?}", hl);
        let members = hl["data"]["nodeProgress"]["approve"]["members"]
            .as_array()
            .unwrap_or_else(|| panic!("approve 成员进度应存在，实得 {:?}", hl["data"]["nodeProgress"]));
        let member = |uid: &str| -> &Json {
            members
                .iter()
                .find(|m| m["id"].as_str() == Some(uid))
                .unwrap_or_else(|| panic!("成员 {uid} 应存在，实得 {members:?}"))
        };
        assert_eq!(
            member("leader")["name"],
            json!("组长张三"),
            "name 必须经 IUserProvider 解析 realName（spec/06 §4.6 义务 3），不得恒空串"
        );
        assert_eq!(
            member("ghost")["name"],
            json!(""),
            "查不到人才允许空串，实得 {:?}",
            member("ghost")
        );
    }

    /// approvalRecord 出口四条口径（spec/06 §4.6，issues/154）：
    /// ④九键齐（含行主键 `id`，且**必须是字符串**）／③任务变量空 ⇒ `ext` 出空对象不回落实例变量／
    /// ②实例 id 不存在 ⇒ 空数组不报错。①行序 `ORDER BY id ASC` 在 SQL 仓那条腿上钉
    /// （`jeeflow-repository-sqlx` 的 `test_mysql_i154_find_history_tasks_is_id_ascending`），
    /// 内存仓的 `find_history_tasks` 走 HashMap 迭代、本就无序，在这一层断言行序只会得到随机答案。
    #[tokio::test]
    async fn test_i154_approval_record_nine_keys_string_id_and_ext_no_fallback() {
        let facade = make_facade();
        let mut define = ProcessDefine {
            id: 0,
            name: "ar-flow".into(),
            display_name: "AR".into(),
            define_type: "approval".into(),
            state: 1,
            content: b"{}".to_vec(),
            version: 1,
            create_time: None,
            create_user: None,
            update_time: None,
            update_user: None,
        };
        facade.repo().save_define(&mut define).unwrap();

        // 实例变量带两份料：任务变量为空时，旧的"回落实例变量"会把它们原样灌进 ext
        let mut inst_vars = FlowData::new();
        inst_vars.insert_i64("amount", 500);
        inst_vars.insert_str("u_realName", "实例级姓名");
        let mut inst = ProcessInstance {
            instance_id: 0,
            parent_id: None,
            define_id: define.id,
            state: 10,
            parent_node_name: None,
            business_no: None,
            operator: "applicant".into(),
            expire_time: None,
            variables: inst_vars,
            tasks: vec![],
            create_time: Some("2026-10-09 10:00:00".into()),
            create_user: Some("applicant".into()),
            update_time: None,
            update_user: None,
            define: None,
        };
        facade.repo().save_instance(&mut inst).unwrap();

        let mut no_vars = FlowData::new();
        no_vars.remove("不存在的键"); // 保持空变量（口径③的实验体）
        let mut with_vars = FlowData::new();
        with_vars.insert_str("opinion", "同意");
        for (name, task_id, vars, state, actor, finish) in [
            ("apply", 915401i64, no_vars.clone(), 20, "applicant", Some("2026-10-09 10:05:00")),
            ("task1", 915402, with_vars, 20, "leader", Some("2026-10-09 11:00:00")),
            ("task2", 915403, no_vars, 10, "director", None),
        ] {
            let mut task = ProcessTask {
                task_id,
                process_instance_id: inst.instance_id,
                task_name: name.into(),
                display_name: name.into(),
                task_type: 0,
                perform_type: 0,
                task_state: state,
                actor_id: Some(actor.into()),
                actor_ids: vec![actor.into()],
                finish_time: finish.map(|s| s.to_string()),
                expire_time: None,
                form_key: None,
                parent_task_id: None,
                variables: vars,
                create_time: Some("2026-10-09 10:00:00".into()),
                create_user: Some(actor.into()),
                update_time: None,
                update_user: None,
            };
            facade.repo().save_task(&mut task).unwrap();
        }

        let mut args = HashMap::new();
        args.insert("id".to_string(), json!(inst.instance_id));
        let resp = facade.flow("processInstance/approvalRecord", &args).await;
        assert_eq!(resp["code"], 0, "{:?}", resp);
        let rows = resp["data"].as_array().expect("approvalRecord 出行数组");
        assert_eq!(rows.len(), 3, "三行任务都应出口，实得 {rows:?}");

        let row_of = |n: &str| -> &Json {
            rows.iter()
                .find(|r| r["taskName"].as_str() == Some(n))
                .unwrap_or_else(|| panic!("行 {n} 应存在，实得 {rows:?}"))
        };

        for row in rows {
            let mut keys: Vec<&str> = row.as_object().unwrap().keys().map(|s| s.as_str()).collect();
            keys.sort();
            assert_eq!(
                keys,
                vec![
                    "displayName", "ext", "finishTime", "id", "operator", "performType",
                    "taskName", "taskState", "taskType"
                ],
                "approvalRecord 出口必须恰为九键（spec/06 §4.6 口径④，旧八键缺 id），实得 {keys:?}"
            );
            assert!(
                row["id"].is_string(),
                "id 必须是字符串——19 位雪花出 number 会被 JS 截精度（口径④／issues/75·92 同族），实得 {:?}",
                row["id"]
            );
            assert!(
                row.get("variable").is_none(),
                "variable 原串不得再出现在出口（issues/124），实得 {row:?}"
            );
        }

        assert_eq!(row_of("apply")["id"], json!("915401"), "行主键＝任务行 id");
        assert_eq!(
            row_of("apply")["ext"],
            json!({}),
            "任务变量为空 ⇒ ext 出空对象，**不得**回落实例变量（spec/06 §4.6 口径③），实得 {:?}",
            row_of("apply")["ext"]
        );
        assert_eq!(row_of("task1")["ext"], json!({"opinion": "同意"}), "任务变量原样出口");

        // 口径②：实例 id 不存在 ⇒ 空数组，视图端点不报「流程实例不存在」
        let mut missing = HashMap::new();
        missing.insert("id".to_string(), json!(424242424i64));
        let resp2 = facade.flow("processInstance/approvalRecord", &missing).await;
        assert_eq!(
            resp2["code"],
            0,
            "实例不存在不得报错（spec/06 §4.6 口径②），实得 {resp2:?}"
        );
        assert_eq!(resp2["data"], json!([]), "实例不存在 ⇒ 空数组");
    }

    /// ccList 分页默认值档位（spec/06 §4.6 ccList 参数表／issues/154④）。
    #[tokio::test]
    async fn test_i154_cc_list_default_page_size_is_ten() {
        // issues/154④（spec/06 §4.6 ccList 参数表）：分页默认值统一 pageNum=1 / pageSize=10，
        // rust 旧默认 20 已判分叉。信封会把查询档位回显出来，所以这一格在内存仓上就能钉死。
        let facade = make_facade();
        let resp = facade.flow("processInstance/ccList", &HashMap::new()).await;
        assert_eq!(resp["code"], 0, "{:?}", resp);
        assert_eq!(resp["data"]["pageSize"], json!(10), "ccList 不传 pageSize 时默认必须是 10");
        assert_eq!(resp["data"]["pageNum"], json!(1), "ccList 不传 pageNum 时默认是 1");
        // 显式传值不受默认档影响（防把"改默认"写成"改强制"）
        let mut args = HashMap::new();
        args.insert("pageSize".to_string(), json!(20));
        let resp2 = facade.flow("processInstance/ccList", &args).await;
        assert_eq!(resp2["data"]["pageSize"], json!(20), "显式 pageSize 必须照传值走，实得 {resp2:?}");
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
    // issues/115 §3-8：门面第 47 个 action `processTask/removeTaskActor` 入账，条数与名单一起前进
    // （owner 2026-10-01 拍 A：条数断言随代次前进）。这格是 manifest↔分派表的一致性门禁：
    // 名单漏记 ⇒ 该 action 不在覆盖内；分派表漏记 ⇒ 落 unknown 分支当场红。

    #[tokio::test]
    async fn test_all_47_actions_dispatchable() {
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
            "processTask/addCandidate", "processTask/transfer", "processTask/latest",
            "processTask/removeTaskActor",
            "processDesign/page", "processDesign/detail",
            "processDesign/save", "processDesign/update",
            "processDesign/updateDefine", "processDesign/remove",
            "processDesign/deploy", "processDesign/redeploy",
            "processDesign/listByType",
            "processSurrogate/page", "processSurrogate/save",
            "processSurrogate/update", "processSurrogate/detail",
            "processSurrogate/remove",
        ];
        assert_eq!(actions.len(), 47,
            "Should have exactly 47 actions (issues/115 §3-8 added processTask/removeTaskActor)");
        // All actions should return a response (not panic)
        for action in &actions {
            let resp = facade.flow(action, &HashMap::new()).await;
            // Should have code field (either success or error)
            assert!(resp.get("code").is_some(), "Action {} should return a response with code", action);
            // ⚠️ 只有上一条判据＝"恒真"：unknown action 也返回 code=99999999 的信封 ⇒ 名单里写了但
            // 分派表漏记的话，前一判照样绿。补这一判才让本格真成"名单 ↔ 分派表"门禁
            // （与 c# FacadeTests.AllActionsInManifest_Dispatch_NoUnknown 同判据）。
            let msg = resp.get("msg").and_then(|v| v.as_str()).unwrap_or("");
            assert!(!msg.contains("未知 action"), "Action {} 落到 unknown 分支：分派表漏记该 action", action);
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

    // ─── issues/106：分页下推（recordCount=总数、pageNum≥2 非空、m_ 过滤跨页口径一致）───

    fn seed_define(facade: &JeeflowFacade, name: &str, display_name: &str, state: i32) {
        let mut define = ProcessDefine {
            id: 0,
            name: name.into(),
            display_name: display_name.into(),
            define_type: "approval".into(),
            state,
            content: Vec::new(),
            version: 1,
            create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        facade.repo().save_define(&mut define).unwrap();
    }

    fn make_facade_with_n_defines(n: usize) -> JeeflowFacade {
        let facade = make_facade();
        for i in 0..n {
            seed_define(&facade, &format!("flow-{}", i), &format!("Flow {}", i), 1);
        }
        facade
    }

    fn make_facade_with_defines_mixed_state() -> JeeflowFacade {
        let facade = make_facade();
        // 3 条 state=1、2 条 state=2（共 5）
        for i in 0..3 {
            seed_define(&facade, &format!("on-{}", i), &format!("On {}", i), 1);
        }
        for i in 0..2 {
            seed_define(&facade, &format!("off-{}", i), &format!("Off {}", i), 2);
        }
        facade
    }

    async fn page_define_page(facade: &JeeflowFacade, page_num: i64, page_size: i64) -> Json {
        let mut args = HashMap::new();
        args.insert("pageNum".to_string(), json!(page_num));
        args.insert("pageSize".to_string(), json!(page_size));
        let resp = facade.flow("processDefine/page", &args).await;
        assert_eq!(resp["code"], 0, "page resp: {}", resp);
        resp["data"].clone()
    }

    async fn filter_define_page(facade: &JeeflowFacade, filter: Json, page_num: i64, page_size: i64) -> Json {
        let mut args = HashMap::new();
        args.insert("pageNum".to_string(), json!(page_num));
        args.insert("pageSize".to_string(), json!(page_size));
        if let Json::Object(map) = filter {
            for (k, v) in map {
                args.insert(k, v);
            }
        }
        let resp = facade.flow("processDefine/page", &args).await;
        assert_eq!(resp["code"], 0, "filter resp: {}", resp);
        resp["data"].clone()
    }

    #[tokio::test]
    async fn test_define_page_multipage_total() {
        let facade = make_facade_with_n_defines(5);
        let r = page_define_page(&facade, 1, 2).await;
        assert_eq!(r["recordCount"], 5); // 总记录数，非本页行数
        assert_eq!(r["totalPage"], 3); // 5/2 向上取整
        assert_eq!(r["rows"].as_array().unwrap().len(), 2);
        let r2 = page_define_page(&facade, 2, 2).await;
        assert_eq!(r2["recordCount"], 5);
        assert_eq!(r2["rows"].as_array().unwrap().len(), 2); // 翻页不再恒空
        let r3 = page_define_page(&facade, 3, 2).await;
        assert_eq!(r3["recordCount"], 5);
        assert_eq!(r3["rows"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_define_page_filter_cross_page() {
        let facade = make_facade_with_defines_mixed_state();
        let r = filter_define_page(&facade, json!({"m_EQ_state": 1}), 1, 2).await;
        assert_eq!(r["recordCount"], 3); // = 命中总数(3)，非本页行数(2)
        assert_eq!(r["totalPage"], 2); // 3/2 向上取整
        assert_eq!(r["rows"].as_array().unwrap().len(), 2);
        let r2 = filter_define_page(&facade, json!({"m_EQ_state": 1}), 2, 2).await;
        assert_eq!(r2["recordCount"], 3);
        assert_eq!(r2["rows"].as_array().unwrap().len(), 1); // 翻页非空
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

    use jeeflow_core::event::{ProcessEvent, ProcessEventType};

    /// 捕获 CC_CREATE 事件的监听器（issues/102·104 P0）。
    struct CcCreateCapture {
        events: std::sync::Mutex<Vec<(i64, Option<String>)>>,
    }

    impl ProcessEventListener for CcCreateCapture {
        fn on_event(&self, event: &ProcessEvent) {
            if event.event_type == ProcessEventType::CcCreate {
                self.events.lock().unwrap()
                    .push((event.source_id, event.cc_actor_id.clone()));
            }
        }
    }

    /// P0：facade 手动补抄送（createCCInstance）→ 逐抄送人 fire CC_CREATE（对齐 Go facade）。
    #[tokio::test]
    async fn test_cc_create_fired_on_manual_create_cc() {
        use jeeflow_core::spi::ProcessEventListener as _;

        let repo = Arc::new(MemoryRepository::new());
        let mut ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
        let capture = Arc::new(CcCreateCapture { events: std::sync::Mutex::new(Vec::new()) });
        ctx.register_event_listener(capture.clone());
        let facade = JeeflowFacade::new(ctx);

        let mut args = HashMap::new();
        args.insert("processInstanceId".to_string(), json!(1001));
        args.insert("operator".to_string(), json!("user1"));
        args.insert("actorIds".to_string(), json!(["u3", "u4"]));
        let resp = facade.flow("processInstance/createCCInstance", &args).await;
        assert_eq!(resp["code"], 0, "createCCInstance 应成功：{}", resp);

        let fired = capture.events.lock().unwrap().clone();
        assert_eq!(fired.len(), 2, "手动补抄送应逐抄送人 fire，实得 {:?}", fired);
        assert!(fired.iter().all(|(sid, _)| *sid == 1001));
        let actors: Vec<String> = fired.iter().map(|(_, a)| a.clone().unwrap()).collect();
        assert_eq!(actors, vec!["u3".to_string(), "u4".to_string()]);
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
    fn test_i120_stats_window_follows_injected_clock() {
        // issues/120：统计的"今天"与"最近 30 天"必须跟引擎时间串同一出口。
        // 种子里落在 2025-01-10 的**实例**只有 1 笔（另有两笔是同日的任务行，不计入今日新增），
        // 把钟注入成那一刻 ⇒ todayNew = 1、范围末日 = 2025-01-10。
        // 注回旧写法（各取一次 chrono::Local::now()）时这两条都拿"真实的今天"当锚点 ⇒ 双双变红，
        // 且不会有任何用例能发现"注入钟改不动统计口径"。
        let _scope = jeeflow_core::clock::ClockScope::injected(|| "2025-01-10 12:00:00".to_string());
        let facade = seed_stats_facade();
        let resp = facade.stats_overview(&HashMap::new()).unwrap();
        assert_eq!(resp["todayNew"], 1,
            "注入 2025-01-10 后，当日那一笔种子实例应计入今日新增（实得 {:?}）", resp["todayNew"]);

        let buckets = stats_enumerate_buckets(None, None, "day");
        assert_eq!(buckets.last().map(|s| s.as_str()), Some("2025-01-10"),
            "缺省范围末日须随注入钟，实得 {:?}", buckets.last());
        assert_eq!(buckets.len(), 31, "缺省窗口应是注入日的最近 30 天，实得 {}", buckets.len());
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

    // ═══════════════════════════════════════════════════════
    // issues/113~116 · withdraw 鉴权 + processTask/transfer（内存仓）
    // ═══════════════════════════════════════════════════════

    /// 部署两步审批流（apply[applicant] → approve[user2] → end），startAndExecute 自动完成
    /// apply，返回 (instance_id, 进行中的 approve 任务 id)。任务参与者为 user2。
    async fn start_two_step_flow(facade: &JeeflowFacade, flow_name: &str) -> (i64, i64) {
        start_two_step_flow_assigned(facade, flow_name, "user2").await
    }

    /// 同 [`start_two_step_flow`]，但审批节点的 assignee 由调用方给
    /// （issues/152 档 4 要用 §2.5 归一缺省 `user1` 当审批人，才验得出"那一行真能被本人待办命中"）。
    async fn start_two_step_flow_assigned(
        facade: &JeeflowFacade,
        flow_name: &str,
        approve_assignee: &str,
    ) -> (i64, i64) {
        let mut a1 = HashMap::new();
        a1.insert("name".to_string(), json!(flow_name));
        a1.insert("displayName".to_string(), json!(flow_name));
        let r1 = facade.flow("processDesign/save", &a1).await;
        assert_eq!(r1["code"], 0, "save failed: {:?}", r1);
        let design_id = r1["data"]["id"].as_str().unwrap().parse::<i64>().unwrap();

        let content = format!(
            r#"{{
                "name":"{n}","displayName":"{n}","type":"approval",
                "nodes":[
                    {{"id":"start","type":"snaker:start","text":{{"value":"Start"}}}},
                    {{"id":"apply","type":"snaker:task","text":{{"value":"Apply"}},"properties":{{"assignee":"applicant"}}}},
                    {{"id":"approve","type":"snaker:task","text":{{"value":"Approve"}},"properties":{{"assignee":"{a}"}}}},
                    {{"id":"end","type":"snaker:end","text":{{"value":"End"}}}}
                ],
                "edges":[
                    {{"id":"e1","sourceNodeId":"start","targetNodeId":"apply"}},
                    {{"id":"e2","sourceNodeId":"apply","targetNodeId":"approve"}},
                    {{"id":"e3","sourceNodeId":"approve","targetNodeId":"end"}}
                ]
            }}"#,
            n = flow_name,
            a = approve_assignee
        );
        let mut a2 = HashMap::new();
        a2.insert("id".to_string(), json!(design_id));
        a2.insert("content".to_string(), json!(content));
        assert_eq!(facade.flow("processDesign/updateDefine", &a2).await["code"], 0);
        let mut a3 = HashMap::new();
        a3.insert("id".to_string(), json!(design_id));
        assert_eq!(facade.flow("processDesign/deploy", &a3).await["code"], 0);

        let mut a4 = HashMap::new();
        a4.insert("name".to_string(), json!(flow_name));
        a4.insert("operator".to_string(), json!("applicant"));
        let r4 = facade.flow("processDefine/startAndExecute", &a4).await;
        assert_eq!(r4["code"], 0, "start failed: {:?}", r4);
        let inst_id: i64 = r4["data"]["processInstanceId"].as_str().unwrap().parse().unwrap();

        let doing = facade.repo().find_doing_tasks(inst_id, &[]).unwrap();
        assert_eq!(doing.len(), 1, "应有一个进行中任务 approve");
        assert_eq!(doing[0].task_name, "approve");
        let task_id = doing[0].task_id;
        let actors = facade.repo().find_task_actors(task_id).unwrap();
        assert!(actors.contains(&approve_assignee.to_string()), "task actors={:?}", actors);
        (inst_id, task_id)
    }

    /// 四节点链式流：`start → apply[applicant] → a[alice] → b[bob] → approve[user2] → end`。
    /// 三段审批依次办结后停在 approve，返回 `(实例 id, approve 任务 id)`。
    ///
    /// 存在的唯一理由：给**跳转(JUMP)** 与**回退(ROLLBACK)** 两条建任务路径各留一条独立委托用例
    /// （规范 06 §4.5 条款 1「每条路径各留一条独立用例」）。两条路径的**落点节点不同**
    /// （JUMP 指定 `a`/alice，回退自动落到上一节点 `b`/bob），于是"只有一条路径漏挂委托"
    /// 时只红自己那一条——不需要为了取证去改引擎做条件注入。
    async fn start_chain_flow(facade: &JeeflowFacade, flow_name: &str) -> (i64, i64) {
        let mut a1 = HashMap::new();
        a1.insert("name".to_string(), json!(flow_name));
        a1.insert("displayName".to_string(), json!(flow_name));
        let r1 = facade.flow("processDesign/save", &a1).await;
        assert_eq!(r1["code"], 0, "design save failed: {:?}", r1);
        let design_id = r1["data"]["id"].as_str().unwrap().parse::<i64>().unwrap();

        let content = format!(
            r#"{{
                "name":"{n}","displayName":"{n}","type":"approval",
                "nodes":[
                    {{"id":"start","type":"snaker:start","text":{{"value":"Start"}}}},
                    {{"id":"apply","type":"snaker:task","text":{{"value":"Apply"}},"properties":{{"assignee":"applicant"}}}},
                    {{"id":"a","type":"snaker:task","text":{{"value":"A"}},"properties":{{"assignee":"alice"}}}},
                    {{"id":"b","type":"snaker:task","text":{{"value":"B"}},"properties":{{"assignee":"bob"}}}},
                    {{"id":"approve","type":"snaker:task","text":{{"value":"Approve"}},"properties":{{"assignee":"user2"}}}},
                    {{"id":"end","type":"snaker:end","text":{{"value":"End"}}}}
                ],
                "edges":[
                    {{"id":"e1","sourceNodeId":"start","targetNodeId":"apply"}},
                    {{"id":"e2","sourceNodeId":"apply","targetNodeId":"a"}},
                    {{"id":"e3","sourceNodeId":"a","targetNodeId":"b"}},
                    {{"id":"e4","sourceNodeId":"b","targetNodeId":"approve"}},
                    {{"id":"e5","sourceNodeId":"approve","targetNodeId":"end"}}
                ]
            }}"#,
            n = flow_name
        );
        let mut a2 = HashMap::new();
        a2.insert("id".to_string(), json!(design_id));
        a2.insert("content".to_string(), json!(content));
        assert_eq!(facade.flow("processDesign/updateDefine", &a2).await["code"], 0);
        let mut a3 = HashMap::new();
        a3.insert("id".to_string(), json!(design_id));
        assert_eq!(facade.flow("processDesign/deploy", &a3).await["code"], 0);

        let mut a4 = HashMap::new();
        a4.insert("name".to_string(), json!(flow_name));
        a4.insert("operator".to_string(), json!("applicant"));
        let r4 = facade.flow("processDefine/startAndExecute", &a4).await;
        assert_eq!(r4["code"], 0, "start failed: {:?}", r4);
        let inst_id: i64 = r4["data"]["processInstanceId"].as_str().unwrap().parse().unwrap();

        // 依次办结 a(alice) → b(bob)，停在 approve(user2)
        for (node, who) in [("a", "alice"), ("b", "bob")] {
            let tid = doing_task_of(facade, inst_id, node);
            assert_eq!(
                exec_task(facade, tid, who, vec![]).await["code"],
                0,
                "办结 {} 应成功", node
            );
        }
        let approve_task = doing_task_of(facade, inst_id, "approve");
        (inst_id, approve_task)
    }

    /// 该实例里指定节点当前的进行中任务 id（不存在即 panic，避免"节点名写错→空集合→假绿"）。
    fn doing_task_of(facade: &JeeflowFacade, inst_id: i64, node: &str) -> i64 {
        let hit: Vec<i64> = facade
            .repo()
            .find_doing_tasks(inst_id, &[])
            .unwrap()
            .into_iter()
            .filter(|t| t.task_name == node)
            .map(|t| t.task_id)
            .collect();
        assert_eq!(hit.len(), 1, "节点 {} 应恰有 1 个进行中任务，实得 {:?}", node, hit);
        hit[0]
    }

    /// 办理提交：默认 submitType=1（同意），`extra` 可覆盖同名键（如 submitType/taskName）。
    async fn exec_task(facade: &JeeflowFacade, task_id: i64, operator: &str, extra: Vec<(&str, Json)>) -> Json {
        let mut m = HashMap::new();
        m.insert("processTaskId".to_string(), json!(task_id));
        m.insert("operator".to_string(), json!(operator));
        m.insert("submitType".to_string(), json!(1));
        for (k, v) in extra {
            m.insert(k.to_string(), v);
        }
        facade.flow("processTask/execute", &m).await
    }

    /// 该任务在参与者表里的当前成员（排序后对账）。
    fn persisted_actors_of(facade: &JeeflowFacade, task_id: i64) -> Vec<String> {
        let mut v = facade.repo().find_task_actors(task_id).unwrap();
        v.sort();
        v
    }

    /// 条款 1 · 跳转(JUMP) 路径独立用例：委托**在起单之后**才配，且只配在跳转落点节点的参与者
    /// (alice) 身上 ⇒ 全实例唯一可能带上 `agentJP` 的行，就是 JUMP 新建出来的那条 a 任务。
    /// 漏挂时该用例必红，而回退用例（另一个参与者 + 另一个代理名）不受影响。
    #[tokio::test]
    async fn test_surrogate_applies_on_jump_path() {
        let facade = make_facade();
        let (inst, approve_task) = start_chain_flow(&facade, "surr-jump-flow").await;

        assert_eq!(
            facade
                .flow(
                    "processSurrogate/save",
                    &args_of(vec![
                        ("operator", json!("alice")),
                        ("surrogate", json!("agentJP")),
                        ("processName", json!("surr-jump-flow")),
                    ])
                )
                .await["code"],
            0,
            "委托台账应保存成功"
        );
        // 配完委托**还没跳转**：此刻任何进行中任务都不得带 agentJP（排除"配台账就生效"的假绿）
        let before: Vec<String> = facade
            .repo()
            .find_doing_tasks(inst, &[])
            .unwrap()
            .into_iter()
            .flat_map(|t| persisted_actors_of(&facade, t.task_id))
            .collect();
        assert!(
            !before.contains(&"agentJP".to_string()),
            "配委托后、建单前应没有任何代理人，实得 {:?}", before
        );

        // JUMP：submitType=4 + taskName=a
        let r = exec_task(
            &facade,
            approve_task,
            "user2",
            vec![("submitType", json!(4)), ("taskName", json!("a"))],
        )
        .await;
        assert_eq!(r["code"], 0, "跳转应成功: {:?}", r);

        let new_a = doing_task_of(&facade, inst, "a");
        let actors = persisted_actors_of(&facade, new_a);
        assert!(
            actors.contains(&"alice".to_string()) && actors.contains(&"agentJP".to_string()),
            "条款 1「跳转(JUMP)」：跳转新建的 a 任务须并入代理人（原人保留），实得 {:?}", actors
        );
        // 条款 3「不级联/不追溯」：跳转前那条**已办结**的 a 行（同一个参与者 alice）不得被回写。
        // 缺这一格时，"把所有 a 行都扩一遍"的错实现照样能过上面那条断言。
        let hist_a: Vec<i64> = facade
            .repo()
            .find_history_tasks(inst)
            .unwrap()
            .into_iter()
            .filter(|t| t.task_name == "a" && t.task_id != new_a)
            .map(|t| t.task_id)
            .collect();
        assert_eq!(hist_a.len(), 1, "发起→推进应留下一条历史 a 行，实得 {:?}", hist_a);
        assert_eq!(
            persisted_actors_of(&facade, hist_a[0]),
            vec!["alice".to_string()],
            "条款 3：委托只对本新建的行生效，历史行的参与者表不得被追溯改写"
        );
    }

    /// 条款 1 · 回退(ROLLBACK，submitType=3) 路径独立用例：委托**在起单之后**才配在
    /// 回退落点节点的参与者 (applicant) 身上 ⇒ 全实例唯一可能带上 `agentRB` 的行，
    /// 就是回退新建出来的那条任务。漏挂时本用例必红，而 JUMP 用例（另一个参与者 + 另一个
    /// 代理名）不受影响，反之亦然。
    ///
    /// ⚠️ 用两步流而非四步流：本栈 `submitType=3` 实际落到**第一个任务节点**而不是上一节点
    /// （`jeeflow-core/src/engine.rs` 的 `execute_and_jump_async` 里 `target_name=None` 走
    /// `get_first_task_node()`，而注释写的是 "go back to previous task node"）——
    /// 该语义错位已单独立账 issues/119，**不在本用例的职责内**：本格只钉"这条建单路径有没有挂委托"。
    #[tokio::test]
    async fn test_surrogate_applies_on_rollback_path() {
        let facade = make_facade();
        let (inst, approve_task) = start_two_step_flow(&facade, "surr-rb-flow").await;

        assert_eq!(
            facade
                .flow(
                    "processSurrogate/save",
                    &args_of(vec![
                        ("operator", json!("applicant")),
                        ("surrogate", json!("agentRB")),
                        ("processName", json!("surr-rb-flow")),
                    ])
                )
                .await["code"],
            0,
            "委托台账应保存成功"
        );
        // 配完委托还没回退：任何进行中任务都不该带代理人（排除"配台账即生效"的假绿）
        let before: Vec<String> = facade
            .repo()
            .find_doing_tasks(inst, &[])
            .unwrap()
            .into_iter()
            .flat_map(|t| persisted_actors_of(&facade, t.task_id))
            .collect();
        assert!(
            !before.contains(&"agentRB".to_string()),
            "配委托后、建单前应没有任何代理人，实得 {:?}", before
        );

        // ROLLBACK：submitType=3，不传 taskName（落点由引擎决定）
        let r = exec_task(&facade, approve_task, "user2", vec![("submitType", json!(3))]).await;
        assert_eq!(r["code"], 0, "回退应成功: {:?}", r);

        let mut rb_tasks: Vec<_> = facade
            .repo()
            .find_doing_tasks(inst, &[])
            .unwrap()
            .into_iter()
            .filter(|t| t.task_id != approve_task)
            .collect();
        assert_eq!(
            rb_tasks.len(),
            1,
            "回退后应恰有 1 条新建的进行中任务（原 approve 任务已办结），实得任务 id {:?}",
            rb_tasks.iter().map(|t| t.task_id).collect::<Vec<_>>()
        );
        let rb_task = rb_tasks.pop().unwrap();
        let actors = persisted_actors_of(&facade, rb_task.task_id);
        assert!(
            // 落点是 apply 节点，其参与者解析为发起人 applicant；原人必须保留（委托是"加人"不是"换人"）
            actors.contains(&"applicant".to_string()) && actors.contains(&"agentRB".to_string()),
            "条款 1「回退(ROLLBACK)」：回退新建的任务须并入代理人且保留原人，落点节点={}，实得 {:?}",
            rb_task.task_name,
            actors
        );
        // 条款 3「不级联/不追溯」：回退前那条**已办结**的 apply 行（同一个参与者 applicant）不得被回写。
        // 本用例的委托正是配在 applicant 身上的，所以"把该参与者所有行都扩一遍"的错实现
        // 会被这一格抓住——上面那条断言单独看不出差别。
        let hist_apply: Vec<i64> = facade
            .repo()
            .find_history_tasks(inst)
            .unwrap()
            .into_iter()
            .filter(|t| t.task_name == "apply" && t.task_id != rb_task.task_id)
            .map(|t| t.task_id)
            .collect();
        assert_eq!(
            hist_apply.len(),
            1,
            "发起路径应留下一条历史 apply 行，实得 {:?}",
            hist_apply
        );
        assert_eq!(
            persisted_actors_of(&facade, hist_apply[0]),
            vec!["applicant".to_string()],
            "条款 3：委托只对本新建的行生效，历史行的参与者表不得被追溯改写"
        );
    }

    /// 从 task.variables（持久 FlowData，非 camelCase HTTP 视图）取 tf_transferHistory 数组。
    fn history_of(facade: &JeeflowFacade, task_id: i64) -> Vec<JsonValue> {
        let t = facade.repo().find_task_by_id(task_id).unwrap().expect("task");
        match t.variables.get("tf_transferHistory") {
            Some(JsonValue::Array(a)) => a.clone(),
            _ => Vec::new(),
        }
    }

    fn field_str(v: &JsonValue, key: &str) -> String {
        match v {
            JsonValue::Object(entries) => entries
                .iter()
                .find(|(k, _)| k == key)
                .and_then(|(_, val)| val.as_str().map(|s| s.to_string()))
                .unwrap_or_default(),
            _ => String::new(),
        }
    }

    fn field_i64(v: &JsonValue, key: &str) -> i64 {
        match v {
            JsonValue::Object(entries) => entries
                .iter()
                .find(|(k, _)| k == key)
                .and_then(|(_, val)| val.as_i64())
                .unwrap_or(-1),
            _ => -1,
        }
    }

    fn transfer_args(task_id: i64, from: &str, to: &str, operator: &str) -> HashMap<String, Json> {
        let mut m = HashMap::new();
        m.insert("processTaskId".to_string(), json!(task_id));
        m.insert("fromActor".to_string(), json!(from));
        m.insert("toActor".to_string(), json!(to));
        m.insert("operator".to_string(), json!(operator));
        m
    }

    // ─── withdraw：operator 硬必填 + 三判据 + update_user 回写 ───

    #[tokio::test]
    async fn test_withdraw_operator_required() {
        let facade = make_facade();
        let (iid, _) = start_two_step_flow(&facade, "wd-op-flow").await;
        // 缺 operator
        let mut a = HashMap::new();
        a.insert("id".to_string(), json!(iid));
        let r = facade.flow("processInstance/withdraw", &a).await;
        assert_eq!(r["code"], 99999999);
        assert_eq!(r["msg"], "operator 必填", "缺 operator 应报统一文案");
        // 空串 operator
        a.insert("operator".to_string(), json!(""));
        let r2 = facade.flow("processInstance/withdraw", &a).await;
        assert_eq!(r2["msg"], "operator 必填", "空串 operator 同样必填");
    }

    #[tokio::test]
    async fn test_withdraw_no_permission() {
        let facade = make_facade();
        let (iid, _) = start_two_step_flow(&facade, "wd-perm-flow").await;
        // stranger：非发起人(applicant)、非进行中任务参与者(user2)、非 auto/admin
        let mut a = HashMap::new();
        a.insert("id".to_string(), json!(iid));
        a.insert("operator".to_string(), json!("stranger"));
        let r = facade.flow("processInstance/withdraw", &a).await;
        assert_eq!(r["code"], 99999999);
        assert_eq!(r["msg"], "无权限撤回该流程实例");
        // 拒绝后不许改写：实例仍进行中、任务仍 DOING
        assert_eq!(
            facade.repo().find_instance_by_id(iid).unwrap().unwrap().state,
            InstanceState::Doing.code()
        );
        assert_eq!(facade.repo().find_doing_tasks(iid, &[]).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_withdraw_by_initiator_writeback_update_user() {
        let facade = make_facade();
        let (iid, task_id) = start_two_step_flow(&facade, "wd-init-flow").await;
        let mut a = HashMap::new();
        a.insert("id".to_string(), json!(iid));
        a.insert("operator".to_string(), json!("applicant")); // 判据 1：发起人
        let r = facade.flow("processInstance/withdraw", &a).await;
        assert_eq!(r["code"], 0, "{:?}", r);

        let inst = facade.repo().find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, InstanceState::Withdraw.code());
        assert_eq!(inst.update_user.as_deref(), Some("applicant"), "实例 update_user 须回写撤回人");

        let task = facade.repo().find_task_by_id(task_id).unwrap().unwrap();
        assert_eq!(task.task_state, TaskState::Withdraw.code(), "进行中任务须落库 30");
        assert_eq!(task.update_user.as_deref(), Some("applicant"), "任务 update_user 须回写撤回人");
        assert!(task.actor_id.is_none(), "撤回不得给任务 actor 列写值");
        assert!(facade.repo().find_doing_tasks(iid, &[]).unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_withdraw_by_participant_and_privileged() {
        // 判据 2：进行中任务参与者
        let facade = make_facade();
        let (iid, _) = start_two_step_flow(&facade, "wd-actor-flow").await;
        let mut a = HashMap::new();
        a.insert("id".to_string(), json!(iid));
        a.insert("operator".to_string(), json!("user2")); // 参与人
        assert_eq!(facade.flow("processInstance/withdraw", &a).await["code"], 0, "参与者应可撤回");

        // 判据 3：flow.admin 放行（新实例，操作人非发起人/参与者）
        let facade2 = make_facade();
        let (iid2, _) = start_two_step_flow(&facade2, "wd-admin-flow").await;
        let mut a2 = HashMap::new();
        a2.insert("id".to_string(), json!(iid2));
        a2.insert("operator".to_string(), json!("flow.admin"));
        assert_eq!(facade2.flow("processInstance/withdraw", &a2).await["code"], 0, "admin 应放行");
    }

    // ─── issues/134 案 A · 撤回的实例状态守卫（非进行中 ⇒ 99999999 ＋ 固定文案，不落库）───
    //
    // 缺陷：issues/113 只管住**任务行**，**实例**层面没判状态 ⇒ 对已办结(20)/已终止(40) 的实例
    // 调撤回会把实例静默改写成 30（已办列表/按状态聚合的统计凭空改历史，且不报错）。
    // 判据（八栈逐字统一）：聚合根 withdraw 时 state != 10 ⇒ 内部码 20010009，**一行都不改、不落库**；
    // 门面沿用 issues/121 口径吞内部码 ⇒ 出口 code=99999999 ＋ msg 逐字 ＝ 固定文案（文案不带码值）。
    // 权威＝Java 参考实现 WithdrawInstanceStateGuardTest ＋ spec 06 §processInstance/withdraw。

    /// 出口文案逐字固定（八栈一致）；L2 门禁按逐字断言，不许用"包含 撤回"这种宽松判据
    const WD134_MSG: &str = "流程实例非进行中，无法撤回";

    /// `{id, operator}` 撤回入参
    fn wd134_withdraw_args(iid: i64, operator: &str) -> HashMap<String, Json> {
        let mut m = HashMap::new();
        m.insert("id".to_string(), json!(iid));
        m.insert("operator".to_string(), json!(operator));
        m
    }

    /// 任务行快照（行 id / 行状态 / 行 update_user），对账"一行都不改"
    fn wd134_task_rows(facade: &JeeflowFacade, iid: i64) -> Vec<(i64, i32, Option<String>)> {
        let mut rows: Vec<(i64, i32, Option<String>)> = facade.repo().find_history_tasks(iid).unwrap()
            .into_iter()
            .map(|t| (t.task_id, t.task_state, t.update_user))
            .collect();
        rows.sort_by_key(|r| r.0);
        rows
    }

    /// 实例快照（state / update_user）
    fn wd134_instance(facade: &JeeflowFacade, iid: i64) -> (i32, Option<String>) {
        let inst = facade.repo().find_instance_by_id(iid).unwrap()
            .unwrap_or_else(|| panic!("实例 {} 应存在", iid));
        (inst.state, inst.update_user)
    }

    /// 负向①（跨栈门禁格 L2-28 同形）：真把实例办到 state=20 再调撤回
    /// ⇒ 出口 code=99999999 ＋ msg 逐字；**再读一次**实例仍 20，update_user 与任务行一行未动。
    /// 撤回人用 `flow.admin` 哨兵（归属判据 3 放行），确保报错来自本案守卫而非鉴权分支。
    #[tokio::test]
    async fn test_withdraw_on_finished_instance_returns_verbatim_msg_and_does_not_persist() {
        let facade = make_facade();
        let (iid, approve_task) = start_two_step_flow(&facade, "wd134-finish-flow").await;
        assert_eq!(exec_task(&facade, approve_task, "user2", vec![]).await["code"], 0, "办结应成功");

        let before = wd134_instance(&facade, iid);
        assert_eq!(before.0, InstanceState::Finished.code(), "夹具前提：实例已办结 state=20");
        let rows_before = wd134_task_rows(&facade, iid);
        assert!(!rows_before.is_empty(), "夹具前提：应有任务行");

        let resp = facade.flow("processInstance/withdraw", &wd134_withdraw_args(iid, "flow.admin")).await;
        assert_eq!(resp["code"], CODE_ERROR, "非进行中实例撤回必须报错（禁止静默成功）: {:?}", resp);
        assert_eq!(resp["msg"], WD134_MSG, "出口 msg 逐字固定，内部码 20010009 不进 msg: {:?}", resp);
        assert!(!resp["msg"].as_str().unwrap_or_default().contains("2001000"),
                "内部码严禁进 msg: {:?}", resp);

        let after = wd134_instance(&facade, iid);
        assert_eq!(after.0, InstanceState::Finished.code(),
                   "被拒后**再读一次**实例 state 必须仍是 20（本案病灶：改前会被静默改写成 30）");
        assert_eq!(after.1, before.1, "被拒的那次不得落库改写实例 update_user");
        assert_eq!(wd134_task_rows(&facade, iid), rows_before, "被拒的那次不得改写任务行");
    }

    /// 负向②：state=40（强行终止）档。门面没有"终止实例"的 action，壳侧同样造不出这一档
    /// （issues/134 §5.2 因此把 L2-28 限定在 20 ＋ 正向 10），故用聚合根自己的 `interrupt`
    /// 命令把存储里的实例自然推到 40，再走门面撤回。
    #[tokio::test]
    async fn test_withdraw_on_interrupted_instance_is_rejected() {
        let facade = make_facade();
        let (iid, _) = start_two_step_flow(&facade, "wd134-interrupt-flow").await;
        let mut inst = facade.repo().find_instance_by_id(iid).unwrap().unwrap();
        inst.interrupt();
        inst.update_user = Some("boss".to_string());
        facade.repo().update_instance(&inst).unwrap();
        assert_eq!(wd134_instance(&facade, iid).0, InstanceState::Interrupt.code(),
                   "夹具前提：实例已终止 state=40");

        let resp = facade.flow("processInstance/withdraw", &wd134_withdraw_args(iid, "flow.admin")).await;
        assert_eq!(resp["code"], CODE_ERROR, "已终止实例撤回必须报错: {:?}", resp);
        assert_eq!(resp["msg"], WD134_MSG, "出口 msg 逐字固定: {:?}", resp);

        let after = wd134_instance(&facade, iid);
        assert_eq!(after.0, InstanceState::Interrupt.code(), "被拒后实例仍 40");
        assert_eq!(after.1.as_deref(), Some("boss"), "被拒后不落库：update_user 仍是终止人");
        for t in facade.repo().find_history_tasks(iid).unwrap() {
            assert_ne!(t.task_state, TaskState::Withdraw.code(), "任务行不得被改成 30：{}", t.task_name);
        }
    }

    /// 正向对照（门面级）：进行中(10) 的实例撤回照旧 code=0，实例落 30、进行中任务落 30 并回写
    /// 撤回人；已完成(20) 的 apply 行仍不被改写（issues/113 的既有保护保持原样）。
    #[tokio::test]
    async fn test_withdraw_on_doing_instance_still_succeeds_and_lands_state30() {
        let facade = make_facade();
        let (iid, approve_task) = start_two_step_flow(&facade, "wd134-doing-flow").await;
        let apply = facade.repo().find_history_tasks(iid).unwrap().into_iter()
            .find(|t| t.task_name == "apply").expect("夹具前提：应有 apply 行");
        assert_eq!(apply.task_state, TaskState::Finished.code(), "夹具前提：apply 行已办结 20");

        let resp = facade.flow("processInstance/withdraw", &wd134_withdraw_args(iid, "applicant")).await;
        assert_eq!(resp["code"], 0, "进行中实例撤回照旧成功（守卫没写反）: {:?}", resp);

        let after = wd134_instance(&facade, iid);
        assert_eq!(after.0, InstanceState::Withdraw.code(), "实例应落 30");
        assert_eq!(after.1.as_deref(), Some("applicant"), "实例 update_user 回写撤回人");
        assert!(facade.repo().find_doing_tasks(iid, &[]).unwrap().is_empty(),
                "整单撤回后不应残留进行中任务");
        let approve = facade.repo().find_task_by_id(approve_task).unwrap().unwrap();
        assert_eq!(approve.task_state, TaskState::Withdraw.code(), "进行中任务行应落 30");
        assert_eq!(approve.update_user.as_deref(), Some("applicant"), "被撤任务 update_user 回写撤回人");
        let apply_after = facade.repo().find_task_by_id(apply.task_id).unwrap().unwrap();
        assert_eq!(apply_after.task_state, TaskState::Finished.code(),
                   "已完成(20) 行仍不得被撤回改写（issues/113 既有保护保持原样）");
    }

    /// 回归：守卫排在**鉴权之后**（issues/114 的两条既有文案顺序未被本案抢答），
    /// 且负向都不改状态、不改行。
    #[tokio::test]
    async fn test_withdraw_state_guard_runs_after_permission_checks() {
        let facade = make_facade();
        let (iid, approve_task) = start_two_step_flow(&facade, "wd134-order-flow").await;
        assert_eq!(exec_task(&facade, approve_task, "user2", vec![]).await["code"], 0, "办结应成功");
        let inst_before = wd134_instance(&facade, iid);
        assert_eq!(inst_before.0, InstanceState::Finished.code(), "夹具前提：实例已办结 state=20");
        let rows_before = wd134_task_rows(&facade, iid);

        let mut no_operator = HashMap::new();
        no_operator.insert("id".to_string(), json!(iid));
        let r1 = facade.flow("processInstance/withdraw", &no_operator).await;
        assert_eq!(r1["code"], CODE_ERROR);
        assert_eq!(r1["msg"], "operator 必填", "缺 operator 仍先命中必填校验: {:?}", r1);

        let r2 = facade.flow("processInstance/withdraw", &wd134_withdraw_args(iid, "stranger")).await;
        assert_eq!(r2["code"], CODE_ERROR);
        assert_eq!(r2["msg"], "无权限撤回该流程实例", "鉴权文案仍排在状态守卫之前（顺序未动）: {:?}", r2);

        assert_eq!(wd134_instance(&facade, iid), inst_before, "两条负向都不该改实例 state/update_user");
        assert_eq!(wd134_task_rows(&facade, iid), rows_before, "两条负向都不该改写任务行");
    }

    // ─── transfer：正向留痕 + 挪待办 ───

    #[tokio::test]
    async fn test_transfer_positive_moves_todo_and_traces() {
        let facade = make_facade();
        let (iid, task_id) = start_two_step_flow(&facade, "tr-pos-flow").await;

        let mut a = transfer_args(task_id, "user2", "lisi", "user2");
        a.insert("reason".to_string(), json!("出差一周"));
        let r = facade.flow("processTask/transfer", &a).await;
        assert_eq!(r["code"], 0, "{:?}", r);

        // 待办从 user2 挪到 lisi
        assert!(!facade.repo().find_task_actors(task_id).unwrap().contains(&"user2".to_string()));
        assert!(facade.repo().find_task_actors(task_id).unwrap().contains(&"lisi".to_string()));
        let mut lisi = HashMap::new();
        lisi.insert("operator".to_string(), json!("lisi"));
        let lisi_rows = facade.flow("processTask/todoList", &lisi).await["data"]["rows"]
            .as_array().unwrap().clone();
        assert!(lisi_rows.iter().any(|t| t["id"].as_str() == Some(&task_id.to_string())),
            "lisi 待办应含该任务: {:?}", lisi_rows);
        let mut u2 = HashMap::new();
        u2.insert("operator".to_string(), json!("user2"));
        let u2_rows = facade.flow("processTask/todoList", &u2).await["data"]["rows"]
            .as_array().unwrap().clone();
        assert!(!u2_rows.iter().any(|t| t["id"].as_str() == Some(&task_id.to_string())),
            "user2 待办不应再含该任务");

        // 三件留痕（读持久值 tf_transferHistory，六键 camelCase + time 格式）
        let hist = history_of(&facade, task_id);
        assert_eq!(hist.len(), 1, "应恰一跳");
        let hop = &hist[0];
        assert_eq!(field_i64(hop, "submitType"), 7);
        assert_eq!(field_str(hop, "fromActor"), "user2");
        assert_eq!(field_str(hop, "toActor"), "lisi");
        assert_eq!(field_str(hop, "reason"), "出差一周");
        assert_eq!(field_str(hop, "operator"), "user2");
        let time = field_str(hop, "time");
        assert!(chrono::NaiveDateTime::parse_from_str(&time, "%Y-%m-%d %H:%M:%S").is_ok(),
            "time 必须是 yyyy-MM-dd HH:mm:ss，实测: {}", time);

        let task = facade.repo().find_task_by_id(task_id).unwrap().unwrap();
        assert_eq!(
            match task.variables.get("submitType") {
                Some(JsonValue::Number(n)) => *n as i64,
                Some(JsonValue::Str(s)) => s.parse::<i64>().unwrap_or(-1),
                _ => -1,
            },
            7,
            "当前槽位 submitType=7"
        );
        assert_eq!(task.variables.get_str("tf_transferTo"), Some("lisi"));
        assert_eq!(task.variables.get_str("tf_transferReason"), Some("出差一周"));
        let comment = task.variables.get_str("tf_approvalComment").unwrap_or("");
        assert!(comment.contains("user2") && comment.contains("转办给") && comment.contains("lisi")
            && comment.contains("出差一周"), "末跳可读文案异常: {}", comment);
        // 严禁覆写 actor 列（进行中该列恒无值）；update_user = 转办操作人
        assert!(task.actor_id.is_none(), "转办严禁写 actor_id 列，实测 {:?}", task.actor_id);
        assert_eq!(task.update_user.as_deref(), Some("user2"), "update_user 记转办操作人");
        // 任务不新建：沿用同一 id；实例仍进行中
        assert_eq!(facade.repo().find_doing_tasks(iid, &[]).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_transfer_history_appends_across_hops() {
        let facade = make_facade();
        let (_, task_id) = start_two_step_flow(&facade, "tr-multi-flow").await;
        // user2 → lisi
        assert_eq!(facade.flow("processTask/transfer", &transfer_args(task_id, "user2", "lisi", "user2")).await["code"], 0);
        // lisi → wangwu（第二跳）
        assert_eq!(facade.flow("processTask/transfer", &transfer_args(task_id, "lisi", "wangwu", "lisi")).await["code"], 0);

        let hist = history_of(&facade, task_id);
        assert_eq!(hist.len(), 2, "两跳都应留档，实测 {}", hist.len());
        // 首跳原样保留（追加不覆盖）
        assert_eq!(field_str(&hist[0], "fromActor"), "user2");
        assert_eq!(field_str(&hist[0], "toActor"), "lisi");
        assert_eq!(field_str(&hist[1], "fromActor"), "lisi");
        assert_eq!(field_str(&hist[1], "toActor"), "wangwu");
        assert_eq!(field_str(&hist[1], "reason"), "", "无 reason 写空串不写 null");
        // 便捷键只留末跳
        let task = facade.repo().find_task_by_id(task_id).unwrap().unwrap();
        assert_eq!(task.variables.get_str("tf_transferTo"), Some("wangwu"));
    }

    #[tokio::test]
    async fn test_transfer_only_removes_own_actor() {
        // 加签只追加 + 转办只摘自己那一行、不动其他参与人（会签/多参与人同判据）
        let facade = make_facade();
        let (_, task_id) = start_two_step_flow(&facade, "tr-own-flow").await;
        // 加签 user3（只追加，user2 仍在）
        let mut s = HashMap::new();
        s.insert("processTaskId".to_string(), json!(task_id));
        s.insert("actorIds".to_string(), json!(["user3"]));
        assert_eq!(facade.flow("processTask/surrogate", &s).await["code"], 0);
        let after_add = facade.repo().find_task_actors(task_id).unwrap();
        assert!(after_add.contains(&"user2".to_string()) && after_add.contains(&"user3".to_string()),
            "加签应只追加: {:?}", after_add);

        // 转办 user2 → lisi：只摘 user2，user3 不动
        assert_eq!(facade.flow("processTask/transfer", &transfer_args(task_id, "user2", "lisi", "user2")).await["code"], 0);
        let after_tr = facade.repo().find_task_actors(task_id).unwrap();
        assert!(!after_tr.contains(&"user2".to_string()));
        assert!(after_tr.contains(&"user3".to_string()), "转办不得误删其他参与人: {:?}", after_tr);
        assert!(after_tr.contains(&"lisi".to_string()));
    }

    #[tokio::test]
    async fn test_transfer_var_merge_order_args_win() {
        let facade = make_facade();
        let (_, task_id) = start_two_step_flow(&facade, "tr-merge-flow").await;
        assert_eq!(facade.flow("processTask/transfer", &transfer_args(task_id, "user2", "lisi", "user2")).await["code"], 0);
        // lisi 办结提交 submitType=1：args 最高，须覆盖转办留下的 7，且 tf_transferHistory 存活
        let mut e = HashMap::new();
        e.insert("processTaskId".to_string(), json!(task_id));
        e.insert("operator".to_string(), json!("lisi"));
        e.insert("submitType".to_string(), json!(1));
        assert_eq!(facade.flow("processTask/execute", &e).await["code"], 0);

        let task = facade.repo().find_task_by_id(task_id).unwrap().unwrap();
        let st = match task.variables.get("submitType") {
            Some(JsonValue::Number(n)) => *n as i64,
            Some(JsonValue::Str(s)) => s.parse::<i64>().unwrap_or(-1),
            _ => -1,
        };
        assert_eq!(st, 1, "办结 args 须覆盖转办的 submitType=7（否则记录失真）");
        assert_eq!(history_of(&facade, task_id).len(), 1, "合并语义下 tf_transferHistory 不得被抹掉");
    }

    #[tokio::test]
    async fn test_transfer_negative_msgs() {
        let facade = make_facade();
        let (_, task_id) = start_two_step_flow(&facade, "tr-neg-flow").await;

        // operator 必填
        let mut a = transfer_args(task_id, "user2", "lisi", "user2");
        a.remove("operator");
        assert_eq!(facade.flow("processTask/transfer", &a).await["msg"], "operator 必填");
        // fromActor 必填
        let mut a = transfer_args(task_id, "user2", "lisi", "user2");
        a.insert("fromActor".to_string(), json!(""));
        assert_eq!(facade.flow("processTask/transfer", &a).await["msg"], "fromActor 必填");
        // toActor 必填
        let mut a = transfer_args(task_id, "user2", "lisi", "user2");
        a.remove("toActor");
        assert_eq!(facade.flow("processTask/transfer", &a).await["msg"], "toActor 必填");
        // 无权限转办该任务：operator 既非 fromActor 也非 auto/admin
        assert_eq!(facade.flow("processTask/transfer", &transfer_args(task_id, "user2", "lisi", "mallory")).await["msg"], "无权限转办该任务");
        // 原办理人不是该任务参与人
        assert_eq!(facade.flow("processTask/transfer", &transfer_args(task_id, "ghost", "lisi", "ghost")).await["msg"], "原办理人不是该任务参与人");
        // 目标人已是该任务参与人
        assert_eq!(facade.flow("processTask/transfer", &transfer_args(task_id, "user2", "user2", "user2")).await["msg"], "目标人已是该任务参与人");
        // 任务非进行中：先办结再转
        let mut e = HashMap::new();
        e.insert("processTaskId".to_string(), json!(task_id));
        e.insert("operator".to_string(), json!("user2"));
        e.insert("submitType".to_string(), json!(1));
        assert_eq!(facade.flow("processTask/execute", &e).await["code"], 0);
        assert_eq!(facade.flow("processTask/transfer", &transfer_args(task_id, "user2", "lisi", "user2")).await["msg"], "任务非进行中，不可转办");
    }

    #[tokio::test]
    async fn test_transfer_privileged_admin_allowed() {
        let facade = make_facade();
        let (_, task_id) = start_two_step_flow(&facade, "tr-admin-flow").await;
        // flow.admin 可代转他人待办（operator != fromActor 但特权放行）
        let r = facade.flow("processTask/transfer", &transfer_args(task_id, "user2", "lisi", "flow.admin")).await;
        assert_eq!(r["code"], 0, "admin 代转应放行: {:?}", r);
    }

    // ─── 回归红线（Node 实测坑）：转办不得覆写 actor 列；转办后撤回该单不凭空出现在 fromActor 已办 ───

    #[tokio::test]
    async fn test_transfer_then_withdraw_not_in_fromactor_done_list() {
        let facade = make_facade();
        let (iid, task_id) = start_two_step_flow(&facade, "tr-done-flow").await;
        assert_eq!(facade.flow("processTask/transfer", &transfer_args(task_id, "user2", "lisi", "user2")).await["code"], 0);
        // 撤回整单（发起人）：任务离开 DOING→30
        let mut w = HashMap::new();
        w.insert("id".to_string(), json!(iid));
        w.insert("operator".to_string(), json!("applicant"));
        assert_eq!(facade.flow("processInstance/withdraw", &w).await["code"], 0);

        // 被摘走的 user2 的已办里绝不能凭空出现这条他没办过的单
        let mut d = HashMap::new();
        d.insert("operator".to_string(), json!("user2"));
        let done = facade.flow("processTask/doneList", &d).await;
        let rows = done["data"]["rows"].as_array().unwrap();
        assert!(!rows.iter().any(|t| t["processInstanceId"].as_str() == Some(&iid.to_string())),
            "user2 未办结却被摘走，不得出现在其已办: {:?}", rows);
        // 且该任务 actor 列恒无值（转办严禁覆写）
        let task = facade.repo().find_task_by_id(task_id).unwrap().unwrap();
        assert!(task.actor_id.is_none(), "撤回后 actor 列应仍为空，实测 {:?}", task.actor_id);
        // ⚠️ 本用例从 issues/117 起才**真的打在判据上**：本栈 doneList 原为
        // `task_state = 20`，撤回行（30）根本不进集合，"冒不冒单"无从判定；
        // 改成 `<> 10` 后这条 30 行进入候选，只剩 `t.operator` 归属判据在挡——
        // 若谁把 operator 写脏（如转办覆写 actor 列），这里就会当场冒出来。
        assert_eq!(task.task_state, 30, "前置核对：撤回行状态须为 30（<>10 家族才认它为已办候选）");
    }

    // ═══════════════════════════════════════════════════════
    // issues/116 批次 D · 委托自动生效（门面端到端：台账 → 建单并入参与者）
    // issues/117 · 「我已办」判据（门面路；仓储路见 memory.rs 与 sqlx 同名用例）
    // ═══════════════════════════════════════════════════════

    /// 走 `processSurrogate/save` 配台账（时间串按契约 `yyyy-MM-dd HH:mm:ss`），
    /// 再发起流程 → **参与者表真行**里必须同时有授权人与代理人（原人保留、任一可办）。
    #[tokio::test]
    async fn test_surrogate_ledger_then_auto_apply_through_facade() {
        let facade = make_facade();
        let args = args_of(vec![
            ("operator", json!("user2")),
            ("surrogate", json!("agent2")),
            ("processName", json!("surr-facade-flow")),
            ("startTime", json!("2000-01-01 00:00:00")),
            ("endTime", json!("2999-12-31 23:59:59")),
            ("createUser", json!("admin")),
        ]);
        let saved = facade.flow("processSurrogate/save", &args).await;
        assert_eq!(saved["code"], 0, "配委托应成功: {}", saved);

        let (_iid, approve_task) = start_two_step_flow(&facade, "surr-facade-flow").await;
        let mut actors = facade.repo().find_task_actors(approve_task).unwrap();
        actors.sort();
        assert_eq!(
            actors,
            vec!["agent2".to_string(), "user2".to_string()],
            "台账配好后建单即自动并入代理人（读 wf_process_task_actor 真行，不是返回码）"
        );
    }

    /// 写侧 `enabled`：0 停用 → 不并入；缺省 → 1；脏值（"abc"）→ **0 停用**，
    /// 不得折叠成启用（判据④的写侧那一半，修复前 save 根本不读该参数、恒落 1）。
    #[tokio::test]
    async fn test_surrogate_enabled_write_side_dirty_value_lands_zero() {
        for (given, want, note) in [
            (Json::Null, 1, "未传 enabled 默认 1"),
            (json!(0), 0, "显式 0 停用"),
            (json!("abc"), 0, "脏值不得当启用"),
            (json!(true), 1, "布尔 true → 1"),
        ] {
            let facade = make_facade();
            let mut pairs = vec![
                ("operator", json!("user2")),
                ("surrogate", json!("agent2")),
                ("processName", json!("surr-enabled-flow")),
            ];
            if !given.is_null() {
                pairs.push(("enabled", given.clone()));
            }
            assert_eq!(
                facade.flow("processSurrogate/save", &args_of(pairs)).await["code"],
                0,
                "{} save 应成功", note
            );
            // 回读台账 enabled 值
            let detail_id = {
                // issues/152 ② 改正既有期望（逐字交代）：这一句原本是不带 operator 的
                // `facade.flow("processSurrogate/page", &args_of(vec![]))`，靠的是"门面不注入归属
                // ⇒ page 返回全库台账"那个旧答案。page 现在自己下发 t.operator EQ 归一后的 operator，
                // 缺省档归一到 user1，而这一轮台账的授权人是 user2 ⇒ 不带 operator 就是 0 行。
                // 本用例钉的是 enabled 写侧，与归属无关，故按新契约显式带归属人。
                let page = facade.flow("processSurrogate/page",
                    &args_of(vec![("operator", json!("user2"))])).await;
                page["data"]["rows"].as_array().unwrap().first()
                    .and_then(|r| r["id"].as_str().map(|s| s.to_string()))
                    .unwrap_or_else(|| panic!("{} 台账应能查到", note))
            };
            let detail = facade
                .flow("processSurrogate/detail", &args_of(vec![("id", json!(detail_id))]))
                .await;
            assert_eq!(
                detail["data"]["enabled"].as_i64(),
                Some(want),
                "{}（回读台账 enabled 应为 {}，实得 {:?}）",
                note, want, detail["data"]["enabled"]
            );
            // 建单是否并入代理人 = enabled 判据的最终结论
            let (_iid, approve_task) = start_two_step_flow(&facade, "surr-enabled-flow").await;
            let actors = facade.repo().find_task_actors(approve_task).unwrap();
            if want == 1 {
                assert!(actors.contains(&"agent2".to_string()), "{} 生效时应并入代理人: {:?}", note, actors);
            } else {
                assert!(!actors.contains(&"agent2".to_string()), "{} 不该并入代理人: {:?}", note, actors);
            }
        }
    }

    // ═══════════════════════════════════════════════════════
    // issues/152 · 委托：① 作用域优先级 / ② 门面归属不变式 / ③ 写侧 operator 三档归一
    // 判据基准＝jeeflow-java 参考实现（JeeflowFacade.surrogatePage + applySurrogateFields +
    // JdbcProcessExtRepository.buildWhere + MemoryProcessExtRepository.pageSurrogates），
    // 条文＝spec 06 §2.5 表（新增 processSurrogate/page 行）＋ §4.5「归属不变式」＋ §4.5 条款 6。
    // ═══════════════════════════════════════════════════════

    /// `processSurrogate/page` 取回行的授权人列（operator=None 即不带这个键）。
    async fn surrogate_page_operators(facade: &JeeflowFacade, operator: Option<&str>) -> Vec<String> {
        let mut args = args_of(vec![("pageSize", json!(50))]);
        if let Some(o) = operator {
            args.insert("operator".to_string(), json!(o));
        }
        let resp = facade.flow("processSurrogate/page", &args).await;
        assert_eq!(resp["code"], 0, "processSurrogate/page 应成功: {}", resp);
        let mut ops: Vec<String> = resp["data"]["rows"].as_array().unwrap().iter()
            .map(|r| r["operator"].as_str().unwrap_or("<null>").to_string())
            .collect();
        ops.sort();
        ops
    }

    /// 走门面配一条委托（时间窗给足，判据只由本用例想钉的那一维决定）。
    async fn save_surrogate_via_facade(
        facade: &JeeflowFacade, operator: Option<&str>, agent: &str, process_name: &str,
    ) -> String {
        let mut pairs = vec![
            ("surrogate", json!(agent)),
            ("processName", json!(process_name)),
            ("startTime", json!("2000-01-01 00:00:00")),
            ("endTime", json!("2999-12-31 23:59:59")),
        ];
        if let Some(op) = operator {
            pairs.push(("operator", json!(op)));
        }
        let saved = facade.flow("processSurrogate/save", &args_of(pairs)).await;
        assert_eq!(saved["code"], 0,
            "save {}→{} 应成功: {}", operator.unwrap_or("<缺键>"), agent, saved);
        saved["data"]["id"].as_str().unwrap().to_string()
    }

    /// 档 1（①）：同授权人并存「全流程」(processName 空) ＋「精确」且都窗内 ⇒ **精确接管**。
    /// 全流程那条**后写、id 更大**：若实现按"跨作用域取最新一条"（＝内置 boot2 版「全流程优先」），
    /// 就会接管成 agent_global —— 本用例钉的就是引擎不得复活那个方向。
    /// 判据层的同一形状已钉在 `jeeflow-core::surrogate::tests::test_pick_surrogate_takes_max_id_and_falls_back`
    /// （含"精确判否 ⇒ 兜底全流程"那一腿，见 test_pick_surrogate_exact_scope_invalid_still_checks_global_scope），
    /// 本条补的是门面＋建单这一整条路。
    #[tokio::test]
    async fn test_i152_surrogate_exact_scope_beats_global_through_facade() {
        let facade = make_facade();
        save_surrogate_via_facade(&facade, Some("user2"), "agent_exact", "i152-scope-flow").await;
        save_surrogate_via_facade(&facade, Some("user2"), "agent_global", "").await;   // id 更大

        let (_iid, task) = start_two_step_flow(&facade, "i152-scope-flow").await;
        let mut actors = facade.repo().find_task_actors(task).unwrap();
        actors.sort();
        assert_eq!(actors, vec!["agent_exact".to_string(), "user2".to_string()],
            "精确作用域那条必须接管（全流程盖精确＝内置版方向，不得复活）");
        assert!(!actors.contains(&"agent_global".to_string()));
    }

    /// 档 4（③ 的门票，改前本栈必红）：门面 save 的 operator 三档（缺键／空串／全空白）
    /// 一律走 §2.5 归一落 `user1`，**不得落空串**——空串行是「死行」：
    /// `get_surrogate` 的 `WHERE operator = ?` 永不命中它，台账看得见、待办永远不并人。
    /// 正面判据：以归一后的授权人（user1）建单，代理人真并进 wf_process_task_actor。
    #[tokio::test]
    async fn test_i152_surrogate_save_operator_three_tiers_land_on_user1_and_apply() {
        for (label, given) in
            [("缺键", None), ("空串", Some("")), ("全空白", Some("   "))]
        {
            let facade = make_facade();
            let id = save_surrogate_via_facade(
                &facade, given, "agent152", "i152-default-flow").await;
            let detail = facade
                .flow("processSurrogate/detail", &args_of(vec![("id", json!(id))])).await;
            assert_eq!(detail["data"]["operator"].as_str(), Some("user1"),
                "{label}档必须落 §2.5 归一缺省 user1（空串＝死行），实得 {:?}: {}",
                detail["data"]["operator"].as_str(), detail);

            // 正面判据：这一行必须能被该缺省用户的后续待办命中
            let (_iid, task) =
                start_two_step_flow_assigned(&facade, "i152-default-flow", "user1").await;
            let actors = facade.repo().find_task_actors(task).unwrap();
            assert!(actors.contains(&"agent152".to_string()),
                "{label}档的行要能被 user1 的待办命中，实得 {:?}", actors);
        }
    }

    /// 档 4 的 update 腿（B）：operator 缺键／空串／全空白一律**保留原授权人**，
    /// 只有显式非空值才覆盖（java `applySurrogateFields` 同形；空白档覆写同样造死行）。
    #[tokio::test]
    async fn test_i152_surrogate_update_operator_tiers_keep_original() {
        for (label, given, want) in [
            ("缺键", None, "op152"),
            ("空串", Some(""), "op152"),
            ("全空白", Some("   "), "op152"),
            ("显式非空", Some("someoneelse"), "someoneelse"),
        ] {
            let facade = make_facade();
            let id = save_surrogate_via_facade(&facade, Some("op152"), "dep152", "i152-upd-flow").await;
            let mut pairs = vec![("id", json!(id.clone())), ("surrogate", json!("dep152b"))];
            if let Some(v) = given {
                pairs.push(("operator", json!(v)));
            }
            let upd = facade.flow("processSurrogate/update", &args_of(pairs)).await;
            assert_eq!(upd["code"], 0, "{label} update 应成功: {}", upd);
            let detail = facade
                .flow("processSurrogate/detail", &args_of(vec![("id", json!(id))])).await;
            assert_eq!(detail["data"]["operator"].as_str(), Some(want),
                "{label}档 update 后授权人应为 {}，实得 {:?}（空白档抹掉原授权人＝造死行）",
                want, detail["data"]["operator"].as_str());
            assert_eq!(detail["data"]["surrogate"].as_str(), Some("dep152b"),
                "{label}档回归：其余字段照常更新");
        }
    }

    /// 档 5（② 门面层）：`processSurrogate/page` 归属——带 operator 只见自己；
    /// 不带／空串／全空白 ⇒ 只出归一缺省 `user1` 的行，**绝不允许退化成全库台账**。
    /// 反向哨兵＝zhangsan 那行真实存在（缺省档若读全库就会多出一行，判据当场红）。
    #[tokio::test]
    async fn test_i152_surrogate_page_ownership_only_own_rows() {
        let facade = make_facade();
        save_surrogate_via_facade(&facade, Some("user1"), "depMine", "i152-page-flow").await;
        save_surrogate_via_facade(&facade, Some("zhangsan"), "depOther", "i152-page-flow").await;

        // 正向对照：两档各有行（否则"只出 user1"是空表自等假绿）
        assert_eq!(surrogate_page_operators(&facade, Some("user1")).await, vec!["user1".to_string()]);
        assert_eq!(surrogate_page_operators(&facade, Some("zhangsan")).await,
            vec!["zhangsan".to_string()]);
        // 缺省三档 ⇒ 归一到 user1，不得全库（2 行）
        for (label, op) in [("缺 operator", None), ("空串", Some("")), ("全空白", Some("   "))] {
            assert_eq!(surrogate_page_operators(&facade, op).await, vec!["user1".to_string()],
                "{label}档不得退化成全库台账");
        }
        assert!(surrogate_page_operators(&facade, Some("nobody152")).await.is_empty(),
            "不存在的人必须 0 行");
    }

    /// 档 5（② 仓储层第二道，本栈内存仓）：绕过门面直调 `page_surrogates`，
    /// 归属通道 `query.operator` 整条没给（None）或给的是空值 ⇒ **空页**。
    /// sqlx 仓同判据（真库读数见 `jeeflow-repository-sqlx` 的 test_mysql_i152_*，属 T1）。
    #[test]
    fn test_i152_surrogate_repo_blank_ownership_returns_empty_page() {
        let repo = Arc::new(MemoryRepository::new());
        for op in ["user1", "zhangsan"] {
            let mut sg = ProcessSurrogate {
                id: 0,
                process_name: "i152-repo-flow".into(),
                operator: op.into(),
                surrogate: format!("dep-{}", op),
                start_time: Some("2000-01-01 00:00:00".into()),
                end_time: Some("2999-12-31 23:59:59".into()),
                enabled: 1,
                create_time: None, create_user: None,
                update_time: None, update_user: None,
            };
            repo.save_surrogate(&mut sg).unwrap();
        }

        for blank in [None, Some(""), Some("   "), Some("\t")] {
            let mut q = PageQuery::new(1, 20);
            q.operator = blank.map(str::to_string);
            assert_eq!(repo.page_surrogates(&q).unwrap().record_count, 0,
                "归属值 [{:?}] 为空不得退化成全库", blank);
        }
        // 正向对照：真值必须出行（防"恒空假绿"）
        for op in ["user1", "zhangsan"] {
            let mut q = PageQuery::new(1, 20);
            q.operator = Some(op.to_string());
            assert_eq!(repo.page_surrogates(&q).unwrap().record_count, 1,
                "{} 应有 1 条委托", op);
        }
    }

    /// `processTask/doneList` 取回已办行的 id 集（op=None 即不传 operator）。
    async fn done_list_ids(facade: &JeeflowFacade, op: Option<&str>) -> Vec<String> {
        let mut d = HashMap::new();
        if let Some(o) = op {
            d.insert("operator".to_string(), json!(o));
        }
        let resp = facade.flow("processTask/doneList", &d).await;
        assert_eq!(resp["code"], 0, "doneList 应成功: {}", resp);
        let rows = resp["data"]["rows"].as_array().unwrap();
        assert_eq!(
            resp["data"]["recordCount"].as_i64(),
            Some(rows.len() as i64),
            "recordCount 与行数须自洽"
        );
        rows.iter().filter_map(|r| r["id"].as_str().map(str::to_string)).collect()
    }

    /// issues/117 门面路：空 operator 不泄漏全库 + 发起人不得因 create_user 沾到
    /// "我发起但非我办理"的行 + 我真正办结的行必须在（正向对照，防"恒空假绿"）。
    #[tokio::test]
    async fn test_i117_done_list_ownership_and_empty_operator() {
        let facade = make_facade();
        let (iid, approve_task) = start_two_step_flow(&facade, "i117-done-flow").await;
        // 撤回整单：approve 行 Doing→Withdraw(30)，它的 create_user 是发起人 applicant、
        // operator 空（没被人办过）。旧 `=20` 判据下这行不进集合 → create_user 偏宽被掩盖；
        // 现 `<>10` 承认经手态，正是它暴露的时刻。
        let w = args_of(vec![("id", json!(iid.to_string())), ("operator", json!("applicant"))]);
        assert_eq!(facade.flow("processInstance/withdraw", &w).await["code"], 0);
        let withdrawn = facade.repo().find_task_by_id(approve_task).unwrap().unwrap();
        assert_eq!(withdrawn.task_state, 30, "前置核对：撤回行应为 30");
        assert_eq!(withdrawn.create_user.as_deref(), Some("applicant"),
            "前置核对：该行创建人是发起人（诱饵成立）");

        // ① operator 为空 → 空页（不得返回全库已办）
        assert!(done_list_ids(&facade, None).await.is_empty(),
            "operator 缺省必须空页，不得泄漏全库");
        assert!(done_list_ids(&facade, Some("   ")).await.is_empty(),
            "operator 全空白同空值");

        // ② 发起人不得因 create_user 沾上他从没办过的 approve 行
        let applicant_rows = done_list_ids(&facade, Some("applicant")).await;
        assert!(!applicant_rows.contains(&approve_task.to_string()),
            "「我发起但非我办理」不得进我的已办（契约 §2.5 点名禁止 create_user 偏宽）：{:?}",
            applicant_rows);

        // ③ 正向对照：applicant 真正办结的 apply 行必须在他的已办里（防恒空假绿）
        let apply_task = facade.repo().find_history_tasks(iid).unwrap()
            .into_iter().find(|t| t.task_name == "apply").expect("应有 apply 行");
        assert!(applicant_rows.contains(&apply_task.task_id.to_string()),
            "已办结的 apply 行应在 applicant 已办里：{:?}", applicant_rows);
    }

    /// issues/120：门面 `NOW()` 占位符与写库审计列必须是**同一个时钟出口**。
    /// 此前此处吃 `chrono::Local`、而 create_time 走 UTC ⇒ 同一次响应里两套基准。
    #[test]
    fn test_i120_facade_now_follows_engine_clock() {
        const SENTINEL: &str = "2099-12-31 23:59:59";
        let ph = Json::String("NOW()".to_string());
        {
            let _scope = jeeflow_core::clock::ClockScope::injected(|| SENTINEL.to_string());
            assert_eq!(
                format_time_value(&ph),
                Json::String(SENTINEL.to_string()),
                "注入时钟后 NOW() 必须给注入值，不得自己吃 chrono::Local"
            );
        }
        let _idle = jeeflow_core::clock::lock_scope();
        match format_time_value(&ph) {
            Json::String(s) => {
                assert!(
                    chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S").is_ok(),
                    "出作用域后 NOW() 仍应解析为 yyyy-MM-dd HH:mm:ss，实得 {s}"
                );
                assert_ne!(s, SENTINEL, "ClockScope 作用域结束后不得残留注入值");
            }
            other => panic!("NOW() 应解析为字符串，实得 {other:?}"),
        }
    }
    /// issues/121 P1：建单必写 parent_task_id 与行级 isFirstTaskNode，且门面出口"行上值优先"。
    /// 夹具是 apply→a→b→approve 四节点链（两步流里"上一节点"与"首任务节点"同格＝断言恒真）。
    #[tokio::test]
    async fn test_i121_p1_lineage_written_on_create() {
        let facade = make_facade();
        let (iid, approve_task) = start_chain_flow(&facade, "lineage121").await;
        let hist = facade.repo().find_history_tasks(iid).unwrap();
        let flag_of = |name: &str| -> Option<bool> {
            hist.iter().find(|t| t.task_name == name)
                .and_then(|t| t.variables.get("isFirstTaskNode"))
                .and_then(|v| v.as_bool())
        };
        let parent_of = |name: &str| -> Option<i64> {
            hist.iter().find(|t| t.task_name == name).and_then(|t| t.parent_task_id)
        };
        let id_of = |name: &str| -> i64 {
            hist.iter().find(|t| t.task_name == name).expect(name).task_id
        };

        // 正向：链式血缘逐条对账（parent＝刚办结的那条）
        assert_eq!(parent_of("apply"), Some(0), "发起那条 execution 无当前任务 ⇒ parent 落 0");
        assert_eq!(parent_of("a"), Some(id_of("apply")), "a.parent 应为 apply.id");
        assert_eq!(parent_of("b"), Some(id_of("a")), "b.parent 应为 a.id");
        let approve = facade.repo().find_task_by_id(approve_task).unwrap().expect("approve 行");
        assert_eq!(approve.parent_task_id, Some(id_of("b")), "approve.parent 应为 b.id");

        // 首节点标记随行存活（apply 此刻已办结）
        assert_eq!(flag_of("apply"), Some(true), "首任务节点行应落 true，且历史行上还在");
        assert_eq!(flag_of("a"), Some(false), "非首节点必须 false");
        assert_eq!(flag_of("b"), Some(false));
        assert_eq!(approve.variables.get("isFirstTaskNode").and_then(|v| v.as_bool()),
                   Some(false), "进行中行的标记也应随行落库");

        // 门面出口读回：历史行也报 true（行上值优先）
        let mut detail_args = HashMap::new();
        detail_args.insert("id".to_string(), json!(iid));
        let ext_of = |resp: &Json, name: &str| -> Option<bool> {
            resp["data"]["tasks"].as_array().unwrap().iter()
                .find(|r| r["taskName"].as_str() == Some(name))
                .and_then(|r| r["ext"]["isFirstTaskNode"].as_bool())
        };
        let resp = facade.flow("processInstance/detail", &detail_args).await;
        assert_eq!(ext_of(&resp, "apply"), Some(true),
            "已办结的 apply 行出口应给行上值 true（纯现算版此处恒 false，正是引擎不能靠现算的理由）");

        // 存量行形状：抹掉行上标记 ⇒ 回退现算 ⇒ 历史行只能给 false
        let mut row = hist.iter().find(|t| t.task_name == "apply").cloned().unwrap();
        row.variables.remove("isFirstTaskNode");
        facade.repo().update_task(&row).unwrap();
        let resp2 = facade.flow("processInstance/detail", &detail_args).await;
        assert_eq!(ext_of(&resp2, "apply"), Some(false),
            "缺键的存量历史行回退现算：仅进行中判定 ⇒ false（不报错、不读成未定义）");
    }

    // ═══════════════════════════════════════════════════════
    // issues/127 ＋ 132 · 事件代码腿（门面侧三支：7 转办 / 8 撤回 / 4 手动抄送）
    //   唯一权威＝规范 11 docs/spec/11-events.md §11.3／§11.7，可判定条目见 08-compliance「事件契约」
    //   场景 33·34。判据形状按 spec 要求：recorder 断"收到 ＋ 顺序 ＋ **时机**"——
    //   监听器在 fire 当场回读仓储，落库在前、fire 在后（§11.2 原则 3，先 fire 后落库即红）。
    // ═══════════════════════════════════════════════════════

    /// fire 当场从仓储读回的状态快照（证明"事件排在落库之后"）
    #[derive(Clone)]
    struct SeenAtFire {
        event: ProcessEvent,
        instance_state: Option<i32>,
        task_states: Vec<(i64, i32)>,
        task_actors: Vec<String>,
    }

    struct FacadeProbe {
        repo: Arc<MemoryRepository>,
        seen: std::sync::Mutex<Vec<SeenAtFire>>,
    }
    impl FacadeProbe {
        fn new(repo: Arc<MemoryRepository>) -> Self {
            FacadeProbe { repo, seen: std::sync::Mutex::new(Vec::new()) }
        }
    }
    impl ProcessEventListener for FacadeProbe {
        fn on_event(&self, event: &ProcessEvent) {
            let iid = event.data.get_i64("instanceId").unwrap_or(event.source_id);
            let instance_state = self.repo.find_instance_by_id(iid).ok().flatten().map(|i| i.state);
            let task_states: Vec<(i64, i32)> = self.repo.find_history_tasks(iid).unwrap_or_default()
                .into_iter().map(|t| (t.task_id, t.task_state)).collect();
            let tid = event.data.get_i64("taskId").unwrap_or(event.source_id);
            let task_actors = self.repo.find_task_actors(tid).unwrap_or_default();
            self.seen.lock().unwrap().push(SeenAtFire {
                event: event.clone(), instance_state, task_states, task_actors });
        }
    }

    /// 装了 probe 的门面（与 `make_facade` 同夹具，只多一个监听器）
    fn make_probe_facade() -> (JeeflowFacade, Arc<MemoryRepository>, Arc<FacadeProbe>) {
        let repo = Arc::new(MemoryRepository::new());
        let probe = Arc::new(FacadeProbe::new(repo.clone()));
        let mut ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));
        ctx.register_event_listener(probe.clone());
        (JeeflowFacade::new(ctx), repo, probe)
    }

    fn seen_names(seen: &[SeenAtFire]) -> Vec<&'static str> {
        seen.iter().map(|s| s.event.event_type.spec_name()).collect()
    }

    /// 08 场景 34 · 码 8 `TASK_WITHDRAW`：撤回把实例写 30、被撤任务行更新完成后 fire
    /// **恰好一次**（不逐任务），载荷 `instanceId` / `operator`，sourceId＝instanceId。
    #[tokio::test]
    async fn test_i132_withdraw_fires_task_withdraw_once_after_persist() {
        let (facade, _repo, probe) = make_probe_facade();
        let (iid, task_id) = start_two_step_flow(&facade, "ev132-wd").await;
        probe.seen.lock().unwrap().clear();   // 只留撤回这一轮

        let resp = facade.flow("processInstance/withdraw", &wd134_withdraw_args(iid, "applicant")).await;
        assert_eq!(resp["code"], 0, "撤回应成功：{:?}", resp);

        let seen = probe.seen.lock().unwrap().clone();
        assert_eq!(seen_names(&seen), vec!["TASK_WITHDRAW"],
            "每轮撤回只 fire 一次码 8，不得逐任务发、也不得捎带其它码");
        let s = &seen[0];
        assert_eq!(s.event.source_id, iid, "码 8 sourceId＝instanceId");
        assert_eq!(s.event.data.get_i64("instanceId"), Some(iid), "载荷键 instanceId");
        assert_eq!(s.event.data.get_str("operator"), Some("applicant"), "载荷键 operator＝撤回人");
        // 时机：fire 当场实例与任务行都已是 30（改序到落库之前 ⇒ 这两格红）
        assert_eq!(s.instance_state, Some(30), "fire 时实例 state=30 必须已落库");
        assert!(s.task_states.contains(&(task_id, 30)),
            "fire 时被撤任务行必须已落 30（issues/113 级联腿），实得 {:?}", s.task_states);
    }

    /// 负向：issues/134 状态守卫拒掉的那一次撤回**一行都不落库** ⇒ 也**不得** fire 码 8。
    #[tokio::test]
    async fn test_i132_guarded_withdraw_fires_nothing() {
        let (facade, _repo, probe) = make_probe_facade();
        let (iid, task_id) = start_two_step_flow(&facade, "ev132-wd-guard").await;
        // 先把实例推到已办结(20)：直接撤掉唯一在办任务 → 走到 end
        let mut a = HashMap::new();
        a.insert("processTaskId".to_string(), json!(task_id));
        a.insert("operator".to_string(), json!("user2"));
        a.insert("submitType".to_string(), json!(1));
        assert_eq!(facade.flow("processTask/execute", &a).await["code"], 0);
        assert_eq!(wd134_instance(&facade, iid).0, 20, "夹具前提：实例已办结");
        probe.seen.lock().unwrap().clear();

        let resp = facade.flow("processInstance/withdraw", &wd134_withdraw_args(iid, "applicant")).await;
        assert_eq!(resp["code"], 99999999, "已办结实例撤回必须报错：{:?}", resp);
        assert_eq!(resp["msg"], WD134_MSG, "文案逐字：{:?}", resp);
        let seen = seen_names(&probe.seen.lock().unwrap().clone());
        assert!(seen.is_empty(), "被守卫拒掉的撤回严禁 fire 码 8（没发生的事实不发消息），实得 {:?}", seen);
    }

    /// 08 场景 34 · 码 7 `TASK_TRANSFER`：参与者被替换并落库之后 fire，
    /// sourceId＝taskId，载荷五键 `instanceId` / `taskId` / `fromActor` / `toActor` / `operator`。
    #[tokio::test]
    async fn test_i132_transfer_fires_task_transfer_after_actor_swap() {
        let (facade, _repo, probe) = make_probe_facade();
        let (iid, task_id) = start_two_step_flow(&facade, "ev132-tr").await;
        probe.seen.lock().unwrap().clear();

        let resp = facade.flow("processTask/transfer", &transfer_args(task_id, "user2", "user9", "user2")).await;
        assert_eq!(resp["code"], 0, "转办应成功：{:?}", resp);

        let seen = probe.seen.lock().unwrap().clone();
        assert_eq!(seen_names(&seen), vec!["TASK_TRANSFER"],
            "转办只 fire 一次码 7；转办不新建任务行 ⇒ 不得捎带码 3");
        let s = &seen[0];
        assert_eq!(s.event.source_id, task_id, "码 7 sourceId＝taskId");
        assert_eq!(s.event.data.get_i64("instanceId"), Some(iid));
        assert_eq!(s.event.data.get_i64("taskId"), Some(task_id));
        assert_eq!(s.event.data.get_str("fromActor"), Some("user2"), "载荷键 fromActor");
        assert_eq!(s.event.data.get_str("toActor"), Some("user9"), "载荷键 toActor");
        assert_eq!(s.event.data.get_str("operator"), Some("user2"), "载荷键 operator");
        // 时机：fire 当场参与者表已换人（先 fire 后落库 ⇒ 监听器会读到旧参与者）
        assert_eq!(s.task_actors, vec!["user9".to_string()],
            "fire 时参与者应已替换为 toActor，实得 {:?}", s.task_actors);
    }

    /// 负向：转办被拒（原办理人不是参与人）⇒ 参与者表不动 ⇒ 不得 fire 码 7。
    #[tokio::test]
    async fn test_i132_rejected_transfer_fires_nothing() {
        let (facade, _repo, probe) = make_probe_facade();
        let (_iid, task_id) = start_two_step_flow(&facade, "ev132-tr-guard").await;
        probe.seen.lock().unwrap().clear();

        let resp = facade.flow("processTask/transfer", &transfer_args(task_id, "ghost", "user9", "ghost")).await;
        assert_eq!(resp["code"], 99999999, "非参与人转办必须报错：{:?}", resp);
        assert!(probe.seen.lock().unwrap().is_empty(), "被拒的转办严禁 fire 码 7");
    }

    /// 08 场景 33 · 手动支与引擎支**归一**（§11.2 原则 1）：门面 `createCCInstance` 逐抄送人
    /// fire 码 4，载荷 `ccActorId` 与事件体同源，且 fire 时 cc 行已落库。
    /// （既有的 `test_cc_create_fired_on_manual_create_cc` 只数了次数，本格补载荷键 ＋ 时机。）
    #[tokio::test]
    async fn test_i132_manual_cc_create_payload_and_timing() {
        let (facade, repo, probe) = make_probe_facade();
        let mut a = HashMap::new();
        a.insert("processInstanceId".to_string(), json!(2002));
        a.insert("operator".to_string(), json!("user1"));
        a.insert("actorIds".to_string(), json!(["u3", "u4"]));
        let resp = facade.flow("processInstance/createCCInstance", &a).await;
        assert_eq!(resp["code"], 0, "{:?}", resp);

        let seen = probe.seen.lock().unwrap().clone();
        assert_eq!(seen_names(&seen), vec!["CC_CREATE", "CC_CREATE"], "逐抄送人各 fire 一次");
        for (s, want) in seen.iter().zip(["u3", "u4"]) {
            assert_eq!(s.event.source_id, 2002, "码 4 sourceId＝instanceId");
            assert_eq!(s.event.cc_actor_id.as_deref(), Some(want));
            assert_eq!(s.event.data.get_str("ccActorId"), Some(want), "载荷键 ccActorId（§11.3 码 4）");
            // 时机：fire 当场 cc 行已在库里（接收人档回读）
            let mut q = PageQuery::new(1, 10);
            q.operator = Some(want.to_string());
            assert_eq!(repo.page_cc_instances(&q).unwrap().record_count, 1,
                "fire 时抄送人 {want} 的 cc 行应已落库");
        }
    }

    /// 不发清单（08 场景 35）在门面侧的对照：`updateCCStatus`（读已读状态回写）不是新事实
    /// ⇒ 不得 fire CC_CREATE；`processInstance/page` 等只读 action 同样零事件。
    #[tokio::test]
    async fn test_i132_no_fire_for_cc_status_update_and_reads() {
        let (facade, _repo, probe) = make_probe_facade();
        let (iid, _) = start_two_step_flow(&facade, "ev132-not-fire").await;
        probe.seen.lock().unwrap().clear();

        let mut a = HashMap::new();
        a.insert("processInstanceId".to_string(), json!(iid));
        a.insert("operator".to_string(), json!("user2"));
        assert_eq!(facade.flow("processInstance/updateCCStatus", &a).await["code"], 0);
        assert_eq!(facade.flow("processTask/todoList", &HashMap::new()).await["code"], 0);
        assert_eq!(facade.flow("processInstance/detail", &a).await["code"], 0);

        let seen = seen_names(&probe.seen.lock().unwrap().clone());
        assert!(seen.is_empty(),
            "已读回写与只读 action 一律不 fire（§11.4 不发清单），实得 {:?}", seen);
    }

    // ═══════════════════════════════════════════════════════
    // issues/142 B 批 · 任务参与者写侧归属值归一（spec 06-facade.md §2.11）
    //   普查实读的 rust 形状（本组用例逐条对着钉）：
    //   · 门面 `arg_actor_ids` 数组腿只 `filter(!s.is_empty())` ⇒ **不 trim**（`"  "` 存活并
    //     真落进 `wf_process_task_actor.actor_id`）、同次调用不折叠，而逗号串腿才 trim
    //     ＝**两形两个答案**；
    //   · `transfer` 的 fromActor/toActor 只判必填、存的是**未 trim 的原值**；
    //   · `processTaskId` 给 `0` 不拦（§2.11 末段：主键另判一档）。
    //   判据本体＝`jeeflow_core::model::normalize_actors`（与抄送侧 §2.10 同一枚单点）。
    //   两仓写侧兜底另有格：内存仓见 `memory.rs::actor_i142_tests`，真库见 repository-sqlx。
    // ═══════════════════════════════════════════════════════

    const I142_TASK: i64 = 914201;

    fn surrogate_args(task_id: i64, actors: Json) -> HashMap<String, Json> {
        let mut m = HashMap::new();
        m.insert("processTaskId".to_string(), json!(task_id));
        m.insert("actorIds".to_string(), actors);
        m
    }

    fn actors_of(facade: &JeeflowFacade, task_id: i64) -> Vec<String> {
        facade.repo().find_task_actors(task_id).unwrap()
    }

    /// 拆形单点本体（**直接打 `arg_actor_ids`**，不等仓储写侧兜底）：两形同判据＋trim＋丢空＋折叠。
    /// 与下面几条端到端格分工不同：端到端格在"两层都挡"（§2.11 硬要求①）之下会被写侧兜住
    /// ——还原门面腿单独跑一次，端到端只有"全空白 ⇒ 信封"那格红（`{"code":0,"msg":"成功"}`），
    /// 门面腿自身的红只有打这一支才照得出来。
    /// 改前实测（还原跑一次的红格读数）：数组腿 `[" i142a ", "", "  ", "i142a", "i142b"]` 返回
    /// `[" i142a ", "  ", "i142a", "i142b"]`——不 trim、不折叠、纯空白活着；逗号串腿才 trim＋丢空
    /// ⇒ 同一批人两形两个答案（`["i142x","i142y"]` vs `[" i142x ","i142y "]`）。
    #[test]
    fn test_i142_b_arg_actor_ids_both_forms_single_judge() {
        for (arr, want) in [
            (vec![" i142a ", "", "  ", "i142a", "i142b"], vec!["i142a", "i142b"]),
            (vec!["0", "00", " ", "a"], vec!["0", "00", "a"]),
            (vec!["i142e", "i142e"], vec!["i142e"]),
        ] {
            let w: Vec<String> = want.iter().map(|s| s.to_string()).collect();
            assert_eq!(arg_actor_ids(&surrogate_args(I142_TASK, json!(arr))), w,
                "§2.11：数组腿（{arr:?}）必须 trim＋丢空＋折叠");
            // 同一批人换逗号串写法 ⇒ 逐字同答案（两形同判据）
            let csv = format!("{} ", want.join(","));
            assert_eq!(arg_actor_ids(&surrogate_args(I142_TASK, json!(csv))), w,
                "§2.11：逗号串腿（{csv:?}）与数组腿同判据");
        }
        // 全空白批次丢完为空（调用方据此走既有"缺参数"档）
        assert!(arg_actor_ids(&surrogate_args(I142_TASK, json!(["", "  ", "\t"]))).is_empty(),
            "§2.11：全空白 ⇒ 空集合");
        assert!(arg_actor_ids(&surrogate_args(I142_TASK, json!(" , , "))).is_empty(),
            "§2.11：逗号串全空白 ⇒ 同一个空集合");
        // 数字元素收成字符串后照样过判据
        assert_eq!(arg_actor_ids(&surrogate_args(I142_TASK, json!([1001, " 1002 ", 1001]))),
            vec!["1001".to_string(), "1002".to_string()], "数字元素 to_string 后仍 trim＋折叠");
        // null 元素丢弃（别栈在这里串化成 "null"/"<nil>"/"None"）
        assert_eq!(arg_actor_ids(&surrogate_args(I142_TASK, json!(["i142f", null, "i142f"]))),
            vec!["i142f".to_string()], "null 元素丢弃且不串化");
        assert!(arg_actor_ids(&HashMap::new()).is_empty(), "整条没给 ⇒ 空集合");
    }

    /// 发起腿 `f_nextNodeOperator` 的**数组形态也要转出去**（§2.11 表第三行的另一半：值还没到
    /// 消费腿就被丢了）。旧写法 `flow_data.get_str("f_nextNodeOperator")` 只认字符串 ⇒
    /// 前端「指定下一节点处理人」UserSelect(multiple) 提交的数组在这一道整条静默丢弃
    /// （与普查点名 moon 消费腿 `get_str` 同一形状），指定根本不进引擎。
    /// 改前实测：approve 的参与者仍是定义里的 `user2`（指定 `user3` 不生效）。
    #[tokio::test]
    async fn test_i142_b_start_leg_forwards_array_next_node_operator() {
        let facade = make_facade();
        let mut a1 = HashMap::new();
        a1.insert("name".to_string(), json!("i142-b-f-next"));
        a1.insert("displayName".to_string(), json!("i142-b-f-next"));
        let r1 = facade.flow("processDesign/save", &a1).await;
        assert_eq!(r1["code"], 0, "{r1}");
        let design_id = r1["data"]["id"].as_str().unwrap().parse::<i64>().unwrap();

        let mut a2 = HashMap::new();
        a2.insert("id".to_string(), json!(design_id));
        a2.insert("content".to_string(), json!(r#"{
            "name":"i142-b-f-next","displayName":"i142-b-f-next","type":"approval",
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
        }"#));
        assert_eq!(facade.flow("processDesign/updateDefine", &a2).await["code"], 0);
        let mut a3 = HashMap::new();
        a3.insert("id".to_string(), json!(design_id));
        assert_eq!(facade.flow("processDesign/deploy", &a3).await["code"], 0);

        let mut a4 = HashMap::new();
        a4.insert("name".to_string(), json!("i142-b-f-next"));
        a4.insert("operator".to_string(), json!("applicant"));
        // 数组形态 ＋ 带空格与空元素：两形同判据 ⇒ 落库值是 trim 后的串
        a4.insert("f_nextNodeOperator".to_string(), json!([" user3 ", "", "  "]));
        let r4 = facade.flow("processDefine/startAndExecute", &a4).await;
        assert_eq!(r4["code"], 0, "{r4}");
        let iid: i64 = r4["data"]["processInstanceId"].as_str().unwrap().parse().unwrap();

        let doing = facade.repo().find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(doing.len(), 1, "夹具前提：停在 approve 一条待办");
        assert_eq!(facade.repo().find_task_actors(doing[0].task_id).unwrap(),
            vec!["user3".to_string()],
            "§2.11：发起腿数组形态要转出到引擎，空元素丢弃、值取 trim 后的串");
    }

    /// 正向对照：正常参与者照旧逐个落台账（钉"归一不许顺手吃掉正常值"，这一格改前也不红）。
    #[tokio::test]
    async fn test_i142_b_surrogate_positive_control_keeps_valid_actors() {
        let facade = make_facade();
        let r = facade.flow("processTask/surrogate", &surrogate_args(I142_TASK, json!(["7501", "7502"]))).await;
        assert_eq!(r["code"], 0, "{r}");
        assert_eq!(actors_of(&facade, I142_TASK), vec!["7501".to_string(), "7502".to_string()],
            "正向对照：非空参与者逐个落库、顺序随入参");
    }

    /// 数组腿：逐元素 trim ⇒ 空串/纯空白丢弃 ⇒ 同一次调用内折叠（§2.11 表第一行＋硬要求②）。
    /// 改前实测：台账里是 `[" i142a ", "", "  ", "i142a", "i142b"]`——空白值活着、同一人两行。
    #[tokio::test]
    async fn test_i142_b_surrogate_array_arm_trims_drops_and_folds() {
        let facade = make_facade();
        let r = facade.flow("processTask/surrogate",
            &surrogate_args(I142_TASK, json!([" i142a ", "", "  ", "i142a", "i142b"]))).await;
        assert_eq!(r["code"], 0, "{r}");
        assert_eq!(actors_of(&facade, I142_TASK), vec!["i142a".to_string(), "i142b".to_string()],
            "§2.11：数组腿必须 trim＋丢空＋折叠，落库值取 trim 后的串");
    }

    /// 两形同判据：同一批人换两种写法 ⇒ 台账集合**逐字相同**（普查点名的"只修一条腿"）。
    /// 改前实测：串腿给 `["i142x","i142y"]`、数组腿给 `[" i142x "," i142y ",""]`，两个答案。
    #[tokio::test]
    async fn test_i142_b_surrogate_csv_and_array_same_judgement() {
        for (i, (csv, arr, want)) in [
            ("i142x, i142y", vec![" i142x ", "i142y "], vec!["i142x", "i142y"]),
            (" i142m ,,i142n,", vec![" i142m ", "", "i142n"], vec!["i142m", "i142n"]),
            ("", Vec::<&str>::new(), Vec::<&str>::new()),
        ]
        .into_iter()
        .enumerate()
        {
            let csv_task = I142_TASK + i as i64 * 10;
            let arr_task = csv_task + 1;
            let facade = make_facade();
            let rc = facade.flow("processTask/surrogate", &surrogate_args(csv_task, json!(csv))).await;
            let ra = facade.flow("processTask/surrogate", &surrogate_args(arr_task, json!(arr))).await;
            if want.is_empty() {
                // 丢完为空 ⇒ 与既有"缺参数"档同判（硬要求③），不是"成功但什么都没写"
                assert_eq!(rc["msg"], "processTaskId/actorIds 缺失", "第 {i} 档逗号串：空集合信封 {rc}");
                assert_eq!(ra["msg"], "processTaskId/actorIds 缺失", "第 {i} 档数组：空集合信封 {ra}");
            } else {
                assert_eq!(rc["code"], 0, "第 {i} 档逗号串应成功：{rc}");
                assert_eq!(ra["code"], 0, "第 {i} 档数组应成功：{ra}");
            }
            assert_eq!(actors_of(&facade, csv_task), actors_of(&facade, arr_task),
                "§2.11 两形同判据第 {i} 档：逗号串 {csv:?} 与数组 {arr:?} 必须同答案");
            assert_eq!(actors_of(&facade, csv_task),
                want.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                "第 {i} 档落库值＝trim 后的人");
        }
    }

    /// 全空白批次 ⇒ 台账零行，并且与"空集合"返回**逐字同档**（§2.11 硬要求③不新造信封）。
    /// 改前实测：`["   "]`／制表符一支返回 code=0 并真落一条 `actor_id='   '` 的行。
    #[tokio::test]
    async fn test_i142_b_surrogate_all_blank_batch_same_bucket_as_missing() {
        for actors in [json!([""]), json!(["   "]), json!(["\t"]), json!(["", "  ", "\t"]),
                       json!("  "), json!(" , , ")] {
            let facade = make_facade();
            let r = facade.flow("processTask/surrogate", &surrogate_args(I142_TASK, actors.clone())).await;
            assert_eq!(r["msg"], "processTaskId/actorIds 缺失",
                "§2.11：全空白（{actors}）必须与既有\"缺参数\"档同判，实得 {r}");
            assert_eq!(r["code"], 99999999, "同档＝同一个码，不新造错误语义：{r}");
            assert!(actors_of(&facade, I142_TASK).is_empty(),
                "§2.11：全空白不得落进 actor_id（{actors}）");

            let empty = facade.flow("processTask/surrogate", &surrogate_args(I142_TASK + 1, json!([]))).await;
            assert_eq!(r, empty, "§2.11：全空白与空集合必须返回逐字一致");
        }
    }

    /// 数组里的数字元素收成字符串后照样过判据（§2.11：不得静默丢弃、不得串化成类型名）。
    #[tokio::test]
    async fn test_i142_b_surrogate_numeric_elements_not_dropped() {
        let facade = make_facade();
        let r = facade.flow("processTask/surrogate",
            &surrogate_args(I142_TASK, json!([1001, "1002", 1003]))).await;
        assert_eq!(r["code"], 0, "{r}");
        assert_eq!(actors_of(&facade, I142_TASK),
            vec!["1001".to_string(), "1002".to_string(), "1003".to_string()],
            "数字 id 收成字符串，与字符串写法同判据");
    }

    /// 反向哨兵（§2.11 硬要求④）：`"0"`、`"00"`、`" "`、`"a"` 是**三个人**。
    /// 改前实测：台账落成 `["0","00"," ","a"]` 四行——纯空白被当成"人"，正是判空没 trim。
    #[tokio::test]
    async fn test_i142_b_surrogate_sentinel_four_are_three_people() {
        let facade = make_facade();
        let r = facade.flow("processTask/surrogate",
            &surrogate_args(I142_TASK, json!(["0", "00", " ", "a"]))).await;
        assert_eq!(r["code"], 0, "{r}");
        assert_eq!(actors_of(&facade, I142_TASK),
            vec!["0".to_string(), "00".to_string(), "a".to_string()],
            "哨兵：'0'/'00'/'a' 都是正常 id，只有 ' ' 是空值");
    }

    /// 主键另判一档（§2.11 末段）：`processTaskId` 给 `0` ⇒ 响亮报错，不得拿 `0` 当 id 落库。
    /// 改前实测：code=0 并往 `process_task_id=0` 挂了两行（php 同款病灶：门面不校验 taskId）。
    #[tokio::test]
    async fn test_i142_b_surrogate_task_id_zero_is_loud_error() {
        let facade = make_facade();
        for bad in [json!(0), json!(-1)] {
            let mut a = surrogate_args(I142_TASK, json!(["i142pk", " i142pk "]));
            a.insert("processTaskId".to_string(), bad.clone());
            let r = facade.flow("processTask/surrogate", &a).await;
            assert_eq!(r["code"], 99999999, "主键 {bad} 必须响亮报错，实得 {r}");
            assert_eq!(r["msg"], "processTaskId/actorIds 缺失",
                "主键档沿用既有\"缺参数\"信封（硬要求③：不新造文案）");
            assert!(actors_of(&facade, 0).is_empty(), "不得拿 0 当 id 落库");
        }
        // 整条没给同档
        let mut none = HashMap::new();
        none.insert("actorIds".to_string(), json!(["i142pk"]));
        assert_eq!(facade.flow("processTask/surrogate", &none).await["msg"],
            "processTaskId/actorIds 缺失");
    }

    /// `addCandidate` 与 `surrogate` 同体（Java 语义：两者都是 addTaskActor）⇒ 同一判据同一信封。
    #[tokio::test]
    async fn test_i142_b_add_candidate_shares_the_same_judge() {
        let facade = make_facade();
        let batch = json!([" i142c ", "", "  ", "i142c", "i142d", "0"]);
        let sur = facade.flow("processTask/surrogate", &surrogate_args(I142_TASK, batch.clone())).await;
        let cand = facade.flow("processTask/addCandidate", &surrogate_args(I142_TASK + 1, batch)).await;
        assert_eq!(sur["code"], 0, "{sur}");
        assert_eq!(cand["code"], 0, "{cand}");
        assert_eq!(actors_of(&facade, I142_TASK), actors_of(&facade, I142_TASK + 1),
            "§2.11：addCandidate 与 surrogate 是同一条腿，两形同判据不许一腿一答案");
        assert_eq!(actors_of(&facade, I142_TASK + 1),
            vec!["i142c".to_string(), "i142d".to_string(), "0".to_string()]);
    }

    /// `transfer` 的 fromActor/toActor **归一后再用**（§2.11 表第二行）：带空格的同一人必须
    /// 命中原参与者行，且落库/留痕取 trim 后的值。
    /// 改前实测：fromActor 存原值 `" user2 "` ⇒ 归属命中判定失败，报「原办理人不是该任务参与人」。
    #[tokio::test]
    async fn test_i142_b_transfer_normalizes_from_and_to_actor() {
        let facade = make_facade();
        let (_iid, task_id) = start_two_step_flow(&facade, "i142_b_transfer").await;

        let r = facade.flow("processTask/transfer",
            &transfer_args(task_id, " user2 ", " lisi ", " user2 ")).await;
        assert_eq!(r["code"], 0, "带空格的同一人应命中：{r}");

        let actors = actors_of(&facade, task_id);
        assert_eq!(actors, vec!["lisi".to_string()],
            "转办后台账＝trim 后的新人，原人不留未 trim 的残行");

        // 留痕同样取归一后的值（六键 tf_transferHistory 的 fromActor/toActor 不得带空格）
        let task = facade.repo().find_task_by_id(task_id).unwrap().unwrap();
        let JsonValue::Array(hops) = task.variables.get("tf_transferHistory").unwrap() else {
            panic!("转办留痕 tf_transferHistory 缺失");
        };
        assert_eq!(hops[0].get_str("fromActor"), Some("user2"), "留痕 fromActor 取归一后的值");
        assert_eq!(hops[0].get_str("toActor"), Some("lisi"), "留痕 toActor 取归一后的值");
        assert_eq!(hops[0].get_str("operator"), Some("user2"), "留痕 operator 与归属判定同一尺度");
    }

    /// `transfer` 的主键档：缺失／`0` 都响亮报错（既有「缺少processTaskId参数」信封不动），
    /// 必填三档的文案逐字不变（硬要求③）。
    #[tokio::test]
    async fn test_i142_b_transfer_missing_or_zero_task_id_is_loud_error() {
        let facade = make_facade();
        let (_iid, task_id) = start_two_step_flow(&facade, "i142_b_transfer_pk").await;

        let mut zero = transfer_args(task_id, "user2", "lisi", "user2");
        zero.insert("processTaskId".to_string(), json!(0));
        let r = facade.flow("processTask/transfer", &zero).await;
        assert_eq!(r["msg"], "缺少processTaskId参数", "主键 0 ⇒ 沿用既有缺参数信封：{r}");
        assert_eq!(actors_of(&facade, 0), Vec::<String>::new(), "不得拿 0 当 id 落库");

        // 必填档逐字不动（归一只改值，不改信封）
        assert_eq!(facade.flow("processTask/transfer",
            &transfer_args(task_id, "user2", "lisi", "   ")).await["msg"], "operator 必填");
        assert_eq!(facade.flow("processTask/transfer",
            &transfer_args(task_id, "  ", "lisi", "user2")).await["msg"], "fromActor 必填");
        assert_eq!(facade.flow("processTask/transfer",
            &transfer_args(task_id, "user2", "\t", "user2")).await["msg"], "toActor 必填");
    }

    // ═══════════════════════════════════════════════════════
    // issues/115 残留 · 门面第 47 个 action processTask/removeTaskActor
    // （spec 06-facade.md §processTask/removeTaskActor · Rust 腿。17 格判据逐条对齐 java 基准腿
    //   jeeflow-core/src/test/java/com/mldong/jeeflow/test/RemoveTaskActorActionTest.java）
    //
    // 判据主线是三个兄弟 action 的分工：surrogate/addCandidate 只加、transfer 换人＋留痕＋fire
    // 码 7、本 action 只摘不加零留痕零事件。每条负向都**同时**断言"参与者一动不动"——摘人是删除
    // 操作，报错却删了一半比不报错更糟。
    // ═══════════════════════════════════════════════════════

    /// **test-only 包装仓储**（java 基准腿 `RemoveTaskActorActionTest.DirtyRowSpyRepo` 同款）：
    /// 语义 5/6 那三格要求"库里真存着修复前落下的历史脏行"（`actor_id=''`／纯空白／`' 9101 '`），
    /// 而内存仓写侧 [`MemoryRepository`] 的 `add_task_actor` 会归一 ⇒ 正常路径**根本建不出**这些行，
    /// 脏行只能从外部塞。这里复刻 JDBC 的形状：
    /// - 读侧：真人＋脏行并起来返回（一条裸 `SELECT` 本来就会把脏行读出来，所以"脏行算不算一个人"
    ///   "脏行会不会被误删"都是门面这一层必须面对的真实判据）；
    /// - 删侧：按 `DELETE ... AND actor_id IN (?)` **逐字语义**处理脏行（字面命中才删，不 trim
    ///   不丢空），并**记录每一次喂进 DELETE 的实参**——语义 6「DELETE 取行上的原值」的判据
    ///   只有落在实参上才咬得住（拿归一值去删＝判成同一人却一条没删的"假成功"）。
    /// 其余方法逐条转调内层内存仓，保证"除了上面两处，本 spy 的行为＝内存仓"。
    #[derive(Clone)]
    struct DirtyRowSpyRepo {
        inner: Arc<MemoryRepository>,
        dirty: Arc<std::sync::Mutex<HashMap<i64, Vec<String>>>>,
        remove_calls: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
    }

    impl DirtyRowSpyRepo {
        fn new(inner: Arc<MemoryRepository>) -> Self {
            DirtyRowSpyRepo {
                inner,
                dirty: Arc::new(std::sync::Mutex::new(HashMap::new())),
                remove_calls: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        /// 塞一条历史脏行（**故意不归一**——要造的就是"修复前落库"的原值行）。
        fn seed_dirty_row(&self, task_id: i64, actor_id: &str) {
            self.dirty
                .lock()
                .unwrap()
                .entry(task_id)
                .or_insert_with(Vec::new)
                .push(actor_id.to_string());
        }

        /// 脏行还剩几条（DELETE 按字面命中的判据）。
        fn dirty_remaining(&self, task_id: i64) -> Vec<String> {
            self.dirty
                .lock()
                .unwrap()
                .get(&task_id)
                .cloned()
                .unwrap_or_default()
        }

        /// 只取"真人"那一半（脏行并进 `find_task_actors` 会干扰别的判据，故取证分离）。
        fn find_real_actors(&self, task_id: i64) -> Vec<String> {
            self.inner.find_task_actors(task_id).unwrap()
        }

        /// 每次 DELETE 的实参（顺序＝调用顺序）。
        fn remove_calls(&self) -> Vec<Vec<String>> {
            self.remove_calls.lock().unwrap().clone()
        }
    }

    impl ProcessRepository for DirtyRowSpyRepo {
        fn find_task_actors(&self, task_id: i64) -> JeeflowResult<Vec<String>> {
            let mut out = self.inner.find_task_actors(task_id)?;
            let rows: Vec<String> = {
                let guard = self.dirty.lock().unwrap();
                guard.get(&task_id).cloned().unwrap_or_default()
            };
            out.extend(rows);
            Ok(out)
        }

        fn remove_task_actor(&self, task_id: i64, actors: &[String]) -> JeeflowResult<()> {
            self.remove_calls.lock().unwrap().push(actors.to_vec());
            // `DELETE ... actor_id IN (?)` 的逐字语义：字面命中才删（空串实参会真删掉空串行，
            // 这正是"归一单点丢空 ⇒ 脏行安全"那一格要照出来的地方）。
            if let Some(rows) = self.dirty.lock().unwrap().get_mut(&task_id) {
                rows.retain(|row| !actors.iter().any(|a| a == row));
            }
            self.inner.remove_task_actor(task_id, actors)
        }

        // ── 以下逐条转调内存仓（本 spy 只在上面两处偏离）──
        fn find_define_by_id(&self, define_id: i64) -> JeeflowResult<Option<ProcessDefine>> {
            self.inner.find_define_by_id(define_id)
        }
        fn save_define(&self, define: &mut ProcessDefine) -> JeeflowResult<()> {
            self.inner.save_define(define)
        }
        fn update_define(&self, define: &ProcessDefine) -> JeeflowResult<()> {
            self.inner.update_define(define)
        }
        fn update_define_state(&self, define_id: i64, state: i32) -> JeeflowResult<()> {
            self.inner.update_define_state(define_id, state)
        }
        fn remove_define(&self, define_id: i64) -> JeeflowResult<()> {
            self.inner.remove_define(define_id)
        }
        fn find_instance_by_id(&self, instance_id: i64) -> JeeflowResult<Option<ProcessInstance>> {
            self.inner.find_instance_by_id(instance_id)
        }
        fn save_instance(&self, instance: &mut ProcessInstance) -> JeeflowResult<()> {
            self.inner.save_instance(instance)
        }
        fn update_instance(&self, instance: &ProcessInstance) -> JeeflowResult<()> {
            self.inner.update_instance(instance)
        }
        fn find_task_by_id(&self, task_id: i64) -> JeeflowResult<Option<ProcessTask>> {
            self.inner.find_task_by_id(task_id)
        }
        fn save_task(&self, task: &mut ProcessTask) -> JeeflowResult<()> {
            self.inner.save_task(task)
        }
        fn update_task(&self, task: &ProcessTask) -> JeeflowResult<()> {
            self.inner.update_task(task)
        }
        fn find_doing_tasks(&self, instance_id: i64, task_names: &[String]) -> JeeflowResult<Vec<ProcessTask>> {
            self.inner.find_doing_tasks(instance_id, task_names)
        }
        fn find_done_tasks(&self, instance_id: i64, task_names: &[String]) -> JeeflowResult<Vec<ProcessTask>> {
            self.inner.find_done_tasks(instance_id, task_names)
        }
        fn find_history_tasks(&self, instance_id: i64) -> JeeflowResult<Vec<ProcessTask>> {
            self.inner.find_history_tasks(instance_id)
        }
        fn add_task_actor(&self, task_id: i64, actors: &[String]) -> JeeflowResult<()> {
            self.inner.add_task_actor(task_id, actors)
        }
        fn create_cc_instance(&self, instance_id: i64, creator: &str, actor_ids: &[String]) -> JeeflowResult<()> {
            self.inner.create_cc_instance(instance_id, creator, actor_ids)
        }
        fn find_cc_actor_ids(&self, instance_id: i64) -> JeeflowResult<Vec<String>> {
            self.inner.find_cc_actor_ids(instance_id)
        }
        fn update_cc_status(&self, instance_id: i64, actor_id: &str) -> JeeflowResult<()> {
            self.inner.update_cc_status(instance_id, actor_id)
        }
        fn page_todo_tasks(&self, query: &PageQuery) -> JeeflowResult<PageResult<TaskRow>> {
            self.inner.page_todo_tasks(query)
        }
        fn page_done_tasks(&self, query: &PageQuery) -> JeeflowResult<PageResult<TaskRow>> {
            self.inner.page_done_tasks(query)
        }
        fn page_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> {
            self.inner.page_instances(query)
        }
        fn page_cc_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> {
            self.inner.page_cc_instances(query)
        }
        fn page_defines(&self, query: &PageQuery) -> JeeflowResult<PageResult<DefineRow>> {
            self.inner.page_defines(query)
        }
        fn count_todo_tasks(&self, user_id: &str) -> JeeflowResult<i64> {
            self.inner.count_todo_tasks(user_id)
        }
        fn get_all_instances(&self) -> JeeflowResult<Vec<ProcessInstance>> {
            self.inner.get_all_instances()
        }
        fn get_all_tasks(&self) -> JeeflowResult<Vec<ProcessTask>> {
            self.inner.get_all_tasks()
        }
    }

    /// 事件录制器：语义 2「不 fire 事件」的取证腿——本栈一切 fire 都走
    /// `ServiceContext::event_listeners`（`JeeflowEngineImpl::fire_event` → `ProcessPublisher::notify`），
    /// 挂上它就能断言"这次调用一条都没发"。探针**有牙**由 `leaves_no_trace` 那一格末段反证
    /// （同一装配下 transfer 必须录得到 TASK_TRANSFER），否则"零事件"可能只是监听器没挂上的空转。
    struct SpyEventListener(Arc<std::sync::Mutex<Vec<String>>>);

    impl ProcessEventListener for SpyEventListener {
        fn on_event(&self, event: &ProcessEvent) {
            self.0.lock().unwrap().push(event.spec_label());
        }
    }

    /// `(门面, spy 仓储, 事件录制)` 三件套：门面与引擎都走 spy 仓储；扩展仓储仍指内存仓本体
    /// （processDesign/* 不经过被覆写的那两处）。
    fn make_remove_actor_fixture() -> (
        JeeflowFacade,
        Arc<DirtyRowSpyRepo>,
        Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        let inner = Arc::new(MemoryRepository::new());
        let repo = Arc::new(DirtyRowSpyRepo::new(inner.clone()));
        let events: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(inner as Arc<dyn ProcessExtRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));
        ctx.register_event_listener(Arc::new(SpyEventListener(events.clone())));
        (JeeflowFacade::new(ctx), repo, events)
    }

    /// `removeTaskActor` 入参：`operator` 给 `None` ＝**整条不传**（必填档的"缺省"形状，
    /// 比传空串更能证明门面没有偷偷回落固定账号）。
    fn remove_actor_args(task_id: Json, actor_ids: Json, operator: Option<&str>) -> HashMap<String, Json> {
        let mut m = HashMap::new();
        m.insert("processTaskId".to_string(), task_id);
        m.insert("actorIds".to_string(), actor_ids);
        if let Some(op) = operator {
            m.insert("operator".to_string(), json!(op));
        }
        m
    }

    async fn call_remove(
        facade: &JeeflowFacade,
        task_id: Json,
        actor_ids: Json,
        operator: Option<&str>,
    ) -> Json {
        facade
            .flow("processTask/removeTaskActor", &remove_actor_args(task_id, actor_ids, operator))
            .await
    }

    /// 造多参与人现场：用兄弟 action（`addCandidate` 追加），不直接塞仓储。
    async fn seed_participants(facade: &JeeflowFacade, task_id: i64, ids: &[&str]) {
        let r = facade
            .flow("processTask/addCandidate", &surrogate_args(task_id, json!(ids)))
            .await;
        assert_eq!(r["code"], 0, "夹具加签失败: {r}");
    }

    fn recorded_events(events: &Arc<std::sync::Mutex<Vec<String>>>) -> Vec<String> {
        events.lock().unwrap().clone()
    }

    fn drain_events(events: &Arc<std::sync::Mutex<Vec<String>>>) {
        events.lock().unwrap().clear();
    }

    // ─── 语义 1「只摘不加」＋ 正向核心 ───

    /// ① 只摘点名的人，其余参与人**按顺序原样保留**；成功信封 code=0／msg=成功／data=null。
    #[tokio::test]
    async fn test_i115_remove_task_actor_removes_only_the_named_actor() {
        let (facade, repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_only").await;
        seed_participants(&facade, task_id, &["9001", "9002"]).await;
        assert_eq!(actors_of(&facade, task_id), vec!["user2", "9001", "9002"], "夹具前提");

        let r = call_remove(&facade, json!(task_id), json!(["9001"]), Some("flow.admin")).await;

        assert_eq!(r["code"], 0, "只摘点名的人应成功: {r}");
        assert_eq!(r["msg"], "成功");
        assert!(r["data"].is_null(), "data 出 null（spec 同节：前端消费面不读 data）: {r}");
        assert_eq!(
            actors_of(&facade, task_id),
            vec!["user2", "9002"],
            "只删点名的 9001，其余参与人原样保留（含顺序）"
        );
        assert_eq!(repo.find_real_actors(task_id), vec!["user2", "9002"]);
        assert_eq!(repo.remove_calls(), vec![vec!["9001".to_string()]], "一次调用一条 DELETE");
    }

    /// ② 一次摘多人（集合语义，不是"一次只能摘一个人"）。
    #[tokio::test]
    async fn test_i115_remove_task_actor_removes_several_in_one_call() {
        let (facade, _repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_multi").await;
        seed_participants(&facade, task_id, &["9001", "9002", "9003"]).await;

        assert_eq!(
            call_remove(&facade, json!(task_id), json!(["9001", "9002"]), Some("flow.admin")).await["code"],
            0
        );

        assert_eq!(actors_of(&facade, task_id), vec!["user2", "9003"]);
    }

    /// ③ 逗号串腿与数组腿同判据（§2.11「两形一把尺子」，摘人腿不得另抄一份）：
    /// `"9001, 9002 "` 带空格也照删，且喂进 DELETE 的是归一后（＝行上原值）的那两个人。
    #[tokio::test]
    async fn test_i115_remove_task_actor_comma_string_form_same_judge() {
        let (facade, repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_comma").await;
        seed_participants(&facade, task_id, &["9001", "9002"]).await;

        assert_eq!(
            call_remove(&facade, json!(task_id), json!("9001, 9002 "), Some("flow.admin")).await["code"],
            0
        );

        assert_eq!(actors_of(&facade, task_id), vec!["user2"], "逗号串带空格照样命中");
        assert_eq!(repo.remove_calls(), vec![vec!["9001".to_string(), "9002".to_string()]]);
    }

    // ─── 语义 3「归属判据同 transfer」：只能摘自己那一票，auto/admin 例外 ───

    /// ④ 本人摘自己那一票不需要特权；`operator` 带空格的同一人也放行（比较**按归一值**做）。
    #[tokio::test]
    async fn test_i115_remove_task_actor_self_removal_needs_no_privilege() {
        let (facade, _repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_self").await;
        seed_participants(&facade, task_id, &["9001", "9002"]).await;

        assert_eq!(call_remove(&facade, json!(task_id), json!(["9001"]), Some("9001")).await["code"], 0);
        assert_eq!(actors_of(&facade, task_id), vec!["user2", "9002"]);
        assert_eq!(
            call_remove(&facade, json!(task_id), json!(["9002"]), Some(" 9002 ")).await["code"],
            0,
            "operator 带空格的同一人必须判成同一人（§2.11 归一后再比）"
        );
        assert_eq!(actors_of(&facade, task_id), vec!["user2"]);
    }

    /// ⑤ 借道摘他人必须拦下（transfer 能"摘 A 加 B"是因为 A＝操作人本人，本 action 同理不得
    /// 成为借道摘他人的口子），而且报错后**一条都不许删**。
    #[tokio::test]
    async fn test_i115_remove_task_actor_cannot_remove_someone_else() {
        let (facade, repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_piggyback").await;
        seed_participants(&facade, task_id, &["9001"]).await;

        let r = call_remove(&facade, json!(task_id), json!(["9001"]), Some("user2")).await;

        assert_eq!(r["msg"], "无权限摘除该任务参与人", "{r}");
        assert_eq!(r["code"], 99999999);
        assert_eq!(actors_of(&facade, task_id), vec!["user2", "9001"], "报错后一条都不许删");
        assert!(repo.remove_calls().is_empty(), "鉴权不过时不得喂 DELETE");
    }

    /// ⑥ `flow.auto` 与 `flow.admin` 同档放行（大小写不敏感沿用本栈既有
    /// `is_privileged_operator`，不另立判据）。
    #[tokio::test]
    async fn test_i115_remove_task_actor_privileged_operators() {
        let (facade, _repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_privileged").await;
        seed_participants(&facade, task_id, &["9001", "9002", "9003"]).await;

        for (who, victim) in [("flow.auto", "9001"), ("FLOW.ADMIN", "9002"), ("flow.Admin", "9003")] {
            assert_eq!(
                call_remove(&facade, json!(task_id), json!([victim]), Some(who)).await["code"],
                0,
                "{who} 摘 {victim} 应放行"
            );
        }
        assert_eq!(actors_of(&facade, task_id), vec!["user2"]);
    }

    // ─── 语义 5「不得摘空」：判据是集合差，不是入参条数 ───

    /// ⑦ 摘空报错——少了这一条，一次误操作就会留下**永远无人可办、也无法撤回重派**的死任务。
    #[tokio::test]
    async fn test_i115_remove_task_actor_never_empties_the_task() {
        let (facade, repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_empty").await;
        assert_eq!(actors_of(&facade, task_id), vec!["user2"], "夹具前提：唯一参与人");

        let r = call_remove(&facade, json!(task_id), json!(["user2"]), Some("user2")).await;

        assert_eq!(r["msg"], "至少需保留一名参与人", "{r}");
        assert_eq!(r["code"], 99999999);
        assert_eq!(actors_of(&facade, task_id), vec!["user2"], "人还在");
        assert!(repo.remove_calls().is_empty(), "下限不过 ⇒ 一条都不喂 DELETE");
    }

    /// ⑧ 绕过档：`actorIds` 里混进非参与者 id，"入参条数 < 参与人数"那种判据会放过去，
    /// 集合差判据必须照样拦下（spec 语义 5 第二句）。
    #[tokio::test]
    async fn test_i115_remove_task_actor_mixed_ghost_cannot_bypass_floor() {
        let (facade, repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_ghost_floor").await;
        seed_participants(&facade, task_id, &["9001"]).await;

        let r = call_remove(&facade, json!(task_id), json!(["user2", "9001", "ghost"]), Some("user2")).await;

        assert_eq!(r["msg"], "至少需保留一名参与人", "集合差为空 ⇒ 混入 ghost 也绕不过下限: {r}");
        assert_eq!(actors_of(&facade, task_id), vec!["user2", "9001"]);
        assert!(repo.remove_calls().is_empty());
    }

    // ─── 语义 4「只作用于进行中任务」 ───

    /// ⑨ 非 DOING 一律拦下，且**历史参与人行不动**（它是 approvalRecord 的取证依据，
    /// 摘它等于改写审批历史）。
    #[tokio::test]
    async fn test_i115_remove_task_actor_finished_task_is_protected() {
        let (facade, repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_finished").await;
        assert_eq!(exec_task(&facade, task_id, "user2", vec![]).await["code"], 0, "夹具办结失败");
        let finished = facade.repo().find_task_by_id(task_id).unwrap().unwrap();
        assert_ne!(finished.task_state, TaskState::Doing.code(), "夹具前提：任务已离开 DOING");

        let r = call_remove(&facade, json!(task_id), json!(["user2"]), Some("flow.admin")).await;

        assert_eq!(r["msg"], "任务非进行中，不可摘除参与人", "{r}");
        assert_eq!(r["code"], 99999999);
        assert_eq!(actors_of(&facade, task_id), vec!["user2"], "历史参与人行不得被改写");
        assert!(repo.remove_calls().is_empty());
    }

    // ─── 语义 2「不留痕且不 fire 事件」 ───

    /// ⑩ 零留痕＋零事件：不置 submitType、不写 tf_transferHistory/tf_transferTo/tf_transferReason/
    /// tf_approvalComment、不覆写任务行 actor_id/update_user/update_time（**取改前快照再比**，
    /// 建单路径本来就会写这两列，不能假定它是 null）；一条事件都不发。
    /// 末段反证事件探针有牙：同一装配下 transfer 确实 fire 码 7。
    #[tokio::test]
    async fn test_i115_remove_task_actor_leaves_no_trace_and_fires_no_event() {
        let (facade, _repo, events) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_trace").await;
        seed_participants(&facade, task_id, &["9001"]).await;
        drain_events(&events); // 夹具（发起/起单）发的码不算在本次账上
        let before = facade.repo().find_task_by_id(task_id).unwrap().unwrap();

        assert_eq!(
            call_remove(&facade, json!(task_id), json!(["9001"]), Some("flow.admin")).await["code"],
            0
        );

        assert!(
            recorded_events(&events).is_empty(),
            "摘人不在 issues/132 定稿事件集里，一律不 fire（码 7 的语义是「参与者被替换」）: {:?}",
            recorded_events(&events)
        );
        let after = facade.repo().find_task_by_id(task_id).unwrap().unwrap();
        assert_eq!(after.variables, before.variables, "不写任何任务变量（整张变量表逐键不变）");
        for key in [
            "submitType",
            "tf_transferHistory",
            "tf_transferTo",
            "tf_transferReason",
            "tf_approvalComment",
        ] {
            assert!(!after.variables.contains_key(key), "不留痕：任务变量里不得出现 {key}");
        }
        assert_eq!(
            after.update_user, before.update_user,
            "不覆写 update_user（判据是「摘人这一步没动它」，不是「它本来是 null」）"
        );
        assert_eq!(after.update_time, before.update_time, "不覆写 update_time");
        assert_eq!(after.actor_id, before.actor_id, "不覆写 actor_id 列");
        assert_eq!(after.task_state, before.task_state, "不改任务状态");

        // 探针有牙的反证：同一次装配里转办必须录得到 TASK_TRANSFER（否则上面的"零事件"不作数）
        assert_eq!(
            facade
                .flow("processTask/transfer", &transfer_args(task_id, "user2", "lisi", "user2"))
                .await["code"],
            0
        );
        assert!(
            recorded_events(&events)
                .iter()
                .any(|e| e.starts_with("TASK_TRANSFER/")),
            "事件探针空转（连转办都没录到），那上面的「零事件」断言就不作数: {:?}",
            recorded_events(&events)
        );
    }

    // ─── 语义 7「幂等」 ───

    /// ⑪ 非参与者静默忽略（不报错）、一个都没命中 ⇒ 空操作仍返回成功信封、重放第二次成功。
    #[tokio::test]
    async fn test_i115_remove_task_actor_is_idempotent() {
        let (facade, repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_idempotent").await;
        seed_participants(&facade, task_id, &["9001"]).await;

        assert_eq!(call_remove(&facade, json!(task_id), json!(["9001"]), Some("flow.admin")).await["code"], 0);
        assert_eq!(actors_of(&facade, task_id), vec!["user2"]);

        let replay = call_remove(&facade, json!(task_id), json!(["9001"]), Some("flow.admin")).await;
        assert_eq!(replay["code"], 0, "同一次摘人重放第二次应得成功信封（前端双点/集成层重放）: {replay}");
        assert_eq!(replay["msg"], "成功");
        assert_eq!(actors_of(&facade, task_id), vec!["user2"]);

        let ghost = call_remove(&facade, json!(task_id), json!(["ghost"]), Some("flow.admin")).await;
        assert_eq!(ghost["code"], 0, "非参与者静默忽略、不报错: {ghost}");
        assert_eq!(actors_of(&facade, task_id), vec!["user2"]);
        assert_eq!(repo.remove_calls().len(), 1, "重放与 ghost 都不该再喂 DELETE");
    }

    // ─── 必填档逐字文案 ＋ 守卫次序（spec 同节钉死，八栈不接受自行排序）───

    /// ⑫ 五个报错档的逐字文案：`operator 必填`（缺省／纯空白两支，严禁回落 user1）／
    /// `processTaskId/actorIds 缺失`（与 surrogate 同族同文案）／`任务不存在`；
    /// 并且五档**一条都不许删**。
    #[tokio::test]
    async fn test_i115_remove_task_actor_missing_arms_reuse_existing_envelope() {
        let (facade, repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_arms").await;
        seed_participants(&facade, task_id, &["9001"]).await;
        let before = actors_of(&facade, task_id);

        assert_eq!(
            call_remove(&facade, json!(task_id), json!(["9001"]), None).await["msg"],
            "operator 必填",
            "缺省 operator ⇒ 必填档（严禁缺省回落 user1）"
        );
        assert_eq!(
            call_remove(&facade, json!(task_id), json!(["9001"]), Some("   ")).await["msg"],
            "operator 必填",
            "纯空白 operator 也不给过"
        );

        // 主键整条没给 ⇒ 与 surrogate/addCandidate 同一逐字文案（同族同文案，不另造）
        let mut no_pk = HashMap::new();
        no_pk.insert("actorIds".to_string(), json!(["9001"]));
        no_pk.insert("operator".to_string(), json!("flow.admin"));
        assert_eq!(
            facade.flow("processTask/removeTaskActor", &no_pk).await["msg"],
            "processTaskId/actorIds 缺失"
        );
        // actorIds 归一后为空 ⇒ 同一档（空串元素也绝不能被喂进 DELETE）
        assert_eq!(
            call_remove(&facade, json!(task_id), json!(["", "  ", null]), Some("flow.admin")).await["msg"],
            "processTaskId/actorIds 缺失"
        );
        // 主键空串/纯空白 ⇒ 缺参数档（spec 语义 8：与缺键、0/负数同判，java `toLong("")` 同档）
        assert_eq!(
            call_remove(&facade, json!(""), json!(["9001"]), Some("flow.admin")).await["msg"],
            "processTaskId/actorIds 缺失",
            "空串主键必须落缺参数档，不得折进本栈「非法id」——八栈这一档逐字同答案"
        );
        assert_eq!(
            call_remove(&facade, json!("   "), json!(["9001"]), Some("flow.admin")).await["msg"],
            "processTaskId/actorIds 缺失",
            "纯空白与空串同档"
        );
        // 主键"给了但不是数字" ⇒ 仍走本栈既有「非法id」信封（spec 明文**不**统一这一档，
        //   兄弟 action 同款形状，本轮不回改它们）
        let bad_rm = call_remove(&facade, json!("abc"), json!(["9001"]), Some("flow.admin")).await;
        assert!(
            bad_rm["msg"].as_str().unwrap_or("").contains("非法id"),
            "非数字主键应沿用本栈既有「非法id」档，实得 {bad_rm}"
        );
        // 任务不存在（本 action 的逐字判据，八栈一致）
        assert_eq!(
            call_remove(&facade, json!(424242), json!(["9001"]), Some("flow.admin")).await["msg"],
            "任务不存在"
        );

        assert_eq!(actors_of(&facade, task_id), before, "以上报错档一条都不许删（写死条数每次加档都要漂，故不写数）");
        assert!(repo.remove_calls().is_empty(), "报错档不得喂 DELETE");
    }

    /// ⑬ 守卫次序两支：`operator 必填` 排在缺参数之前（否则鉴权缺口会被参数报错藏起来）；
    /// 权限档排在非进行中之前（否则外人可以靠"任务已完成"探到别人的任务状态）。
    #[tokio::test]
    async fn test_i115_remove_task_actor_guard_order_is_fixed_across_stacks() {
        let (facade, _repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_order").await;

        let mut nothing = HashMap::new();
        nothing.insert("operator".to_string(), json!("   "));
        assert_eq!(
            facade.flow("processTask/removeTaskActor", &nothing).await["msg"],
            "operator 必填",
            "参数全缺时 operator 必填先判：主键缺失不得抢在它前面"
        );

        assert_eq!(exec_task(&facade, task_id, "user2", vec![]).await["code"], 0);
        assert_eq!(
            call_remove(&facade, json!(task_id), json!(["user2"]), Some("outsider")).await["msg"],
            "无权限摘除该任务参与人",
            "权限档先于非进行中档"
        );
    }

    // ─── 语义 5＋6 的脏行腿（test-only spy 仓储）───

    /// ⑭ 带空格入参删得掉真人 ∧ 空值不误删 `actor_id=''` 脏行 ∧ 喂进仓储删除的实参不含空值。
    #[tokio::test]
    async fn test_i115_remove_task_actor_whitespace_padded_and_dirty_rows_survive() {
        let (facade, repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_whitespace").await;
        seed_participants(&facade, task_id, &["9001", "9002"]).await;
        repo.seed_dirty_row(task_id, ""); // 历史脏行：内存仓写侧归一，正常路径建不出来
        repo.seed_dirty_row(task_id, "   ");

        assert_eq!(
            call_remove(
                &facade,
                json!(task_id),
                json!([" 9001 ", "", null, "   ", "9002"]),
                Some("flow.admin")
            )
            .await["code"],
            0
        );

        assert_eq!(
            repo.find_real_actors(task_id),
            vec!["user2"],
            "带空格的入参删得掉真人，其余参与人不动"
        );
        assert_eq!(
            repo.dirty_remaining(task_id),
            vec!["", "   "],
            "空串/纯空白绝不能喂进 DELETE ⇒ 历史脏行必须原样还在"
        );
        for call in repo.remove_calls() {
            for actor in &call {
                assert!(!actor.trim().is_empty(), "喂给 DELETE 的实参不得含空串/纯空白: {:?}", call);
            }
        }
    }

    /// ⑮ 语义 6 的正体：库里的行是修复前落下的未 trim 原值 `" 9101 "`，入参给 `"9101"` ⇒
    /// **判成同一个人并真删掉**，且喂进 DELETE 的实参是**那一行的原值**。
    /// 反面形状＝拿归一值去删：判成同一人却一条没删，门面报成功而被摘的人待办还在（假成功）。
    #[tokio::test]
    async fn test_i115_remove_task_actor_untrimmed_row_matched_and_deleted_by_row_value() {
        let (facade, repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_untrimmed").await;
        repo.seed_dirty_row(task_id, " 9101 ");

        assert_eq!(
            call_remove(&facade, json!(task_id), json!(["9101"]), Some("flow.admin")).await["code"],
            0
        );

        assert_eq!(repo.find_real_actors(task_id), vec!["user2"], "其余参与人不动");
        assert_eq!(
            repo.dirty_remaining(task_id),
            Vec::<String>::new(),
            "未 trim 的历史行被归一匹配命中并真删掉了"
        );
        let calls = repo.remove_calls();
        assert_eq!(calls.len(), 1, "{:?}", calls);
        assert_eq!(
            calls[0],
            vec![" 9101 ".to_string()],
            "DELETE 的实参是行上的原值，不是归一后的值（否则一条没删＝假成功）"
        );
    }

    /// ⑯ 「至少剩一名参与人」的下限按**能办单的人数**算：库里只剩 `actor_id=''`/纯空白脏行时，
    /// 摘走最后一个真人仍须报错——脏行谁也办不了，拿它撑住下限等于让"摘空"伪装成成功。
    #[tokio::test]
    async fn test_i115_remove_task_actor_dirty_rows_do_not_prop_up_the_keep_one_floor() {
        let (facade, repo, _) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_dirty_floor").await;
        repo.seed_dirty_row(task_id, "");
        repo.seed_dirty_row(task_id, "   ");

        let r = call_remove(&facade, json!(task_id), json!(["user2"]), Some("flow.admin")).await;

        assert_eq!(r["msg"], "至少需保留一名参与人", "脏行不算一个人: {r}");
        assert_eq!(repo.find_real_actors(task_id), vec!["user2"], "报错后真人那行还在");
        assert_eq!(repo.dirty_remaining(task_id), vec!["", "   "], "报错后脏行也不动");
        assert!(repo.remove_calls().is_empty(), "下限不过 ⇒ 一条都不喂 DELETE");
    }

    // ─── 兄弟 action 回归（三条混用是本组判据最怕的分叉）───

    /// ⑰ `surrogate` 仍旧只加不摘、`transfer` 仍旧换人＋写 submitType=7＋fire 码 7，
    /// 本 action 夹在中间"只摘不加零留痕"——三条各自的形状都不许被这次改动带偏。
    #[tokio::test]
    async fn test_i115_remove_task_actor_siblings_keep_their_own_semantics() {
        let (facade, _repo, events) = make_remove_actor_fixture();
        let (_iid, task_id) = start_two_step_flow(&facade, "ra115_siblings").await;

        assert_eq!(
            facade.flow("processTask/surrogate", &surrogate_args(task_id, json!(["9101"]))).await["code"],
            0
        );
        assert_eq!(actors_of(&facade, task_id), vec!["user2", "9101"], "surrogate 仍旧只加不摘");

        assert_eq!(call_remove(&facade, json!(task_id), json!(["9101"]), Some("flow.admin")).await["code"], 0);
        assert_eq!(actors_of(&facade, task_id), vec!["user2"], "摘人不带加人");
        assert!(
            !facade
                .repo()
                .find_task_by_id(task_id)
                .unwrap()
                .unwrap()
                .variables
                .contains_key("submitType"),
            "摘人不置 submitType（那是 transfer 的留痕）"
        );

        drain_events(&events);
        assert_eq!(
            facade
                .flow("processTask/transfer", &transfer_args(task_id, "user2", "lisi", "user2"))
                .await["code"],
            0
        );
        assert_eq!(actors_of(&facade, task_id), vec!["lisi"], "transfer 换人语义不变");
        let task = facade.repo().find_task_by_id(task_id).unwrap().unwrap();
        let submit_type = match task.variables.get("submitType") {
            Some(JsonValue::Number(n)) => *n as i64,
            Some(JsonValue::Str(s)) => s.parse::<i64>().unwrap_or(-1),
            _ => -1,
        };
        assert_eq!(submit_type, 7, "transfer 仍写 submitType=7 留痕");
        assert!(
            recorded_events(&events)
                .iter()
                .any(|e| e.starts_with("TASK_TRANSFER/")),
            "transfer 仍 fire 码 7: {:?}",
            recorded_events(&events)
        );
    }
}
