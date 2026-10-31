//! 「退回修改、重提直返」的引擎内部状态读写与分支归属解析（对齐 java `ReapplySupport`）。
//!
//! 状态落点：**不新增表/列**，全部寄存在任务变量 JSON 的保留键里：
//! - [`VAR_BRANCH_CTX`]（`_branchCtx`）＝分支归属栈（外层→内层），每个元素
//!   `{fork, join, round, root}`；
//! - [`VAR_REAPPLY_CTX`]（`_reapplyContext`）＝本次退回绑定，只出现在**申请待办**上，
//!   形如 `{mode:"RETURN_TO_SOURCE", sourceTaskId, sourceNode, branch:[...]}`。
//!
//! 保留键由引擎写入，对外办理入口必须先剥离客户端传入的同名键（[`strip_reserved`]）。
//! JSON 数值形状：本栈 id 是 `i64`，写入一律用数字；读取侧对数字/数字串**都**认
//! （`JSON` 往返后别栈可能给字符串，判据不能因此掉档）。

use crate::error::{JeeflowError, JeeflowResult};
use crate::json::{FlowData, JsonValue};
use crate::model::ProcessTask;
use crate::parser::{NodeType, ProcessModel};
use std::collections::HashSet;

// ═══════════════════════════════════════════════════════
// 常量
// ═══════════════════════════════════════════════════════

/// 节点属性键：退回发起人后的重提方式
pub const PROP_REAPPLY_MODE: &str = "reapplyMode";
/// 任务变量保留键：分支归属栈（外层→内层）
pub const VAR_BRANCH_CTX: &str = "_branchCtx";
/// 任务变量保留键：本次退回绑定（只出现在申请待办上）
pub const VAR_REAPPLY_CTX: &str = "_reapplyContext";

pub const KEY_MODE: &str = "mode";
pub const KEY_SOURCE_TASK: &str = "sourceTaskId";
pub const KEY_SOURCE_NODE: &str = "sourceNode";
pub const KEY_BRANCH: &str = "branch";
pub const KEY_FORK: &str = "fork";
pub const KEY_JOIN: &str = "join";
pub const KEY_ROUND: &str = "round";
pub const KEY_ROOT: &str = "root";

// ═══════════════════════════════════════════════════════
// 重提方式（reapplyMode）
// ═══════════════════════════════════════════════════════

/// 退回发起人后的重提方式（流程节点 `properties.reapplyMode`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReapplyMode {
    /// 退回发起人后按申请节点的正常出边重提（缺省，保持既有行为）
    Normal,
    /// 退回发起人后，重提直接回到本来源节点建待办
    ReturnToSource,
}

impl Default for ReapplyMode {
    fn default() -> Self {
        ReapplyMode::Normal
    }
}

impl ReapplyMode {
    /// 解析配置值。缺省/空串 ⇒ NORMAL；非法值**必须**报配置错误，不得静默当缺省。
    /// 文案逐字对齐 java `ReapplyModeEnum.parse`。
    pub fn parse(value: Option<&JsonValue>, node_id: &str) -> JeeflowResult<Self> {
        let raw = match value {
            None | Some(JsonValue::Null) => return Ok(ReapplyMode::Normal),
            Some(v) => match v.as_str() {
                Some(s) => s.trim().to_string(),
                // 非字符串（数字/布尔/对象）一律按非法档处理，原文进错误信息
                None => v.to_json_string(),
            },
        };
        if raw.is_empty() {
            return Ok(ReapplyMode::Normal);
        }
        match raw.as_str() {
            "NORMAL" => Ok(ReapplyMode::Normal),
            "RETURN_TO_SOURCE" => Ok(ReapplyMode::ReturnToSource),
            other => Err(JeeflowError::Business(format!(
                "节点[{}]的 reapplyMode 配置非法: {}",
                node_id, other
            ))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ReapplyMode::Normal => "NORMAL",
            ReapplyMode::ReturnToSource => "RETURN_TO_SOURCE",
        }
    }
}

// ═══════════════════════════════════════════════════════
// 分支归属
// ═══════════════════════════════════════════════════════

/// 一档分支归属：所在分叉/汇合点、本轮号（分支根那次建单的父任务 id）、分支根任务 id。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub fork: String,
    pub join: String,
    pub round: i64,
    pub root: i64,
}

/// 重提直返上下文（只出现在申请待办上）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReapplyCtx {
    pub mode: String,
    pub source_task_id: i64,
    pub source_node: String,
    pub branch: Vec<Scope>,
}

impl ReapplyCtx {
    pub fn new(source_task_id: i64, source_node: impl Into<String>, branch: Vec<Scope>) -> Self {
        ReapplyCtx {
            mode: ReapplyMode::ReturnToSource.as_str().to_string(),
            source_task_id,
            source_node: source_node.into(),
            branch,
        }
    }
}

// ═══════════════════════════════════════════════════════
// 模型静态分析
// ═══════════════════════════════════════════════════════

/// 节点所在的全部分支范围（外层→内层）。含该节点的汇合点按集合大小降序排列即为外→内。
pub fn static_scopes(model: &ProcessModel, node_id: &str) -> Vec<Scope> {
    if node_id.is_empty() {
        return Vec::new();
    }
    let mut matched: Vec<(String, String, usize)> = Vec::new();
    // 直接遍历节点表：分支归属推导不能被一条脏边打断（对齐 java `ReapplySupport.staticScopes`）
    for join in model.nodes.iter().filter(|n| n.node_type == NodeType::Join) {
        let fork = match fork_for_join(model, &join.id) {
            Some(f) => f,
            None => continue,
        };
        let inside = backward_task_names(model, &join.id, &fork);
        if inside.iter().any(|n| n == node_id) {
            matched.push((fork, join.id.clone(), inside.len()));
        }
    }
    // 稳定降序：集合大的（外层）在前
    matched.sort_by(|a, b| b.2.cmp(&a.2));
    matched
        .into_iter()
        .map(|(fork, join, _)| Scope { fork, join, round: 0, root: 0 })
        .collect()
}

/// 从节点回溯到指定分叉之间的任务节点 id 集合；可穿过嵌套的内层 fork，遇到 stop_fork 即止。
fn backward_task_names(model: &ProcessModel, start_id: &str, stop_fork: &str) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut stack: Vec<String> = model
        .get_input_edges(start_id)
        .iter()
        .map(|e| e.source_node_id.clone())
        .collect();
    while let Some(cur_id) = stack.pop() {
        let cur = match model.get_node(&cur_id) {
            Some(n) => n,
            None => continue,
        };
        if !visited.insert(cur.id.clone()) {
            continue;
        }
        if cur.node_type == NodeType::Fork && cur.id == stop_fork {
            continue;
        }
        if cur.node_type == NodeType::Task {
            names.push(cur.id.clone());
        }
        for e in model.get_input_edges(&cur.id) {
            stack.push(e.source_node_id.clone());
        }
    }
    names
}

fn outgoing_ids(model: &ProcessModel, node_id: &str) -> Vec<String> {
    model
        .get_output_edges(node_id)
        .iter()
        .map(|e| e.target_node_id.clone())
        .collect()
}

/// 汇合点对应的分叉：取「最近后支配汇合点 == 本 join」的 fork，多候选取最外层。
/// 不能取"回溯遇到的第一个 fork"——嵌套并行时外层汇合点沿内层分支回溯会先撞到内层 fork。
fn fork_for_join(model: &ProcessModel, join_id: &str) -> Option<String> {
    let mut best: Option<String> = None;
    let mut best_depth: i32 = -1;
    for node in model.nodes.iter().filter(|n| n.node_type == NodeType::Fork) {
        if nearest_post_dominating_join(model, &node.id).as_deref() != Some(join_id) {
            continue;
        }
        let d = max_distance_to_join(model, &node.id, join_id);
        if d > best_depth {
            best_depth = d;
            best = Some(node.id.clone());
        }
    }
    best
}

fn nearest_post_dominating_join(model: &ProcessModel, fork_id: &str) -> Option<String> {
    let mut best: Option<String> = None;
    let mut best_max: Option<i32> = None;
    for node in model.nodes.iter().filter(|n| n.node_type == NodeType::Join) {
        if !post_dominates(model, fork_id, &node.id) {
            continue;
        }
        let d = max_distance_to_join(model, fork_id, &node.id);
        if d >= 0 && best_max.map(|m| d < m).unwrap_or(true) {
            best_max = Some(d);
            best = Some(node.id.clone());
        }
    }
    best
}

/// 屏蔽该汇合点后，fork 的任一分支仍能走到末端即返回 false。
fn post_dominates(model: &ProcessModel, fork_id: &str, join_id: &str) -> bool {
    let mut visited: HashSet<String> = HashSet::new();
    let mut stack = outgoing_ids(model, fork_id);
    while let Some(cur) = stack.pop() {
        if cur == join_id || !visited.insert(cur.clone()) {
            continue;
        }
        let outs = outgoing_ids(model, &cur);
        if outs.is_empty() {
            return false;
        }
        stack.extend(outs);
    }
    true
}

/// 分支走到汇合点的最大跳数（外层程度）；不可达返回 -1。
fn max_distance_to_join(model: &ProcessModel, fork_id: &str, join_id: &str) -> i32 {
    let mut visited: HashSet<String> = HashSet::new();
    let mut frontier: Vec<String> = Vec::new();
    for t in outgoing_ids(model, fork_id) {
        if visited.insert(t.clone()) {
            frontier.push(t);
        }
    }
    let mut d = 1;
    let mut found: i32 = -1;
    while !frontier.is_empty() {
        let mut next: Vec<String> = Vec::new();
        for cur in &frontier {
            if cur == join_id {
                found = d;
            }
            for t in outgoing_ids(model, cur) {
                if visited.insert(t.clone()) {
                    next.push(t);
                }
            }
        }
        frontier = next;
        d += 1;
    }
    found
}

// ═══════════════════════════════════════════════════════
// 任务变量的读写
// ═══════════════════════════════════════════════════════

/// 读任务变量里的分支归属栈（缺键/形状不对 ⇒ 空表；坏元素单条丢弃）。
pub fn scopes_of(vars: &FlowData) -> Vec<Scope> {
    match vars.get(VAR_BRANCH_CTX) {
        Some(JsonValue::Array(items)) => items.iter().filter_map(scope_from_json).collect(),
        _ => Vec::new(),
    }
}

/// 写分支归属栈（空表也写——与 java `putScopes(emptyScopes())` 同形，
/// 让"该节点不在任何分支内"这件事在变量里有明确一档）。
pub fn put_scopes(task: &mut ProcessTask, scopes: &[Scope]) {
    let arr = scopes_to_json(scopes);
    task.variables.insert(VAR_BRANCH_CTX.to_string(), arr);
}

pub fn scopes_to_json(scopes: &[Scope]) -> JsonValue {
    JsonValue::Array(
        scopes
            .iter()
            .map(|s| {
                JsonValue::Object(vec![
                    (KEY_FORK.to_string(), JsonValue::Str(s.fork.clone())),
                    (KEY_JOIN.to_string(), JsonValue::Str(s.join.clone())),
                    (KEY_ROUND.to_string(), JsonValue::Number(s.round as f64)),
                    (KEY_ROOT.to_string(), JsonValue::Number(s.root as f64)),
                ])
            })
            .collect(),
    )
}

fn scope_from_json(v: &JsonValue) -> Option<Scope> {
    let fork = v.get(KEY_FORK)?.as_str()?.to_string();
    let join = v.get(KEY_JOIN)?.as_str()?.to_string();
    let round = json_i64(v.get(KEY_ROUND)?)?;
    let root = json_i64(v.get(KEY_ROOT)?)?;
    Some(Scope { fork, join, round, root })
}

fn obj_get<'a>(entries: &'a [(String, JsonValue)], key: &str) -> Option<&'a JsonValue> {
    entries
        .iter()
        .find(|(k, _)| k.as_str() == key)
        .map(|(_, v)| v)
}

/// 读申请待办上的直返上下文（缺键/形状不对 ⇒ None）。
pub fn reapply_of(vars: &FlowData) -> Option<ReapplyCtx> {
    let obj = match vars.get(VAR_REAPPLY_CTX) {
        Some(JsonValue::Object(entries)) if !entries.is_empty() => entries.as_slice(),
        _ => return None,
    };
    let mode = obj_get(obj, KEY_MODE)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let source_task_id = obj_get(obj, KEY_SOURCE_TASK)
        .and_then(json_i64)
        .unwrap_or(0);
    let source_node = obj_get(obj, KEY_SOURCE_NODE)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let branch = obj_get(obj, KEY_BRANCH)
        .and_then(|v| v.as_array())
        .map(|items| items.iter().filter_map(scope_from_json).collect())
        .unwrap_or_default();
    Some(ReapplyCtx { mode, source_task_id, source_node, branch })
}

pub fn put_reapply(task: &mut ProcessTask, ctx: &ReapplyCtx) {
    let mut entries: Vec<(String, JsonValue)> = Vec::with_capacity(4);
    entries.push((KEY_MODE.to_string(), JsonValue::Str(ctx.mode.clone())));
    entries.push((
        KEY_SOURCE_TASK.to_string(),
        JsonValue::Number(ctx.source_task_id as f64),
    ));
    entries.push((
        KEY_SOURCE_NODE.to_string(),
        JsonValue::Str(ctx.source_node.clone()),
    ));
    entries.push((KEY_BRANCH.to_string(), scopes_to_json(&ctx.branch)));
    task.variables
        .insert(VAR_REAPPLY_CTX.to_string(), JsonValue::Object(entries));
}

/// 在范围里找指定汇合点的那一档。
pub fn find_scope<'a>(scopes: &'a [Scope], join_id: &str) -> Option<&'a Scope> {
    scopes.iter().find(|s| s.join == join_id)
}

/// 客户端不得伪造/覆盖引擎控制键（办理入口先剥再合）。
pub fn strip_reserved(args: &mut FlowData) {
    args.remove(VAR_BRANCH_CTX);
    args.remove(VAR_REAPPLY_CTX);
}

/// 结构损坏的直返上下文明确失败，不按无标记处理。
pub fn require_source_task_id(ctx: &ReapplyCtx) -> JeeflowResult<i64> {
    if ctx.source_task_id <= 0 {
        return Err(JeeflowError::Business(
            "重提直返上下文缺少有效来源任务".to_string(),
        ));
    }
    Ok(ctx.source_task_id)
}

// ═══════════════════════════════════════════════════════
// 建单时计算分支归属
// ═══════════════════════════════════════════════════════

/// 新任务的分支归属：静态范围逐档继承父任务同档的 `round`/`root`；父任务不在该档
/// （本任务是该分叉的分支根）时 `round=父任务 id`、`root=本任务 id`（落库后由引擎回填，
/// 建单时 id 尚未生成 ⇒ 传 0）。
pub fn build_scopes(
    parent: Option<&ProcessTask>,
    model: &ProcessModel,
    node_id: &str,
    self_task_id: i64,
) -> Vec<Scope> {
    let statics = static_scopes(model, node_id);
    if statics.is_empty() {
        return statics;
    }
    let parent_scopes = parent.map(|p| scopes_of(&p.variables)).unwrap_or_default();
    let parent_id = parent.map(|p| p.task_id).unwrap_or(0);
    statics
        .into_iter()
        .map(|s| match parent_scopes
            .iter()
            .find(|p| p.fork == s.fork && p.join == s.join)
        {
            Some(inherited) => Scope {
                fork: s.fork,
                join: s.join,
                round: inherited.round,
                root: inherited.root,
            },
            None => Scope {
                fork: s.fork,
                join: s.join,
                round: parent_id,
                root: self_task_id,
            },
        })
        .collect()
}

/// 分支根任务落库拿到 id 后回填自身 `root`（建单时 id 尚未生成）；返回是否发生回填。
pub fn fill_root_self(task: &mut ProcessTask) -> bool {
    if task.task_id <= 0 {
        return false;
    }
    let mut scopes = scopes_of(&task.variables);
    let mut changed = false;
    for s in scopes.iter_mut() {
        if s.root == 0 {
            s.root = task.task_id;
            changed = true;
        }
    }
    if changed {
        put_scopes(task, &scopes);
    }
    changed
}

// ═══════════════════════════════════════════════════════
// 基础转换（JSON 往返后数字可能是整数/浮点/字符串）
// ═══════════════════════════════════════════════════════

fn json_i64(v: &JsonValue) -> Option<i64> {
    match v {
        JsonValue::Number(n) => Some(*n as i64),
        JsonValue::Str(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::parse_json;
    use crate::parser::ModelParser;

    fn flow_json() -> &'static str {
        r#"{
            "name": "reapply-unit",
            "nodes": [
                {"id": "start", "type": "snaker:start"},
                {"id": "apply", "type": "snaker:task", "properties": {"assignee": "applicant"}},
                {"id": "fork1", "type": "snaker:fork"},
                {"id": "a", "type": "snaker:task", "properties": {"reapplyMode": "RETURN_TO_SOURCE"}},
                {"id": "b", "type": "snaker:task"},
                {"id": "join1", "type": "snaker:join"},
                {"id": "c", "type": "snaker:task"},
                {"id": "end", "type": "snaker:end"}
            ],
            "edges": [
                {"sourceNodeId": "start", "targetNodeId": "apply"},
                {"sourceNodeId": "apply", "targetNodeId": "fork1"},
                {"sourceNodeId": "fork1", "targetNodeId": "a"},
                {"sourceNodeId": "fork1", "targetNodeId": "b"},
                {"sourceNodeId": "a", "targetNodeId": "join1"},
                {"sourceNodeId": "b", "targetNodeId": "join1"},
                {"sourceNodeId": "join1", "targetNodeId": "c"},
                {"sourceNodeId": "c", "targetNodeId": "end"}
            ]
        }"#
    }

    fn task(id: i64, node: &str, vars: FlowData) -> ProcessTask {
        ProcessTask {
            task_id: id,
            process_instance_id: 1,
            task_name: node.to_string(),
            display_name: node.to_string(),
            task_type: 0,
            perform_type: 0,
            task_state: 10,
            actor_id: None,
            actor_ids: vec![],
            finish_time: None,
            expire_time: None,
            form_key: None,
            parent_task_id: None,
            variables: vars,
            create_time: None,
            create_user: None,
            update_time: None,
            update_user: None,
        }
    }

    #[test]
    fn test_static_scopes_inner_outer_order() {
        let model = ModelParser::parse(flow_json()).unwrap();
        let scopes = static_scopes(&model, "a");
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].fork, "fork1");
        assert_eq!(scopes[0].join, "join1");
        assert!(static_scopes(&model, "apply").is_empty(), "分叉外节点没有归属");
        assert!(static_scopes(&model, "c").is_empty(), "汇合后节点没有归属");
    }

    #[test]
    fn test_build_scopes_inherits_or_roots() {
        let model = ModelParser::parse(flow_json()).unwrap();
        let parent_task = task(7, "apply", FlowData::new());
        let rooted = build_scopes(Some(&parent_task), &model, "a", 0);
        assert_eq!(rooted.len(), 1);
        assert_eq!(rooted[0].round, 7, "分支根 round＝父任务 id");
        assert_eq!(rooted[0].root, 0, "建单时自身 id 未生成");

        let mut parent = task(9, "a", FlowData::new());
        put_scopes(
            &mut parent,
            &[Scope { fork: "fork1".into(), join: "join1".into(), round: 7, root: 11 }],
        );
        let inherited = build_scopes(Some(&parent), &model, "a", 0);
        assert_eq!(inherited[0].round, 7);
        assert_eq!(inherited[0].root, 11, "同档继承父任务的 round/root");
    }

    #[test]
    fn test_fill_root_self_backfills_only_zero_roots() {
        let mut t = task(42, "a", FlowData::new());
        put_scopes(
            &mut t,
            &[Scope { fork: "fork1".into(), join: "join1".into(), round: 7, root: 0 }],
        );
        assert!(fill_root_self(&mut t));
        assert_eq!(scopes_of(&t.variables)[0].root, 42);
        assert!(!fill_root_self(&mut t), "已回填过不再变");
    }

    #[test]
    fn test_reapply_ctx_roundtrip_and_defensive_number_parse() {
        let mut t = task(1, "apply", FlowData::new());
        put_reapply(
            &mut t,
            &ReapplyCtx::new(5, "a", vec![Scope { fork: "fork1".into(), join: "join1".into(), round: 7, root: 11 }]),
        );
        let ctx = reapply_of(&t.variables).unwrap();
        assert_eq!(ctx.mode, "RETURN_TO_SOURCE");
        assert_eq!(ctx.source_task_id, 5);
        assert_eq!(ctx.source_node, "a");
        assert_eq!(ctx.branch.len(), 1);

        // 字符串数字形状（跨语言/JSON 往返）也必须认
        let raw = parse_json(
            r#"{"_reapplyContext":{"mode":"RETURN_TO_SOURCE","sourceTaskId":"5","sourceNode":"a",
                "branch":[{"fork":"fork1","join":"join1","round":"7","root":"11"}]}}"#,
        )
        .unwrap();
        let vars = FlowData::from_map(raw.to_map());
        let ctx = reapply_of(&vars).unwrap();
        assert_eq!(ctx.source_task_id, 5);
        assert_eq!(ctx.branch[0].round, 7);
        assert_eq!(ctx.branch[0].root, 11);
    }

    #[test]
    fn test_strip_reserved_removes_forged_keys() {
        let mut args = FlowData::new();
        args.insert_str(VAR_BRANCH_CTX, "x");
        args.insert_str(VAR_REAPPLY_CTX, "y");
        args.insert_str("submitType", "5");
        strip_reserved(&mut args);
        assert!(!args.contains_key(VAR_BRANCH_CTX));
        assert!(!args.contains_key(VAR_REAPPLY_CTX));
        assert_eq!(args.get_str("submitType"), Some("5"));
    }

    #[test]
    fn test_require_source_task_id_rejects_bad_ctx() {
        let bad = ReapplyCtx::new(0, "a", vec![]);
        let err = require_source_task_id(&bad).unwrap_err();
        assert!(err.message().contains("缺少有效来源任务"));
    }
}
