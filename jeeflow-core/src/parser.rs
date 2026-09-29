//! Model parser — LogicFlow JSON → ProcessModel（spec/02 的 8 种节点类型 ＋ 一个未知档）。
//! spec/01, spec/02: Process model structure.
//! ⚠️ 类型表**没有**兜底臂：认不出来的类型落 `NodeType::Unknown`（记可诊断日志后由执行腿跳过），
//! 不再像旧形状那样一律当 Custom —— 详见 [`NodeType::from_snaker_type`]。

use crate::json::{JsonValue, parse_json};
use crate::error::{JeeflowError, JeeflowResult};
use std::collections::HashMap;

// ═══════════════════════════════════════════════════════
// Process Model
// ═══════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct ProcessModel {
    pub name: String,
    pub display_name: String,
    pub model_type: String,
    pub expire_time: Option<String>,
    pub persist_mode: Option<String>,     // ARCHIVE / SYNC
    pub rel_table_name: Option<String>,
    pub nodes: Vec<NodeModel>,
    pub edges: Vec<EdgeModel>,
}

impl ProcessModel {
    /// Find the start node.
    pub fn get_start(&self) -> Option<&NodeModel> {
        self.nodes.iter().find(|n| n.node_type == NodeType::Start)
    }

    /// Find a node by ID.
    pub fn get_node(&self, id: &str) -> Option<&NodeModel> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Find nodes by type.
    pub fn get_nodes_by_type(&self, node_type: NodeType) -> Vec<&NodeModel> {
        self.nodes.iter().filter(|n| n.node_type == node_type).collect()
    }

    /// Get outgoing edges from a node.
    pub fn get_output_edges(&self, node_id: &str) -> Vec<&EdgeModel> {
        self.edges.iter().filter(|e| e.source_node_id == node_id).collect()
    }

    /// Get incoming edges to a node.
    pub fn get_input_edges(&self, node_id: &str) -> Vec<&EdgeModel> {
        self.edges.iter().filter(|e| e.target_node_id == node_id).collect()
    }

    /// Get the target node of an edge.
    pub fn get_target_node(&self, edge: &EdgeModel) -> Option<&NodeModel> {
        self.get_node(&edge.target_node_id)
    }

    /// Get the first task node after start.
    pub fn get_first_task_node(&self) -> Option<&NodeModel> {
        if let Some(start) = self.get_start() {
            let edges = self.get_output_edges(&start.id);
            if let Some(edge) = edges.first() {
                return self.get_node(&edge.target_node_id);
            }
        }
        None
    }

    /// Check if this is the first task node (apply node).
    pub fn is_first_task_node(&self, node_id: &str) -> bool {
        if let Some(first) = self.get_first_task_node() {
            return first.id == node_id;
        }
        false
    }

    /// 能否退回到 `parent_id` 所在节点——照 mldong-boot2 `NodeModel.canRejected` 的形状：
    /// 自 current 的入边递归回溯，只穿越 fork/join/start 三类节点，其余节点即"上一步"本身。
    /// 与 boot2 同样不带 visited 集（保持语义一致；纯任务节点组成的回环理论上会无限回溯，
    /// 那是 boot2 继承来的性质，不在这里悄悄改掉判据）。
    pub fn can_rejected(&self, current_id: &str, parent_id: &str) -> bool {
        for edge in self.get_input_edges(current_id) {
            let source = &edge.source_node_id;
            if source == parent_id {
                return true;
            }
            match self.get_node(source) {
                Some(n) if matches!(n.node_type, NodeType::Fork | NodeType::Join | NodeType::Start) => continue,
                Some(_) => {
                    if self.can_rejected(source, parent_id) {
                        return true;
                    }
                }
                None => {}
            }
        }
        false
    }

    /// Get all end nodes.
    pub fn get_end_nodes(&self) -> Vec<&NodeModel> {
        self.get_nodes_by_type(NodeType::End)
    }
}

// ═══════════════════════════════════════════════════════
// Node types (spec/02 的 8 档 ＋ Unknown)
// ═══════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Start,
    Task,
    Decision,
    Fork,
    Join,
    End,
    Custom,
    SubProcess,
    /// 类型表里没有的档（issues/142 A 批：把旧 `_ => Custom` 兜底臂拆出来的那一半）。
    ///
    /// 存在的理由：兜底臂把**任何**未知串（含 `snaker:Custom`／`subProcess` 这类拼错大小写、
    /// 含 `snaker:custom` 本身）一律收成 Custom，而 spec/02 §6.1 把 custom 定性成**记录类**
    /// 之后，"兜底臂 = Custom"就等于"未知节点一律按记录类处理"——正是 §6.1 禁止的形状①
    /// （当任务类建 DOING 行）与 G4 义务 2 点名要防的静默分叉。
    /// 未知档的行为见 [`NodeType::from_snaker_type`] 与 `engine::execute_node` 的
    /// `NodeType::Unknown` 分支（记日志＋跳过节点，不建行、不沿出边推进，与 java 同形）。
    Unknown,
}

/// 解析期发出的未知类型诊断文案（spec/02「类型键的三条义务」第 2 条 · issues/141 G4 立法）。
///
/// 拆成**纯函数**的理由：本栈 core 零依赖、没有可注入的 logger 门面，落点是 stderr
/// （与 `event.rs::ProcessPublisher`／`surrogate.rs::expand_actors` 那两处同一口径），
/// 用例只能钉文案本身。形状逐字对齐 java `ModelParser.java:86-88` 那句 WARNING
/// ——**带节点 id 与实得类型串**，这是"可诊断"的最低要求（旧兜底臂一声不吭）。
pub fn unknown_node_warning(node_id: &str, raw_type: &str) -> String {
    format!("[jeeflow] 流程定义里的节点类型不在类型表内，该节点将被跳过（不建行、不沿出边推进）: nodeId={}, type={}", node_id, raw_type)
}

impl NodeType {
    /// 类型串 → 类型档。
    ///
    /// 两条判据口径（都按 spec/02 的条文走，不在本栈自造语义）：
    ///
    /// 1. **先剥 `snaker:` 前缀再查表**——spec/02「节点类型总览」那张表的两列
    ///      （`snaker:task` / `task`）都是合法写法，java 侧同样是
    ///      `ModelParser.java:77` 先 `replace(NODE_NAME_PREFIX, "")` 再按裸名查表。
    ///      ⚠️ 这一支**不是**大小写归一（G4 义务 1 另批做，java `ModelParser.java:81-82`
    ///      那条注释立的就是同一句"先补别名再谈归一化"）：查表仍逐字精确匹配，
    ///      所以 `snaker:Task` 这类拼错大小写的串在本栈落进 [`NodeType::Unknown`]，
    ///      而不再像旧兜底臂那样被当成 Custom。
    /// 2. **custom 是显式一档**（issues/142 A 批要求拆开的那件事的另一半）：
    ///      只有 `snaker:custom` / `custom` 才是记录类节点，其余未命中一律 Unknown。
    ///
    /// ⚠️ **子流程的大写两档（`subProcess`／`wfSubProcess`，java
    /// `Configuration.java:49-52` 注册的真别名）本轮**故意**不落 SubProcess**，
    /// 与 spec/02 义务 3 暂时分叉。理由不是省事：本栈的 SubProcess 执行腿
    /// （`engine.rs:442`）目前是**空壳**（只取属性、既不建子实例也不推进），
    /// 把这两档现在并过去，等于把"记一条 WARNING 后跳过"换成"一声不吭地吞掉节点"
    /// ——义务 3 的别名要和子流程真实现**同一批**落，否则是在扩大静默面。
    /// 挂账见 issues/141 G4 待办（补 SubProcess 实现时一并收别名）。
    ///
    /// 与 java 的一处**有意**差异：java 在解析期就把未知节点**丢掉**（`continue`），
    /// 本栈把节点留在模型里、标成 `Unknown`，由执行腿跳过。两条路的**可观测结果相同**
    /// （都不建行、令牌都到不了该节点的下游），留着是为了
    /// ① 字面兑现"未知档不得静默丢节点"（节点还在模型里，`get_node`/入出边都还查得到）；
    /// ② 诊断面完整（门面回显 `json_object`、血缘回溯都不必再猜）。
    pub fn from_snaker_type(t: &str) -> Self {
        let bare = t.strip_prefix("snaker:").unwrap_or(t);
        match bare {
            "start" => NodeType::Start,
            "task" => NodeType::Task,
            "decision" => NodeType::Decision,
            "fork" => NodeType::Fork,
            "join" => NodeType::Join,
            "end" => NodeType::End,
            "custom" => NodeType::Custom,
            "subprocess" => NodeType::SubProcess,
            _ => NodeType::Unknown,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            NodeType::Start => "start",
            NodeType::Task => "task",
            NodeType::Decision => "decision",
            NodeType::Fork => "fork",
            NodeType::Join => "join",
            NodeType::End => "end",
            NodeType::Custom => "custom",
            NodeType::SubProcess => "subprocess",
            NodeType::Unknown => "unknown",
        }
    }
}

// ═══════════════════════════════════════════════════════
// Node model
// ═══════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct NodeModel {
    pub id: String,
    pub node_type: NodeType,
    pub display_name: String,
    pub properties: HashMap<String, JsonValue>,
}

impl NodeModel {
    /// Get string property.
    pub fn prop_str(&self, key: &str) -> Option<String> {
        self.properties.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
    }

    /// Get i64 property.
    pub fn prop_i64(&self, key: &str) -> Option<i64> {
        self.properties.get(key).and_then(|v| v.as_i64())
    }

    /// Assignee (fixed participant).
    pub fn assignee(&self) -> Option<String> {
        self.prop_str("assignee")
    }

    /// Assignment handler (dynamic participant).
    pub fn assignment_handler(&self) -> Option<String> {
        self.prop_str("assignmentHandler")
    }

    /// Perform type (0=normal, 1=countersign).
    /// Handles both JSON number (1) and string ("1") for cross-language compatibility.
    pub fn perform_type(&self) -> i32 {
        // Try numeric first (handles JSON number 1 and string "1")
        if let Some(n) = self.prop_i64("performType") {
            return match n {
                1 => 1,
                _ => 0,
            };
        }
        // Fall back to string parsing
        self.prop_str("performType")
            .map(|s| match s.to_uppercase().as_str() {
                "1" | "ALL" | "COUNTERSIGN" => 1,
                _ => s.parse::<i32>().unwrap_or(0),
            })
            .unwrap_or(0)
    }

    /// Task type (0=major, 1=assistant, 2=record).
    pub fn task_type(&self) -> i32 {
        self.prop_i64("taskType").unwrap_or(0) as i32
    }

    /// Countersign type.
    pub fn countersign_type(&self) -> String {
        self.prop_str("countersignType").unwrap_or_else(|| "PARALLEL".to_string())
    }

    /// Countersign completion condition (from top-level properties or field sub-object).
    pub fn countersign_completion_condition(&self) -> Option<String> {
        // Check top-level properties first
        if let Some(cond) = self.prop_str("countersignCompletionCondition") {
            return Some(cond);
        }
        // Fallback: check inside field sub-object (some flow definitions nest it there)
        if let Some(field_val) = self.properties.get("field") {
            if let Some(field_obj) = field_val.as_object() {
                for (k, v) in field_obj {
                    if k == "countersignCompletionCondition" {
                        return v.as_str().map(|s| s.to_string());
                    }
                }
            }
        }
        None
    }

    /// Expression (for decision edges).
    pub fn expr(&self) -> Option<String> {
        self.prop_str("expr")
    }

    /// Form key.
    pub fn form_key(&self) -> Option<String> {
        self.prop_str("form")
    }

    /// Candidate users (comma-separated).
    pub fn candidate_users(&self) -> Option<String> {
        self.prop_str("candidateUsers")
    }

    /// Candidate groups (comma-separated).
    pub fn candidate_groups(&self) -> Option<String> {
        self.prop_str("candidateGroups")
    }

    /// Field permissions (PERMISSION_f_xxx → 1/2/3).
    pub fn field_permissions(&self) -> HashMap<String, i32> {
        let mut perms = HashMap::new();
        for (k, v) in &self.properties {
            if k.starts_with("PERMISSION_") {
                if let Some(val) = v.as_i64() {
                    perms.insert(k.clone(), val as i32);
                }
            }
        }
        perms
    }

    /// Sub-process define name.
    pub fn sub_process_name(&self) -> Option<String> {
        self.prop_str("subProcessName")
    }

    /// Is this node a countersign?
    pub fn is_countersign(&self) -> bool {
        self.perform_type() == 1
    }
}

// ═══════════════════════════════════════════════════════
// Edge model
// ═══════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct EdgeModel {
    pub id: String,
    pub source_node_id: String,
    pub target_node_id: String,
    pub label: Option<String>,
    pub properties: HashMap<String, JsonValue>,
}

impl EdgeModel {
    /// Get expression (for decision branches).
    pub fn expr(&self) -> Option<String> {
        self.properties.get("expr")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }
}

// ═══════════════════════════════════════════════════════
// ModelParser — LogicFlow JSON → ProcessModel
// ═══════════════════════════════════════════════════════

pub struct ModelParser;

impl ModelParser {
    /// Parse LogicFlow JSON string into ProcessModel.
    pub fn parse(json_str: &str) -> JeeflowResult<ProcessModel> {
        // issues/139：对外 msg 逐字用基准（jeeflow-java `ModelParser`）那句
        // 「读取流程定义 JSON 失败」，底层解析器的原文只挂错误链（`Error::source`）。
        // 旧形状 `ParseError(format!("JSON parse error: {}", e))` 既拼底层文本又是英文，
        // 经门面 `error_response(&e.message())` 一字不差透出到 msg（deploy/redeploy/designRedeploy
        // 三条腿同一个收口，13 栈 L2 的对外文案因此对不齐 java）。
        let root = parse_json(json_str)
            .map_err(|e| JeeflowError::parse_failure(
                crate::error::MSG_READ_PROCESS_DEFINE_JSON_FAILED, e))?;

        let name = root.get_str("name").unwrap_or("unknown").to_string();
        let display_name = root.get_str("displayName").unwrap_or(&name).to_string();
        let model_type = root.get_str("type").unwrap_or("approval").to_string();
        let expire_time = root.get_str("expireTime").map(|s| s.to_string());
        let persist_mode = root.get_str("persistMode").map(|s| s.to_string());
        let rel_table_name = root.get_str("relTableName")
            .or_else(|| root.get_str("name"))
            .map(|s| s.to_string());

        // Parse nodes
        let mut nodes = Vec::new();
        if let Some(nodes_arr) = root.get("nodes").and_then(|v| v.as_array()) {
            for node_val in nodes_arr {
                let id = node_val.get_str("id").unwrap_or("").to_string();
                let raw_type = node_val.get_str("type").unwrap_or("snaker:task");
                let node_type = NodeType::from_snaker_type(raw_type);
                // G4 义务 2（issues/141 立法 · issues/142 A 批落地）：类型表里没有的串
                // **必须留一条可诊断记录**再决定跳过。旧形状是兜底臂静默收成 Custom，
                // 连"这里有个节点我没认出来"都不说，排查时只能靠猜。
                // 节点本身留在模型里标成 Unknown（java 是解析期直接 continue，
                // 可观测结果一致，差异与理由见 [`NodeType::from_snaker_type`]）。
                if node_type == NodeType::Unknown {
                    eprintln!("{}", unknown_node_warning(&id, raw_type));
                }

                // Display name from text.value or properties.displayName
                let display_name = node_val.get("text")
                    .and_then(|t| t.get_str("value"))
                    .or_else(|| node_val.get_str("displayName"))
                    .unwrap_or("")
                    .to_string();

                // Properties
                let mut properties = HashMap::new();
                if let Some(props) = node_val.get("properties").and_then(|v| v.as_object()) {
                    for (k, v) in props {
                        properties.insert(k.clone(), v.clone());
                    }
                }

                nodes.push(NodeModel {
                    id,
                    node_type,
                    display_name,
                    properties,
                });
            }
        }

        // Parse edges
        let mut edges = Vec::new();
        if let Some(edges_arr) = root.get("edges").and_then(|v| v.as_array()) {
            for edge_val in edges_arr {
                let id = edge_val.get_str("id").unwrap_or("").to_string();
                let source_node_id = edge_val.get_str("sourceNodeId").unwrap_or("").to_string();
                let target_node_id = edge_val.get_str("targetNodeId").unwrap_or("").to_string();
                let label = edge_val.get("text")
                    .and_then(|t| t.get_str("value"))
                    .map(|s| s.to_string());

                let mut properties = HashMap::new();
                if let Some(props) = edge_val.get("properties").and_then(|v| v.as_object()) {
                    for (k, v) in props {
                        properties.insert(k.clone(), v.clone());
                    }
                }

                edges.push(EdgeModel {
                    id,
                    source_node_id,
                    target_node_id,
                    label,
                    properties,
                });
            }
        }

        Ok(ProcessModel {
            name,
            display_name,
            model_type,
            expire_time,
            persist_mode,
            rel_table_name,
            nodes,
            edges,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn test_parse_simple_flow() {
        let json = r#"{
            "name": "test-flow",
            "displayName": "Test Flow",
            "type": "approval",
            "nodes": [
                {"id": "start", "type": "snaker:start", "text": {"value": "开始"}},
                {"id": "apply", "type": "snaker:task", "text": {"value": "申请"},
                 "properties": {"assignee": "applicant"}},
                {"id": "end", "type": "snaker:end", "text": {"value": "结束"}}
            ],
            "edges": [
                {"id": "e1", "sourceNodeId": "start", "targetNodeId": "apply"},
                {"id": "e2", "sourceNodeId": "apply", "targetNodeId": "end"}
            ]
        }"#;

        let model = ModelParser::parse(json).unwrap();
        assert_eq!(model.name, "test-flow");
        assert_eq!(model.display_name, "Test Flow");
        assert_eq!(model.nodes.len(), 3);
        assert_eq!(model.edges.len(), 2);

        let start = model.get_start().unwrap();
        assert_eq!(start.id, "start");
        assert_eq!(start.node_type, NodeType::Start);

        let first_task = model.get_first_task_node().unwrap();
        assert_eq!(first_task.id, "apply");
        assert_eq!(first_task.assignee(), Some("applicant".to_string()));
    }

    #[test]
    fn test_parse_decision_flow() {
        let json = r#"{
            "name": "decision-flow",
            "displayName": "Decision",
            "type": "approval",
            "nodes": [
                {"id": "start", "type": "snaker:start", "text": {"value": "开始"}},
                {"id": "d1", "type": "snaker:decision", "text": {"value": "判断"}},
                {"id": "end", "type": "snaker:end", "text": {"value": "结束"}}
            ],
            "edges": [
                {"id": "e1", "sourceNodeId": "start", "targetNodeId": "d1"},
                {"id": "e2", "sourceNodeId": "d1", "targetNodeId": "end",
                 "properties": {"expr": "amount > 1000"}}
            ]
        }"#;

        let model = ModelParser::parse(json).unwrap();
        let decision = model.get_node("d1").unwrap();
        assert_eq!(decision.node_type, NodeType::Decision);

        let edges = model.get_output_edges("d1");
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].expr(), Some("amount > 1000".to_string()));
    }

    #[test]
    fn test_parse_countersign() {
        let json = r#"{
            "name": "cs-flow",
            "displayName": "Countersign",
            "type": "approval",
            "nodes": [
                {"id": "start", "type": "snaker:start", "text": {"value": "开始"}},
                {"id": "cs", "type": "snaker:task", "text": {"value": "会签"},
                 "properties": {"performType": "1", "countersignType": "PARALLEL"}},
                {"id": "end", "type": "snaker:end", "text": {"value": "结束"}}
            ],
            "edges": [
                {"id": "e1", "sourceNodeId": "start", "targetNodeId": "cs"},
                {"id": "e2", "sourceNodeId": "cs", "targetNodeId": "end"}
            ]
        }"#;

        let model = ModelParser::parse(json).unwrap();
        let cs = model.get_node("cs").unwrap();
        assert!(cs.is_countersign());
        assert_eq!(cs.countersign_type(), "PARALLEL");
    }

    #[test]
    fn test_parse_empty_nodes() {
        let json = r#"{"name": "empty", "displayName": "Empty", "type": "approval", "nodes": [], "edges": []}"#;
        let model = ModelParser::parse(json).unwrap();
        assert_eq!(model.nodes.len(), 0);
        assert!(model.get_start().is_none());
    }

    #[test]
    fn test_parse_fork_join_flow() {
        let json = r#"{
            "name": "fork-flow", "displayName": "Fork", "type": "approval",
            "nodes": [
                {"id": "start", "type": "snaker:start", "text": {"value": "Start"}},
                {"id": "fork1", "type": "snaker:fork", "text": {"value": "Fork"}},
                {"id": "t1", "type": "snaker:task", "text": {"value": "Task1"}, "properties": {"assignee": "user1"}},
                {"id": "t2", "type": "snaker:task", "text": {"value": "Task2"}, "properties": {"assignee": "user2"}},
                {"id": "join1", "type": "snaker:join", "text": {"value": "Join"}},
                {"id": "end", "type": "snaker:end", "text": {"value": "End"}}
            ],
            "edges": [
                {"id": "e1", "sourceNodeId": "start", "targetNodeId": "fork1"},
                {"id": "e2", "sourceNodeId": "fork1", "targetNodeId": "t1"},
                {"id": "e3", "sourceNodeId": "fork1", "targetNodeId": "t2"},
                {"id": "e4", "sourceNodeId": "t1", "targetNodeId": "join1"},
                {"id": "e5", "sourceNodeId": "t2", "targetNodeId": "join1"},
                {"id": "e6", "sourceNodeId": "join1", "targetNodeId": "end"}
            ]
        }"#;
        let model = ModelParser::parse(json).unwrap();
        assert_eq!(model.nodes.len(), 6);
        let fork = model.get_node("fork1").unwrap();
        assert_eq!(fork.node_type, NodeType::Fork);
        let join = model.get_node("join1").unwrap();
        assert_eq!(join.node_type, NodeType::Join);
        let output_edges = model.get_output_edges("fork1");
        assert_eq!(output_edges.len(), 2);
    }

    #[test]
    fn test_parse_node_properties() {
        let json = r#"{
            "name": "prop-flow", "displayName": "Props", "type": "approval",
            "nodes": [
                {"id": "start", "type": "snaker:start", "text": {"value": "S"}},
                {"id": "t1", "type": "snaker:task", "text": {"value": "T"},
                 "properties": {"assignee": "user1", "form": "form1", "expireTime": "2d"}},
                {"id": "end", "type": "snaker:end", "text": {"value": "E"}}
            ],
            "edges": [
                {"id": "e1", "sourceNodeId": "start", "targetNodeId": "t1"},
                {"id": "e2", "sourceNodeId": "t1", "targetNodeId": "end"}
            ]
        }"#;
        let model = ModelParser::parse(json).unwrap();
        let t1 = model.get_node("t1").unwrap();
        assert_eq!(t1.assignee(), Some("user1".to_string()));
        assert_eq!(t1.form_key(), Some("form1".to_string()));
    }

    #[test]
    fn test_parse_invalid_json() {
        let result = ModelParser::parse("not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_end_nodes() {
        let json = r#"{
            "name": "test", "displayName": "T", "type": "approval",
            "nodes": [
                {"id": "start", "type": "snaker:start", "text": {"value": "S"}},
                {"id": "end1", "type": "snaker:end", "text": {"value": "E1"}},
                {"id": "end2", "type": "snaker:end", "text": {"value": "E2"}}
            ],
            "edges": [
                {"id": "e1", "sourceNodeId": "start", "targetNodeId": "end1"},
                {"id": "e2", "sourceNodeId": "start", "targetNodeId": "end2"}
            ]
        }"#;
        let model = ModelParser::parse(json).unwrap();
        let ends = model.get_end_nodes();
        assert_eq!(ends.len(), 2);
    }

    #[test]
    fn test_get_all_task_nodes() {
        let json = r#"{
            "name": "test", "displayName": "T", "type": "approval",
            "nodes": [
                {"id": "start", "type": "snaker:start", "text": {"value": "S"}},
                {"id": "t1", "type": "snaker:task", "text": {"value": "T1"}, "properties": {"assignee": "u1"}},
                {"id": "t2", "type": "snaker:task", "text": {"value": "T2"}, "properties": {"assignee": "u2"}},
                {"id": "end", "type": "snaker:end", "text": {"value": "E"}}
            ],
            "edges": [
                {"id": "e1", "sourceNodeId": "start", "targetNodeId": "t1"},
                {"id": "e2", "sourceNodeId": "t1", "targetNodeId": "t2"},
                {"id": "e3", "sourceNodeId": "t2", "targetNodeId": "end"}
            ]
        }"#;
        let model = ModelParser::parse(json).unwrap();
        let task_count = model.nodes.iter().filter(|n| n.node_type == NodeType::Task).count();
        assert_eq!(task_count, 2);
    }

    // ═══ issues/139 · 解析失败的对外 msg 用 java 逐字原文，底层文本只上错误链 ═══

    /// 正向＋负向：`msg` 逐字＝「读取流程定义 JSON 失败」，既不含底层英文原文也不含包装前缀；
    /// 底层文本仍在错误链上（`Error::source`），排障不丢信息。
    /// 改前实测：`msg = "解析错误: JSON parse error: Unexpected character at position 2"`
    /// ——拼底层文本＋英文，经门面逐字透出到对外 msg。
    #[test]
    fn test_i139_parse_failure_msg_is_java_verbatim() {
        let bad = "{ 这不是合法的流程定义 JSON";
        let err = ModelParser::parse(bad).unwrap_err();

        assert_eq!(err.message(), crate::error::MSG_READ_PROCESS_DEFINE_JSON_FAILED,
            "对外 msg 必须逐字＝java 基准原文，实得 {:?}", err.message());
        assert_eq!(err.message(), "读取流程定义 JSON 失败", "同上，把基准文案钉死在字面量上");
        assert_eq!(err.code(), crate::error::ERR_BUSINESS, "仍走业务失败码 99999999");

        let shown = format!("{}", err);
        assert!(!shown.contains("JSON parse error"), "msg 里不许有底层英文原文：{shown}");
        assert!(!shown.contains("解析错误"), "msg 里不许有包装前缀：{shown}");
        assert!(!shown.contains("position"), "msg 里不许有底层位置细节：{shown}");

        // 原始异常只留在错误链上（java 的 RuntimeException(msg, e) 那一档）
        let src = err.source().expect("底层解析文本必须挂在 Error::source 上，不能直接丢掉");
        let src_text = src.to_string();
        assert!(!src_text.is_empty(), "错误链上的底层原文不得为空");
        assert_ne!(src_text, err.message(), "链上那份必须是与对外 msg 不同的底层原文");
    }

    /// 改动面哨兵：`ParseError` 那一档也改成"payload 即 msg"（不再套前缀），
    /// 与 `Business` 同规则；既有格 `error::tests::test_parse_error_message` 用的是
    /// `contains`，两边都不破。
    #[test]
    fn test_i139_parse_error_payload_is_the_message() {
        let err = JeeflowError::ParseError("读取流程定义 JSON 失败".into());
        assert_eq!(err.message(), "读取流程定义 JSON 失败");
        assert!(err.source().is_none(), "没带底层异常时错误链为空，不该凭空造一层");
    }

    // ═══ issues/142 A 批 · 类型表拆臂：custom 显式一档 ＋ 未知另立一档 ═══

    /// 八档类型表逐档认，且 spec/02「节点类型总览」的**两列兼容写法**都认
    /// （`snaker:task` 与 `task`；java 那边同样是先剥 `snaker:` 前缀再按裸名查表）。
    #[test]
    fn test_i142_type_table_recognises_all_documented_types() {
        let cases = [
            ("snaker:start", NodeType::Start), ("start", NodeType::Start),
            ("snaker:task", NodeType::Task), ("task", NodeType::Task),
            ("snaker:decision", NodeType::Decision), ("decision", NodeType::Decision),
            ("snaker:fork", NodeType::Fork), ("fork", NodeType::Fork),
            ("snaker:join", NodeType::Join), ("join", NodeType::Join),
            ("snaker:end", NodeType::End), ("end", NodeType::End),
            ("snaker:custom", NodeType::Custom), ("custom", NodeType::Custom),
            ("snaker:subprocess", NodeType::SubProcess), ("subprocess", NodeType::SubProcess),
        ];
        for (raw, want) in cases {
            assert_eq!(NodeType::from_snaker_type(raw), want, "类型串 {raw} 应落 {want:?}");
        }
    }

    /// **本单的核心那一半**：未知类型不再被兜底臂收成 Custom。
    /// 旧形状 `_ => NodeType::Custom` 让 `snaker:Custom` 这类拼错的串
    /// 一律"按记录类处理"，而 custom 已经定性成记录类（spec/02 §6.1）⇒ 那是把
    /// "认不出来"当成"认得、而且是记录类"，静默分叉。现在它们落 Unknown。
    ///
    /// ⚠️ 后三个串（`subProcess`／`snaker:subProcess`／`wfSubProcess`）在 java 是**真别名**，
    /// 本栈暂落 Unknown 属**有意分叉**，理由与销账条件写在 [`NodeType::from_snaker_type`]
    /// 的那段"子流程大写两档故意不落 SubProcess"里（本栈 SubProcess 执行腿还是空壳）。
    /// 这一档**不是**"归一化已完成"的判据，别照它推断大小写归一的状态。
    #[test]
    fn test_i142_unknown_type_is_not_custom_anymore() {
        for raw in ["snaker:Custom", "snaker:TASK", "SUBPROCESS", "Process",
                    "snaker:approve", "custom2", "", "snaker:",
                    "subProcess", "snaker:subProcess", "wfSubProcess"] {
            let got = NodeType::from_snaker_type(raw);
            assert_eq!(got, NodeType::Unknown, "未知串 {raw:?} 必须落 Unknown，实得 {got:?}");
            assert_ne!(got, NodeType::Custom, "未知串 {raw:?} 绝不能再被兜底臂当成记录类");
        }
    }

    /// 未知节点：**留在模型里**（"不静默丢节点"的字面兑现）＋落一条带 nodeId 与实得类型串的
    /// 可诊断日志（G4 义务 2）。旧形状这里既不打日志、又把节点悄悄变成 Custom。
    #[test]
    fn test_i142_parse_keeps_unknown_node_and_logs_diagnosis() {
        let json = r#"{
            "name": "unknown-flow", "displayName": "U", "type": "approval",
            "nodes": [
                {"id": "start", "type": "snaker:start", "text": {"value": "S"}},
                {"id": "typo1", "type": "snaker:Custom", "text": {"value": "拼错大小写"}},
                {"id": "end", "type": "snaker:end", "text": {"value": "E"}}
            ],
            "edges": [
                {"id": "e1", "sourceNodeId": "start", "targetNodeId": "typo1"},
                {"id": "e2", "sourceNodeId": "typo1", "targetNodeId": "end"}
            ]
        }"#;
        let model = ModelParser::parse(json).unwrap();
        // 节点没被丢：三个节点、边也照旧连得上
        assert_eq!(model.nodes.len(), 3, "未知档节点必须留在模型里（不许静默丢）");
        let typo = model.get_node("typo1").expect("typo1 应仍在模型里可查");
        assert_eq!(typo.node_type, NodeType::Unknown, "落 Unknown 档，而不是 Custom");
        assert_eq!(typo.node_type, NodeType::from_snaker_type("snaker:Custom"));
        assert_eq!(model.get_output_edges("typo1").len(), 1, "它的出边也还在（不连带丢出边）");
    }

    /// 可诊断日志文案单点：带 nodeId ＋ 实得类型串（spec/02 G4 义务 2 的两个必备字段），
    /// 且与本仓 stderr 日志同一 `[jeeflow]` 前缀口径。落点本身是 eprintln
    /// （core 零依赖、无可注入 logger 门面，同 `event.rs`／`surrogate.rs` 两处）。
    #[test]
    fn test_i142_unknown_node_warning_carries_node_id_and_raw_type() {
        let msg = unknown_node_warning("typo1", "snaker:Custom");
        assert!(msg.starts_with("[jeeflow]"), "前缀与本仓既有 stderr 日志同口径：{msg}");
        assert!(msg.contains("typo1"), "必须带节点 id：{msg}");
        assert!(msg.contains("snaker:Custom"), "必须带**实得**类型串：{msg}");
        assert!(msg.contains("跳过"), "要说清楚后果（该节点被跳过）：{msg}");
    }

    /// 形状哨兵：`as_str` 对九档各给稳定小写名，未知档不得伪装成 "custom"
    /// （门面/日志/跨栈对账读的就是这个名，落 "unknown" 才不会被误读成记录类）。
    #[test]
    fn test_i142_as_str_covers_unknown_without_lying_custom() {
        assert_eq!(NodeType::Unknown.as_str(), "unknown");
        assert_ne!(NodeType::Unknown.as_str(), NodeType::Custom.as_str());
        assert_eq!(NodeType::Custom.as_str(), "custom");
    }
}
