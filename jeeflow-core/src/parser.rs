//! Model parser — LogicFlow JSON → ProcessModel (8 node types).
//! spec/01, spec/02: Process model structure.

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

    /// Get all end nodes.
    pub fn get_end_nodes(&self) -> Vec<&NodeModel> {
        self.get_nodes_by_type(NodeType::End)
    }
}

// ═══════════════════════════════════════════════════════
// Node types (8 types)
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
}

impl NodeType {
    pub fn from_snaker_type(t: &str) -> Self {
        match t {
            "snaker:start" => NodeType::Start,
            "snaker:task" => NodeType::Task,
            "snaker:decision" => NodeType::Decision,
            "snaker:fork" => NodeType::Fork,
            "snaker:join" => NodeType::Join,
            "snaker:end" => NodeType::End,
            "snaker:subprocess" => NodeType::SubProcess,
            _ => NodeType::Custom,
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
        let root = parse_json(json_str)
            .map_err(|e| JeeflowError::ParseError(format!("JSON parse error: {}", e)))?;

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
}
