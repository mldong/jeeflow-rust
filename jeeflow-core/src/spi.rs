//! SPI trait definitions — aligned with Java reference implementation.
//! All repository and provider traits are SYNCHRONOUS (no async/BoxFuture).
//! Only the engine methods are async. This matches Go/Java patterns.
//! spec/05-spi.md

use crate::error::JeeflowResult;
use crate::json::JsonValue;
use crate::model::*;
use std::collections::HashMap;

// ═══════════════════════════════════════════════════════
// IProcessRepository (21+ methods) — spec/05
// ═══════════════════════════════════════════════════════

pub trait ProcessRepository: Send + Sync {
    // ═══ Define operations ═══
    fn find_define_by_id(&self, define_id: i64) -> JeeflowResult<Option<ProcessDefine>>;
    fn save_define(&self, define: &mut ProcessDefine) -> JeeflowResult<()>;
    fn update_define(&self, define: &ProcessDefine) -> JeeflowResult<()>;
    fn update_define_state(&self, define_id: i64, state: i32) -> JeeflowResult<()>;
    fn remove_define(&self, define_id: i64) -> JeeflowResult<()>;

    // ═══ Instance operations ═══
    fn find_instance_by_id(&self, instance_id: i64) -> JeeflowResult<Option<ProcessInstance>>;
    fn save_instance(&self, instance: &mut ProcessInstance) -> JeeflowResult<()>;
    fn update_instance(&self, instance: &ProcessInstance) -> JeeflowResult<()>;

    // ═══ Task operations ═══
    fn find_task_by_id(&self, task_id: i64) -> JeeflowResult<Option<ProcessTask>>;
    fn save_task(&self, task: &mut ProcessTask) -> JeeflowResult<()>;
    fn update_task(&self, task: &ProcessTask) -> JeeflowResult<()>;
    fn find_doing_tasks(&self, instance_id: i64, task_names: &[String]) -> JeeflowResult<Vec<ProcessTask>>;
    fn find_done_tasks(&self, instance_id: i64, task_names: &[String]) -> JeeflowResult<Vec<ProcessTask>>;
    fn find_history_tasks(&self, instance_id: i64) -> JeeflowResult<Vec<ProcessTask>>;

    // ═══ Task actor operations ═══
    fn find_task_actors(&self, task_id: i64) -> JeeflowResult<Vec<String>>;
    fn add_task_actor(&self, task_id: i64, actors: &[String]) -> JeeflowResult<()>;
    fn remove_task_actor(&self, task_id: i64, actors: &[String]) -> JeeflowResult<()>;

    // ═══ CC operations ═══
    fn create_cc_instance(&self, instance_id: i64, creator: &str, actor_ids: &[String]) -> JeeflowResult<()>;
    fn update_cc_status(&self, instance_id: i64, actor_id: &str) -> JeeflowResult<()>;

    // ═══ Page queries ═══
    fn page_todo_tasks(&self, query: &PageQuery) -> JeeflowResult<PageResult<TaskRow>>;
    fn page_done_tasks(&self, query: &PageQuery) -> JeeflowResult<PageResult<TaskRow>>;
    fn page_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>>;
    fn page_cc_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>>;
    fn page_defines(&self, query: &PageQuery) -> JeeflowResult<PageResult<DefineRow>>;
    fn count_todo_tasks(&self, user_id: &str) -> JeeflowResult<i64>;
}

// ═══════════════════════════════════════════════════════
// IProcessExtRepository (14 methods) — spec/05
// ═══════════════════════════════════════════════════════

pub trait ProcessExtRepository: Send + Sync {
    // ═══ Design operations ═══
    fn find_design_by_id(&self, design_id: i64) -> JeeflowResult<Option<ProcessDesign>>;
    fn save_design(&self, design: &mut ProcessDesign) -> JeeflowResult<()>;
    fn update_design(&self, design: &ProcessDesign) -> JeeflowResult<()>;
    fn remove_design(&self, design_id: i64) -> JeeflowResult<()>;
    fn page_designs(&self, query: &PageQuery) -> JeeflowResult<PageResult<ProcessDesign>>;

    // ═══ Design history ═══
    fn save_design_his(&self, his: &mut ProcessDesignHis) -> JeeflowResult<()>;
    fn list_design_his(&self, design_id: i64) -> JeeflowResult<Vec<ProcessDesignHis>>;

    // ═══ Surrogate operations ═══
    fn find_surrogate_by_id(&self, surrogate_id: i64) -> JeeflowResult<Option<ProcessSurrogate>>;
    fn save_surrogate(&self, surrogate: &mut ProcessSurrogate) -> JeeflowResult<()>;
    fn update_surrogate(&self, surrogate: &ProcessSurrogate) -> JeeflowResult<()>;
    fn remove_surrogate(&self, surrogate_id: i64) -> JeeflowResult<()>;
    fn page_surrogates(&self, query: &PageQuery) -> JeeflowResult<PageResult<ProcessSurrogate>>;
    fn get_surrogate(&self, operator: &str, process_name: &str, time: &str) -> JeeflowResult<Option<ProcessSurrogate>>;
}

// ═══════════════════════════════════════════════════════
// IUserProvider — spec/05
// ═══════════════════════════════════════════════════════

pub trait UserProvider: Send + Sync {
    fn get_user(&self, user_id: &str) -> JeeflowResult<Option<UserInfo>>;
}

// ═══════════════════════════════════════════════════════
// IOrgUserProvider — spec/05 (v1.6.0)
// ═══════════════════════════════════════════════════════

pub trait OrgUserProvider: Send + Sync {
    fn find_dept_leaders(&self, dept_id: &str) -> JeeflowResult<Vec<String>>;
    fn find_dept_main_leaders(&self, dept_id: &str) -> JeeflowResult<Vec<String>>;
    fn find_by_role(&self, role_code: &str) -> JeeflowResult<Vec<String>>;
}

// ═══════════════════════════════════════════════════════
// IUserSearchProvider — spec/06 §4.3/§6 (v1.2.0)
// ═══════════════════════════════════════════════════════

pub trait UserSearchProvider: Send + Sync {
    fn page(&self, query: &PageQuery) -> JeeflowResult<PageResult<HashMap<String, JsonValue>>>;
    fn find_by_id(&self, user_id: &str) -> JeeflowResult<Option<HashMap<String, JsonValue>>>;
}

// ═══════════════════════════════════════════════════════
// IJsonProvider — spec/05
// ═══════════════════════════════════════════════════════

pub trait JsonProvider: Send + Sync {
    fn to_json(&self, value: &JsonValue) -> String;
    fn from_json(&self, json: &str) -> JeeflowResult<JsonValue>;
    fn is_json(&self, s: &str) -> bool;
}

// ═══════════════════════════════════════════════════════
// IExpressionEvaluator — spec/05
// ═══════════════════════════════════════════════════════

pub trait ExpressionEvaluator: Send + Sync {
    fn eval(&self, expression: &str, context: &HashMap<String, JsonValue>) -> JeeflowResult<JsonValue>;
}

// ═══════════════════════════════════════════════════════
// ITransactionTemplate — spec/05
// ═══════════════════════════════════════════════════════

pub trait TransactionTemplate: Send + Sync {
    /// Execute an action within a transaction.
    /// Uses a boxed closure for dyn compatibility.
    fn execute_in_tx(&self, action: Box<dyn FnOnce() -> JeeflowResult<Box<dyn std::any::Any + Send>> + Send>) -> JeeflowResult<Box<dyn std::any::Any + Send>>;
}

// ═══════════════════════════════════════════════════════
// IIdGenerator — spec/05
// ═══════════════════════════════════════════════════════

pub trait IdGenerator: Send + Sync {
    fn next_id(&self) -> i64;
}

// ═══════════════════════════════════════════════════════
// IActionPermissionProvider — spec/06 §2.6 (v1.8.3)
// ═══════════════════════════════════════════════════════

pub trait ActionPermissionProvider: Send + Sync {
    fn permission_codes(&self, action: &str) -> Vec<String>;
}

/// Default implementation: wf:{action / → :}
pub struct DefaultActionPermissionProvider;

impl ActionPermissionProvider for DefaultActionPermissionProvider {
    fn permission_codes(&self, action: &str) -> Vec<String> {
        let code = format!("wf:{}", action.replace('/', ":"));
        vec![code]
    }
}

// ═══════════════════════════════════════════════════════
// FlowInterceptor — concepts/04
// ═══════════════════════════════════════════════════════

pub trait FlowInterceptor: Send + Sync {
    fn intercept(&self, execution: &mut crate::engine::Execution) -> JeeflowResult<()>;
    fn order(&self) -> i32 { 0 }
}

// ═══════════════════════════════════════════════════════
// BizDataReader — processInstance/bizData 读侧（issues/30）
// ═══════════════════════════════════════════════════════

/// 按 relTableName + process_instance_id 回显业务表单条。
pub trait BizDataReader: Send + Sync {
    fn read_by_process_instance(
        &self,
        table_name: &str,
        process_instance_id: i64,
    ) -> JeeflowResult<Option<HashMap<String, JsonValue>>>;
}

// ═══════════════════════════════════════════════════════
// AssignmentHandler — guides/07
// ═══════════════════════════════════════════════════════

pub trait AssignmentHandler: Send + Sync {
    fn assign(&self, execution: &crate::engine::Execution) -> JeeflowResult<String>;
}

// ═══════════════════════════════════════════════════════
// DecisionHandler
// ═══════════════════════════════════════════════════════

pub trait DecisionHandler: Send + Sync {
    fn decide(&self, execution: &crate::engine::Execution) -> JeeflowResult<String>;
}

// ═══════════════════════════════════════════════════════
// CandidateHandler
// ═══════════════════════════════════════════════════════

pub trait CandidateHandler: Send + Sync {
    fn handle(&self, node: &crate::parser::NodeModel) -> JeeflowResult<Vec<Candidate>>;
}

#[derive(Debug, Clone)]
pub struct Candidate {
    pub id: String,
    pub display_name: String,
}

// ═══════════════════════════════════════════════════════
// ProcessEventListener — concepts/04 §4.1
// ═══════════════════════════════════════════════════════

pub trait ProcessEventListener: Send + Sync {
    fn on_event(&self, event: &crate::event::ProcessEvent);
}
