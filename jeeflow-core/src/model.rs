//! Domain model — DDD aggregate root (ProcessInstance) + sub-entity (ProcessTask).
//! Aligned with Java reference implementation: spec/03 (state machine), spec/04 (engine ops).

use crate::json::{JsonValue, FlowData};
use std::collections::HashMap;

// ═══════════════════════════════════════════════════════
// State enums (spec/03 + spec/07)
// ═══════════════════════════════════════════════════════

/// Process define state (wf_process_define_state).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefineState {
    Disable = 0,
    Enable = 1,
}

impl DefineState {
    pub fn from_code(code: i32) -> Self {
        match code {
            0 => DefineState::Disable,
            _ => DefineState::Enable,
        }
    }
    pub fn code(&self) -> i32 { *self as i32 }
}

/// Process instance state (wf_process_instance_state).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceState {
    Doing = 10,
    Finished = 20,
    Withdraw = 30,
    Interrupt = 40,
    Reject = 45,
    Pending = 50,
    Abandon = 99,
}

impl InstanceState {
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            10 => Some(InstanceState::Doing),
            20 => Some(InstanceState::Finished),
            30 => Some(InstanceState::Withdraw),
            40 => Some(InstanceState::Interrupt),
            45 => Some(InstanceState::Reject),
            50 => Some(InstanceState::Pending),
            99 => Some(InstanceState::Abandon),
            _ => None,
        }
    }
    pub fn code(&self) -> i32 { *self as i32 }
}

/// Process task state (wf_process_task_state).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Doing = 10,
    Finished = 20,
    Withdraw = 30,
    Interrupt = 40,
    Pending = 50,
    Abandon = 99,
}

impl TaskState {
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            10 => Some(TaskState::Doing),
            20 => Some(TaskState::Finished),
            30 => Some(TaskState::Withdraw),
            40 => Some(TaskState::Interrupt),
            50 => Some(TaskState::Pending),
            99 => Some(TaskState::Abandon),
            _ => None,
        }
    }
    pub fn code(&self) -> i32 { *self as i32 }
}

/// Task type (wf_process_task_type).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskType {
    Major = 0,     // 主办
    Assistant = 1, // 协办
    Record = 2,    // 记录
}

impl TaskType {
    pub fn from_code(code: i32) -> Self {
        match code {
            1 => TaskType::Assistant,
            2 => TaskType::Record,
            _ => TaskType::Major,
        }
    }
    pub fn code(&self) -> i32 { *self as i32 }
}

/// Task perform type (wf_process_task_perform_type).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PerformType {
    Normal = 0,      // 普通
    Countersign = 1, // 会签
}

impl PerformType {
    pub fn from_code(code: i32) -> Self {
        match code {
            1 => PerformType::Countersign,
            _ => PerformType::Normal,
        }
    }
    pub fn code(&self) -> i32 { *self as i32 }
}

/// Countersign type (wf_countersign_type).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountersignType {
    Parallel = 0,   // 并行会签
    Sequential = 1, // 串行会签
}

impl CountersignType {
    pub fn from_str_name(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "SEQUENTIAL" | "SERIAL" => CountersignType::Sequential,
            _ => CountersignType::Parallel,
        }
    }
}

/// Submit type (wf_process_submit_type) — spec/06 §2.8.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitType {
    Apply = 0,                  // 发起申请
    Agree = 1,                  // 同意
    Reject = 2,                 // 拒绝（流程结束）
    Rollback = 3,               // 退回上一步
    Jump = 4,                   // 跳转指定节点
    ReApply = 5,                // 重新提交
    RollbackToOperator = 6,     // 退回发起人
    CountersignDisagree = 20,   // 会签拒绝
}

impl SubmitType {
    pub fn from_code(code: i64) -> Option<Self> {
        match code {
            0 => Some(SubmitType::Apply),
            1 => Some(SubmitType::Agree),
            2 => Some(SubmitType::Reject),
            3 => Some(SubmitType::Rollback),
            4 => Some(SubmitType::Jump),
            5 => Some(SubmitType::ReApply),
            6 => Some(SubmitType::RollbackToOperator),
            20 => Some(SubmitType::CountersignDisagree),
            _ => None,
        }
    }
    pub fn code(&self) -> i64 { *self as i64 }
}

// ═══════════════════════════════════════════════════════
// Process Define (embedded in instance creation)
// ═══════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct ProcessDefine {
    pub id: i64,
    pub name: String,
    pub display_name: String,
    pub define_type: String,
    pub state: i32,
    pub content: Vec<u8>,  // LogicFlow JSON bytes
    pub version: i32,
    pub create_time: Option<String>,
    pub create_user: Option<String>,
    pub update_time: Option<String>,
    pub update_user: Option<String>,
}

impl ProcessDefine {
    pub fn content_str(&self) -> String {
        String::from_utf8_lossy(&self.content).to_string()
    }
}

// ═══════════════════════════════════════════════════════
// Process Instance — DDD Aggregate Root
// ═══════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct ProcessInstance {
    pub instance_id: i64,
    pub parent_id: Option<i64>,
    pub define_id: i64,
    pub state: i32,
    pub parent_node_name: Option<String>,
    pub business_no: Option<String>,
    pub operator: String,
    pub expire_time: Option<String>,
    pub variables: FlowData,
    pub tasks: Vec<ProcessTask>,
    pub create_time: Option<String>,
    pub create_user: Option<String>,
    pub update_time: Option<String>,
    pub update_user: Option<String>,
    // Transient: the define info (not persisted in instance table)
    pub define: Option<ProcessDefine>,
}

impl ProcessInstance {
    /// Factory: create a new instance (state=DOING=10).
    pub fn create(define: &ProcessDefine, operator: &str, args: &FlowData) -> Self {
        let mut variables = FlowData::new();
        variables.merge(args);
        ProcessInstance {
            instance_id: 0, // will be assigned by ID generator
            parent_id: None,
            define_id: define.id,
            state: InstanceState::Doing.code(),
            parent_node_name: None,
            business_no: args.get_str("BUSINESS_NO").map(|s| s.to_string()),
            operator: operator.to_string(),
            expire_time: None,
            variables,
            tasks: Vec::new(),
            create_time: Some(current_time_str()),
            create_user: Some(operator.to_string()),
            update_time: None,
            update_user: None,
            define: Some(define.clone()),
        }
    }

    /// Factory with parent (for sub-processes).
    pub fn create_with_parent(define: &ProcessDefine, operator: &str, args: &FlowData,
                               parent_id: i64, parent_node_name: &str) -> Self {
        let mut inst = Self::create(define, operator, args);
        inst.parent_id = Some(parent_id);
        inst.parent_node_name = Some(parent_node_name.to_string());
        inst
    }

    /// Complete a task: finish the task + merge f_ variables.
    pub fn complete_task(&mut self, task_id: i64, operator: &str, args: &FlowData) -> Result<(), String> {
        let task = self.tasks.iter_mut()
            .find(|t| t.task_id == task_id)
            .ok_or_else(|| format!("Task {} not found in instance {}", task_id, self.instance_id))?;

        // Merge f_ variables from args into instance variables
        for (k, v) in args.iter() {
            if k.starts_with("f_") {
                self.variables.insert(k.clone(), v.clone());
            }
        }

        task.finish(operator, args)?;
        Ok(())
    }

    /// Abandon a single task.
    pub fn abandon_task(&mut self, task_id: i64, _operator: &str) -> Result<(), String> {
        let task = self.tasks.iter_mut()
            .find(|t| t.task_id == task_id)
            .ok_or_else(|| format!("Task {} not found", task_id))?;
        task.abandon()?;
        self.state = InstanceState::Abandon.code();
        Ok(())
    }

    /// Abandon all DOING tasks.
    pub fn abandon_all_doing(&mut self) {
        for task in &mut self.tasks {
            if task.task_state == TaskState::Doing.code() {
                let _ = task.abandon();
            }
        }
    }

    /// Finish the instance (state → FINISHED=20).
    pub fn finish(&mut self) {
        self.state = InstanceState::Finished.code();
    }

    /// Reject the instance (state → REJECT=45).
    pub fn reject(&mut self) {
        self.state = InstanceState::Reject.code();
    }

    /// Interrupt (state → INTERRUPT=40).
    pub fn interrupt(&mut self) {
        for task in &mut self.tasks {
            if task.task_state == TaskState::Doing.code() {
                let _ = task.interrupt();
            }
        }
        self.state = InstanceState::Interrupt.code();
    }

    /// Resume from interrupt (state → DOING=10).
    pub fn resume(&mut self) {
        for task in &mut self.tasks {
            if task.task_state == TaskState::Interrupt.code() {
                task.task_state = TaskState::Doing.code();
            }
        }
        self.state = InstanceState::Doing.code();
    }

    /// Pending (state → PENDING=50).
    pub fn pending(&mut self) {
        for task in &mut self.tasks {
            if task.task_state == TaskState::Doing.code() {
                let _ = task.pending();
            }
        }
        self.state = InstanceState::Pending.code();
    }

    /// Withdraw (state → WITHDRAW=30).
    pub fn withdraw(&mut self) {
        for task in &mut self.tasks {
            if task.task_state == TaskState::Doing.code() {
                task.withdraw();
            }
        }
        self.state = InstanceState::Withdraw.code();
    }

    /// Add variables.
    pub fn add_variable(&mut self, args: &FlowData) {
        self.variables.merge(args);
    }

    /// Remove variables by keys.
    pub fn remove_variables(&mut self, keys: &[&str]) {
        for key in keys {
            self.variables.remove(key);
        }
    }

    /// Create a task (sub-entity factory).
    pub fn create_task(&mut self, task_name: &str, display_name: &str,
                        actor_ids: &[String], operator: &str,
                        task_type: TaskType, perform_type: PerformType,
                        form_key: Option<String>, parent_task_id: Option<i64>) -> ProcessTask {
        let task = ProcessTask {
            task_id: 0, // assigned by ID generator
            process_instance_id: self.instance_id,
            task_name: task_name.to_string(),
            display_name: display_name.to_string(),
            task_type: task_type.code(),
            perform_type: perform_type.code(),
            task_state: TaskState::Doing.code(),
            actor_id: None,
            actor_ids: actor_ids.to_vec(),
            finish_time: None,
            expire_time: None,
            form_key,
            parent_task_id,
            variables: FlowData::new(),
            create_time: Some(current_time_str()),
            create_user: Some(operator.to_string()),
            update_time: None,
            update_user: None,
        };
        self.tasks.push(task.clone());
        task
    }

    /// Create countersign tasks (one per actor, each with independent task).
    pub fn create_countersign_tasks(&mut self, task_name: &str, display_name: &str,
                                      actor_ids: &[String], operator: &str,
                                      task_type: TaskType, form_key: Option<String>,
                                      parent_task_id: Option<i64>) -> Vec<ProcessTask> {
        actor_ids.iter().map(|actor| {
            self.create_task(task_name, display_name, &[actor.clone()], operator,
                           task_type, PerformType::Countersign, form_key.clone(), parent_task_id)
        }).collect()
    }

    /// Create a history task (already FINISHED, for custom nodes).
    pub fn create_history_task(&mut self, task_name: &str, display_name: &str,
                                operator: &str, task_type: TaskType) -> ProcessTask {
        let mut task = self.create_task(task_name, display_name, &[operator.to_string()],
                                         operator, task_type, PerformType::Normal, None, None);
        task.task_state = TaskState::Finished.code();
        task.actor_id = Some(operator.to_string());
        // Update in the tasks list
        if let Some(t) = self.tasks.iter_mut().find(|t| t.task_id == task.task_id) {
            t.task_state = TaskState::Finished.code();
            t.actor_id = Some(operator.to_string());
        }
        task
    }

    /// Create a reject task (退回上一步 — new task for previous node).
    pub fn reject_task(&mut self, task_name: &str, display_name: &str,
                        actor_ids: &[String], operator: &str,
                        parent_task_id: i64) -> ProcessTask {
        self.create_task(task_name, display_name, actor_ids, operator,
                        TaskType::Major, PerformType::Normal, None, Some(parent_task_id))
    }

    // ═══ Query methods ═══

    pub fn get_doing_tasks(&self) -> Vec<&ProcessTask> {
        self.tasks.iter().filter(|t| t.task_state == TaskState::Doing.code()).collect()
    }

    pub fn get_doing_tasks_by_names(&self, names: &[String]) -> Vec<&ProcessTask> {
        self.tasks.iter().filter(|t| {
            t.task_state == TaskState::Doing.code() && names.contains(&t.task_name)
        }).collect()
    }

    pub fn get_finished_tasks(&self) -> Vec<&ProcessTask> {
        self.tasks.iter().filter(|t| t.task_state == TaskState::Finished.code()).collect()
    }

    pub fn get_done_tasks_by_names(&self, names: &[String]) -> Vec<&ProcessTask> {
        self.tasks.iter().filter(|t| {
            t.task_state == TaskState::Finished.code() && names.contains(&t.task_name)
        }).collect()
    }

    pub fn get_history_tasks(&self) -> Vec<&ProcessTask> {
        self.tasks.iter().collect()
    }

    pub fn is_all_tasks_finished(&self) -> bool {
        !self.tasks.iter().any(|t| t.task_state == TaskState::Doing.code())
    }

    pub fn is_doing(&self) -> bool {
        self.state == InstanceState::Doing.code()
    }

    pub fn is_finished(&self) -> bool {
        self.state == InstanceState::Finished.code()
    }
}

// ═══════════════════════════════════════════════════════
// Process Task — Sub-entity
// ═══════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct ProcessTask {
    pub task_id: i64,
    pub process_instance_id: i64,
    pub task_name: String,
    pub display_name: String,
    pub task_type: i32,
    pub perform_type: i32,
    pub task_state: i32,
    pub actor_id: Option<String>,
    pub actor_ids: Vec<String>,
    pub finish_time: Option<String>,
    pub expire_time: Option<String>,
    pub form_key: Option<String>,
    pub parent_task_id: Option<i64>,
    pub variables: FlowData,
    pub create_time: Option<String>,
    pub create_user: Option<String>,
    pub update_time: Option<String>,
    pub update_user: Option<String>,
}

impl ProcessTask {
    /// Finish this task (state → FINISHED=20).
    pub fn finish(&mut self, operator: &str, _args: &FlowData) -> Result<(), String> {
        if self.task_state != TaskState::Doing.code() {
            return Err(format!("Task {} is not in DOING state (current={})", self.task_id, self.task_state));
        }
        if !self.is_allowed(operator) {
            return Err(format!("Operator {} is not allowed on task {}", operator, self.task_id));
        }
        self.task_state = TaskState::Finished.code();
        self.actor_id = Some(operator.to_string());
        self.finish_time = Some(current_time_str());
        Ok(())
    }

    /// Abandon this task (state → ABANDON=99).
    pub fn abandon(&mut self) -> Result<(), String> {
        if self.task_state != TaskState::Doing.code() {
            return Err(format!("Task {} is not in DOING state", self.task_id));
        }
        self.task_state = TaskState::Abandon.code();
        Ok(())
    }

    /// Withdraw this task (state → WITHDRAW=30).
    pub fn withdraw(&mut self) {
        self.task_state = TaskState::Withdraw.code();
    }

    /// Interrupt (state → INTERRUPT=40).
    pub fn interrupt(&mut self) -> Result<(), String> {
        if self.task_state != TaskState::Doing.code() {
            return Err(format!("Task {} is not in DOING state", self.task_id));
        }
        self.task_state = TaskState::Interrupt.code();
        Ok(())
    }

    /// Pending (state → PENDING=50).
    pub fn pending(&mut self) -> Result<(), String> {
        if self.task_state != TaskState::Doing.code() {
            return Err(format!("Task {} is not in DOING state", self.task_id));
        }
        self.task_state = TaskState::Pending.code();
        Ok(())
    }

    /// Resume from interrupt (state → DOING=10).
    pub fn resume(&mut self) {
        if self.task_state == TaskState::Interrupt.code() {
            self.task_state = TaskState::Doing.code();
        }
    }

    /// Check if operator is allowed to operate this task.
    /// "flow.auto" / "flow.admin" bypass all checks.
    pub fn is_allowed(&self, operator: &str) -> bool {
        if operator == "flow.auto" || operator == "flow.admin" {
            return true;
        }
        self.is_doing() && self.actor_ids.contains(&operator.to_string())
    }

    pub fn is_doing(&self) -> bool {
        self.task_state == TaskState::Doing.code()
    }

    pub fn is_finished(&self) -> bool {
        self.task_state == TaskState::Finished.code()
    }
}

// ═══════════════════════════════════════════════════════
// Extended domain objects (Design, Surrogate, etc.)
// ═══════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct ProcessDesign {
    pub id: i64,
    pub name: String,
    pub display_name: String,
    pub design_type: String,
    pub icon: Option<String>,
    pub is_deployed: i32,
    pub remark: Option<String>,
    pub create_time: Option<String>,
    pub create_user: Option<String>,
    pub update_time: Option<String>,
    pub update_user: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProcessDesignHis {
    pub id: i64,
    pub process_design_id: i64,
    pub content: Vec<u8>,
    pub create_time: Option<String>,
    pub create_user: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProcessSurrogate {
    pub id: i64,
    pub process_name: String,
    pub operator: String,
    pub surrogate: String,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub enabled: i32,
    pub create_time: Option<String>,
    pub create_user: Option<String>,
    pub update_time: Option<String>,
    pub update_user: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CcInstance {
    pub id: i64,
    pub process_instance_id: i64,
    pub actor_id: String,
    pub state: i32, // 0=unread, 1=read
    pub create_time: Option<String>,
    pub create_user: Option<String>,
    pub update_time: Option<String>,
    pub update_user: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TaskActor {
    pub id: i64,
    pub process_task_id: i64,
    pub actor_id: String,
    pub create_time: Option<String>,
    pub create_user: Option<String>,
}

// ═══════════════════════════════════════════════════════
// Query / Page types
// ═══════════════════════════════════════════════════════

/// Filter operator for m_ three-segment query (C8).
#[derive(Debug, Clone, PartialEq)]
pub enum FilterOp {
    Eq, Ne, Like, Gt, Lt, Ge, Le, In, Nin, Bt,
}

impl FilterOp {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "EQ" => Some(FilterOp::Eq),
            "NE" => Some(FilterOp::Ne),
            "LIKE" => Some(FilterOp::Like),
            "LLIKE" => Some(FilterOp::Like), // treat as LIKE
            "RLIKE" => Some(FilterOp::Like), // treat as LIKE
            "GT" => Some(FilterOp::Gt),
            "LT" => Some(FilterOp::Lt),
            "GE" => Some(FilterOp::Ge),
            "LE" => Some(FilterOp::Le),
            "IN" => Some(FilterOp::In),
            "NIN" => Some(FilterOp::Nin),
            "BT" => Some(FilterOp::Bt),
            _ => None,
        }
    }
}

/// Parsed m_ filter condition (C8: spec/06 §2.2).
#[derive(Debug, Clone)]
pub struct QueryFilter {
    /// Table alias: "t" (main), "pd" (process define), etc.
    pub alias: String,
    pub op: FilterOp,
    /// snake_case column name
    pub column: String,
    pub value: String,
}

#[derive(Debug, Clone, Default)]
pub struct PageQuery {
    pub page_num: i64,
    pub page_size: i64,
    pub operator: Option<String>,
    pub conditions: HashMap<String, JsonValue>,
    /// Parsed m_ filter conditions (C8).
    pub filters: Vec<QueryFilter>,
}

impl PageQuery {
    pub fn new(page_num: i64, page_size: i64) -> Self {
        PageQuery {
            page_num: if page_num < 1 { 1 } else { page_num },
            page_size: if page_size < 1 { 20 } else { page_size },
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct PageResult<T> {
    pub page_num: i64,
    pub page_size: i64,
    pub record_count: i64,
    pub total_page: i64,
    pub rows: Vec<T>,
}

impl<T> PageResult<T> {
    pub fn new(page_num: i64, page_size: i64, record_count: i64, rows: Vec<T>) -> Self {
        let total_page = if record_count == 0 { 0 } else { (record_count + page_size - 1) / page_size };
        PageResult { page_num, page_size, record_count, total_page, rows }
    }

    pub fn empty() -> Self {
        PageResult { page_num: 1, page_size: 20, record_count: 0, total_page: 0, rows: Vec::new() }
    }
}

/// Task row for page queries (todoList/doneList).
#[derive(Debug, Clone, Default)]
pub struct TaskRow {
    pub id: i64,
    pub process_instance_id: i64,
    pub task_name: String,
    pub display_name: String,
    pub task_type: i32,
    pub perform_type: i32,
    pub task_state: i32,
    pub operator: Option<String>,
    pub actor_id: Option<String>,
    pub finish_time: Option<String>,
    pub expire_time: Option<String>,
    pub form_key: Option<String>,
    pub task_parent_id: Option<i64>,
    pub variable: Option<String>,
    pub create_time: Option<String>,
    pub create_user: Option<String>,
    pub update_time: Option<String>,
    pub update_user: Option<String>,
    // Joined from instance
    pub process_define_id: Option<i64>,
    pub instance_state: Option<i32>,
    pub instance_operator: Option<String>,
    pub business_no: Option<String>,
    /// Instance variable JSON (pi.variable) — Java TaskRow.instanceVariable parity
    pub instance_variable: Option<String>,
    /// Instance create_time (pi.create_time) — Java TaskRow.instanceCreateTime parity
    pub instance_create_time: Option<String>,
    // Joined from define
    pub define_name: Option<String>,
    pub define_display_name: Option<String>,
    pub define_version: Option<i32>,
}

/// Instance row for page queries.
#[derive(Debug, Clone, Default)]
pub struct InstanceRow {
    pub id: i64,
    pub parent_id: Option<i64>,
    pub process_define_id: i64,
    pub state: i32,
    pub parent_node_name: Option<String>,
    pub business_no: Option<String>,
    pub operator: String,
    pub expire_time: Option<String>,
    pub variable: Option<String>,
    pub create_time: Option<String>,
    pub create_user: Option<String>,
    pub update_time: Option<String>,
    pub update_user: Option<String>,
    // Joined from define
    pub define_name: Option<String>,
    pub define_display_name: Option<String>,
    pub define_version: Option<i32>,
}

/// Define row for page queries.
#[derive(Debug, Clone, Default)]
pub struct DefineRow {
    pub id: i64,
    pub name: String,
    pub display_name: String,
    pub define_type: String,
    pub state: i32,
    pub version: i32,
    pub create_time: Option<String>,
    pub create_user: Option<String>,
    pub update_time: Option<String>,
    pub update_user: Option<String>,
}

/// User info (from IUserProvider).
#[derive(Debug, Clone, Default)]
pub struct UserInfo {
    pub user_id: String,
    pub real_name: String,
    pub dept_id: String,
    pub dept_name: String,
    pub post_id: String,
    pub post_name: String,
}

// ═══════════════════════════════════════════════════════
// Utility
// ═══════════════════════════════════════════════════════

/// Current time as string (yyyy-MM-dd HH:mm:ss, UTC).
/// Core stays chrono-free; UTC is enough for demo/契约展示（与占位符 NOW() 不同，UI 可解析）。
pub fn current_time_str() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    format_unix_utc(secs)
}

/// Format unix seconds (UTC) as `yyyy-MM-dd HH:mm:ss`.
fn format_unix_utc(secs: i64) -> String {
    // civil_from_days (Howard Hinnant) — days since 1970-01-01
    let z = secs.div_euclid(86400) + 719468;
    let era = if z >= 0 { z } else { z - 146096 }.div_euclid(146097);
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let tod = secs.rem_euclid(86400) as u32;
    let hh = tod / 3600;
    let mm = (tod % 3600) / 60;
    let ss = tod % 60;
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, hh, mm, ss)
}

/// Format time to spec format (yyyy-MM-dd HH:mm:ss).
pub fn format_time(s: &str) -> String {
    s.to_string()
}

/// Convert snake_case to camelCase.
pub fn to_camel_case(s: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_define(id: i64, name: &str) -> ProcessDefine {
        ProcessDefine {
            id,
            name: name.to_string(),
            display_name: format!("{} Display", name),
            define_type: "approval".to_string(),
            state: DefineState::Enable.code(),
            content: b"{}".to_vec(),
            version: 1,
            create_time: None, create_user: None,
            update_time: None, update_user: None,
        }
    }

    #[test]
    fn test_instance_create() {
        let define = make_define(1, "test");
        let args = FlowData::new();
        let inst = ProcessInstance::create(&define, "user1", &args);
        assert_eq!(inst.state, InstanceState::Doing.code());
        assert_eq!(inst.operator, "user1");
        assert_eq!(inst.define_id, 1);
    }

    #[test]
    fn test_task_lifecycle() {
        let define = make_define(1, "test");
        let args = FlowData::new();
        let mut inst = ProcessInstance::create(&define, "user1", &args);
        let mut task = inst.create_task("task1", "Task 1", &["user1".to_string()],
                                         "user1", TaskType::Major, PerformType::Normal, None, None);
        task.task_id = 100;
        inst.tasks.last_mut().unwrap().task_id = 100;

        assert!(task.is_doing());
        assert!(task.is_allowed("user1"));
        assert!(!task.is_allowed("user2"));

        let finish_args = FlowData::new();
        task.finish("user1", &finish_args).unwrap();
        assert!(task.is_finished());
        assert_eq!(task.actor_id, Some("user1".to_string()));
    }

    #[test]
    fn test_instance_finish() {
        let define = make_define(1, "test");
        let mut inst = ProcessInstance::create(&define, "user1", &FlowData::new());
        assert!(inst.is_doing());
        inst.finish();
        assert!(inst.is_finished());
        assert_eq!(inst.state, InstanceState::Finished.code());
    }

    #[test]
    fn test_instance_reject() {
        let define = make_define(1, "test");
        let mut inst = ProcessInstance::create(&define, "user1", &FlowData::new());
        inst.reject();
        assert_eq!(inst.state, InstanceState::Reject.code());
    }

    #[test]
    fn test_submit_type_codes() {
        assert_eq!(SubmitType::from_code(0), Some(SubmitType::Apply));
        assert_eq!(SubmitType::from_code(1), Some(SubmitType::Agree));
        assert_eq!(SubmitType::from_code(2), Some(SubmitType::Reject));
        assert_eq!(SubmitType::from_code(3), Some(SubmitType::Rollback));
        assert_eq!(SubmitType::from_code(4), Some(SubmitType::Jump));
        assert_eq!(SubmitType::from_code(5), Some(SubmitType::ReApply));
        assert_eq!(SubmitType::from_code(6), Some(SubmitType::RollbackToOperator));
        assert_eq!(SubmitType::from_code(20), Some(SubmitType::CountersignDisagree));
        assert_eq!(SubmitType::from_code(99), None);
    }

    #[test]
    fn test_camel_case() {
        assert_eq!(to_camel_case("process_instance_id"), "processInstanceId");
        assert_eq!(to_camel_case("task_name"), "taskName");
        assert_eq!(to_camel_case("id"), "id");
    }

    #[test]
    fn test_current_time_str_format() {
        let t = current_time_str();
        assert_eq!(t.len(), 19, "expected yyyy-MM-dd HH:mm:ss, got {t}");
        assert_eq!(&t[4..5], "-");
        assert_eq!(&t[7..8], "-");
        assert_eq!(&t[10..11], " ");
        assert_eq!(&t[13..14], ":");
        assert_eq!(&t[16..17], ":");
        assert_eq!(format_unix_utc(0), "1970-01-01 00:00:00");
        assert_eq!(format_unix_utc(1_704_067_200), "2024-01-01 00:00:00");
    }

    #[test]
    fn test_flow_auto_bypass() {
        let task = ProcessTask {
            task_id: 1, process_instance_id: 1,
            task_name: "t".into(), display_name: "T".into(),
            task_type: 0, perform_type: 0, task_state: TaskState::Doing.code(),
            actor_id: None, actor_ids: vec!["user1".into()],
            finish_time: None, expire_time: None, form_key: None,
            parent_task_id: None, variables: FlowData::new(),
            create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        assert!(task.is_allowed("flow.auto"));
        assert!(task.is_allowed("flow.admin"));
        assert!(task.is_allowed("user1"));
        assert!(!task.is_allowed("user2"));
    }

    #[test]
    fn test_page_result() {
        let pr = PageResult::<i32>::new(1, 10, 25, vec![]);
        assert_eq!(pr.total_page, 3);
        let pr2 = PageResult::<i32>::new(1, 10, 0, vec![]);
        assert_eq!(pr2.total_page, 0);
    }

    #[test]
    fn test_page_result_exact_page() {
        let pr = PageResult::<i32>::new(1, 10, 20, vec![]);
        assert_eq!(pr.total_page, 2);
    }

    #[test]
    fn test_page_result_one_item() {
        let pr = PageResult::<i32>::new(1, 10, 1, vec![]);
        assert_eq!(pr.total_page, 1);
    }

    #[test]
    fn test_define_state_codes() {
        assert_eq!(DefineState::Enable.code(), 1);
        assert_eq!(DefineState::Disable.code(), 0);
        assert_eq!(DefineState::from_code(1), DefineState::Enable);
        assert_eq!(DefineState::from_code(0), DefineState::Disable);
        assert_eq!(DefineState::from_code(99), DefineState::Enable);
    }

    #[test]
    fn test_instance_state_codes() {
        assert_eq!(InstanceState::Doing.code(), 10);
        assert_eq!(InstanceState::Finished.code(), 20);
        assert_eq!(InstanceState::Withdraw.code(), 30);
        assert_eq!(InstanceState::Interrupt.code(), 40);
        assert_eq!(InstanceState::Reject.code(), 45);
        assert_eq!(InstanceState::from_code(10), Some(InstanceState::Doing));
        assert_eq!(InstanceState::from_code(99), Some(InstanceState::Abandon));
        assert_eq!(InstanceState::from_code(999), None);
    }

    #[test]
    fn test_task_state_codes() {
        assert_eq!(TaskState::Doing.code(), 10);
        assert_eq!(TaskState::Finished.code(), 20);
        assert_eq!(TaskState::Withdraw.code(), 30);
        assert_eq!(TaskState::Interrupt.code(), 40);
        assert_eq!(TaskState::from_code(10), Some(TaskState::Doing));
        assert_eq!(TaskState::from_code(99), Some(TaskState::Abandon));
        assert_eq!(TaskState::from_code(999), None);
    }

    #[test]
    fn test_task_type_codes() {
        assert_eq!(TaskType::Major.code(), 0);
        assert_eq!(TaskType::Assistant.code(), 1);
        assert_eq!(TaskType::Record.code(), 2);
        assert_eq!(TaskType::from_code(0), TaskType::Major);
        assert_eq!(TaskType::from_code(1), TaskType::Assistant);
        assert_eq!(TaskType::from_code(2), TaskType::Record);
    }

    #[test]
    fn test_perform_type_codes() {
        assert_eq!(PerformType::Normal.code(), 0);
        assert_eq!(PerformType::Countersign.code(), 1);
        assert_eq!(PerformType::from_code(0), PerformType::Normal);
        assert_eq!(PerformType::from_code(1), PerformType::Countersign);
    }

    #[test]
    fn test_task_reject() {
        let define = make_define(1, "test");
        let mut inst = ProcessInstance::create(&define, "user1", &FlowData::new());
        let mut task = inst.create_task("task1", "Task 1", &["user1".to_string()],
                                         "user1", TaskType::Major, PerformType::Normal, None, None);
        assert!(task.is_doing());
        task.withdraw();
        assert_eq!(task.task_state, TaskState::Withdraw.code());
    }

    #[test]
    fn test_task_finish_wrong_actor() {
        let define = make_define(1, "test");
        let mut inst = ProcessInstance::create(&define, "user1", &FlowData::new());
        let mut task = inst.create_task("task1", "Task 1", &["user1".to_string()],
                                         "user1", TaskType::Major, PerformType::Normal, None, None);
        let result = task.finish("user2", &FlowData::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_task_already_finished() {
        let define = make_define(1, "test");
        let mut inst = ProcessInstance::create(&define, "user1", &FlowData::new());
        let mut task = inst.create_task("task1", "Task 1", &["user1".to_string()],
                                         "user1", TaskType::Major, PerformType::Normal, None, None);
        task.finish("user1", &FlowData::new()).unwrap();
        let result = task.finish("user1", &FlowData::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_instance_withdraw_state() {
        let define = make_define(1, "test");
        let mut inst = ProcessInstance::create(&define, "user1", &FlowData::new());
        inst.withdraw();
        assert_eq!(inst.state, InstanceState::Withdraw.code());
    }

    #[test]
    fn test_camel_case_multiple_underscores() {
        assert_eq!(to_camel_case("a_b_c"), "aBC");
        assert_eq!(to_camel_case("_leading"), "Leading");
        assert_eq!(to_camel_case("trailing_"), "trailing");
    }

    #[test]
    fn test_camel_case_already_camel() {
        assert_eq!(to_camel_case("camelCase"), "camelCase");
    }

    #[test]
    fn test_current_time_str() {
        let t = current_time_str();
        assert!(!t.is_empty());
        assert_ne!(t, "NOW()");
        assert_eq!(t.len(), 19);
    }
}
