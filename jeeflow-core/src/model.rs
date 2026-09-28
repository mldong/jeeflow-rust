//! Domain model — DDD aggregate root (ProcessInstance) + sub-entity (ProcessTask).
//! Aligned with Java reference implementation: spec/03 (state machine), spec/04 (engine ops).

use crate::error::{JeeflowError, JeeflowResult};
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
    ///
    /// issues/134 案 A（owner 2026-09-28 拍板）：撤回只允许**进行中(10)** 的实例。实例不是 10
    /// （已完成 20 / 已撤回 30 / 强行终止 40 / 已拒绝 45 / 挂起 50 / 已废弃 99）⇒ 报错，
    /// **一行都不改、不落库**——守卫排在下面的任务行循环之前，否则已办结实例会被静默改写成 30
    /// （已办列表与按状态聚合的统计凭空改历史，且调用方看不到任何报错）。
    /// 任务行层面那句"已完成(20)/已终止(40) 行不改写"的既有保护（下方 `TaskState::Doing` 判据）
    /// 保持原样，实例级守卫排在它之前。
    ///
    /// 对外 msg 用固定中文文案、不含引擎内部码：本栈 `JeeflowError::code()` 恒 99999999，
    /// 与 20010007 / 20010008（`engine::rollback_to_parent`）同形——内部码 **20010009**
    /// 只留在规范与本注释，门面出 `code=99999999` ＋ 这句原文，不拼码、不加前缀（issues/121 口径）。
    ///
    /// Err: `JeeflowError::Business`（内部码 20010009 实例非进行中）
    pub fn withdraw(&mut self) -> JeeflowResult<()> {
        const NOT_DOING: &str = "流程实例非进行中，无法撤回";   // 内部码 20010009

        if self.state != InstanceState::Doing.code() {
            return Err(JeeflowError::Business(NOT_DOING.to_string()));
        }
        for task in &mut self.tasks {
            if task.task_state == TaskState::Doing.code() {
                task.withdraw();
            }
        }
        self.state = InstanceState::Withdraw.code();
        Ok(())
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
    ///
    /// ⚠️ issues/126 案 A 普查：本函数**全仓零调用者**（回退新建实际走 `engine.rs` 的
    /// `rollback_to_parent`，那条已接到期写点④）。按启动词 §1.9-1 口径**不接线、不删、不为它造测试**：
    /// 本签名没有节点引用 ⇒ 拿不到到期表达式，接一次就要改公开签名（发布 crate 的破坏性改动）。
    /// 将来复活它时记得补 `expire_time::apply_expire_time`（Java 同名方法 `rejectTask` 是五处写点之一）。
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
    /// 变量合并序（契约 spec/06 §4.3 第 5 条，转办留痕存活的前置条件）：
    /// 任务既有变量为底 ← 本次提交参数 args 最高。用 `merge`（args 覆盖同名键，
    /// 不在 args 里的既有键如 `tf_transferHistory` 原样保留），既不全量替换任务变量，
    /// 也不让既有变量压过 args（否则转办的 submitType=7 会反噬 B 提交的 1/2/20）。
    pub fn finish(&mut self, operator: &str, args: &FlowData) -> Result<(), String> {
        if self.task_state != TaskState::Doing.code() {
            return Err(format!("Task {} is not in DOING state (current={})", self.task_id, self.task_state));
        }
        if !self.is_allowed(operator) {
            return Err(format!("Operator {} is not allowed on task {}", operator, self.task_id));
        }
        self.task_state = TaskState::Finished.code();
        self.actor_id = Some(operator.to_string());
        self.variables.merge(args);
        self.finish_time = Some(current_time_str());
        self.update_time = Some(current_time_str());
        self.update_user = Some(operator.to_string());
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

/// Current time as string (yyyy-MM-dd HH:mm:ss).
/// 基准由宿主注入的时钟决定，引擎不自取（issues/120）；未注入时回落 UTC
/// —— core 保持 chrono-free，`std` 只有 epoch、拿不到本地时区偏移。
pub fn current_time_str() -> String {
    crate::clock::current_time_str()
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
        // issues/134 案 A：withdraw 现在会因实例状态守卫返回 Result——这里夹具是进行中(10)，
        // 期望值未动（仍断实例落 30），只按新签名 unwrap。
        inst.withdraw().unwrap();
        assert_eq!(inst.state, InstanceState::Withdraw.code());
    }

    // ═══════════════════════════════════════════════════════
    // issues/134 案 A · 撤回的实例状态守卫（聚合根 ProcessInstance::withdraw）
    //
    // 缺陷：issues/113 只落了**任务行**层面的保护（已完成 20 / 已终止 40 的行不被撤回改写），
    // **实例**层面没判状态——对已办结(20)/已终止(40) 的实例调撤回会把实例静默改写成 30，
    // 已办列表与按状态聚合的统计凭空改历史，调用方还看不到任何报错。
    // 判据（八栈逐字统一）：state != 10(进行中) ⇒ 内部码 20010009，文案固定、**一行都不改**，
    // 守卫排在任务行循环之前。权威＝Java 参考实现 WithdrawInstanceStateGuardTest + spec 06。
    // ═══════════════════════════════════════════════════════

    /// 出口文案逐字固定（八栈一致）；按逐字断言，不许用"包含 撤回"这种宽松判据。
    /// 内部码 20010009 不进 msg（issues/121 口径；本栈 `JeeflowError::code()` 恒 99999999）。
    const WD134_MSG: &str = "流程实例非进行中，无法撤回";

    /// 一个进行中(10) 的实例：一行进行中任务 task1，参与者 leader，发起人 zhangsan。
    /// 夹具刻意不设 update_user，负向档的"一行都不改"才照得出来。
    fn wd134_doing_instance() -> ProcessInstance {
        let define = make_define(9527, "134-guard");
        let mut inst = ProcessInstance::create(&define, "zhangsan", &FlowData::new());
        inst.instance_id = 9001;
        inst.create_task("task1", "审批", &["leader".to_string()], "zhangsan",
                         TaskType::Major, PerformType::Normal, None, None);
        inst
    }

    /// 把实例**自然**办到 state=20（行真办结 + 聚合根 finish），不用手改 state 造假形状。
    fn wd134_finished_instance() -> ProcessInstance {
        let mut inst = wd134_doing_instance();
        inst.tasks[0].finish("leader", &FlowData::new()).unwrap();
        inst.finish();
        assert_eq!(inst.state, InstanceState::Finished.code(), "夹具前提：实例已办结");
        assert_eq!(inst.tasks[0].task_state, TaskState::Finished.code(), "夹具前提：任务行已办结");
        inst
    }

    /// 断负向：错误变体 ＋ 文案逐字相等 ＋ 不含内部码 ＋ 实例状态/任务行一行未改。
    fn wd134_assert_rejected_without_touching_rows(inst: &mut ProcessInstance, original_state: i32) {
        use crate::error::ERR_BUSINESS;
        let row_states_before: Vec<i32> = inst.tasks.iter().map(|t| t.task_state).collect();
        let row_users_before: Vec<Option<String>> =
            inst.tasks.iter().map(|t| t.update_user.clone()).collect();
        let update_user_before = inst.update_user.clone();

        let err = inst.withdraw().err()
            .expect("非进行中实例撤回必须报错，不得静默改写为 30");
        match &err {
            JeeflowError::Business(msg) => {
                assert_eq!(msg, WD134_MSG, "文案逐字固定（不带码值、不带前缀）");
            }
            other => panic!("本栈形状应与 20010007/20010008 同形（Business 变体），实得 {:?}", other),
        }
        assert_eq!(err.code(), ERR_BUSINESS, "本栈出口码恒 99999999");
        assert!(!err.message().contains("2001000"),
                "内部码 20010009 严禁进 msg，实得 {}", err.message());

        assert_eq!(inst.state, original_state, "被拒后实例状态必须仍是原值 {}", original_state);
        // 病灶判据：被拒绝不能把实例静默改成 30。负向③（已撤回 30 二次撤）原值就是 30，
        // 那条由上一行的"仍是原值"覆盖，这里只对 20/40 两档显式钉"不得变成 30"。
        if original_state != InstanceState::Withdraw.code() {
            assert_ne!(inst.state, InstanceState::Withdraw.code(), "实例状态严禁被改写成 30(已撤回)");
        }
        assert_eq!(inst.update_user, update_user_before, "被拒的那次不得写实例 update_user");
        let row_states_after: Vec<i32> = inst.tasks.iter().map(|t| t.task_state).collect();
        let row_users_after: Vec<Option<String>> =
            inst.tasks.iter().map(|t| t.update_user.clone()).collect();
        assert_eq!(row_states_after, row_states_before, "任务行状态不得被改写");
        assert_eq!(row_users_after, row_users_before, "任务行 update_user 不得被改写");
    }

    /// 负向①：已完成(20) 的实例调撤回 ⇒ 20010009 ＋ 固定文案，实例仍 20、行仍 20
    #[test]
    fn test_withdraw_on_finished_instance_is_rejected_and_keeps_state() {
        let mut inst = wd134_finished_instance();
        wd134_assert_rejected_without_touching_rows(&mut inst, InstanceState::Finished.code());
    }

    /// 负向②：强行终止(40) 的实例调撤回 ⇒ 同样拒绝，实例仍 40、行仍 40
    /// （门面/壳侧造不出这一档，issues/134 §5.2 把 L2-28 限定在 20 ＋ 正向 10，故由本栈单测钉住）
    #[test]
    fn test_withdraw_on_interrupted_instance_is_rejected_and_keeps_state() {
        let mut inst = wd134_doing_instance();
        inst.interrupt();
        assert_eq!(inst.state, InstanceState::Interrupt.code(), "夹具前提：实例已终止");
        assert_eq!(inst.tasks[0].task_state, TaskState::Interrupt.code(), "夹具前提：任务行已终止");
        wd134_assert_rejected_without_touching_rows(&mut inst, InstanceState::Interrupt.code());
    }

    /// 负向③：已撤回(30) 的实例二次撤回同样被拒——重复撤不得把状态再翻一次
    #[test]
    fn test_withdraw_on_already_withdrawn_instance_is_rejected_on_second_call() {
        let mut inst = wd134_doing_instance();
        inst.withdraw().expect("首次：进行中，照旧成功");
        assert_eq!(inst.state, InstanceState::Withdraw.code());
        wd134_assert_rejected_without_touching_rows(&mut inst, InstanceState::Withdraw.code());
    }

    /// 正向对照：进行中(10) 的实例撤回照旧成功，实例与进行中任务都落 30（防"守卫写反"假绿）
    #[test]
    fn test_withdraw_on_doing_instance_still_succeeds_and_lands_state30() {
        let mut inst = wd134_doing_instance();
        inst.withdraw().expect("进行中实例撤回应成功（守卫没写反）");
        assert_eq!(inst.state, InstanceState::Withdraw.code(), "进行中实例撤回应落 30(WITHDRAW)");
        assert_eq!(inst.tasks[0].task_state, TaskState::Withdraw.code(), "进行中任务行应落 30");
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
