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
    ///
    /// **2026-09-30 issues/142 A 批接线**：调用者＝`engine.rs::execute_custom_node`
    /// （记录类节点 `snaker:custom` 的执行腿，spec/02 §6.1／§6.2）。此前它是"有形状、
    /// 引擎零调用者"（issues/137 B 拍板不删、当时不接线），接线的那一单就是本轮。
    /// 公开签名没动；**行形状动了一列**（下面 1bis 那段：`finish_time` 改为赋值），
    /// 钉它的 `tests::test_i137b_create_history_task_matches_main_path` 与
    /// `..._does_not_disturb_sibling_rows` 两处**跟着裁定改判**（不是把断言改松：
    /// 改的是"审计三列都不写"这条已被 §6.2 1bis 作废的旧期望）。
    ///
    /// ⚠️ issues/137 B 留的那句"复活时记得补 `expire_time::apply_expire_time`
    /// （Java 同名方法是 126 的五处写点之一）"经本轮核实**前提不成立**，按基准落 NULL：
    /// java HEAD 的 `applyExpireTime` 调用点是 createTask / 串行首成员 / 并行全员 /
    /// rejectTask 四处 ＋ 一处 `applyNodeExpireTime`（给 CountersignHandler 用），
    /// **不含** `createHistoryTask`；它收的是 `CustomModel`（无 expireTime 属性，
    /// spec/02 §6 的 custom 字典只有 clazz/methodName/args/val），四列传 null。
    /// python 同判（其 `create_history_task` 注释明写"无 expireTime"）。
    /// ⇒ 到期判定落在调用侧的注释与用例里（`engine.rs::persist_history_task`），
    ///   本签名继续不引节点引用，公开形状不破。
    pub fn create_history_task(&mut self, task_name: &str, display_name: &str,
                                operator: &str, task_type: TaskType) -> ProcessTask {
        let mut task = self.create_task(task_name, display_name, &[operator.to_string()],
                                         operator, task_type, PerformType::Normal, None, None);
        task.task_state = TaskState::Finished.code();
        task.actor_id = Some(operator.to_string());
        // spec/02 §6.2 第 **1bis** 条（issues/142 A 批收口补，owner 2026-09-30 拍）：
        // 这条 DONE 行必须写处理人**与完成时间**。`processTask/doneList`
        // （`task_state<>10 AND operator=?`）与 `processInstance/approvalRecord` 都按
        // `operator`／`finish_time` 两列取数，已完成行不带完成时间，在用户面上等于这条留痕没落过
        // ——与第 1 条"查不到的留痕＝没留痕"同一把尺子。java `createHistoryTask`（本轮 33ba48f）
        // 与 python 同形。
        // ⚠️ 条文里"两列写、**一列不写**"那一列指的是 **`expire_time`**（§6 的 custom 属性字典
        // 无 expireTime，写它就得臆造属性，也与 issues/126 owner 口径"节点没配就保持 NULL"同向）
        // ⇒ 别顺手把时间列全补上。`update_time`/`update_user` 本栈 `create_task` 本就不写
        // （与 java `ProcessTask.create` 顺手写这两列不同形），属本栈既有形状，不在本条判据内。
        task.finish_time = Some(current_time_str());
        // Update in the tasks list —— **按刚 push 的那一格定位**，不按 task_id 找：
        // 本函数返回的行 id 是 0（真 id 由 `persist_tasks` 后置分配），拿 `task_id == 0` 去
        // `find` 会命中聚合里**第一条**未分配 id 的行——实例先建了别的 DOING 行时就会把
        // 别人的行改成 FINISHED，而返回的那一行反而是已办结的，两行分叉（issues/137 B
        // 直调对拍用例实测到的形状）。push 之后最后一格必然是本行。
        if let Some(t) = self.tasks.last_mut() {
            // 整格覆盖而不是逐列挑着写：本函数在 `task` 上动了三列（state/actor_id/finish_time），
            // 逐列写就要在每一处新增列上重复一次，漏一列就是"返回行已办结、聚合里那格还差一列"
            // 的分叉——1bis 加 finish_time 时正是这个形状最容易复发的时候。
            *t = task.clone();
        }
        task
    }

    /// Create a reject task (退回上一步 — new task for previous node).
    ///
    /// ⚠️ **当前零调用者，但承担契约形状义务（issues/137 B ＋ 126 案 A 普查）**：本函数
    /// 全仓无人调用——回退新建实际走 `engine.rs` 的 `rollback_to_parent`（那条已接到期写点④）。
    /// owner 2026-09-29 裁定**不删、不接线、补用例钉形状**：接一次就要改公开签名（发布 crate
    /// 的破坏性改动），而形状本身是别栈的对照基准。用例见
    /// `tests::test_i137b_reject_task_matches_main_path`。将来复活它时记得补
    /// `expire_time::apply_expire_time`（Java 同名方法 `rejectTask` 是五处写点之一）。
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

    /// issues/141 G1：落在**归属列** `cc.actor_id` 上的 m_ 过滤条件。
    ///
    /// 本仓的抄送归属有两个等价通道（门面 `ccList` 走①，直连仓储/自定义 SPI 两条都可能走②）：
    /// ① [`PageQuery::operator`]——两仓都把它绑到 `cc.actor_id`；
    /// ② `m_cc_actorId_EQ_xxx` 形态的 [`QueryFilter`]（alias `cc` / column `actor_id`）。
    /// 判据 [`has_effective_cc_ownership`] 两通道一起看，两仓共用同一条，不自创第三种形状。
    pub fn cc_ownership_filters(&self) -> Vec<&QueryFilter> {
        self.filters.iter().filter(|f| is_cc_ownership_col(&f.alias, &f.column)).collect()
    }

    /// 归属列之外的那些 m_ 过滤条件（继续打在实例行上，语义不变）。
    pub fn non_cc_ownership_filters(&self) -> Vec<&QueryFilter> {
        self.filters.iter().filter(|f| !is_cc_ownership_col(&f.alias, &f.column)).collect()
    }
}

/// issues/141 G1 · 抄送分页的归属列（spec 06 §2.5；java 基准 `hasEffectiveCondition` 钉的同一列）。
pub const CC_OWNERSHIP_COLUMN: &str = "cc.actor_id";

/// 某一 `(alias, column)` 是不是归属列 `cc.actor_id`。
pub fn is_cc_ownership_col(alias: &str, column: &str) -> bool {
    alias == "cc" && (column == "actor_id" || column == "actorId")
}

/// 字符串条件值算不算"填了"：去空白后非空即算（空串／全空白＝没填）。
/// `In`/`Nin` 的值是逗号集合，与 java「集合非空」同一档：拆完一个非空段都没有＝空集合＝没填。
pub fn is_effective_filter_value(op: &FilterOp, value: &str) -> bool {
    match op {
        FilterOp::In | FilterOp::Nin =>
            value.split(',').any(|s| !s.trim().is_empty()),
        _ => !value.trim().is_empty(),
    }
}

/// JSON 条件值算不算"填了"：非 null、字符串去空白后非空、集合非空（对齐 java `hasEffectiveCondition`）。
pub fn is_effective_json_value(value: &JsonValue) -> bool {
    match value {
        JsonValue::Null => false,
        JsonValue::Str(s) => !s.trim().is_empty(),
        JsonValue::Array(items) => !items.iter().any(|v| matches!(v, JsonValue::Null))
            && !items.is_empty(),
        _ => true,
    }
}

/// **抄送分页归属条件必填**的唯一判据出口（issues/141 G1 · spec 06 §2.5）。
///
/// `page_cc_instances` 只有在返回 `true` 时才许出行；`false`（条件整条没给，或给了是空值）
/// 一律**空页**（`record_count=0`、`rows=[]`），不得退化成"这条条件不加"返回全部实例。
/// 内存仓与 sqlx 仓必须调这同一支，两仓在同一条判据上给同一个答案
/// （issues/117 场景 27 那把尺子扩到 ccList）。
///
/// 只管归属列：**非归属列的空值放行不在本函数职责内**，各仓既有语义不变。
pub fn has_effective_cc_ownership(query: &PageQuery) -> bool {
    // 通道①：PageQuery.operator（门面 ccList 恒挂这一条）
    if let Some(op) = query.operator.as_deref() {
        if !op.trim().is_empty() {
            return true;
        }
    }
    // 通道②：m_ 过滤直接打在 cc.actor_id 上
    if query.cc_ownership_filters().iter()
        .any(|f| is_effective_filter_value(&f.op, &f.value)) {
        return true;
    }
    // 通道③：conditions 台账（java 基准 `PageQuery.add("cc.actor_id", …)` 的同形通道）
    if let Some(v) = query.conditions.get(CC_OWNERSHIP_COLUMN) {
        return is_effective_json_value(v);
    }
    false
}

/// **归属值集合归一**的唯一判据出口（spec 06-facade.md §2.10 ＋ §2.11；
/// 基准＝jeeflow-java `5fbd5ac` 的 `StringUtils.normalizeCcActors`；issues/142 B 批按 owner
/// 拍板「八栈一起收：两形同判据＋写侧兜底＋trim＋哨兵」，把同一枚尺子从抄送侧搬到任务侧，
/// **严禁另抄第二份判据**——两份判据迟早分叉）。
///
/// 判据三件事，顺序固定：**逐元素 trim ⇒ 空串/纯空白丢弃 ⇒ 同一次调用内的重复折叠（顺序保持）**。
/// 落库与比较一律取 **trim 后的串**（`" 123 "` 与 `"123"` 是同一个人；不 trim 就会与
/// issues/141 G2 的写侧判重错开，同一人落两行）。
/// 覆盖的写点（spec §2.11 表，全部走这一支，**逗号串与数组两形同判据**）：
/// - 抄送三条入口：发起 `f_ccActors`／办理 `tf_ccActors`／门面手动 `createCCInstance`
///   —— 丢完为空 ⇒ 调用方**不建任何 cc 行、也不 fire `CC_CREATE`(码 4)**；
/// - 任务侧 `processTask/addCandidate`／`processTask/surrogate` 的 `actorIds`
///   （门面 `arg_actor_ids` 两条腿）与 `transfer` 的 `fromActor`/`toActor`；
/// - 消费腿 `f_nextNodeOperator`／`tf_nextNodeOperator`（数组元素**不得**被静默丢弃，
///   数字元素 `to_string` 之后照样过这一支）；
/// - 两仓 `add_task_actor`／`create_cc_instance` 的**写侧兜底**（本函数不认主键，
///   只归一归属值集合）。
///
/// 判据落在**两层**，缺一层就还能灌进空值（spec §2.10 实现要求①／§2.11 硬要求①）：
/// ① 漏斗层＝`parse_cc_actors`（引擎两条腿共用）＋门面腿；
/// ② 写侧层＝两仓 `add_task_actor`／`create_cc_instance`（[`crate::memory::MemoryRepository`] /
///    `SqlxRepository`）与 [`crate::spi::ProcessRepository::create_cc_instance_if_absent`]
///    default——绕过引擎/门面的第三方调用方（集成层）同样灌不进空值。
///
/// 空入参档不在本函数：本函数只归一；返回空集合后由调用方按**各仓既有的"缺参数"错误信封**
/// 报错（§2.11 硬要求③，不新造错误码/文案）。主键类参数（`processTaskId`）另判一档、
/// 不参与归属值归一——归属值可有可无，主键没给就是调用方写错了。
///
/// 反向哨兵（spec §2.10 实现要求④／§2.11 硬要求④）：这一支**只吃空值**，
/// `"0"`／`"00"`／`" "`／`"a"` 是四个人，`"0"` 这类"看起来像空"的正常 id **不得**被丢掉；
/// 判空一律 `trim().is_empty()`，严禁不 trim 就 `is_empty()`（旧形状：数组腿只
/// `filter(!s.is_empty())` ⇒ `"  "` 存活并真落进 `actor_id`，而串腿才 trim＝两条腿两个答案）。
pub fn normalize_actors(raw: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(raw.len());
    for actor in raw {
        let trimmed = actor.trim();
        if trimmed.is_empty() {
            continue; // 空串／纯空白：丢弃（G10）
        }
        let trimmed = trimmed.to_string();
        if !out.contains(&trimmed) {
            out.push(trimmed); // 同一次调用内的重复折叠；值取 trim 后的串（与 G2 判重同一条尺子）
        }
    }
    out
}

/// 抄送侧的旧名转发（issues/141 G10 当年落地的就是这一枚单点；spec 06 §2.11 尾注
/// 「各栈的归一判据请复用 §2.10 已落地的那一枚单点……必要时改名成通用的 `normalizeActors`」）。
///
/// 判据本体已上移为 [`normalize_actors`]；**保留本公开名是为了不破坏已发布 crate 的 API 面**——
/// 两仓 `create_cc_instance`、引擎 `parse_cc_actors`、门面手动腿的调用点逐字不变。
/// ⚠️ 任务侧新写点一律用 [`normalize_actors`]，不要再在这里长出抄送专属的第二条腿。
pub fn normalize_cc_actors(raw: &[String]) -> Vec<String> {
    normalize_actors(raw)
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

    // ═══════════════════════════════════════════════════════
    // issues/137 B · 零调用者的建单函数（不删，补"直调＝主路径"的逐维用例）
    // 判据形状照 jeeflow-moon `core/model/i137b_zero_caller_create_test.mbt`：
    // ProcessTask 没有 PartialEq derive ⇒ "全字段等价"显式写成逐列断言，别只比一列。
    // 时钟经 ClockScope 注入定住（两条路各自一个实例，逐次取值的钟会让 create_time 假分叉）。
    // ═══════════════════════════════════════════════════════

    static I137B_TICK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    /// 恒定注入钟：两条路各自取时间也必须落到**同一个读数**，否则"逐维等价"会被时间戳假分叉吃掉
    /// （本文件只钉行形状，不钉"两次取钟相同"）。计数器留作可切换的极端形态，勿删即换。
    fn i137b_clock() -> String {
        let _ = I137B_TICK.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        "2026-09-29 10:00:00".to_string()
    }

    /// 建单行**全部 18 列**逐列等价（两条路形状完全同谱时用这一支）。
    fn assert_row_shape_full(a: &ProcessTask, b: &ProcessTask) {
        assert_row_shape_except_audit(a, b);
        assert_eq!(a.finish_time, b.finish_time, "finish_time");
        assert_eq!(a.update_time, b.update_time, "update_time");
        assert_eq!(a.update_user, b.update_user, "update_user");
    }

    /// 除办理审计三列（`finish_time`/`update_time`/`update_user`）外的逐列等价——
    /// 建单行形状本体。审计三列是"谁办过"的留痕，不属于建单形状，由各自用例显式钉
    /// （jeeflow-moon `i137b_zero_caller_create_test.mbt::assert_row_shape_same` 同一划分）。
    fn assert_row_shape_except_audit(a: &ProcessTask, b: &ProcessTask) {
        assert_eq!(a.task_id, b.task_id, "task_id");
        assert_eq!(a.process_instance_id, b.process_instance_id, "process_instance_id");
        assert_eq!(a.task_name, b.task_name, "task_name");
        assert_eq!(a.display_name, b.display_name, "display_name");
        assert_eq!(a.task_type, b.task_type, "task_type");
        assert_eq!(a.perform_type, b.perform_type, "perform_type");
        assert_eq!(a.task_state, b.task_state, "task_state");
        assert_eq!(a.actor_id, b.actor_id, "actor_id");
        assert_eq!(a.actor_ids, b.actor_ids, "actor_ids");
        assert_eq!(a.expire_time, b.expire_time, "expire_time");
        assert_eq!(a.form_key, b.form_key, "form_key");
        assert_eq!(a.parent_task_id, b.parent_task_id, "parent_task_id");
        assert_eq!(a.variables.len(), b.variables.len(), "行变量条数");
        assert_eq!(a.create_time, b.create_time, "create_time");
        assert_eq!(a.create_user, b.create_user, "create_user");
    }

    /// `reject_task` 直调 ⇒ 行形状逐维等于主路径建单
    /// （`create_task(同名单/同参与者/Major/Normal/form_key=None/parent=Some)`）。
    #[test]
    fn test_i137b_reject_task_matches_main_path() {
        let _scope = crate::clock::ClockScope::injected(i137b_clock);
        let define = make_define(1, "i137b");
        let actors = vec!["user2".to_string(), "user3".to_string()];

        // A 组：直调被测函数
        let mut inst_a = ProcessInstance::create(&define, "user1", &FlowData::new());
        let via_fn = inst_a.reject_task("approve", "审批", &actors, "user1", 900);
        // B 组：主路径那一手建单（引擎各建单点调的就是这八个入参的 create_task）
        let mut inst_b = ProcessInstance::create(&define, "user1", &FlowData::new());
        let via_main = inst_b.create_task("approve", "审批", &actors, "user1",
                                          TaskType::Major, PerformType::Normal, None, Some(900));

        assert_row_shape_full(&via_fn, &via_main);
        // 再钉一遍契约本体（将来新增列若绕过上面的逐维比，这组硬判据仍照得住）
        assert_eq!(via_fn.task_id, 0, "建单行 id 由 persist_tasks 后置分配");
        assert_eq!(via_fn.task_state, TaskState::Doing.code(), "新建行必须是进行中");
        assert_eq!(via_fn.task_type, TaskType::Major.code());
        assert_eq!(via_fn.perform_type, PerformType::Normal.code());
        assert_eq!(via_fn.parent_task_id, Some(900), "退回上一步必须带血缘父任务");
        assert_eq!(via_fn.actor_ids, actors, "参与者集合按入参原样");
        assert_eq!(via_fn.actor_id, None, "进行中任务该列恒无值");
        assert_eq!(via_fn.form_key, None);
        assert_eq!(via_fn.expire_time, None, "本签名拿不到节点表达式 ⇒ 到期留空（126 案 A 普查那句）");
        assert_eq!(via_fn.create_user.as_deref(), Some("user1"));
        assert!(via_fn.variables.is_empty(), "reject_task 不塞行变量");
        // 建单审计三列都还没写（与主路径同为"未办"）
        assert_eq!(via_fn.finish_time, None);
        assert_eq!(via_fn.update_time, None);
        assert_eq!(via_fn.update_user, None);
        // 行同样落进聚合的 tasks 集合（主路径靠它做 assign_ids / save_task）
        assert_eq!(inst_a.tasks.len(), 1);
        assert_eq!(inst_a.tasks[0].task_name, "approve");
        assert!(inst_a.tasks[0].is_doing());
        assert_eq!(inst_b.tasks.len(), 1, "两条路各自只建一行，互不串");
    }

    /// `create_history_task` 直调 ⇒ 除办理审计三列外逐维等于"主路径建单后办结"，
    /// 三列里 **`finish_time` 现在也写**（spec/02 §6.2 1bis，2026-09-30 裁定，本用例同批改判），
    /// 另两列（`update_time`/`update_user`）差异仍有据：记录类没有"办理"这一步。
    #[test]
    fn test_i137b_create_history_task_matches_main_path() {
        let _scope = crate::clock::ClockScope::injected(i137b_clock);
        let define = make_define(2, "i137b-hist");

        // A 组：直调被测函数（自动节点：一落地就是 FINISHED）
        let mut inst_a = ProcessInstance::create(&define, "user1", &FlowData::new());
        let hist = inst_a.create_history_task("auto", "自动节点", "flow.auto", TaskType::Major);
        // B 组：主路径那一手——先建 DOING 行，再经 ProcessTask::finish 办结
        // （注入钟恒定 ⇒ 两边 create_time 同一读数，不需要事先对齐）
        let mut inst_b = ProcessInstance::create(&define, "user1", &FlowData::new());
        let main = inst_b.create_task("auto", "自动节点", &["flow.auto".to_string()],
                                      "flow.auto", TaskType::Major, PerformType::Normal, None, None);
        inst_b.tasks = vec![main];
        inst_b.tasks[0].finish("flow.auto", &FlowData::new()).unwrap();
        let main = &inst_b.tasks[0];

        assert_row_shape_except_audit(&hist, main);
        // 一致的核心两列：办结行的 state 与处理人列（与 finish 写的同一判据）
        assert_eq!(hist.task_state, TaskState::Finished.code(), "历史行一落地就是已办结");
        assert_eq!(hist.task_state, main.task_state);
        assert_eq!(hist.actor_id.as_deref(), Some("flow.auto"), "已办结行必须挂处理人");
        assert_eq!(hist.actor_id, main.actor_id);
        assert_eq!(hist.actor_ids, vec!["flow.auto".to_string()]);
        // 有据差异：记录类没有"办理"这一步 ⇒ **只补 finish_time 一列**，
        // `update_time`/`update_user` 两列办理审计继续留 None（spec/02 §6.2 1bis 原话
        // "两列写、一列不写，别顺手一起补"里的"别顺手"这半）。
        // ⚠️ 本行期望值 **2026-09-30 跟着裁定改判**：issues/137 B 当时这里钉的是
        // `assert_eq!(hist.finish_time, None)`，理由写的"自动节点没有办理这一步 ⇒ 不写办理审计三列"
        // 已被 §6.2 1bis 作废（同一把尺子：查不到的留痕＝没留痕；doneList/approvalRecord
        // 按 operator＋finish_time 取数）。改判只动这一条期望，不是把断言改松。
        assert_eq!(hist.finish_time.as_deref(), Some("2026-09-29 10:00:00"),
            "§6.2 1bis：DONE 留痕行必须带完成时间（注入钟与主路径同一读数）");
        assert_eq!(hist.finish_time, main.finish_time, "与主路径办结写的同一列、同一基准");
        assert_eq!(hist.update_time, None);
        assert_eq!(hist.update_user, None);
        assert_eq!(main.finish_time.as_deref(), Some("2026-09-29 10:00:00"), "主路径办结写审计列（对照用）");
        assert_eq!(main.update_user.as_deref(), Some("flow.auto"));
        // 下游读路径认它（已办/历史按 task_state==FINISHED 取行）
        assert_eq!(inst_a.get_finished_tasks().len(), 1);
        assert!(hist.is_finished());
        assert_eq!(inst_a.tasks.len(), 1, "聚合里也只有一行");
    }

    /// 聚合内已有别的未分配 id 行时，`create_history_task` 只动自己那一行
    /// （旧形状按 `task_id == 0` 找行 ⇒ 命中聚合里**第一条**未落 id 的行，把别人的 DOING
    /// 改成 FINISHED，返回行与落库行分叉。改前实测红）。
    #[test]
    fn test_i137b_create_history_task_does_not_disturb_sibling_rows() {
        let _scope = crate::clock::ClockScope::injected(i137b_clock);
        let define = make_define(3, "i137b-sibling");
        let mut inst = ProcessInstance::create(&define, "user1", &FlowData::new());

        let sibling = inst.create_task("apply", "申请", &["user9".to_string()], "user9",
                                       TaskType::Major, PerformType::Normal, None, None);
        let hist = inst.create_history_task("auto", "自动节点", "flow.auto", TaskType::Major);

        assert_eq!(sibling.task_id, hist.task_id, "夹具前提：两行都还没分配 id（都是 0）");
        assert_eq!(inst.tasks.len(), 2, "两行都在聚合里");
        assert!(inst.tasks[0].is_doing(),
            "先建的那一行必须还是进行中（旧形状在这里被改成 FINISHED）");
        assert_eq!(inst.tasks[0].actor_id, None, "先建那行的处理人列不得被历史行占用");
        assert_eq!(inst.tasks[0].task_name, "apply");
        assert!(inst.tasks[1].is_finished(), "后建的自己那一行才该是已办结");
        assert_eq!(inst.tasks[1].actor_id.as_deref(), Some("flow.auto"));
        // 聚合里那一格与返回行必须是**同一形状**（整格覆盖的理由见被测函数注释）：
        // 1bis 的 finish_time 只写在返回行、不写聚合那一格，就等于留了个"落库行缺列"的口子
        // ——`persist_history_task` 后面 INSERT 绑的是聚合外的 task 副本，但监听器/回显读的是聚合那份。
        assert_eq!(inst.tasks[1].finish_time.as_deref(), Some("2026-09-29 10:00:00"),
            "§6.2 1bis：聚合里那格也得带完成时间，不许与返回行分叉");
        assert_eq!(inst.tasks[0].finish_time, None, "先建那行不得被顺手写上办理时间");
        assert_eq!(inst.get_doing_tasks().len(), 1, "进行中仍是一行");
        assert_eq!(inst.get_finished_tasks().len(), 1, "已办结仍是一行");
    }

    // ─────────── issues/141 G10 · 抄送人归一判据本体 ───────────

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    // ─────────── issues/142 B 批 · 归属值写侧归一（§2.11 把 §2.10 的尺子搬到任务侧）───────────

    /// 判据本体（任务侧与抄送侧共用同一枚 `normalize_actors`）：trim ⇒ 丢空 ⇒ 折叠，顺序保持。
    #[test]
    fn test_i142_b_normalize_actors_trims_drops_and_folds() {
        assert_eq!(normalize_actors(&v(&["7501", "7502"])), v(&["7501", "7502"]),
            "正向对照：正常值一个不吃、顺序不动");
        assert_eq!(normalize_actors(&v(&[" 8301 ", "\tu9\n", "8302"])), v(&["8301", "u9", "8302"]),
            "§2.11 要求②：落库与比较一律取 trim 后的值");
        assert_eq!(normalize_actors(&v(&["8401", " 8401 ", "8401"])), v(&["8401"]),
            "§2.11 要求②：trim 后同值＝同一个人 ⇒ 同一次调用内折叠（不 trim 就与写侧判重错开落两行）");
        for blank in ["", " ", "   ", "\t", "\n", "\r\n", " \t\n "] {
            assert!(normalize_actors(&v(&[blank])).is_empty(),
                "§2.11：纯空白 {blank:?} 必须丢完 ⇒ 空集合，实得 {:?}", normalize_actors(&v(&[blank])));
        }
        assert_eq!(normalize_actors(&v(&["7601", "", "  ", "7602"])), v(&["7601", "7602"]),
            "混给只丢空的");
    }

    /// 反向哨兵原文四档：`"0"`、`"00"`、`" "`、`"a"` 是**三个人**（`" "` 才是空值）。
    /// 判空一律 `trim().is_empty()`，严禁拿"看起来像空/像假值"的判据吃正常 id。
    #[test]
    fn test_i142_b_normalize_actors_sentinel_four_are_three_people() {
        assert_eq!(normalize_actors(&v(&["0", "00", " ", "a"])), v(&["0", "00", "a"]),
            "§2.11 硬要求④：'0'/'00'/'a' 都是正常 id，只有纯空白 ' ' 是空值");
        assert_eq!(normalize_actors(&v(&["0", "0"])), v(&["0"]), "同值折叠，但 '0' 本身不许被丢");
        assert_eq!(normalize_actors(&v(&["00", "0"])), v(&["00", "0"]),
            "'00' 与 '0' 是两个人（严禁松散比较把第二个人静默吞掉——php 本轮实测到的两把尺子）");
    }

    /// 一枚判据两个名字：`normalize_cc_actors`（§2.10 旧公开名）必须转发到 `normalize_actors`，
    /// 两支对同一批入参**逐字同答案**——分叉的起点就是"抄第二份"。
    #[test]
    fn test_i142_b_cc_alias_forwards_to_the_single_judge() {
        for batch in [
            v(&["7501", " 7501 ", "", "  ", "0"]),
            v(&["", "   ", "\t"]),
            v(&["0", "00", " ", "a"]),
            v(&[]),
            v(&["x", "y", "x", " y "]),
        ] {
            assert_eq!(normalize_cc_actors(&batch), normalize_actors(&batch),
                "§2.11 尾注：旧名只是转发，两枚名字必须同一判据（入参 {batch:?}）");
        }
    }

    /// 正向对照：非空抄送人原样保留、顺序不动（判据不吃正常值）。
    #[test]
    fn test_i141_g10_normalize_keeps_valid_actors_in_order() {
        assert_eq!(normalize_cc_actors(&v(&["7501", "7502"])), v(&["7501", "7502"]));
        assert_eq!(normalize_cc_actors(&v(&["a", "b", "c"])), v(&["a", "b", "c"]),
            "顺序必须保持（fire 码 4 的入参顺序＝请求顺序，engine.rs 既有判据）");
    }

    /// 空串／纯空白（空格·制表·换行）全部丢弃；丢完为空 ⇒ 空集合（调用方据此不建行、不 fire）。
    #[test]
    fn test_i141_g10_normalize_drops_empty_and_whitespace_only() {
        for blank in ["", " ", "   ", "\t", "\n", "\r\n", " \t\n "] {
            assert!(normalize_cc_actors(&v(&[blank])).is_empty(),
                "G10：纯空白 {blank:?} 必须丢完 ⇒ 空集合，实得 {:?}", normalize_cc_actors(&v(&[blank])));
        }
        assert!(normalize_cc_actors(&v(&["", "  ", "\t"])).is_empty(), "全空白一批 ⇒ 空集合");
        assert!(normalize_cc_actors(&v(&[])).is_empty(), "空入参 ⇒ 空集合");
    }

    /// 混给只丢空的：有效元素一个不少、顺序保持。
    #[test]
    fn test_i141_g10_normalize_drops_only_the_blanks() {
        assert_eq!(normalize_cc_actors(&v(&["7601", "", "  ", "7602"])), v(&["7601", "7602"]));
    }

    /// 落库与比较值取 trim 后的串（spec §2.10 实现要求②）。
    #[test]
    fn test_i141_g10_normalize_trims_values() {
        assert_eq!(normalize_cc_actors(&v(&[" 8301 ", "8302"])), v(&["8301", "8302"]),
            "G10：带空格的入参归一为 trim 后的串");
        assert_eq!(normalize_cc_actors(&v(&["\tu9\n"])), v(&["u9"]), "Unicode 空白一并 trim");
    }

    /// 同一次调用内的重复折叠（trim 之后同值＝同一个人，与 issues/141 G2 写侧判重同一条尺子）。
    #[test]
    fn test_i141_g10_normalize_folds_duplicates_after_trim() {
        assert_eq!(normalize_cc_actors(&v(&["8401", " 8401 ", "8401"])), v(&["8401"]),
            "G10：'8401' 与 ' 8401 ' 判为同一个人 ⇒ 折叠成一条（否则 G2 判重被打穿，落两行）");
    }

    /// 反向哨兵（spec §2.10 实现要求④）：判据只吃空值，不吃 "0" 这类"看起来像空"的正常 id。
    #[test]
    fn test_i141_g10_normalize_does_not_eat_looks_like_empty_ids() {
        assert_eq!(normalize_cc_actors(&v(&["0"])), v(&["0"]),
            "G10 反向哨兵：'0' 是正常用户 id，不得被当成空值丢掉");
        assert_eq!(normalize_cc_actors(&v(&["0", "", "user-1", "  "])), v(&["0", "user-1"]),
            "反向哨兵混给空值：只丢空的，'0' 与 'user-1' 都在");
        for not_blank in ["0", "false", "null", "None", "null-id"] {
            let one = not_blank.trim();
            let got = normalize_cc_actors(&v(&[not_blank]));
            if one.is_empty() { continue; }
            assert_eq!(got, v(&[one]), "{not_blank:?} 不该被判成空值");
        }
    }
}
