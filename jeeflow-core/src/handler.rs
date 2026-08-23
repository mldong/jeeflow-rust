//! Built-in handlers — 5 IHandler implementations.
//! CreateTask, EndProcess, MergeBranch, StartSubProcess, Countersign.

use crate::engine::Execution;
use crate::error::JeeflowResult;
use crate::model::ProcessTask;

/// IHandler trait — node type handler.
pub trait IHandler: Send + Sync {
    fn handle(&self, execution: &mut Execution) -> JeeflowResult<()>;
}

/// CreateTaskHandler — creates tasks for task nodes.
pub struct CreateTaskHandler;

impl IHandler for CreateTaskHandler {
    fn handle(&self, _execution: &mut Execution) -> JeeflowResult<()> {
        // Task creation is handled inline in engine.execute_node for Task type
        Ok(())
    }
}

/// EndProcessHandler — finishes/rejects the process instance.
pub struct EndProcessHandler;

impl IHandler for EndProcessHandler {
    fn handle(&self, execution: &mut Execution) -> JeeflowResult<()> {
        let has_reject = execution.args.get_str("reject")
            .map(|v| v == "true").unwrap_or(false);
        if has_reject {
            execution.process_instance.reject();
        } else {
            execution.process_instance.finish();
        }
        execution.instance_finished = true;
        Ok(())
    }
}

/// MergeBranchHandler — waits for all parallel branches to complete.
pub struct MergeBranchHandler;

impl IHandler for MergeBranchHandler {
    fn handle(&self, execution: &mut Execution) -> JeeflowResult<()> {
        let doing = execution.process_instance.get_doing_tasks();
        if doing.is_empty() {
            execution.is_merged = true;
        }
        Ok(())
    }
}

/// StartSubProcessHandler — starts a sub-process instance.
pub struct StartSubProcessHandler;

impl IHandler for StartSubProcessHandler {
    fn handle(&self, _execution: &mut Execution) -> JeeflowResult<()> {
        // Sub-process handling delegated to engine
        Ok(())
    }
}

/// CountersignHandler — evaluates countersign completion conditions.
/// Refactored from dead IHandler impl to pure functions for engine gate logic (issues/94).
pub struct CountersignHandler;

impl CountersignHandler {
    /// Check if a countersign node should merge after a task completion.
    /// Implements: one-vote veto gate + SEQUENTIAL/PARALLEL/RATIO completion conditions.
    ///
    /// - `same_node_tasks`: all tasks belonging to the countersign node (FINISHED/DOING/ABANDON)
    /// - `submit_type`: the submit type from exec args (Some(20) = countersign disagree)
    /// - `completion_condition`: the node's countersignCompletionCondition (trimmed)
    /// - `countersign_type`: SEQUENTIAL / PARALLEL
    pub fn check_merge(same_node_tasks: &[&ProcessTask], submit_type: Option<i64>,
                        completion_condition: Option<&str>, countersign_type: &str) -> bool {
        let total = same_node_tasks.len();
        let finished = same_node_tasks.iter().filter(|t| t.is_finished()).count();

        // One-vote veto gate: submitType==20 AND condition == ONE_VOTE_VETO (case-insensitive)
        if submit_type == Some(20) {
            if let Some(cond) = completion_condition {
                if cond.trim().eq_ignore_ascii_case("ONE_VOTE_VETO") {
                    return true;
                }
            }
        }

        match countersign_type.to_uppercase().as_str() {
            "SEQUENTIAL" | "SERIAL" => finished >= total,
            _ => {
                // PARALLEL (default)
                if let Some(cond) = completion_condition {
                    let cond = cond.trim();
                    if cond.is_empty() || cond.eq_ignore_ascii_case("ONE_VOTE_VETO") {
                        // No special condition (or ONE_VOTE_VETO but veto gate not hit) → all must finish
                        finished >= total
                    } else {
                        // Expression condition (e.g. "#nrOfCompletedInstances==2")
                        // evaluated by the engine with gate vars; this pure function
                        // cannot evaluate expressions, so the engine handles it separately.
                        // Return false here; engine will evaluate the expression itself.
                        false
                    }
                } else {
                    finished >= total
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;
    use crate::json::FlowData;

    fn make_task(name: &str, state: i32) -> ProcessTask {
        ProcessTask {
            task_id: 0, process_instance_id: 1,
            task_name: name.to_string(), display_name: "T".to_string(),
            task_type: 0, perform_type: 1, task_state: state,
            actor_id: None, actor_ids: vec!["u".to_string()],
            finish_time: None, expire_time: None, form_key: None,
            parent_task_id: None, variables: FlowData::new(),
            create_time: None, create_user: None,
            update_time: None, update_user: None,
        }
    }

    #[test]
    fn test_countersign_parallel_all_done() {
        let tasks: Vec<ProcessTask> = (0..3).map(|_| make_task("t", TaskState::Finished.code())).collect();
        let refs: Vec<&ProcessTask> = tasks.iter().collect();
        assert!(CountersignHandler::check_merge(&refs, None, None, "PARALLEL"));
    }

    #[test]
    fn test_countersign_parallel_not_all_done() {
        let mut tasks: Vec<ProcessTask> = (0..3).map(|_| make_task("t", TaskState::Finished.code())).collect();
        tasks[2].task_state = TaskState::Doing.code();
        let refs: Vec<&ProcessTask> = tasks.iter().collect();
        assert!(!CountersignHandler::check_merge(&refs, None, None, "PARALLEL"));
    }

    #[test]
    fn test_countersign_sequential_not_all() {
        let mut tasks: Vec<ProcessTask> = (0..2).map(|_| make_task("t", TaskState::Finished.code())).collect();
        tasks[1].task_state = TaskState::Doing.code();
        let refs: Vec<&ProcessTask> = tasks.iter().collect();
        assert!(!CountersignHandler::check_merge(&refs, None, None, "SEQUENTIAL"));
    }

    #[test]
    fn test_countersign_sequential_all_done() {
        let tasks: Vec<ProcessTask> = (0..2).map(|_| make_task("t", TaskState::Finished.code())).collect();
        let refs: Vec<&ProcessTask> = tasks.iter().collect();
        assert!(CountersignHandler::check_merge(&refs, None, None, "SEQUENTIAL"));
    }

    #[test]
    fn test_countersign_one_vote_veto() {
        let mut tasks: Vec<ProcessTask> = (0..3).map(|_| make_task("t", TaskState::Doing.code())).collect();
        tasks[0].task_state = TaskState::Finished.code();
        let refs: Vec<&ProcessTask> = tasks.iter().collect();
        // submitType=20 + ONE_VOTE_VETO → merged
        assert!(CountersignHandler::check_merge(&refs, Some(20), Some("ONE_VOTE_VETO"), "PARALLEL"));
        // submitType=20 + no condition → soft reject, NOT merged
        assert!(!CountersignHandler::check_merge(&refs, Some(20), None, "PARALLEL"));
        // submitType=1 + ONE_VOTE_VETO → not merged (veto not triggered)
        assert!(!CountersignHandler::check_merge(&refs, Some(1), Some("ONE_VOTE_VETO"), "PARALLEL"));
    }
}
