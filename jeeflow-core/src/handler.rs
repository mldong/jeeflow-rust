//! Built-in handlers — 5 IHandler implementations.
//! CreateTask, EndProcess, MergeBranch, StartSubProcess, Countersign.

use crate::engine::Execution;
use crate::error::JeeflowResult;

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
pub struct CountersignHandler;

impl CountersignHandler {
    /// Check if countersign is complete based on type and completed count.
    pub fn is_complete(total: usize, completed: usize, countersign_type: &str,
                        ratio: Option<f64>, has_rejected: bool) -> bool {
        if has_rejected {
            return true; // One veto = complete
        }
        match countersign_type.to_uppercase().as_str() {
            "SEQUENTIAL" | "SERIAL" => completed >= total,
            "RATIO" => {
                if let Some(r) = ratio {
                    completed as f64 / total as f64 >= r
                } else {
                    completed >= total
                }
            }
            _ => { // PARALLEL (default)
                completed >= total
            }
        }
    }
}

impl IHandler for CountersignHandler {
    fn handle(&self, execution: &mut Execution) -> JeeflowResult<()> {
        // Countersign completion check
        if let Some(task) = &execution.process_task {
            let task_name = &task.task_name;
            let all_tasks = &execution.process_instance.tasks;
            let same_node_tasks: Vec<_> = all_tasks.iter()
                .filter(|t| t.task_name == *task_name)
                .collect();
            let total = same_node_tasks.len();
            let completed = same_node_tasks.iter()
                .filter(|t| t.is_finished())
                .count();
            let has_rejected = same_node_tasks.iter()
                .any(|t| t.task_state == crate::model::TaskState::Abandon.code());

            let cs_type = execution.current_node.as_ref()
                .map(|n| n.countersign_type())
                .unwrap_or_else(|| "PARALLEL".to_string());

            if Self::is_complete(total, completed, &cs_type, None, has_rejected) {
                execution.is_merged = true;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_countersign_parallel() {
        assert!(!CountersignHandler::is_complete(3, 1, "PARALLEL", None, false));
        assert!(!CountersignHandler::is_complete(3, 2, "PARALLEL", None, false));
        assert!(CountersignHandler::is_complete(3, 3, "PARALLEL", None, false));
    }

    #[test]
    fn test_countersign_sequential() {
        assert!(!CountersignHandler::is_complete(2, 1, "SEQUENTIAL", None, false));
        assert!(CountersignHandler::is_complete(2, 2, "SEQUENTIAL", None, false));
    }

    #[test]
    fn test_countersign_ratio() {
        assert!(CountersignHandler::is_complete(4, 3, "RATIO", Some(0.75), false));
        assert!(!CountersignHandler::is_complete(4, 2, "RATIO", Some(0.75), false));
    }

    #[test]
    fn test_countersign_veto() {
        assert!(CountersignHandler::is_complete(3, 0, "PARALLEL", None, true));
    }
}
