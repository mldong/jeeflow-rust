//! In-memory repository implementation for testing (T0).
//! Implements ProcessRepository + ProcessExtRepository with HashMap storage.
//! All methods are SYNCHRONOUS — no async/await.

use crate::error::JeeflowResult;
use crate::model::*;
use crate::spi::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

/// In-memory process repository for testing.

fn flow_data_json_string(fd: &crate::json::FlowData) -> Option<String> {
    let entries: Vec<(String, crate::json::JsonValue)> = fd
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    Some(crate::json::JsonValue::Object(entries).to_json_string())
}

pub struct MemoryRepository {
    id_counter: AtomicI64,
    defines: Mutex<HashMap<i64, ProcessDefine>>,
    instances: Mutex<HashMap<i64, ProcessInstance>>,
    tasks: Mutex<HashMap<i64, ProcessTask>>,
    task_actors: Mutex<HashMap<i64, Vec<String>>>,
    cc_instances: Mutex<Vec<CcInstance>>,
    designs: Mutex<HashMap<i64, ProcessDesign>>,
    design_his: Mutex<Vec<ProcessDesignHis>>,
    surrogates: Mutex<HashMap<i64, ProcessSurrogate>>,
}

impl MemoryRepository {
    pub fn new() -> Self {
        MemoryRepository {
            id_counter: AtomicI64::new(100000),
            defines: Mutex::new(HashMap::new()),
            instances: Mutex::new(HashMap::new()),
            tasks: Mutex::new(HashMap::new()),
            task_actors: Mutex::new(HashMap::new()),
            cc_instances: Mutex::new(Vec::new()),
            designs: Mutex::new(HashMap::new()),
            design_his: Mutex::new(Vec::new()),
            surrogates: Mutex::new(HashMap::new()),
        }
    }

    fn next_id(&self) -> i64 {
        self.id_counter.fetch_add(1, Ordering::SeqCst)
    }
}

impl Default for MemoryRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessRepository for MemoryRepository {
    fn find_define_by_id(&self, define_id: i64) -> JeeflowResult<Option<ProcessDefine>> {
        let defines = self.defines.lock().unwrap();
        Ok(defines.get(&define_id).cloned())
    }

    fn save_define(&self, define: &mut ProcessDefine) -> JeeflowResult<()> {
        if define.id == 0 { define.id = self.next_id(); }
        let mut defines = self.defines.lock().unwrap();
        defines.insert(define.id, define.clone());
        Ok(())
    }

    fn update_define(&self, define: &ProcessDefine) -> JeeflowResult<()> {
        let mut defines = self.defines.lock().unwrap();
        defines.insert(define.id, define.clone());
        Ok(())
    }

    fn update_define_state(&self, define_id: i64, state: i32) -> JeeflowResult<()> {
        let mut defines = self.defines.lock().unwrap();
        if let Some(d) = defines.get_mut(&define_id) {
            d.state = state;
        }
        Ok(())
    }

    fn remove_define(&self, define_id: i64) -> JeeflowResult<()> {
        let mut defines = self.defines.lock().unwrap();
        defines.remove(&define_id);
        Ok(())
    }

    fn find_instance_by_id(&self, instance_id: i64) -> JeeflowResult<Option<ProcessInstance>> {
        let mut inst = {
            let instances = self.instances.lock().unwrap();
            instances.get(&instance_id).cloned()
        };
        // 任务落在独立 map；聚合内 tasks 可能仍是 task_id=0 的草稿——读侧用仓储真相回填
        if let Some(ref mut i) = inst {
            let tasks = self.tasks.lock().unwrap();
            i.tasks = tasks
                .values()
                .filter(|t| t.process_instance_id == instance_id)
                .cloned()
                .collect();
        }
        Ok(inst)
    }

    fn save_instance(&self, instance: &mut ProcessInstance) -> JeeflowResult<()> {
        if instance.instance_id == 0 { instance.instance_id = self.next_id(); }
        let mut instances = self.instances.lock().unwrap();
        instances.insert(instance.instance_id, instance.clone());
        Ok(())
    }

    fn update_instance(&self, instance: &ProcessInstance) -> JeeflowResult<()> {
        let mut instances = self.instances.lock().unwrap();
        instances.insert(instance.instance_id, instance.clone());
        Ok(())
    }

    fn find_task_by_id(&self, task_id: i64) -> JeeflowResult<Option<ProcessTask>> {
        let tasks = self.tasks.lock().unwrap();
        Ok(tasks.get(&task_id).cloned())
    }

    fn save_task(&self, task: &mut ProcessTask) -> JeeflowResult<()> {
        if task.task_id == 0 { task.task_id = self.next_id(); }
        let mut tasks = self.tasks.lock().unwrap();
        tasks.insert(task.task_id, task.clone());
        Ok(())
    }

    fn update_task(&self, task: &ProcessTask) -> JeeflowResult<()> {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.insert(task.task_id, task.clone());
        Ok(())
    }

    fn find_doing_tasks(&self, instance_id: i64, task_names: &[String]) -> JeeflowResult<Vec<ProcessTask>> {
        let tasks = self.tasks.lock().unwrap();
        let result: Vec<ProcessTask> = tasks.values()
            .filter(|t| {
                t.process_instance_id == instance_id
                    && t.task_state == TaskState::Doing.code()
                    && (task_names.is_empty() || task_names.contains(&t.task_name))
            })
            .cloned()
            .collect();
        Ok(result)
    }

    fn find_done_tasks(&self, instance_id: i64, task_names: &[String]) -> JeeflowResult<Vec<ProcessTask>> {
        let tasks = self.tasks.lock().unwrap();
        let result: Vec<ProcessTask> = tasks.values()
            .filter(|t| {
                t.process_instance_id == instance_id
                    && t.task_state == TaskState::Finished.code()
                    && (task_names.is_empty() || task_names.contains(&t.task_name))
            })
            .cloned()
            .collect();
        Ok(result)
    }

    fn find_history_tasks(&self, instance_id: i64) -> JeeflowResult<Vec<ProcessTask>> {
        let tasks = self.tasks.lock().unwrap();
        let result: Vec<ProcessTask> = tasks.values()
            .filter(|t| t.process_instance_id == instance_id)
            .cloned()
            .collect();
        Ok(result)
    }

    fn find_task_actors(&self, task_id: i64) -> JeeflowResult<Vec<String>> {
        let actors = self.task_actors.lock().unwrap();
        Ok(actors.get(&task_id).cloned().unwrap_or_default())
    }

    fn add_task_actor(&self, task_id: i64, new_actors: &[String]) -> JeeflowResult<()> {
        let mut actors = self.task_actors.lock().unwrap();
        let entry = actors.entry(task_id).or_insert_with(Vec::new);
        for a in new_actors {
            if !entry.contains(a) { entry.push(a.clone()); }
        }
        Ok(())
    }

    fn remove_task_actor(&self, task_id: i64, remove_actors: &[String]) -> JeeflowResult<()> {
        let mut actors = self.task_actors.lock().unwrap();
        if let Some(entry) = actors.get_mut(&task_id) {
            entry.retain(|a| !remove_actors.contains(a));
        }
        Ok(())
    }

    fn create_cc_instance(&self, instance_id: i64, creator: &str, actor_ids: &[String]) -> JeeflowResult<()> {
        let mut ccs = self.cc_instances.lock().unwrap();
        for actor_id in actor_ids {
            ccs.push(CcInstance {
                id: self.next_id(),
                process_instance_id: instance_id,
                actor_id: actor_id.clone(),
                state: 0,
                create_time: None,
                create_user: Some(creator.to_string()),
                update_time: None,
                update_user: None,
            });
        }
        Ok(())
    }

    fn update_cc_status(&self, instance_id: i64, actor_id: &str) -> JeeflowResult<()> {
        let mut ccs = self.cc_instances.lock().unwrap();
        for cc in ccs.iter_mut() {
            if cc.process_instance_id == instance_id && cc.actor_id == actor_id {
                cc.state = 1;
            }
        }
        Ok(())
    }

    fn page_todo_tasks(&self, query: &PageQuery) -> JeeflowResult<PageResult<TaskRow>> {
        let tasks = self.tasks.lock().unwrap();
        let instances = self.instances.lock().unwrap();
        let defines = self.defines.lock().unwrap();

        let rows: Vec<TaskRow> = tasks.values()
            .filter(|t| {
                t.task_state == TaskState::Doing.code()
                    && (query.operator.is_none() || t.actor_ids.contains(query.operator.as_ref().unwrap()))
            })
            .map(|t| {
                let inst = instances.get(&t.process_instance_id);
                let define = inst.and_then(|i| defines.get(&i.define_id));
                TaskRow {
                    id: t.task_id,
                    process_instance_id: t.process_instance_id,
                    task_name: t.task_name.clone(),
                    display_name: t.display_name.clone(),
                    task_type: t.task_type,
                    perform_type: t.perform_type,
                    task_state: t.task_state,
                    operator: t.actor_id.clone(),
                    actor_id: t.actor_ids.first().cloned(),
                    finish_time: t.finish_time.clone(),
                    expire_time: t.expire_time.clone(),
                    form_key: t.form_key.clone(),
                    task_parent_id: t.parent_task_id,
                    variable: flow_data_json_string(&t.variables),
                    create_time: t.create_time.clone(),
                    create_user: t.create_user.clone(),
                    update_time: t.update_time.clone(),
                    update_user: t.update_user.clone(),
                    process_define_id: inst.map(|i| i.define_id),
                    instance_state: inst.map(|i| i.state),
                    instance_operator: inst.map(|i| i.operator.clone()),
                    business_no: inst.and_then(|i| i.business_no.clone()),
                    instance_variable: inst.and_then(|i| flow_data_json_string(&i.variables)),
                    instance_create_time: inst.and_then(|i| i.create_time.clone()),
                    define_name: define.map(|d| d.name.clone()),
                    define_display_name: define.map(|d| d.display_name.clone()),
                    define_version: define.map(|d| d.version),
                }
            })
            .collect();

        let total = rows.len() as i64;
        let start = ((query.page_num - 1) * query.page_size) as usize;
        let end = std::cmp::min(start + query.page_size as usize, rows.len());
        let page_rows = if start < rows.len() { rows[start..end].to_vec() } else { vec![] };

        Ok(PageResult::new(query.page_num, query.page_size, total, page_rows))
    }

    fn page_done_tasks(&self, query: &PageQuery) -> JeeflowResult<PageResult<TaskRow>> {
        let tasks = self.tasks.lock().unwrap();
        let instances = self.instances.lock().unwrap();
        let defines = self.defines.lock().unwrap();

        let rows: Vec<TaskRow> = tasks
            .values()
            .filter(|t| {
                t.task_state == TaskState::Finished.code()
                    && (query.operator.is_none()
                        || t.actor_id.as_ref() == query.operator.as_ref()
                        || t.create_user.as_ref() == query.operator.as_ref())
            })
            .map(|t| {
                let inst = instances.get(&t.process_instance_id);
                let define = inst.and_then(|i| defines.get(&i.define_id));
                TaskRow {
                    id: t.task_id,
                    process_instance_id: t.process_instance_id,
                    task_name: t.task_name.clone(),
                    display_name: t.display_name.clone(),
                    task_type: t.task_type,
                    perform_type: t.perform_type,
                    task_state: t.task_state,
                    operator: t.actor_id.clone(),
                    actor_id: t.actor_ids.first().cloned(),
                    finish_time: t.finish_time.clone(),
                    expire_time: t.expire_time.clone(),
                    form_key: t.form_key.clone(),
                    task_parent_id: t.parent_task_id,
                    variable: flow_data_json_string(&t.variables),
                    create_time: t.create_time.clone(),
                    create_user: t.create_user.clone(),
                    update_time: t.update_time.clone(),
                    update_user: t.update_user.clone(),
                    process_define_id: inst.map(|i| i.define_id),
                    instance_state: inst.map(|i| i.state),
                    instance_operator: inst.map(|i| i.operator.clone()),
                    business_no: inst.and_then(|i| i.business_no.clone()),
                    instance_variable: inst.and_then(|i| flow_data_json_string(&i.variables)),
                    instance_create_time: inst.and_then(|i| i.create_time.clone()),
                    define_name: define.map(|d| d.name.clone()),
                    define_display_name: define.map(|d| d.display_name.clone()),
                    define_version: define.map(|d| d.version),
                }
            })
            .collect();

        let total = rows.len() as i64;
        let start = ((query.page_num - 1) * query.page_size) as usize;
        let end = std::cmp::min(start + query.page_size as usize, rows.len());
        let page_rows = if start < rows.len() {
            rows[start..end].to_vec()
        } else {
            vec![]
        };
        Ok(PageResult::new(query.page_num, query.page_size, total, page_rows))
    }

    fn page_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> {
        let instances = self.instances.lock().unwrap();
        let defines = self.defines.lock().unwrap();
        let rows: Vec<InstanceRow> = instances
            .values()
            .filter(|i| {
                query
                    .operator
                    .as_ref()
                    .map(|op| &i.operator == op)
                    .unwrap_or(true)
            })
            .map(|i| {
                let define = defines.get(&i.define_id);
                InstanceRow {
                    id: i.instance_id,
                    parent_id: i.parent_id,
                    process_define_id: i.define_id,
                    state: i.state,
                    parent_node_name: i.parent_node_name.clone(),
                    business_no: i.business_no.clone(),
                    operator: i.operator.clone(),
                    expire_time: i.expire_time.clone(),
                    variable: flow_data_json_string(&i.variables),
                    create_time: i.create_time.clone(),
                    create_user: i.create_user.clone(),
                    update_time: i.update_time.clone(),
                    update_user: i.update_user.clone(),
                    define_name: define.map(|d| d.name.clone()),
                    define_display_name: define.map(|d| d.display_name.clone()),
                    define_version: define.map(|d| d.version),
                }
            })
            .collect();

        let total = rows.len() as i64;
        let start = ((query.page_num - 1) * query.page_size) as usize;
        let end = std::cmp::min(start + query.page_size as usize, rows.len());
        let page_rows = if start < rows.len() {
            rows[start..end].to_vec()
        } else {
            vec![]
        };
        Ok(PageResult::new(query.page_num, query.page_size, total, page_rows))
    }

    fn page_cc_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> {
        let ccs = self.cc_instances.lock().unwrap();
        let instances = self.instances.lock().unwrap();
        let defines = self.defines.lock().unwrap();
        let rows: Vec<InstanceRow> = ccs
            .iter()
            .filter(|cc| {
                query
                    .operator
                    .as_ref()
                    .map(|op| &cc.actor_id == op)
                    .unwrap_or(true)
            })
            .map(|cc| {
            let inst = instances.get(&cc.process_instance_id);
            let define = inst.and_then(|i| defines.get(&i.define_id));
            InstanceRow {
                id: cc.id,
                parent_id: None,
                process_define_id: inst.map(|i| i.define_id).unwrap_or(0),
                state: cc.state,
                parent_node_name: None,
                business_no: inst.and_then(|i| i.business_no.clone()),
                operator: cc.actor_id.clone(),
                expire_time: None,
                variable: None,
                create_time: cc.create_time.clone(),
                create_user: cc.create_user.clone(),
                update_time: cc.update_time.clone(),
                update_user: cc.update_user.clone(),
                define_name: define.map(|d| d.name.clone()),
                define_display_name: define.map(|d| d.display_name.clone()),
                define_version: define.map(|d| d.version),
            }
        }).collect();
        let total = rows.len() as i64;
        let start = ((query.page_num - 1) * query.page_size) as usize;
        let end = std::cmp::min(start + query.page_size as usize, rows.len());
        let page_rows = if start < rows.len() { rows[start..end].to_vec() } else { vec![] };
        Ok(PageResult::new(query.page_num, query.page_size, total, page_rows))
    }

    fn page_defines(&self, query: &PageQuery) -> JeeflowResult<PageResult<DefineRow>> {
        let defines = self.defines.lock().unwrap();
        let rows: Vec<DefineRow> = defines.values().map(|d| DefineRow {
            id: d.id,
            name: d.name.clone(),
            display_name: d.display_name.clone(),
            define_type: d.define_type.clone(),
            state: d.state,
            version: d.version,
            create_time: d.create_time.clone(),
            create_user: d.create_user.clone(),
            update_time: d.update_time.clone(),
            update_user: d.update_user.clone(),
        }).collect();
        let total = rows.len() as i64;
        let start = ((query.page_num - 1) * query.page_size) as usize;
        let end = std::cmp::min(start + query.page_size as usize, rows.len());
        let page_rows = if start < rows.len() { rows[start..end].to_vec() } else { vec![] };
        Ok(PageResult::new(query.page_num, query.page_size, total, page_rows))
    }

    fn count_todo_tasks(&self, user_id: &str) -> JeeflowResult<i64> {
        let tasks = self.tasks.lock().unwrap();
        let count = tasks.values()
            .filter(|t| t.task_state == TaskState::Doing.code() && t.actor_ids.contains(&user_id.to_string()))
            .count() as i64;
        Ok(count)
    }

    fn get_all_instances(&self) -> JeeflowResult<Vec<ProcessInstance>> {
        let instances = self.instances.lock().unwrap();
        Ok(instances.values().cloned().collect())
    }

    fn get_all_tasks(&self) -> JeeflowResult<Vec<ProcessTask>> {
        let tasks = self.tasks.lock().unwrap();
        let task_actors = self.task_actors.lock().unwrap();
        Ok(tasks.values().map(|t| {
            let mut task = t.clone();
            if task.actor_ids.is_empty() {
                if let Some(actors) = task_actors.get(&t.task_id) {
                    task.actor_ids = actors.clone();
                }
            }
            task
        }).collect())
    }
}

impl ProcessExtRepository for MemoryRepository {
    fn find_design_by_id(&self, design_id: i64) -> JeeflowResult<Option<ProcessDesign>> {
        let designs = self.designs.lock().unwrap();
        Ok(designs.get(&design_id).cloned())
    }

    fn save_design(&self, design: &mut ProcessDesign) -> JeeflowResult<()> {
        if design.id == 0 { design.id = self.next_id(); }
        let mut designs = self.designs.lock().unwrap();
        designs.insert(design.id, design.clone());
        Ok(())
    }

    fn update_design(&self, design: &ProcessDesign) -> JeeflowResult<()> {
        let mut designs = self.designs.lock().unwrap();
        designs.insert(design.id, design.clone());
        Ok(())
    }

    fn remove_design(&self, design_id: i64) -> JeeflowResult<()> {
        let mut designs = self.designs.lock().unwrap();
        designs.remove(&design_id);
        Ok(())
    }

    fn page_designs(&self, query: &PageQuery) -> JeeflowResult<PageResult<ProcessDesign>> {
        let designs = self.designs.lock().unwrap();
        let rows: Vec<ProcessDesign> = designs.values().cloned().collect();
        let total = rows.len() as i64;
        let start = ((query.page_num - 1) * query.page_size) as usize;
        let end = std::cmp::min(start + query.page_size as usize, rows.len());
        let page_rows = if start < rows.len() { rows[start..end].to_vec() } else { vec![] };
        Ok(PageResult::new(query.page_num, query.page_size, total, page_rows))
    }

    fn save_design_his(&self, his: &mut ProcessDesignHis) -> JeeflowResult<()> {
        if his.id == 0 { his.id = self.next_id(); }
        let mut h = self.design_his.lock().unwrap();
        h.push(his.clone());
        Ok(())
    }

    fn list_design_his(&self, design_id: i64) -> JeeflowResult<Vec<ProcessDesignHis>> {
        let h = self.design_his.lock().unwrap();
        // Newest first (index 0) — align Java JDBC order / design detail jsonObject
        let mut list: Vec<ProcessDesignHis> = h
            .iter()
            .filter(|d| d.process_design_id == design_id)
            .cloned()
            .collect();
        list.reverse();
        Ok(list)
    }

    fn find_surrogate_by_id(&self, surrogate_id: i64) -> JeeflowResult<Option<ProcessSurrogate>> {
        let s = self.surrogates.lock().unwrap();
        Ok(s.get(&surrogate_id).cloned())
    }

    fn save_surrogate(&self, surrogate: &mut ProcessSurrogate) -> JeeflowResult<()> {
        if surrogate.id == 0 { surrogate.id = self.next_id(); }
        let mut s = self.surrogates.lock().unwrap();
        s.insert(surrogate.id, surrogate.clone());
        Ok(())
    }

    fn update_surrogate(&self, surrogate: &ProcessSurrogate) -> JeeflowResult<()> {
        let mut s = self.surrogates.lock().unwrap();
        s.insert(surrogate.id, surrogate.clone());
        Ok(())
    }

    fn remove_surrogate(&self, surrogate_id: i64) -> JeeflowResult<()> {
        let mut s = self.surrogates.lock().unwrap();
        s.remove(&surrogate_id);
        Ok(())
    }

    fn page_surrogates(&self, query: &PageQuery) -> JeeflowResult<PageResult<ProcessSurrogate>> {
        let s = self.surrogates.lock().unwrap();
        let rows: Vec<ProcessSurrogate> = s.values().cloned().collect();
        let total = rows.len() as i64;
        let start = ((query.page_num - 1) * query.page_size) as usize;
        let end = std::cmp::min(start + query.page_size as usize, rows.len());
        let page_rows = if start < rows.len() { rows[start..end].to_vec() } else { vec![] };
        Ok(PageResult::new(query.page_num, query.page_size, total, page_rows))
    }

    fn get_surrogate(&self, operator: &str, process_name: &str, _time: &str) -> JeeflowResult<Option<ProcessSurrogate>> {
        let s = self.surrogates.lock().unwrap();
        Ok(s.values().find(|sg| {
            sg.operator == operator && sg.process_name == process_name && sg.enabled == 1
        }).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::FlowData;

    #[test]
    fn test_define_crud() {
        let repo = MemoryRepository::new();
        let mut define = ProcessDefine {
            id: 0, name: "test".into(), display_name: "Test".into(),
            define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
            version: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_define(&mut define).unwrap();
        assert!(define.id > 0);

        let found = repo.find_define_by_id(define.id).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "test");

        repo.update_define_state(define.id, 0).unwrap();
        let found = repo.find_define_by_id(define.id).unwrap().unwrap();
        assert_eq!(found.state, 0);

        repo.remove_define(define.id).unwrap();
        let found = repo.find_define_by_id(define.id).unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn test_instance_and_tasks() {
        let repo = MemoryRepository::new();
        let define = ProcessDefine {
            id: 1, name: "test".into(), display_name: "Test".into(),
            define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
            version: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        let mut defines = repo.defines.lock().unwrap();
        defines.insert(1, define);
        drop(defines);

        let mut inst = ProcessInstance {
            instance_id: 0, parent_id: None, define_id: 1, state: 10,
            parent_node_name: None, business_no: None, operator: "user1".into(),
            expire_time: None, variables: FlowData::new(), tasks: vec![],
            create_time: None, create_user: Some("user1".into()),
            update_time: None, update_user: None, define: None,
        };
        repo.save_instance(&mut inst).unwrap();
        assert!(inst.instance_id > 0);

        let mut task = ProcessTask {
            task_id: 0, process_instance_id: inst.instance_id,
            task_name: "apply".into(), display_name: "Apply".into(),
            task_type: 0, perform_type: 0, task_state: 10,
            actor_id: None, actor_ids: vec!["user1".into()],
            finish_time: None, expire_time: None, form_key: None,
            parent_task_id: None, variables: FlowData::new(),
            create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_task(&mut task).unwrap();
        assert!(task.task_id > 0);

        // Find doing tasks
        let doing = repo.find_doing_tasks(inst.instance_id, &[]).unwrap();
        assert_eq!(doing.len(), 1);

        // Add actor
        repo.add_task_actor(task.task_id, &["user2".into()]).unwrap();
        let actors = repo.find_task_actors(task.task_id).unwrap();
        assert!(actors.contains(&"user2".into()));

        // Count todo
        let count = repo.count_todo_tasks("user1").unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_cc_instance() {
        let repo = MemoryRepository::new();
        repo.create_cc_instance(1, "user1", &["user2".into(), "user3".into()]).unwrap();
        // CC created successfully (no error)
        repo.update_cc_status(1, "user2").unwrap();
        // Status updated
    }

    #[test]
    fn test_design_crud() {
        let repo = MemoryRepository::new();
        let mut design = ProcessDesign {
            id: 0, name: "design1".into(), display_name: "Design 1".into(),
            design_type: "approval".into(), icon: None, is_deployed: 0,
            remark: None, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_design(&mut design).unwrap();
        assert!(design.id > 0);

        let found = repo.find_design_by_id(design.id).unwrap();
        assert!(found.is_some());

        repo.remove_design(design.id).unwrap();
        let found = repo.find_design_by_id(design.id).unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn test_surrogate_crud() {
        let repo = MemoryRepository::new();
        let mut sg = ProcessSurrogate {
            id: 0, process_name: "test".into(), operator: "user1".into(),
            surrogate: "user2".into(), start_time: None, end_time: None,
            enabled: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_surrogate(&mut sg).unwrap();
        assert!(sg.id > 0);

        let found = repo.get_surrogate("user1", "test", "NOW").unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().surrogate, "user2");
    }
}
