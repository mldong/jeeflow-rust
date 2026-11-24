//! issues/257 子流程主链定向 T0（内存仓，无服务/无 DB/无共享 flows）。

use jeeflow_core::error::{JeeflowError, ERR_BUSINESS, ERR_SUB_PROCESS_DEFINE_NOT_FOUND};
use jeeflow_core::event::{ProcessEvent, ProcessEventType};
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::json::FlowData;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::{
    InstanceState, ProcessDefine, ProcessInstance, ProcessTask, SubmitType, TaskState, TaskType,
};
use jeeflow_core::parser::{ModelParser, NodeType, SubProcessModel};
use jeeflow_core::spi::{ProcessEventListener, ProcessRepository};
use jeeflow_core::{JeeflowEngineImpl, JeeflowResult, ServiceContext};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

struct Capture {
    repo: Arc<MemoryRepository>,
    events: Mutex<Vec<(ProcessEvent, Option<i32>)>>,
}

impl ProcessEventListener for Capture {
    fn on_event(&self, event: &ProcessEvent) {
        let state = if matches!(event.event_type,
            ProcessEventType::ProcessInstanceStart | ProcessEventType::ProcessInstanceEnd) {
            self.repo.find_instance_by_id(event.source_id)
                .ok().flatten().map(|i| i.state)
        } else {
            None
        };
        self.events.lock().unwrap().push((event.clone(), state));
    }
}

fn harness() -> (JeeflowEngineImpl, Arc<MemoryRepository>, Arc<Capture>) {
    let repo = Arc::new(MemoryRepository::new());
    let capture = Arc::new(Capture {
        repo: repo.clone(),
        events: Mutex::new(Vec::new()),
    });
    let mut ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
    ctx.register_event_listener(capture.clone());
    (JeeflowEngineImpl::new(ctx), repo, capture)
}

fn node(id: &str, kind: &str, properties: Value) -> Value {
    json!({
        "id": id,
        "type": format!("snaker:{kind}"),
        "x": 0,
        "y": 0,
        "properties": properties,
        "text": {"value": id}
    })
}

fn edge(id: &str, source: &str, target: &str) -> Value {
    json!({"id": id, "sourceNodeId": source, "targetNodeId": target, "properties": {}})
}

fn flow(name: &str, nodes: Vec<Value>, edges: Vec<Value>) -> String {
    json!({"name": name, "displayName": name, "type": "approval",
        "nodes": nodes, "edges": edges}).to_string()
}

fn linear(name: &str, specs: &[(&str, &str, Value)]) -> String {
    let nodes: Vec<Value> = specs.iter()
        .map(|(id, kind, props)| node(id, kind, props.clone())).collect();
    let edges: Vec<Value> = specs.windows(2).enumerate()
        .map(|(i, pair)| edge(&format!("e{i}"), pair[0].0, pair[1].0)).collect();
    flow(name, nodes, edges)
}

fn apply() -> Value {
    json!({"assignee": "applicant", "taskType": 0, "performType": 0})
}

fn task(actor: &str) -> Value {
    json!({"assignee": actor, "taskType": 0, "performType": 0})
}

fn sub_process(name: &str, version: Option<i32>) -> Value {
    json!({"processName": name, "form": "__wrong__", "version": version})
}

fn add_define(
    repo: &Arc<MemoryRepository>,
    id: i64,
    name: &str,
    version: i32,
    state: i32,
    content: String,
) -> ProcessDefine {
    let mut define = ProcessDefine {
        id,
        name: name.to_string(),
        display_name: name.to_string(),
        define_type: "approval".to_string(),
        state,
        content: content.into_bytes(),
        version,
        create_time: None,
        create_user: None,
        update_time: None,
        update_user: None,
    };
    repo.save_define(&mut define).unwrap();
    define
}

fn child_flow(name: &str, review: &str, with_apply: bool) -> String {
    if with_apply {
        linear(name, &[
            ("start", "start", json!({})),
            ("apply", "task", apply()),
            (review, "task", task("leader")),
            ("end", "end", json!({})),
        ])
    } else {
        linear(name, &[
            ("start", "start", json!({})),
            ("first", "task", task("leader")),
            (review, "task", task("manager")),
            ("end", "end", json!({})),
        ])
    }
}

fn parent_flow(name: &str, sub_id: &str, target: &str, version: Option<i32>) -> String {
    linear(name, &[
        ("start", "start", json!({})),
        ("apply", "task", apply()),
        (sub_id, "subProcess", sub_process(target, version)),
        ("parentAfter", "task", task("manager")),
        ("end", "end", json!({})),
    ])
}

fn start_parent(
    engine: &JeeflowEngineImpl,
    parent_id: i64,
) -> ProcessInstance {
    let parent = engine.start_sync(parent_id, "applicant", &FlowData::new()).unwrap();
    engine.apply_initial_tasks(parent.instance_id, "applicant", &FlowData::new()).unwrap();
    parent
}

fn only_doing(repo: &Arc<MemoryRepository>, instance_id: i64) -> ProcessTask {
    let tasks = repo.find_doing_tasks(instance_id, &[]).unwrap();
    assert_eq!(tasks.len(), 1, "实例 {instance_id} 应恰有一条 DOING 任务，实得 {:?}", tasks);
    tasks[0].clone()
}

fn only_child(repo: &Arc<MemoryRepository>, parent_id: i64) -> ProcessInstance {
    let children: Vec<ProcessInstance> = repo.get_all_instances().unwrap().into_iter()
        .filter(|i| i.parent_id == Some(parent_id)).collect();
    assert_eq!(children.len(), 1, "父实例 {parent_id} 应恰有一个子实例，实得 {:?}", children);
    children[0].clone()
}

fn done_task(repo: &Arc<MemoryRepository>, instance_id: i64, name: &str) -> ProcessTask {
    let tasks = repo.find_done_tasks(instance_id, &[name.to_string()]).unwrap();
    assert_eq!(tasks.len(), 1, "实例 {instance_id} 应恰有一条已完成任务 {name}");
    tasks[0].clone()
}

fn submit(submit_type: SubmitType) -> FlowData {
    let mut args = FlowData::new();
    args.insert_i64("submitType", submit_type.code());
    args
}

fn count_event(capture: &Arc<Capture>, event_type: ProcessEventType, source_id: i64) -> usize {
    capture.events.lock().unwrap().iter()
        .filter(|(event, _)| event.event_type == event_type && event.source_id == source_id)
        .count()
}

fn seed_doing_task(
    repo: &Arc<MemoryRepository>,
    instance_id: i64,
    name: &str,
    actor: &str,
    task_type: TaskType,
) -> ProcessTask {
    let mut task = ProcessTask {
        task_id: 0,
        process_instance_id: instance_id,
        task_name: name.to_string(),
        display_name: name.to_string(),
        task_type: task_type.code(),
        perform_type: 0,
        task_state: TaskState::Doing.code(),
        actor_id: None,
        actor_ids: vec![actor.to_string()],
        finish_time: None,
        expire_time: None,
        form_key: None,
        parent_task_id: None,
        variables: FlowData::new(),
        create_time: None,
        create_user: None,
        update_time: None,
        update_user: None,
    };
    repo.save_task(&mut task).unwrap();
    repo.add_task_actor(task.task_id, &[actor.to_string()]).unwrap();
    task
}

#[test]
fn i257_parser_aliases_and_process_name_fallbacks() {
    for raw in ["snaker:subprocess", "snaker:subProcess", "snaker:wfSubProcess"] {
        let model = ModelParser::parse(&linear("alias", &[
            ("start", "start", json!({})),
            ("sub", raw.trim_start_matches("snaker:"), json!({
                "processName": "target", "name": "wrong", "form": "__wrong__", "version": "2"
            })),
            ("end", "end", json!({})),
        ])).unwrap();
        let node = model.get_node("sub").unwrap();
        assert_eq!(node.node_type, NodeType::SubProcess);
        assert_eq!(node.sub_process_model(), SubProcessModel {
            process_name: "target".to_string(), version: Some(2), form: Some("__wrong__".to_string()),
        });
    }

    let by_name = ModelParser::parse(&linear("byName", &[
        ("start", "start", json!({})),
        ("sub", "subProcess", json!({"name": "target", "form": "f1"})),
        ("end", "end", json!({})),
    ])).unwrap();
    assert_eq!(by_name.get_node("sub").unwrap().sub_process_model().process_name, "target");

    let by_id = ModelParser::parse(&linear("byId", &[
        ("start", "start", json!({})),
        ("nodeId", "wfSubProcess", json!({"subProcessName": "legacyMustBeIgnored"})),
        ("end", "end", json!({})),
    ])).unwrap();
    let sub = by_id.get_node("nodeId").unwrap().sub_process_model();
    assert_eq!(sub.process_name, "nodeId", "旧 subProcessName 不得作为查找分叉");
    assert_eq!(sub.version, None);
}

#[test]
fn i257_definition_lookup_latest_exact_and_no_state_filter() {
    let (engine, repo, _capture) = harness();
    add_define(&repo, 2571001, "versioned", 1, 0,
        child_flow("versioned", "reviewV1", true));
    add_define(&repo, 2571002, "versioned", 2, 1,
        child_flow("versioned", "reviewV2", true));
    add_define(&repo, 2571003, "versioned", 2, 0,
        child_flow("versioned", "reviewV2LatestId", true));

    assert_eq!(repo.find_define_by_name_and_version("versioned", None).unwrap().unwrap().id, 2571003);
    assert_eq!(repo.find_define_by_name_and_version("versioned", Some(1)).unwrap().unwrap().id, 2571001);
    assert!(repo.find_define_by_name_and_version("versioned", Some(99)).unwrap().is_none());
    assert!(repo.find_define_by_name_and_version("  ", None).unwrap().is_none());

    let latest_parent = add_define(&repo, 2571101, "parentLatest", 1, 1,
        parent_flow("parentLatest", "sub", "versioned", None));
    let latest = start_parent(&engine, latest_parent.id);
    assert_eq!(only_child(&repo, latest.instance_id).define_id, 2571003,
        "缺省版本取 version desc、id desc，且停用定义不额外过滤");
    assert_eq!(only_doing(&repo, only_child(&repo, latest.instance_id).instance_id).task_name,
        "reviewV2LatestId");

    let exact_parent = add_define(&repo, 2571102, "parentExact", 1, 1,
        parent_flow("parentExact", "sub", "versioned", Some(1)));
    let exact = start_parent(&engine, exact_parent.id);
    assert_eq!(only_child(&repo, exact.instance_id).define_id, 2571001,
        "显式版本精确匹配，不因 state=0 被替换");
}

#[test]
fn i257_parent_child_closes_loop_and_events_fire_once() {
    let (engine, repo, capture) = harness();
    let child = add_define(&repo, 2572001, "child", 1, 1,
        child_flow("child", "childReview", true));
    let parent = add_define(&repo, 2572002, "parent", 1, 1,
        parent_flow("parent", "sub", "child", Some(1)));

    let parent_instance = start_parent(&engine, parent.id);
    let child_instance = only_child(&repo, parent_instance.instance_id);
    assert_eq!(child_instance.parent_id, Some(parent_instance.instance_id));
    assert_eq!(child_instance.parent_node_name.as_deref(), Some("sub"));
    assert_eq!(child_instance.define_id, child.id);
    assert_eq!(child_instance.state, InstanceState::Doing.code());

    let child_apply = done_task(&repo, child_instance.instance_id, "apply");
    assert_eq!(child_apply.actor_id.as_deref(), Some("flow.auto"));
    assert!(repo.find_task_actors(child_apply.task_id).unwrap().contains(&"applicant".to_string()));
    let child_review = only_doing(&repo, child_instance.instance_id);
    assert_eq!(child_review.task_name, "childReview");
    assert_eq!(count_event(&capture, ProcessEventType::ProcessTaskStart, child_review.task_id), 1,
        "子任务不得由父 execution 再保存/再 fire 一次");
    assert!(repo.find_doing_tasks(parent_instance.instance_id, &["parentAfter".to_string()])
        .unwrap().is_empty(), "子未办结时父不得推进");
    assert!(repo.find_history_tasks(parent_instance.instance_id).unwrap().iter()
        .all(|t| t.process_instance_id == parent_instance.instance_id), "子任务不得混入父任务表集合");

    let mut child_submit = submit(SubmitType::Agree);
    child_submit.insert_str("f_childOnly", "must-not-write-back");
    let returned = engine.execute_task_sync(child_review.task_id, "leader", &child_submit)
        .unwrap();
    assert!(returned.iter().any(|t| t.task_name == "parentAfter"),
        "子办结回父产生的已落库父任务应进入结果集");
    assert_eq!(repo.find_instance_by_id(child_instance.instance_id).unwrap().unwrap().state,
        InstanceState::Finished.code());
    let parent_after = only_doing(&repo, parent_instance.instance_id);
    assert_eq!(parent_after.task_name, "parentAfter");
    let parent_after_instance = repo.find_instance_by_id(parent_instance.instance_id).unwrap().unwrap();
    assert!(parent_after_instance.variables.get("f_childOnly").is_none(),
        "回父任务必须使用父 vars，不得把子提交变量写回父");

    let events = capture.events.lock().unwrap();
    let child_end = events.iter().position(|(e, _)| {
        e.event_type == ProcessEventType::ProcessInstanceEnd && e.source_id == child_instance.instance_id
    }).unwrap();
    let parent_task_start = events.iter().position(|(e, _)| {
        e.event_type == ProcessEventType::ProcessTaskStart && e.source_id == parent_after.task_id
    }).unwrap();
    assert!(child_end < parent_task_start, "子 END 必须先落库/先播，再回父产生后继任务");
    assert_eq!(events[child_end].1, Some(InstanceState::Finished.code()),
        "子 END 事件触发时必须已从仓储读到终态");
    drop(events);

    engine.execute_task_sync(parent_after.task_id, "manager", &submit(SubmitType::Agree)).unwrap();
    assert_eq!(repo.find_instance_by_id(parent_instance.instance_id).unwrap().unwrap().state,
        InstanceState::Finished.code());
    for (event_type, instance_id) in [
        (ProcessEventType::ProcessInstanceStart, child_instance.instance_id),
        (ProcessEventType::ProcessInstanceEnd, child_instance.instance_id),
        (ProcessEventType::ProcessInstanceStart, parent_instance.instance_id),
        (ProcessEventType::ProcessInstanceEnd, parent_instance.instance_id),
    ] {
        assert_eq!(count_event(&capture, event_type, instance_id), 1,
            "{event_type:?}/{instance_id} 必须恰好一次");
    }
}

#[test]
fn i257_real_operator_fallback_and_child_args_strip() {
    let (engine, repo, _capture) = harness();
    add_define(&repo, 2573001, "childArgs", 1, 1,
        child_flow("childArgs", "childReview", true));
    let parent = add_define(&repo, 2573002, "parentArgs", 1, 1,
        parent_flow("parentArgs", "sub", "childArgs", Some(1)));
    let mut parent_instance = engine.start_sync(parent.id, "applicant", &FlowData::new()).unwrap();
    parent_instance.variables.insert_str("f_business", "keep-me");
    parent_instance.variables.insert_i64("submitType", 1);
    parent_instance.variables.insert_str("tf_parentOnly", "drop-me");
    parent_instance.variables.insert_str("f_nextNodeOperator", "wrong-parent-next");
    parent_instance.variables.insert_str("tf_nextNodeOperator", "wrong-parent-next");
    parent_instance.variables.insert_str("_branchCtx", "drop-me");
    parent_instance.variables.insert_str("_reapplyContext", "drop-me");
    repo.update_instance(&parent_instance).unwrap();

    let parent_apply = only_doing(&repo, parent_instance.instance_id);
    engine.execute_task_sync(parent_apply.task_id, "flow.admin", &submit(SubmitType::Agree)).unwrap();
    let child = only_child(&repo, parent_instance.instance_id);
    assert_eq!(child.operator, "applicant", "flow.admin 触发须回退父 operator");
    assert_eq!(child.variables.get_str("f_business"), Some("keep-me"));
    for key in ["submitType", "tf_parentOnly", "f_nextNodeOperator", "tf_nextNodeOperator",
                "_branchCtx", "_reapplyContext"] {
        assert!(child.variables.get(key).is_none(), "子 args 不得继承父控制键 {key}");
    }
    assert_eq!(only_doing(&repo, child.instance_id).task_name, "childReview");
}

#[test]
fn i257_child_without_apply_self_handles_first_task() {
    let (engine, repo, _capture) = harness();
    add_define(&repo, 2574001, "childNoApply", 1, 1,
        child_flow("childNoApply", "second", false));
    let parent = add_define(&repo, 2574002, "parentNoApply", 1, 1,
        parent_flow("parentNoApply", "sub", "childNoApply", Some(1)));
    let parent_instance = start_parent(&engine, parent.id);
    let child = only_child(&repo, parent_instance.instance_id);
    assert_eq!(only_doing(&repo, child.instance_id).task_name, "second");
    let first = done_task(&repo, child.instance_id, "first");
    assert_eq!(first.actor_id.as_deref(), Some("flow.auto"));
    assert!(repo.find_task_actors(first.task_id).unwrap().contains(&"applicant".to_string()));
}

fn missing_definition_action(engine: &JeeflowEngineImpl, task_id: i64, mode: u8)
    -> JeeflowResult<Vec<ProcessTask>> {
    match mode {
        0 => engine.execute_task_sync(task_id, "applicant", &submit(SubmitType::Agree)),
        1 => engine.execute_and_jump_sync(task_id, "applicant", &submit(SubmitType::Jump), Some("end")),
        _ => engine.execute_and_jump_to_end_sync(task_id, "applicant", &submit(SubmitType::Reject)),
    }
}

#[test]
fn i257_missing_definition_preflight_is_zero_write_on_all_completion_entries() {
    for mode in 0u8..3 {
        let (engine, repo, capture) = harness();
        let parent = add_define(&repo, 2575000 + mode as i64, "parentMissing", 1, 1,
            parent_flow("parentMissing", "sub", "missing257", Some(1)));
        let parent_instance = engine.start_sync(parent.id, "applicant", &FlowData::new()).unwrap();
        let parent_apply = only_doing(&repo, parent_instance.instance_id);
        let task_count = repo.get_all_tasks().unwrap().len();
        let instance_count = repo.get_all_instances().unwrap().len();
        let event_count = capture.events.lock().unwrap().len();

        let err = missing_definition_action(&engine, parent_apply.task_id, mode).unwrap_err();
        assert!(matches!(err, JeeflowError::SubProcessDefineNotFound(_)));
        assert_eq!(err.internal_code(), Some(ERR_SUB_PROCESS_DEFINE_NOT_FOUND));
        assert_eq!(err.code(), ERR_BUSINESS, "Rust 对外仍走统一业务信封");
        assert_eq!(err.message(), "子流程missing257定义不存在");
        assert_eq!(repo.find_task_by_id(parent_apply.task_id).unwrap().unwrap().task_state,
            TaskState::Doing.code());
        assert_eq!(repo.get_all_tasks().unwrap().len(), task_count);
        assert_eq!(repo.get_all_instances().unwrap().len(), instance_count);
        assert_eq!(capture.events.lock().unwrap().len(), event_count);
        assert_eq!(repo.find_instance_by_id(parent_instance.instance_id).unwrap().unwrap().state,
            InstanceState::Doing.code());
    }
}

#[test]
fn i257_end_auto_completes_assistant_but_rejects_unfinished_major() {
    let (engine, repo, capture) = harness();
    add_define(&repo, 2576001, "childAssistant", 1, 1,
        child_flow("childAssistant", "childReview", true));
    let parent = add_define(&repo, 2576002, "parentAssistant", 1, 1,
        parent_flow("parentAssistant", "sub", "childAssistant", Some(1)));
    let parent_instance = start_parent(&engine, parent.id);
    let child = only_child(&repo, parent_instance.instance_id);
    let assistant = seed_doing_task(&repo, child.instance_id, "assistant", "helper", TaskType::Assistant);
    // 夹具里另有协办行，因此按节点名定位主办行。
    let review = repo.find_doing_tasks(child.instance_id, &["childReview".to_string()])
        .unwrap().remove(0);
    engine.execute_task_sync(review.task_id, "leader", &submit(SubmitType::Agree)).unwrap();
    let assistant_done = repo.find_task_by_id(assistant.task_id).unwrap().unwrap();
    assert_eq!(assistant_done.task_state, TaskState::Finished.code());
    assert_eq!(assistant_done.actor_id.as_deref(), Some("flow.auto"));
    assert_eq!(repo.find_instance_by_id(child.instance_id).unwrap().unwrap().state,
        InstanceState::Finished.code());
    assert_eq!(only_doing(&repo, parent_instance.instance_id).task_name, "parentAfter");
    assert_eq!(count_event(&capture, ProcessEventType::ProcessInstanceEnd, child.instance_id), 1);

    let (engine2, repo2, _capture2) = harness();
    add_define(&repo2, 2576101, "childMajor", 1, 1,
        child_flow("childMajor", "childReview", true));
    let parent2 = add_define(&repo2, 2576102, "parentMajor", 1, 1,
        parent_flow("parentMajor", "sub", "childMajor", Some(1)));
    let parent_instance2 = start_parent(&engine2, parent2.id);
    let child2 = only_child(&repo2, parent_instance2.instance_id);
    let extra_major = seed_doing_task(&repo2, child2.instance_id, "extraMajor", "leader", TaskType::Major);
    let review2 = repo2.find_doing_tasks(child2.instance_id, &["childReview".to_string()])
        .unwrap().remove(0);
    let err = engine2.execute_task_sync(review2.task_id, "leader", &submit(SubmitType::Agree))
        .unwrap_err();
    assert_eq!(err.message(), "存在未完成的主办任务,请确认.");
    assert_eq!(repo2.find_instance_by_id(child2.instance_id).unwrap().unwrap().state,
        InstanceState::Doing.code());
    assert_eq!(repo2.find_task_by_id(extra_major.task_id).unwrap().unwrap().task_state,
        TaskState::Doing.code());
    assert!(repo2.find_doing_tasks(parent_instance2.instance_id, &["parentAfter".to_string()])
        .unwrap().is_empty(), "子未合法结束不得回父");
}

#[test]
fn i257_child_reject_resumes_parent_once() {
    let (engine, repo, capture) = harness();
    add_define(&repo, 2577001, "childReject", 1, 1,
        child_flow("childReject", "childReview", true));
    let parent = add_define(&repo, 2577002, "parentReject", 1, 1,
        parent_flow("parentReject", "sub", "childReject", Some(1)));
    let parent_instance = start_parent(&engine, parent.id);
    let child = only_child(&repo, parent_instance.instance_id);
    let review = only_doing(&repo, child.instance_id);
    engine.execute_and_jump_to_end_sync(review.task_id, "leader", &submit(SubmitType::Reject))
        .unwrap();
    assert_eq!(repo.find_instance_by_id(child.instance_id).unwrap().unwrap().state,
        InstanceState::Reject.code());
    assert_eq!(only_doing(&repo, parent_instance.instance_id).task_name, "parentAfter");
    assert_eq!(count_event(&capture, ProcessEventType::ProcessInstanceEnd, child.instance_id), 1);
}

#[test]
fn i257_can_rejected_skips_subprocess_and_countersign_parent() {
    let model = ModelParser::parse(&linear("guard", &[
        ("start", "start", json!({})),
        ("apply", "task", apply()),
        ("sub", "subProcess", sub_process("child", Some(1))),
        ("after", "task", task("manager")),
        ("end", "end", json!({})),
    ])).unwrap();
    assert!(!model.can_rejected("after", "sub"));
    assert!(!model.can_rejected("after", "apply"), "驳回不得穿越子流程边界");

    let countersign = ModelParser::parse(&linear("countersign", &[
        ("start", "start", json!({})),
        ("apply", "task", apply()),
        ("cs", "task", json!({"assignee": "leader", "performType": 1})),
        ("after", "task", task("manager")),
        ("end", "end", json!({})),
    ])).unwrap();
    assert!(!countersign.can_rejected("after", "cs"), "会签父节点不可退");
    assert!(countersign.can_rejected("after", "apply"), "普通父节点回归仍可退");
}
