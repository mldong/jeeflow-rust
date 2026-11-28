//! issues/259：任务节点关联岗位 / 表单指定岗位的公开 API 与运行时注册契约。

use jeeflow_core::interceptor::register_builtin_assignment_handlers;
use jeeflow_core::metadata::HandlerRegistry;
use jeeflow_core::parser::{NodeModel, NodeType, ProcessModel};
use jeeflow_core::reapply::ReapplyMode;
use jeeflow_core::{
    AssignmentHandler, Execution, FlowData, FormPostAssignmentHandler, JeeflowError, JeeflowResult,
    JsonValue, PostUserProvider, ProcessDefine, ProcessInstance, ServiceContext,
    TaskPostAssigneeHandler,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const TASK_POST: &str =
    "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$TaskPostAssigneeHandler";
const FORM_POST: &str = "com.mldong.jeeflow.interceptor.impl.FormPostAssignmentHandler";
const FORM_FIELD: &str = "com.mldong.jeeflow.interceptor.impl.FormFieldAssigneeHandler";
const TASK_ROLE: &str =
    "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$TaskRoleAssigneeHandler";

#[derive(Default)]
struct RecordingPostProvider {
    members: HashMap<String, Vec<String>>,
    calls: Mutex<Vec<String>>,
}

impl PostUserProvider for RecordingPostProvider {
    fn find_by_post(&self, post_code: &str) -> JeeflowResult<Vec<String>> {
        self.calls.lock().unwrap().push(post_code.to_string());
        if post_code == "err" {
            return Err(JeeflowError::Internal("post lookup failed".into()));
        }
        Ok(self.members.get(post_code).cloned().unwrap_or_default())
    }
}

fn provider() -> Arc<RecordingPostProvider> {
    let mut members = HashMap::new();
    members.insert("postA".into(), vec!["userA".into(), "shared".into()]);
    members.insert("postB".into(), vec!["userB".into(), "shared".into()]);
    members.insert("postC".into(), vec!["userC1".into(), "userC2".into()]);
    members.insert("ordered".into(), vec!["userC2".into(), "userC1".into()]);
    members.insert("0".into(), vec!["zeroUser".into()]);
    members.insert("empty".into(), vec![]);
    members.insert("allBlank".into(), vec!["".into(), "  ".into()]);
    members.insert(
        "dirty".into(),
        vec![
            " userA ".into(),
            "".into(),
            "shared".into(),
            "userA".into(),
            "  ".into(),
            "0".into(),
        ],
    );
    Arc::new(RecordingPostProvider {
        members,
        calls: Mutex::new(Vec::new()),
    })
}

fn context(post: Option<Arc<RecordingPostProvider>>) -> ServiceContext {
    let mut ctx = ServiceContext::new();
    if let Some(post) = post {
        ctx = ctx.with_post_user_provider(post);
    }
    register_builtin_assignment_handlers(&mut ctx);
    ctx
}

fn execution(node_id: Option<&str>, args: FlowData) -> Execution {
    let define = ProcessDefine {
        id: 1,
        name: "i259".into(),
        display_name: "i259".into(),
        define_type: "approval".into(),
        state: 1,
        content: b"{}".to_vec(),
        version: 1,
        create_time: None,
        create_user: None,
        update_time: None,
        update_user: None,
    };
    let instance = ProcessInstance::create(&define, "clerk", &FlowData::new());
    let model = ProcessModel {
        name: "i259".into(),
        display_name: "i259".into(),
        model_type: "approval".into(),
        expire_time: None,
        persist_mode: None,
        rel_table_name: None,
        nodes: vec![],
        edges: vec![],
        ..Default::default()
    };
    let mut exec = Execution::new(instance, model, define, "clerk", args);
    if let Some(node_id) = node_id {
        exec.current_node = Some(NodeModel {
            id: node_id.into(),
            node_type: NodeType::Task,
            display_name: node_id.into(),
            properties: HashMap::new(),
            reapply_mode: ReapplyMode::Normal,
            allow_add_sign: false,
            allow_recall: false,
            dynamic_countersign: false,
            add_sign_max_rounds: 0,
        });
    }
    exec
}

fn args(pairs: &[(&str, &str)]) -> FlowData {
    let mut data = FlowData::new();
    for (key, value) in pairs {
        data.insert_str(*key, *value);
    }
    data
}

fn arg_value(key: &str, value: JsonValue) -> FlowData {
    let mut data = FlowData::new();
    data.insert(key.to_string(), value);
    data
}

fn assign_task(ctx: &ServiceContext, node_id: Option<&str>, data: FlowData) -> String {
    ctx.find_assignment_handler(TASK_POST)
        .expect("TaskPost 必须已注册")
        .assign(&execution(node_id, data))
        .unwrap()
}

fn assign_form(ctx: &ServiceContext, node_id: Option<&str>, data: FlowData) -> String {
    ctx.find_assignment_handler(FORM_POST)
        .expect("FormPost 必须已注册")
        .assign(&execution(node_id, data))
        .unwrap()
}

fn calls(provider: &Arc<RecordingPostProvider>) -> Vec<String> {
    provider.calls.lock().unwrap().clone()
}

fn assert_handler_type<T: AssignmentHandler>() {}

#[test]
fn public_exports_and_runtime_registration_names_resolve() {
    assert_handler_type::<TaskPostAssigneeHandler>();
    assert_handler_type::<FormPostAssignmentHandler>();

    let ctx = context(Some(provider()));
    assert_eq!(ctx.assignment_handlers.len(), 13);
    assert!(TASK_POST.contains('$'));
    assert!(!FORM_POST.contains('$'));
    assert_eq!(assign_task(&ctx, Some("postA"), FlowData::new()), "userA,shared");
    assert_eq!(
        assign_form(&ctx, Some("posts"), args(&[("posts", "postA")])),
        "userA,shared"
    );
}

#[test]
fn task_post_uses_node_code_and_preserves_provider_order() {
    let post = provider();
    let ctx = context(Some(post.clone()));
    assert_eq!(assign_task(&ctx, Some("ordered"), FlowData::new()), "userC2,userC1");
    assert_eq!(assign_task(&ctx, Some("postC"), FlowData::new()), "userC1,userC2");
    assert_eq!(assign_task(&ctx, Some("0"), FlowData::new()), "zeroUser");
    assert_eq!(calls(&post), vec!["ordered", "postC", "0"]);
}

#[test]
fn task_post_empty_missing_error_and_no_provider_are_empty() {
    let post = provider();
    let ctx = context(Some(post.clone()));
    for node_id in [None, Some(""), Some("ghost"), Some("empty"), Some("err")] {
        assert_eq!(assign_task(&ctx, node_id, FlowData::new()), "");
    }
    assert_eq!(calls(&post), vec!["ghost", "empty", "err"]);

    let without_spi = context(None);
    assert_eq!(assign_task(&without_spi, Some("postA"), FlowData::new()), "");
}

#[test]
fn form_post_four_tier_lookup_and_priority() {
    let ctx = context(Some(provider()));
    let cases = [
        ("f_ 优先", "posts", args(&[("f_posts", "postA"), ("posts", "postB")])),
        ("裸名回落", "posts", args(&[("posts", "postB")])),
        ("# 右半裸名优先", "manager#postCodes", args(&[("postCodes", "postA"), ("f_postCodes", "postB")])),
        ("# 右半 f_ 回落", "manager#postCodes", args(&[("f_postCodes", "postB")])),
        ("_NN f_ 优先", "posts_01", args(&[("f_posts", "postA"), ("posts", "postB")])),
        ("_NN 裸名回落", "posts_01", args(&[("posts", "postA")])),
    ];
    for (label, node_id, data) in cases {
        let expected = if label == "裸名回落" || label == "# 右半 f_ 回落" {
            "userB,shared"
        } else {
            "userA,shared"
        };
        assert_eq!(assign_form(&ctx, Some(node_id), data), expected, "{}", label);
    }
}

#[test]
fn form_post_hash_uses_last_separator_and_lazy_suffix_base() {
    let ctx = context(Some(provider()));
    assert_eq!(
        assign_form(&ctx, Some("a#b#postCodes"), args(&[("postCodes", "postA")])),
        "userA,shared"
    );
    assert_eq!(
        assign_form(&ctx, Some("a_b_01"), args(&[("a_b", "postA"), ("a", "postB")])),
        "userA,shared"
    );
    assert_eq!(assign_form(&ctx, Some("_01"), args(&[("f_", "postA")])), "");
    assert_eq!(
        assign_form(&ctx, Some("posts_01x"), args(&[("posts", "postA")])),
        ""
    );
}

#[test]
fn form_post_key_existence_blocks_lower_priority_fallback() {
    let post = provider();
    let ctx = context(Some(post.clone()));
    for value in [JsonValue::Null, JsonValue::Str(String::new())] {
        let mut data = arg_value("f_posts", value);
        data.insert_str("posts", "postB");
        assert_eq!(assign_form(&ctx, Some("posts"), data), "");
    }
    let mut data = arg_value("manager#postCodes", JsonValue::Null);
    data.insert_str("postCodes", "postA");
    assert_eq!(assign_form(&ctx, Some("manager#postCodes"), data), "");
    assert!(calls(&post).is_empty(), "null/空键命中后不得回退或查询低档");
}

#[test]
fn form_post_comma_string_trims_drops_empty_and_keeps_order() {
    let ctx = context(Some(provider()));
    assert_eq!(
        assign_form(&ctx, Some("posts"), args(&[("posts", " postA, ,postB,, ")])),
        "userA,shared,userB"
    );
}

#[test]
fn form_post_array_is_element_wise_without_second_split() {
    let post = provider();
    let ctx = context(Some(post.clone()));
    let data = arg_value(
        "posts",
        JsonValue::Array(vec![
            JsonValue::Str(" postA ".into()),
            JsonValue::Str("postB".into()),
        ]),
    );
    assert_eq!(assign_form(&ctx, Some("posts"), data), "userA,shared,userB");

    let data = arg_value(
        "posts",
        JsonValue::Array(vec![JsonValue::Str("postA,postB".into())]),
    );
    assert_eq!(assign_form(&ctx, Some("posts"), data), "");
    assert_eq!(calls(&post), vec!["postA", "postB", "postA,postB"]);
}

#[test]
fn form_post_dedupes_posts_before_lookup_and_users_after_merge() {
    let post = provider();
    let ctx = context(Some(post.clone()));
    assert_eq!(
        assign_form(&ctx, Some("posts"), args(&[("posts", "postA,postB,postA")])),
        "userA,shared,userB"
    );
    assert_eq!(calls(&post), vec!["postA", "postB"]);
}

#[test]
fn form_post_zero_string_and_number_are_legal_codes() {
    let ctx = context(Some(provider()));
    for value in [JsonValue::Str("0".into()), JsonValue::Number(0.0)] {
        assert_eq!(assign_form(&ctx, Some("posts"), arg_value("posts", value)), "zeroUser");
    }
    let data = arg_value(
        "posts",
        JsonValue::Array(vec![
            JsonValue::Str("postA".into()),
            JsonValue::Number(0.0),
            JsonValue::Str("postB".into()),
        ]),
    );
    assert_eq!(assign_form(&ctx, Some("posts"), data), "userA,shared,zeroUser,userB");
}

#[test]
fn form_post_single_post_error_is_isolated() {
    let post = provider();
    let ctx = context(Some(post.clone()));
    assert_eq!(
        assign_form(&ctx, Some("posts"), args(&[("posts", "err,postB")])),
        "userB,shared"
    );
    assert_eq!(assign_form(&ctx, Some("posts"), args(&[("posts", "err")])), "");
    assert_eq!(calls(&post), vec!["err", "postB", "err"]);
}

#[test]
fn form_post_user_ids_are_trimmed_deduped_and_keep_zero() {
    let ctx = context(Some(provider()));
    assert_eq!(
        assign_form(&ctx, Some("posts"), args(&[("posts", "dirty")])),
        "userA,shared,0"
    );
    assert_eq!(
        assign_form(&ctx, Some("posts"), args(&[("posts", "allBlank")])),
        ""
    );
}

#[test]
fn form_post_missing_empty_null_and_non_scalar_values_are_empty() {
    let post = provider();
    let ctx = context(Some(post.clone()));
    let cases = vec![
        FlowData::new(),
        args(&[("f_posts", "")]),
        args(&[("f_posts", " , , ")]),
        arg_value("f_posts", JsonValue::Null),
        arg_value("f_posts", JsonValue::Array(vec![])),
        arg_value(
            "f_posts",
            JsonValue::Array(vec![JsonValue::Null, JsonValue::Str("   ".into())]),
        ),
        arg_value("f_posts", JsonValue::Object(vec![])),
        arg_value(
            "f_posts",
            JsonValue::Array(vec![JsonValue::Array(vec![JsonValue::Str("postA".into())])]),
        ),
    ];
    for data in cases {
        assert_eq!(assign_form(&ctx, Some("posts"), data), "");
    }
    assert!(calls(&post).is_empty());
}

#[test]
fn form_post_no_provider_no_node_or_empty_node_is_empty() {
    let data = args(&[("f_posts", "postA")]);
    assert_eq!(assign_form(&context(None), Some("posts"), data.clone()), "");

    let post = provider();
    let ctx = context(Some(post.clone()));
    for node_id in [None, Some("")] {
        assert_eq!(assign_form(&ctx, node_id, data.clone()), "");
    }
    assert!(calls(&post).is_empty());
}

#[test]
fn metadata_count_names_and_order_match_java() {
    let registry = HandlerRegistry::new();
    let handlers = registry.list_handlers("AssignmentHandler");
    assert_eq!(handlers.len(), 13);
    let orders: Vec<i32> = handlers.iter().map(|handler| handler.order).collect();
    assert_eq!(orders, vec![-9999, 10, 20, 30, 40, 50, 60, 65, 70, 80, 90, 100, 110]);

    let task_post = handlers.iter().find(|handler| handler.class_name == TASK_POST).unwrap();
    assert_eq!(task_post.display_name, "根据任务节点唯一编码关联岗位分配参与者");
    assert_eq!(task_post.order, 65);
    assert_eq!(task_post.group, "built-in");

    let form_post = handlers.iter().find(|handler| handler.class_name == FORM_POST).unwrap();
    assert_eq!(form_post.display_name, "表单指定岗位分配参与者");
    assert_eq!(form_post.order, 110);
    assert_eq!(form_post.group, "built-in");
}

#[test]
fn existing_handlers_and_metadata_are_unchanged() {
    let ctx = context(Some(provider()));
    for name in [
        "com.mldong.jeeflow.interceptor.impl.OperatorAssignmentHandler",
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$ApplicantDeptLeaderAssignmentHandler",
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$ApplicantDeptMainLeaderAssignmentHandler",
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$DeptLeaderAssignmentHandler",
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$DeptMainLeaderAssignmentHandler",
        FORM_FIELD,
        TASK_ROLE,
        "com.mldong.jeeflow.interceptor.impl.FormUserDeptLeaderAssignmentHandler",
        "com.mldong.jeeflow.interceptor.impl.FormUserDeptMainLeaderAssignmentHandler",
        "com.mldong.jeeflow.interceptor.impl.FormDeptLeaderAssignmentHandler",
        "com.mldong.jeeflow.interceptor.impl.FormDeptMainLeaderAssignmentHandler",
    ] {
        assert!(ctx.find_assignment_handler(name).is_some(), "旧 handler 不得丢失：{}", name);
    }

    // 旧 FormField 仍只认自己的字段查找，不接收 # 右半；TaskRole 不偷用 Post Provider。
    let legacy = ctx.find_assignment_handler(FORM_FIELD).unwrap();
    assert_eq!(
        legacy.assign(&execution(Some("manager#userId"), args(&[("f_userId", "userA")]))).unwrap(),
        ""
    );
    assert_eq!(assign_task(&ctx, Some("postA"), FlowData::new()), "userA,shared");
    assert_eq!(
        ctx.find_assignment_handler(TASK_ROLE)
            .unwrap()
            .assign(&execution(Some("postA"), FlowData::new()))
            .unwrap(),
        ""
    );
}
