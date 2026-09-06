//! T003: 业务数据种子 driver —— 引擎真实启动（startAndExecute + execute），不直插 repo。
//! 矩阵 = 八语言共用 canonical 矩阵（day-shift 已在 Rust demo 实测全绿）：
//! 16 进行中(state=10) + 9 已完成(advance 推到 state=20) + 8 委托。
//! 8 用户 × 5 菜单（待办/已办/发起/抄送/委托）全覆盖（每格 ≥1）。

use jeeflow_facade::JeeflowFacade;
use serde_json::{json, Value as Json};
use std::collections::HashMap;

/// (defineId, operator, extraVars, 抄送 actorIds)
type Row = (i64, &'static str, Json, Vec<&'static str>);

/// 进行中 16 条：发起后停在 state=10（I3/I15 冻结在决策/驳回前，I14 发起后再办两节点停 boss）
fn in_progress() -> &'static [Row] {
    use std::sync::LazyLock;
    static ROWS: LazyLock<Vec<Row>> = LazyLock::new(|| {
        vec![
            (1, "user1", json!({}), vec!["userA", "userB"]),           // I1
            (2, "user1", json!({}), vec![]),                           // I2
            (3, "userA", json!({"amount": 500}), vec![]),              // I3 冻结 task1/leader（决策前）
            (4, "manager", json!({}), vec!["userC", "leader"]),        // I4
            (5, "userB", json!({}), vec![]),                           // I5
            (6, "director", json!({}), vec!["manager", "boss"]),       // I6
            (7, "userC", json!({}), vec!["user1"]),                    // I7
            (1, "boss", json!({}), vec![]),                            // I8 boss「发起」来源
            (12, "user1", json!({"deptLeader": "manager"}), vec![]),   // I9
            (12, "userC", json!({"deptLeader": "director"}), vec![]),  // I10
            (12, "userB", json!({"deptLeader": "user1"}), vec![]),     // I11 user1/张三「待办」来源
            (15, "userA", json!({}), vec!["boss"]),                    // I12
            (14, "leader", json!({}), vec!["director", "userC"]),      // I13
            (2, "userA", json!({}), vec![]),                           // I14 发起后再办 leader/manager → 停 boss
            (10, "userB", json!({}), vec![]),                          // I15 冻结 task1/leader（驳回前）
            (8, "user1", json!({}), vec![]),                           // I16
        ]
    });
    &ROWS
}

/// 已完成 9 条：advance() 推到 state=20（分支无关）
fn finished() -> &'static [Row] {
    use std::sync::LazyLock;
    static ROWS: LazyLock<Vec<Row>> = LazyLock::new(|| {
        vec![
            (1, "userA", json!({}), vec!["user1", "director"]),        // F1
            (8, "userB", json!({}), vec!["boss", "manager"]),          // F2
            (2, "manager", json!({}), vec!["boss"]),                   // F3
            (10, "director", json!({}), vec![]),                       // F4
            (12, "userC", json!({"deptLeader": "leader"}), vec![]),    // F5
            (1, "director", json!({}), vec![]),                        // F6
            (5, "manager", json!({}), vec![]),                         // F7
            (12, "userA", json!({"deptLeader": "director"}), vec![]),  // F8
            (12, "userB", json!({"deptLeader": "user1"}), vec![]),     // F9
        ]
    });
    &ROWS
}

/// 委托 8 条：processSurrogate/page 无 operator 过滤 → 8 用户委托菜单全非空
const SURROGATES: &[(&str, &str)] = &[
    ("user1", "userA"),
    ("userA", "userB"),
    ("userB", "userC"),
    ("userC", "leader"),
    ("leader", "manager"),
    ("manager", "director"),
    ("director", "boss"),
    ("boss", "user1"),
];

fn to_args(v: Json) -> HashMap<String, Json> {
    match v.as_object() {
        Some(map) => map.clone().into_iter().collect(),
        None => HashMap::new(),
    }
}

async fn flow(facade: &JeeflowFacade, action: &str, args: Json) -> Json {
    facade.flow(action, &to_args(args)).await
}

fn code_of(resp: &Json) -> i64 {
    resp.get("code").and_then(|v| v.as_i64()).unwrap_or(-1)
}

fn as_i64(v: &Json) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

fn json_to_string(v: &Json) -> String {
    match v {
        Json::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// startAndExecute 返回键 = data.processInstanceId（8 语言一致；id 可能被 stringify）
async fn start_instance(facade: &JeeflowFacade, define_id: i64, op: &str, extra: &Json) -> Option<i64> {
    let mut args = json!({"processDefineId": define_id, "operator": op});
    if let (Some(dst), Some(src)) = (args.as_object_mut(), extra.as_object()) {
        for (k, v) in src {
            dst.insert(k.clone(), v.clone());
        }
    }
    let resp = flow(facade, "processDefine/startAndExecute", args).await;
    if code_of(&resp) != 0 {
        eprintln!("[seed] startAndExecute define={} op={} FAILED: {}", define_id, op, resp);
        return None;
    }
    resp.get("data")
        .and_then(|d| d.get("processInstanceId"))
        .and_then(as_i64)
}

async fn add_cc(facade: &JeeflowFacade, iid: i64, op: &str, actors: &[&str]) {
    let resp = flow(
        facade,
        "processInstance/createCCInstance",
        json!({"processInstanceId": iid, "operator": op, "actorIds": actors}),
    )
    .await;
    if code_of(&resp) != 0 {
        eprintln!("[seed] createCCInstance iid={} FAILED: {}", iid, resp);
    }
}

/// advance 原语：循环读 detail，对每个 doing 任务以其自身 actor execute(submitType=1)，直到 state != 10。
/// doing 任务 operator 为 null，actor 取 taskActorIdList[0]。
async fn advance(facade: &JeeflowFacade, iid: i64) -> i64 {
    for _ in 0..30 {
        let d = flow(facade, "processInstance/detail", json!({"id": iid})).await;
        let state = d.pointer("/data/state").and_then(as_i64).unwrap_or(0);
        if state != 10 {
            return state;
        }
        let doing: Vec<Json> = d
            .pointer("/data/tasks")
            .and_then(|t| t.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|t| t.get("taskState").and_then(as_i64) == Some(10))
            .collect();
        if doing.is_empty() {
            return state;
        }
        let mut progress = false;
        for t in doing {
            let actor = t
                .get("operator")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .or_else(|| {
                    t.get("taskActorIdList")
                        .and_then(|v| v.as_array())
                        .and_then(|a| a.first())
                        .and_then(|v| v.as_str())
                });
            let Some(actor) = actor else { continue };
            let Some(tid) = t.get("id").and_then(as_i64) else { continue };
            let r = flow(
                facade,
                "processTask/execute",
                json!({"processTaskId": tid, "operator": actor, "submitType": 1}),
            )
            .await;
            if code_of(&r) == 0 {
                progress = true;
            } else {
                eprintln!("[seed] advance execute iid={} task={} actor={} FAILED: {}", iid, tid, actor, r);
            }
        }
        if !progress {
            return state;
        }
    }
    let d = flow(facade, "processInstance/detail", json!({"id": iid})).await;
    d.pointer("/data/state").and_then(as_i64).unwrap_or(0)
}

/// 仅 I14 用：在该实例里找 op 的 doing 任务行（todoList 行 processInstanceId 是字符串化 id）
async fn todo_row(facade: &JeeflowFacade, op: &str, iid: i64) -> Option<Json> {
    let r = flow(
        facade,
        "processTask/todoList",
        json!({"operator": op, "pageNum": 1, "pageSize": 200}),
    )
    .await;
    let rows = r.pointer("/data/rows")?.as_array()?;
    rows.iter()
        .find(|row| {
            row.get("processInstanceId").map(json_to_string).as_deref() == Some(iid.to_string().as_str())
                && row.get("taskState").and_then(as_i64) == Some(10)
        })
        .cloned()
}

/// 种业务数据：16 进行中 + 9 已完成 + 8 委托。失败逐条打日志不 panic（demo 启动不被单条卡死）。
pub async fn seed_business(facade: &JeeflowFacade) {
    println!("[seed_business] start: 16 in-progress + 9 finished + 8 surrogates");
    let mut ok_in = 0usize;
    for (idx, (define_id, op, extra, cc)) in in_progress().iter().enumerate() {
        let Some(iid) = start_instance(facade, *define_id, op, extra).await else {
            continue;
        };
        // I14：发起后再办 leader、manager 两节点 → 停在 boss
        if *define_id == 2 && *op == "userA" {
            for actor in ["leader", "manager"] {
                if let Some(row) = todo_row(facade, actor, iid).await {
                    let Some(tid) = row.get("id").and_then(as_i64) else { continue };
                    let r = flow(
                        facade,
                        "processTask/execute",
                        json!({"processTaskId": tid, "operator": actor, "submitType": 1}),
                    )
                    .await;
                    if code_of(&r) != 0 {
                        eprintln!("[seed] I14 execute {} FAILED: {}", actor, r);
                    }
                } else {
                    eprintln!("[seed] I14 todo_row {} iid={} not found", actor, iid);
                }
            }
        }
        if !cc.is_empty() {
            add_cc(facade, iid, op, cc).await;
        }
        ok_in += 1;
        let _ = idx;
    }

    let mut ok_fin = 0usize;
    for (define_id, op, extra, cc) in finished().iter() {
        let Some(iid) = start_instance(facade, *define_id, op, extra).await else {
            continue;
        };
        let state = advance(facade, iid).await;
        if state != 20 {
            eprintln!("[seed] FIN define={} op={} iid={} ended state={} (expected 20)", define_id, op, iid, state);
        }
        if !cc.is_empty() {
            add_cc(facade, iid, op, cc).await;
        }
        ok_fin += 1;
    }

    let mut ok_surr = 0usize;
    for (op, surrogate) in SURROGATES.iter() {
        let r = flow(
            facade,
            "processSurrogate/save",
            json!({
                "operator": op,
                "surrogate": surrogate,
                "processName": "",
                "startTime": "2026-01-01 00:00:00",
                "endTime": "2027-12-31 23:59:59"
            }),
        )
        .await;
        if code_of(&r) == 0 {
            ok_surr += 1;
        } else {
            eprintln!("[seed] surrogate {}->{} FAILED: {}", op, surrogate, r);
        }
    }

    println!(
        "[seed_business] done: in-progress {}/16, finished {}/9, surrogates {}/8",
        ok_in, ok_fin, ok_surr
    );
}
