//! Event system — 事件契约的唯一权威是 `jeeflow-doc/docs/spec/11-events.md` §11.3
//! （issues/127＋132 的事件代码腿）。码值取 **A 套整型**（java 血缘 1..4 连续扩展到 1..9）。

use crate::json::{FlowData, JsonValue};

/// 规范 11 §11.3 码表：**规范名是权威**，码值只是本栈内部的附带数值。
/// 集成层跨语言判据一律用规范名（[`ProcessEventType::spec_name`]），
/// **不得拿数字码当判据**（go/node 本轮整表重排到 A 套，已发布版本里数字码不可混用，见 §11.6）。
///
/// Rust 变体名按本栈惯例用 CamelCase，与规范名（SCREAMING_SNAKE）一一对应见下表与
/// [`ProcessEventType::spec_name`]；本栈事件名与规范名的映射没有分叉
/// （`InstanceEnd` 合并"办结＋拒绝"，靠载荷 `state` 分——正是 §11.6 收口后的基准形状）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessEventType {
    /// `PROCESS_INSTANCE_START`（code=1）实例发起成功，sourceId＝instanceId。
    ProcessInstanceStart = 1,
    /// `PROCESS_INSTANCE_END`（code=2）实例进入终态（办结/拒绝共用，靠载荷 `state` 分）。
    ProcessInstanceEnd = 2,
    /// `PROCESS_TASK_START`（code=3）新待办生成（含会签逐人、回退复活行、子流程任务），sourceId＝taskId。
    ProcessTaskStart = 3,
    /// `CC_CREATE`（code=4）新增一条抄送记录，sourceId＝instanceId，**逐抄送人 fire 一次**。
    ///
    /// 4 号位复用：原 `ProcessTaskEnd` 引擎从不 fire（全联邦零引用的死码），让位给 `CcCreate`
    /// 与 Java/PHP（CC_CREATE=4）码值对齐；1/2/3 不重排（spec §11.6）。
    CcCreate = 4,
    /// `TASK_COMPLETE`（code=5）任务被办掉（同意/跳转/会签办理），sourceId＝taskId。
    ///
    /// 历史上被废弃的 `PROCESS_TASK_END` **不再复活**（spec §11.4-4）：这一支就是"任务被办掉"。
    TaskComplete = 5,
    /// `TASK_REJECT`（code=6）任务被退回/拒绝（含退发起人、软拒绝、跳转回退），sourceId＝taskId。
    ///
    /// 拒绝/跳转/退发起人**共用这一号**，靠载荷（及反查）的 `submitType` 区分（§11.2 原则 2）；
    /// 与码 5 互斥——同一动作走 reject 就不再 fire complete。
    TaskReject = 6,
    /// `TASK_TRANSFER`（code=7）转办发生（参与者被替换并落库），sourceId＝taskId。
    TaskTransfer = 7,
    /// `TASK_WITHDRAW`（code=8）撤回发生（实例进入 30），sourceId＝instanceId。
    ///
    /// **每轮撤回只 fire 一次**，不逐任务（§11.3 码 8 触发时机）。
    TaskWithdraw = 8,
    /// `INSTANCE_TERMINATED`（code=9）实例被终止（40），sourceId＝instanceId。
    ///
    /// ⚠️ 本栈门面**没有"终止实例"的 action**（issues/134 §5.2 同记），故当前无引擎内触发点；
    /// 号位与规范名先按 §11.3 占齐，终止 action 落地时接这根腿
    /// ——严禁集成层"主动补发"代替（§11.1）。
    InstanceTerminated = 9,
    // 10+ **预留**（超时催办 / 超时自动通过 …）：本轮不发，仅占号防分叉（§11.3 末行＋§11.4-1，
    // 八栈都没有时钟扫描器，发了没有触发源）。
}

impl ProcessEventType {
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            1 => Some(ProcessEventType::ProcessInstanceStart),
            2 => Some(ProcessEventType::ProcessInstanceEnd),
            3 => Some(ProcessEventType::ProcessTaskStart),
            4 => Some(ProcessEventType::CcCreate),
            5 => Some(ProcessEventType::TaskComplete),
            6 => Some(ProcessEventType::TaskReject),
            7 => Some(ProcessEventType::TaskTransfer),
            8 => Some(ProcessEventType::TaskWithdraw),
            9 => Some(ProcessEventType::InstanceTerminated),
            _ => None,
        }
    }
    pub fn code(&self) -> i32 { *self as i32 }

    /// 规范 11 §11.3 的**规范名**（跨语言判据）。逐字对齐 spec 码表，改动即破契约。
    pub fn spec_name(&self) -> &'static str {
        match self {
            ProcessEventType::ProcessInstanceStart => "PROCESS_INSTANCE_START",
            ProcessEventType::ProcessInstanceEnd => "PROCESS_INSTANCE_END",
            ProcessEventType::ProcessTaskStart => "PROCESS_TASK_START",
            ProcessEventType::CcCreate => "CC_CREATE",
            ProcessEventType::TaskComplete => "TASK_COMPLETE",
            ProcessEventType::TaskReject => "TASK_REJECT",
            ProcessEventType::TaskTransfer => "TASK_TRANSFER",
            ProcessEventType::TaskWithdraw => "TASK_WITHDRAW",
            ProcessEventType::InstanceTerminated => "INSTANCE_TERMINATED",
        }
    }
}

impl std::fmt::Display for ProcessEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.spec_name())
    }
}

/// Process event — `eventType`（规范名见 `ProcessEventType::spec_name`）＋ `sourceId` ＋ 载荷 `data`。
///
/// 载荷键名一律 camelCase，必备键见 spec §11.3「直传载荷键」列；
/// 直传键之外的字段允许监听器反查仓储得到（任务名/流程名/发起人）。
#[derive(Debug, Clone)]
pub struct ProcessEvent {
    pub event_type: ProcessEventType,
    pub source_id: i64,
    pub data: FlowData,
    /// 抄送人 id（仅 [`ProcessEventType::CcCreate`] 用，issues/102·104）。
    ///
    /// 直传事件体，集成层监听器免反查 cc 表；非 CC_CREATE 事件恒 `None`（向后兼容）。
    pub cc_actor_id: Option<String>,
}

impl ProcessEvent {
    pub fn new(event_type: ProcessEventType, source_id: i64) -> Self {
        ProcessEvent {
            event_type,
            source_id,
            data: FlowData::new(),
            cc_actor_id: None,
        }
    }

    pub fn with_data(mut self, data: FlowData) -> Self {
        self.data = data;
        self
    }

    /// 逐键附加载荷（`data.insert(key, value)` 的链式形状，便于调用点按 §11.3 键表写全）。
    pub fn with_pair(mut self, key: impl Into<String>, value: JsonValue) -> Self {
        let key = key.into();
        self.data.insert(key, value);
        self
    }

    /// 附加抄送人 id（仅 CC_CREATE；对齐 Java `ProcessEvent.builder().ccActorId(..)`）。
    /// 同时镜像进载荷 `ccActorId`——§11.3 码 4 的「直传载荷键」列钉的就是这个键名。
    pub fn with_cc_actor_id(mut self, cc_actor_id: impl Into<String>) -> Self {
        let cc_actor_id = cc_actor_id.into();
        self.data.insert_str("ccActorId", cc_actor_id.clone());
        self.cc_actor_id = Some(cc_actor_id);
        self
    }

    /// `"<规范名>/<sourceId>"` 便于日志与 recorder 断言（判据仍用 [`Self::event_type`]）。
    pub fn spec_label(&self) -> String {
        format!("{}/{}", self.event_type.spec_name(), self.source_id)
    }
}

/// Event publisher — broadcasts to all registered listeners.
pub struct ProcessPublisher;

impl ProcessPublisher {
    /// 发布事件到全部监听器。
    ///
    /// 兜底语义（issues/104 P2 统一口径 ＋ 规范 11 §11.5「异常隔离」条）：**逐监听器** catch，
    /// 单监听器 panic 只记日志——① 不回滚主流程，② 不中断后续监听器（对齐 PHP per-listener
    /// catch；Rust 侧 `on_event` 无返回值、异常形态为 panic，故用 catch_unwind 兜底，
    /// 错误**不经 `?` 传播**）。零注册时循环不执行，安全返回。
    pub fn notify(event: &ProcessEvent, listeners: &[std::sync::Arc<dyn crate::spi::ProcessEventListener>]) {
        for (index, listener) in listeners.iter().enumerate() {
            if let Err(err) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| listener.on_event(event))) {
                let detail = err
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| err.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "<non-string panic>".to_string());
                eprintln!(
                    "[jeeflow] 事件监听器异常已隔离 event={} listenerIndex={} err={}（不回滚主流程、不中断后续监听器）",
                    event.spec_label(), index, detail
                );
            }
        }
    }
}

#[cfg(test)]
mod publisher_tests {
    use super::*;
    use crate::spi::ProcessEventListener;
    use std::sync::Arc;

    struct Panicky;
    impl ProcessEventListener for Panicky {
        fn on_event(&self, _event: &ProcessEvent) { panic!("boom"); }
    }

    struct Recorder(std::sync::Mutex<Vec<i64>>);
    impl ProcessEventListener for Recorder {
        fn on_event(&self, event: &ProcessEvent) {
            self.0.lock().unwrap().push(event.source_id);
        }
    }

    /// 兜底语义（issues/104 P2）：单监听器 panic 不传播、不中断后续监听器。
    #[test]
    fn test_publisher_listener_panic_isolated() {
        let recorder = Arc::new(Recorder(std::sync::Mutex::new(Vec::new())));
        let listeners: Vec<std::sync::Arc<dyn ProcessEventListener>> =
            vec![Arc::new(Panicky), recorder.clone()];
        let event = ProcessEvent::new(ProcessEventType::ProcessInstanceStart, 42);
        ProcessPublisher::notify(&event, &listeners);
        assert_eq!(*recorder.0.lock().unwrap(), vec![42], "panic 后后续监听器应仍被调用");
    }

    /// 规范 11 §11.3 码表自证：码值 A 套 1..9、规范名逐字对齐、`from_code` 全覆盖，
    /// 10+ 是预留号（本轮不发，`from_code` 必须拒）。
    #[test]
    fn test_event_type_code_table_matches_spec() {
        let table: [(ProcessEventType, i32, &str); 9] = [
            (ProcessEventType::ProcessInstanceStart, 1, "PROCESS_INSTANCE_START"),
            (ProcessEventType::ProcessInstanceEnd, 2, "PROCESS_INSTANCE_END"),
            (ProcessEventType::ProcessTaskStart, 3, "PROCESS_TASK_START"),
            (ProcessEventType::CcCreate, 4, "CC_CREATE"),
            (ProcessEventType::TaskComplete, 5, "TASK_COMPLETE"),
            (ProcessEventType::TaskReject, 6, "TASK_REJECT"),
            (ProcessEventType::TaskTransfer, 7, "TASK_TRANSFER"),
            (ProcessEventType::TaskWithdraw, 8, "TASK_WITHDRAW"),
            (ProcessEventType::InstanceTerminated, 9, "INSTANCE_TERMINATED"),
        ];
        for (variant, code, name) in table {
            assert_eq!(variant.code(), code, "码值须是 A 套 1..9：{:?}", variant);
            assert_eq!(variant.spec_name(), name, "规范名逐字对齐 spec §11.3");
            assert_eq!(variant.to_string(), name, "Display 即规范名（集成层判据用名不用码）");
            assert_eq!(ProcessEventType::from_code(code), Some(variant), "from_code 反查须一致");
        }
        // 10+ 预留 ⇒ 未发号不得被认成有效码（发了就是分叉）
        for reserved in [0, 10, 11, 99, i32::MIN, i32::MAX] {
            assert_eq!(ProcessEventType::from_code(reserved), None, "{} 号位本轮不发", reserved);
        }
    }

    /// CC_CREATE 载荷键（§11.3 码 4「直传载荷键＝ccActorId」）：字段与 data 双份同源。
    #[test]
    fn test_cc_actor_id_mirrored_into_payload() {
        let event = ProcessEvent::new(ProcessEventType::CcCreate, 7).with_cc_actor_id("u9");
        assert_eq!(event.cc_actor_id.as_deref(), Some("u9"));
        assert_eq!(event.data.get_str("ccActorId"), Some("u9"));
    }

    /// 订阅形状（§11.5 ＋ 08 场景 36）：**一次 fire 送给全部已注册监听器**，注册顺序＝回调顺序。
    /// 单回调实现（python 旧状）在此即红——两个监听器同码注册必须都被调到。
    #[test]
    fn test_all_listeners_called_in_registration_order() {
        struct Named(std::sync::Mutex<Vec<String>>);
        impl ProcessEventListener for Named {
            fn on_event(&self, event: &ProcessEvent) {
                self.0.lock().unwrap().push(event.spec_label());
            }
        }
        let a = Arc::new(Named(std::sync::Mutex::new(Vec::new())));
        let b = Arc::new(Named(std::sync::Mutex::new(Vec::new())));
        let listeners: Vec<Arc<dyn ProcessEventListener>> = vec![a.clone(), b.clone()];
        for t in [ProcessEventType::ProcessInstanceStart, ProcessEventType::TaskComplete] {
            ProcessPublisher::notify(&ProcessEvent::new(t, 1), &listeners);
        }
        let want = vec!["PROCESS_INSTANCE_START/1".to_string(), "TASK_COMPLETE/1".to_string()];
        assert_eq!(*a.0.lock().unwrap(), want, "先注册的监听器须收到全部事件");
        assert_eq!(*b.0.lock().unwrap(), want, "后注册的监听器不得覆盖先注册的（§11.5 回调条）");
    }

    /// 零注册时 fire 安全返回（§11.5「无监听器」条：不得 panic）。
    #[test]
    fn test_notify_without_listener_is_safe() {
        let listeners: Vec<Arc<dyn ProcessEventListener>> = Vec::new();
        ProcessPublisher::notify(&ProcessEvent::new(ProcessEventType::TaskWithdraw, 3), &listeners);
    }
}
