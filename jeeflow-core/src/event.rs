//! Event system — 4 event types (spec/concepts/04 §4.1).

use crate::json::FlowData;

/// Process event types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessEventType {
    /// Process instance start (code=1).
    ProcessInstanceStart = 1,
    /// Process instance end (code=2).
    ProcessInstanceEnd = 2,
    /// Process task start (code=3).
    ProcessTaskStart = 3,
    /// CC 知会事件（code=4，issues/102·104）。
    ///
    /// 4 号位复用：原 `ProcessTaskEnd` 引擎从不 fire（全联邦零引用的死码，spec §7
    /// 同款事实），复用于 `CcCreate` 与 Java/PHP（CC_CREATE=4）码值对齐；1/2/3 不重排。
    CcCreate = 4,
}

impl ProcessEventType {
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            1 => Some(ProcessEventType::ProcessInstanceStart),
            2 => Some(ProcessEventType::ProcessInstanceEnd),
            3 => Some(ProcessEventType::ProcessTaskStart),
            4 => Some(ProcessEventType::CcCreate),
            _ => None,
        }
    }
    pub fn code(&self) -> i32 { *self as i32 }
}

/// Process event.
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

    /// 附加抄送人 id（仅 CC_CREATE；对齐 Java `ProcessEvent.builder().ccActorId(..)`）。
    pub fn with_cc_actor_id(mut self, cc_actor_id: impl Into<String>) -> Self {
        self.cc_actor_id = Some(cc_actor_id.into());
        self
    }
}

/// Event publisher — broadcasts to all registered listeners.
pub struct ProcessPublisher;

impl ProcessPublisher {
    /// 发布事件到全部监听器。
    ///
    /// 兜底语义（issues/104 P2 统一口径）：单监听器 panic 只捕获不传播——
    /// 不得影响引擎主流程，也不得中断后续监听器（对齐 PHP per-listener catch；
    /// Rust 侧 `on_event` 无返回值、异常形态为 panic，故用 catch_unwind 兜底）。
    pub fn notify(event: &ProcessEvent, listeners: &[std::sync::Arc<dyn crate::spi::ProcessEventListener>]) {
        for listener in listeners {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| listener.on_event(event)));
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
}
