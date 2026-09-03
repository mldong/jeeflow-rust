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
    pub fn notify(event: &ProcessEvent, listeners: &[std::sync::Arc<dyn crate::spi::ProcessEventListener>]) {
        for listener in listeners {
            listener.on_event(event);
        }
    }
}
