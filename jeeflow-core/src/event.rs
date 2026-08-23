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
    /// Process task end (code=4).
    ProcessTaskEnd = 4,
}

impl ProcessEventType {
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            1 => Some(ProcessEventType::ProcessInstanceStart),
            2 => Some(ProcessEventType::ProcessInstanceEnd),
            3 => Some(ProcessEventType::ProcessTaskStart),
            4 => Some(ProcessEventType::ProcessTaskEnd),
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
}

impl ProcessEvent {
    pub fn new(event_type: ProcessEventType, source_id: i64) -> Self {
        ProcessEvent {
            event_type,
            source_id,
            data: FlowData::new(),
        }
    }

    pub fn with_data(mut self, data: FlowData) -> Self {
        self.data = data;
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
