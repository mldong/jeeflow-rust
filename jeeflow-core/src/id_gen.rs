//! Default ID generator — snowflake-like algorithm.
//! Reference: Java SnowflakeIdGenerator.
//! Engine core provides this as default; integrations can inject their own via IIdGenerator SPI.

use crate::spi::IdGenerator;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

/// Default snowflake ID generator.
/// Structure: 1 bit sign + 41 bits timestamp + 10 bits worker + 12 bits sequence.
pub struct DefaultIdGenerator {
    worker_id: i64,
    state: Mutex<SnowflakeState>,
}

struct SnowflakeState {
    last_timestamp: i64,
    sequence: i64,
}

const EPOCH: i64 = 1577836800000;
const WORKER_ID_BITS: i64 = 10;
const SEQUENCE_BITS: i64 = 12;
const MAX_WORKER_ID: i64 = (1 << WORKER_ID_BITS) - 1;
const MAX_SEQUENCE: i64 = (1 << SEQUENCE_BITS) - 1;
const TIMESTAMP_SHIFT: i64 = WORKER_ID_BITS + SEQUENCE_BITS;
const WORKER_ID_SHIFT: i64 = SEQUENCE_BITS;

impl DefaultIdGenerator {
    pub fn new(worker_id: i64) -> Self {
        assert!(worker_id >= 0 && worker_id <= MAX_WORKER_ID,
                "worker_id must be between 0 and {}", MAX_WORKER_ID);
        DefaultIdGenerator {
            worker_id,
            state: Mutex::new(SnowflakeState {
                last_timestamp: -1,
                sequence: 0,
            }),
        }
    }

    fn current_time_millis() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    fn wait_next_millis(last: i64) -> i64 {
        let mut ts = Self::current_time_millis();
        while ts <= last {
            ts = Self::current_time_millis();
        }
        ts
    }
}

impl IdGenerator for DefaultIdGenerator {
    fn next_id(&self) -> i64 {
        let mut state = self.state.lock().unwrap();
        let mut timestamp = Self::current_time_millis();

        if timestamp < state.last_timestamp {
            timestamp = Self::wait_next_millis(state.last_timestamp);
        }

        if timestamp == state.last_timestamp {
            state.sequence = (state.sequence + 1) & MAX_SEQUENCE;
            if state.sequence == 0 {
                timestamp = Self::wait_next_millis(state.last_timestamp);
            }
        } else {
            state.sequence = 0;
        }

        state.last_timestamp = timestamp;

        let id = ((timestamp - EPOCH) << TIMESTAMP_SHIFT)
            | (self.worker_id << WORKER_ID_SHIFT)
            | state.sequence;

        id
    }
}

/// Simple atomic counter ID generator for testing.
pub struct AtomicIdGenerator {
    counter: AtomicI64,
}

impl AtomicIdGenerator {
    pub fn new(start: i64) -> Self {
        AtomicIdGenerator {
            counter: AtomicI64::new(start),
        }
    }
}

impl IdGenerator for AtomicIdGenerator {
    fn next_id(&self) -> i64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_id_generator() {
        let gen = DefaultIdGenerator::new(1);
        let id1 = gen.next_id();
        let id2 = gen.next_id();
        assert!(id1 > 0);
        assert!(id2 > id1);
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_atomic_id_generator() {
        let gen = AtomicIdGenerator::new(100);
        assert_eq!(gen.next_id(), 100);
        assert_eq!(gen.next_id(), 101);
        assert_eq!(gen.next_id(), 102);
    }

    #[test]
    fn test_snowflake_uniqueness() {
        let gen = DefaultIdGenerator::new(0);
        let mut ids = std::collections::HashSet::new();
        for _ in 0..1000 {
            let id = gen.next_id();
            assert!(ids.insert(id), "Duplicate ID: {}", id);
        }
    }
}
