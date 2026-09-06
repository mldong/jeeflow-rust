//! jeeflow-core: Workflow engine core — zero external dependencies.
//!
//! This crate implements the DDD aggregate root, state machine, engine orchestration,
//! 8 node types, 5 built-in handlers, 7 assignment handlers, event system,
//! metadata registries, and service locator — all with zero third-party crate dependencies.

pub mod error;
pub mod model;
pub mod json;
pub mod spi;
pub mod context;
pub mod event;
pub mod id_gen;
pub mod metadata;
pub mod parser;
pub mod engine;
pub mod handler;
pub mod interceptor;
pub mod memory;
pub mod filter_sql;
#[cfg(any(test, feature = "dev-flows"))]
pub mod flowsdir;

pub use error::{JeeflowError, JeeflowResult};
pub use model::*;
pub use json::{JsonValue, FlowData};
pub use spi::*;
pub use context::ServiceContext;
pub use event::*;
pub use id_gen::DefaultIdGenerator;
pub use metadata::{EnumDictRegistry, HandlerRegistry, HandlerMeta};
pub use engine::{JeeflowEngine, JeeflowEngineImpl, Execution};
pub use memory::MemoryRepository;
