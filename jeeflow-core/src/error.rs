//! Error types for jeeflow engine core.
//! Zero dependencies — uses custom enum, no thiserror/anyhow.

use std::fmt;

/// Engine error codes.
/// Business failure = 99999999 (spec/06 §2.1).
pub const ERR_BUSINESS: i64 = 99999999;

#[derive(Debug, Clone)]
pub enum JeeflowError {
    /// Business logic failure (code=99999999)
    Business(String),
    /// Unknown action
    UnknownAction(String),
    /// Process definition not found
    DefineNotFound(i64),
    /// Process instance not found
    InstanceNotFound(i64),
    /// Task not found
    TaskNotFound(i64),
    /// Permission denied (operator not allowed)
    PermissionDenied(String),
    /// Invalid state transition
    InvalidState(String),
    /// Invalid submit type
    InvalidSubmitType(i64),
    /// Parse error (JSON / model)
    ParseError(String),
    /// Internal error
    Internal(String),
}

impl JeeflowError {
    pub fn code(&self) -> i64 {
        ERR_BUSINESS
    }

    pub fn message(&self) -> String {
        match self {
            JeeflowError::Business(msg) => msg.clone(),
            JeeflowError::UnknownAction(a) => format!("未知 action: {}", a),
            JeeflowError::DefineNotFound(id) => format!("流程定义不存在: {}", id),
            JeeflowError::InstanceNotFound(id) => format!("流程实例不存在: {}", id),
            JeeflowError::TaskNotFound(id) => format!("任务不存在: {}", id),
            JeeflowError::PermissionDenied(msg) => format!("权限不足: {}", msg),
            JeeflowError::InvalidState(msg) => format!("非法状态转换: {}", msg),
            JeeflowError::InvalidSubmitType(st) => format!("非法 submitType: {}", st),
            JeeflowError::ParseError(msg) => format!("解析错误: {}", msg),
            JeeflowError::Internal(msg) => format!("内部错误: {}", msg),
        }
    }
}

impl fmt::Display for JeeflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl std::error::Error for JeeflowError {}

pub type JeeflowResult<T> = Result<T, JeeflowError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_always_business() {
        assert_eq!(JeeflowError::Business("test".into()).code(), ERR_BUSINESS);
        assert_eq!(JeeflowError::UnknownAction("x".into()).code(), ERR_BUSINESS);
        assert_eq!(JeeflowError::DefineNotFound(1).code(), ERR_BUSINESS);
        assert_eq!(JeeflowError::TaskNotFound(1).code(), ERR_BUSINESS);
        assert_eq!(JeeflowError::PermissionDenied("x".into()).code(), ERR_BUSINESS);
        assert_eq!(JeeflowError::Internal("x".into()).code(), ERR_BUSINESS);
    }

    #[test]
    fn test_error_messages() {
        assert!(JeeflowError::Business("msg".into()).message().contains("msg"));
        assert!(JeeflowError::UnknownAction("test/act".into()).message().contains("test/act"));
        assert!(JeeflowError::DefineNotFound(42).message().contains("42"));
        assert!(JeeflowError::TaskNotFound(99).message().contains("99"));
    }

    #[test]
    fn test_error_display() {
        let err = JeeflowError::Business("test error".into());
        let s = format!("{}", err);
        assert_eq!(s, "test error");
    }

    #[test]
    fn test_error_business_code_constant() {
        assert_eq!(ERR_BUSINESS, 99999999);
    }

    #[test]
    fn test_invalid_submit_type_message() {
        let err = JeeflowError::InvalidSubmitType(99);
        assert!(err.message().contains("99"));
    }

    #[test]
    fn test_parse_error_message() {
        let err = JeeflowError::ParseError("bad json".into());
        assert!(err.message().contains("bad json"));
    }
}
