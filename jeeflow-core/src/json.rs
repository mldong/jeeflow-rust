//! Minimal JSON value type for jeeflow-core.
//! Zero dependencies — core does not depend on serde/serde_json.
//! The facade layer uses serde_json; core uses this lightweight representation.

use std::collections::HashMap;
use std::fmt;

/// A minimal JSON value representation.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(f64),
    Str(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

impl JsonValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            JsonValue::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            JsonValue::Number(n) => Some(*n as i64),
            JsonValue::Str(s) => s.parse::<i64>().ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            JsonValue::Number(n) => Some(*n),
            JsonValue::Str(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            JsonValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&Vec<(String, JsonValue)>> {
        match self {
            JsonValue::Object(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Vec<JsonValue>> {
        match self {
            JsonValue::Array(a) => Some(a),
            _ => None,
        }
    }

    /// Get a field from an object by key.
    pub fn get(&self, key: &str) -> Option<&JsonValue> {
        match self {
            JsonValue::Object(entries) => {
                entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
            }
            _ => None,
        }
    }

    /// Get string field from object.
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|v| v.as_str())
    }

    /// Get i64 field from object.
    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.get(key).and_then(|v| v.as_i64())
    }

    /// Convert to a HashMap<String, JsonValue> (for object values).
    pub fn to_map(&self) -> HashMap<String, JsonValue> {
        let mut map = HashMap::new();
        if let JsonValue::Object(entries) = self {
            for (k, v) in entries {
                map.insert(k.clone(), v.clone());
            }
        }
        map
    }

    /// Create an object JsonValue from entries.
    pub fn object(entries: Vec<(String, JsonValue)>) -> Self {
        JsonValue::Object(entries)
    }

    /// Create a string JsonValue.
    pub fn string(s: impl Into<String>) -> Self {
        JsonValue::Str(s.into())
    }

    /// Create a number JsonValue.
    pub fn number(n: f64) -> Self {
        JsonValue::Number(n)
    }

    /// Check if this is null.
    pub fn is_null(&self) -> bool {
        matches!(self, JsonValue::Null)
    }

    /// Serialize to JSON string.
    pub fn to_json_string(&self) -> String {
        match self {
            JsonValue::Null => "null".to_string(),
            JsonValue::Bool(b) => if *b { "true" } else { "false" }.to_string(),
            JsonValue::Number(n) => {
                if *n == (*n as i64) as f64 {
                    format!("{}", *n as i64)
                } else {
                    format!("{}", n)
                }
            }
            JsonValue::Str(s) => format!("\"{}\"", escape_json_str(s)),
            JsonValue::Array(arr) => {
                let items: Vec<String> = arr.iter().map(|v| v.to_json_string()).collect();
                format!("[{}]", items.join(","))
            }
            JsonValue::Object(entries) => {
                let items: Vec<String> = entries.iter()
                    .map(|(k, v)| format!("\"{}\":{}", escape_json_str(k), v.to_json_string()))
                    .collect();
                format!("{{{}}}", items.join(","))
            }
        }
    }
}

impl fmt::Display for JsonValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_json_string())
    }
}

fn escape_json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < '\x20' => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Parse a JSON string into JsonValue.
pub fn parse_json(input: &str) -> Result<JsonValue, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(JsonValue::Null);
    }
    let mut parser = JsonParser::new(trimmed);
    parser.parse_value()
}

struct JsonParser<'a> {
    chars: &'a [u8],
    pos: usize,
}

/// UTF-8 首字节 → 码点字节宽度（非法首字节按 1 处理，后续 from_utf8 会报错）
fn utf8_char_width(first: u8) -> usize {
    if first < 0x80 {
        1
    } else if first >> 5 == 0b110 {
        2
    } else if first >> 4 == 0b1110 {
        3
    } else if first >> 3 == 0b11110 {
        4
    } else {
        1
    }
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a str) -> Self {
        JsonParser {
            chars: input.as_bytes(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.chars.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<u8> {
        let c = self.chars.get(self.pos).copied();
        if c.is_some() { self.pos += 1; }
        c
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c == b' ' || c == b'\n' || c == b'\r' || c == b'\t' {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn expect(&mut self, expected: u8) -> Result<(), String> {
        self.skip_ws();
        match self.advance() {
            Some(c) if c == expected => Ok(()),
            Some(c) => Err(format!("Expected '{}', got '{}' at pos {}", expected as char, c as char, self.pos)),
            None => Err(format!("Expected '{}', got EOF", expected as char)),
        }
    }

    fn parse_value(&mut self) -> Result<JsonValue, String> {
        self.skip_ws();
        match self.peek() {
            None => Ok(JsonValue::Null),
            Some(b'"') => self.parse_string().map(JsonValue::Str),
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b't') => self.parse_literal("true", JsonValue::Bool(true)),
            Some(b'f') => self.parse_literal("false", JsonValue::Bool(false)),
            Some(b'n') => self.parse_literal("null", JsonValue::Null),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.parse_number(),
            Some(c) => Err(format!("Unexpected char '{}' at pos {}", c as char, self.pos)),
        }
    }

    fn parse_literal(&mut self, expected: &str, value: JsonValue) -> Result<JsonValue, String> {
        let end = self.pos + expected.len();
        if end <= self.chars.len() && &self.chars[self.pos..end] == expected.as_bytes() {
            self.pos = end;
            Ok(value)
        } else {
            Err(format!("Invalid literal at pos {}", self.pos))
        }
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.expect(b'"')?;
        let mut s = String::new();
        loop {
            match self.advance() {
                None => return Err("Unterminated string".to_string()),
                Some(b'"') => return Ok(s),
                Some(b'\\') => {
                    match self.advance() {
                        Some(b'"') => s.push('"'),
                        Some(b'\\') => s.push('\\'),
                        Some(b'/') => s.push('/'),
                        Some(b'n') => s.push('\n'),
                        Some(b'r') => s.push('\r'),
                        Some(b't') => s.push('\t'),
                        Some(b'b') => s.push('\x08'),
                        Some(b'f') => s.push('\x0c'),
                        Some(b'u') => {
                            let hex = self.read_n(4)?;
                            let code = u32::from_str_radix(&hex, 16)
                                .map_err(|_| format!("Invalid unicode escape: {}", hex))?;
                            if let Some(ch) = char::from_u32(code) {
                                s.push(ch);
                            }
                        }
                        _ => return Err("Invalid escape".to_string()),
                    }
                }
                // UTF-8 多字节：按首字节长度一次读完整码点（禁止 `c as char`，否则中文乱码）
                Some(c) => {
                    let width = utf8_char_width(c);
                    if width == 1 {
                        s.push(c as char);
                    } else {
                        let start = self.pos - 1;
                        let end = start + width;
                        if end > self.chars.len() {
                            return Err("Invalid UTF-8: truncated multi-byte sequence".into());
                        }
                        let ch = std::str::from_utf8(&self.chars[start..end])
                            .map_err(|_| "Invalid UTF-8 in string".to_string())?
                            .chars()
                            .next()
                            .ok_or_else(|| "Invalid UTF-8 in string".to_string())?;
                        s.push(ch);
                        self.pos = end;
                    }
                }
            }
        }
    }

    fn read_n(&mut self, n: usize) -> Result<String, String> {
        let end = self.pos + n;
        if end > self.chars.len() {
            return Err("Unexpected end of input".to_string());
        }
        let s = std::str::from_utf8(&self.chars[self.pos..end])
            .map_err(|_| "Invalid UTF-8".to_string())?
            .to_string();
        self.pos = end;
        Ok(s)
    }

    fn parse_number(&mut self) -> Result<JsonValue, String> {
        let start = self.pos;
        if self.peek() == Some(b'-') { self.pos += 1; }
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() { self.pos += 1; } else { break; }
        }
        if self.peek() == Some(b'.') {
            self.pos += 1;
            while let Some(c) = self.peek() {
                if c.is_ascii_digit() { self.pos += 1; } else { break; }
            }
        }
        if self.peek() == Some(b'e') || self.peek() == Some(b'E') {
            self.pos += 1;
            if self.peek() == Some(b'+') || self.peek() == Some(b'-') { self.pos += 1; }
            while let Some(c) = self.peek() {
                if c.is_ascii_digit() { self.pos += 1; } else { break; }
            }
        }
        let num_str = std::str::from_utf8(&self.chars[start..self.pos])
            .map_err(|_| "Invalid number".to_string())?;
        let n: f64 = num_str.parse().map_err(|_| format!("Invalid number: {}", num_str))?;
        Ok(JsonValue::Number(n))
    }

    fn parse_object(&mut self) -> Result<JsonValue, String> {
        self.expect(b'{')?;
        let mut entries = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(JsonValue::Object(entries));
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            self.expect(b':')?;
            let value = self.parse_value()?;
            entries.push((key, value));
            self.skip_ws();
            match self.peek() {
                Some(b',') => { self.pos += 1; }
                Some(b'}') => { self.pos += 1; return Ok(JsonValue::Object(entries)); }
                _ => return Err(format!("Expected ',' or '}}' at pos {}", self.pos)),
            }
        }
    }

    fn parse_array(&mut self) -> Result<JsonValue, String> {
        self.expect(b'[')?;
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(JsonValue::Array(items));
        }
        loop {
            let value = self.parse_value()?;
            items.push(value);
            self.skip_ws();
            match self.peek() {
                Some(b',') => { self.pos += 1; }
                Some(b']') => { self.pos += 1; return Ok(JsonValue::Array(items)); }
                _ => return Err(format!("Expected ',' or ']' at pos {}", self.pos)),
            }
        }
    }
}

/// FlowData — a HashMap<String, JsonValue> wrapper for process/task variables.
/// Equivalent to Java's FlowData (LinkedHashMap<String, Object>).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FlowData {
    inner: HashMap<String, JsonValue>,
}

impl FlowData {
    pub fn new() -> Self {
        FlowData { inner: HashMap::new() }
    }

    pub fn from_map(map: HashMap<String, JsonValue>) -> Self {
        FlowData { inner: map }
    }

    pub fn get(&self, key: &str) -> Option<&JsonValue> {
        self.inner.get(key)
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.inner.get(key).and_then(|v| v.as_str())
    }

    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.inner.get(key).and_then(|v| v.as_i64())
    }

    pub fn insert(&mut self, key: String, value: JsonValue) {
        self.inner.insert(key, value);
    }

    pub fn insert_str(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.inner.insert(key.into(), JsonValue::Str(value.into()));
    }

    pub fn insert_i64(&mut self, key: impl Into<String>, value: i64) {
        self.inner.insert(key.into(), JsonValue::Number(value as f64));
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.inner.contains_key(key)
    }

    pub fn remove(&mut self, key: &str) -> Option<JsonValue> {
        self.inner.remove(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &JsonValue)> {
        self.inner.iter()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn merge(&mut self, other: &FlowData) {
        for (k, v) in &other.inner {
            self.inner.insert(k.clone(), v.clone());
        }
    }

    pub fn inner(&self) -> &HashMap<String, JsonValue> {
        &self.inner
    }

    pub fn into_inner(self) -> HashMap<String, JsonValue> {
        self.inner
    }

    /// Get i64 or default.
    pub fn get_i64_or(&self, key: &str, default: i64) -> i64 {
        self.get_i64(key).unwrap_or(default)
    }

    /// Get string or default.
    pub fn get_str_or(&self, key: &str, default: &str) -> String {
        self.get_str(key).map(|s| s.to_string()).unwrap_or_else(|| default.to_string())
    }

    /// Get all keys with a given prefix.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.inner.keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    /// Get f_ prefixed fields (form data).
    pub fn form_fields(&self) -> HashMap<String, JsonValue> {
        self.inner.iter()
            .filter(|(k, _)| k.starts_with("f_"))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Get tf_ prefixed fields (task form data).
    pub fn task_form_fields(&self) -> HashMap<String, JsonValue> {
        self.inner.iter()
            .filter(|(k, _)| k.starts_with("tf_"))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_object() {
        let json = r#"{"name":"test","value":42,"flag":true,"nothing":null}"#;
        let val = parse_json(json).unwrap();
        assert_eq!(val.get_str("name"), Some("test"));
        assert_eq!(val.get_i64("value"), Some(42));
        assert_eq!(val.get("flag").and_then(|v| v.as_bool()), Some(true));
        assert!(val.get("nothing").unwrap().is_null());
    }

    #[test]
    fn test_parse_nested() {
        let json = r#"{"a":{"b":"c"},"d":[1,2,3]}"#;
        let val = parse_json(json).unwrap();
        assert_eq!(val.get("a").unwrap().get_str("b"), Some("c"));
        let arr = val.get("d").unwrap().as_array().unwrap();
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn test_flow_data() {
        let mut fd = FlowData::new();
        fd.insert_str("f_name", "张三");
        fd.insert_i64("f_amount", 1000);
        fd.insert_str("operator", "user1");
        assert_eq!(fd.get_str("f_name"), Some("张三"));
        assert_eq!(fd.form_fields().len(), 2);
        assert!(!fd.task_form_fields().is_empty() || fd.task_form_fields().is_empty());
    }

    #[test]
    fn test_json_roundtrip() {
        let json = r#"{"name":"test","items":[1,"two",null,true]}"#;
        let val = parse_json(json).unwrap();
        let serialized = val.to_json_string();
        let reparsed = parse_json(&serialized).unwrap();
        assert_eq!(val, reparsed);
    }

    #[test]
    fn test_parse_empty_object() {
        let val = parse_json("{}").unwrap();
        assert!(val.as_object().unwrap().is_empty());
    }

    #[test]
    fn test_parse_empty_array() {
        let val = parse_json("[]").unwrap();
        assert!(val.as_array().unwrap().is_empty());
    }

    #[test]
    fn test_json_value_is_methods() {
        assert!(JsonValue::Null.is_null());
        assert!(JsonValue::Bool(true).as_bool() == Some(true));
        assert!(JsonValue::Number(3.14).as_f64() == Some(3.14));
        assert!(JsonValue::Str("hello".into()).as_str() == Some("hello"));
    }

    #[test]
    fn test_json_value_as_i64() {
        assert_eq!(JsonValue::Number(42.0).as_i64(), Some(42));
        assert_eq!(JsonValue::Str("not a number".into()).as_i64(), None);
        assert_eq!(JsonValue::Null.as_i64(), None);
    }

    #[test]
    fn test_flow_data_merge() {
        let mut fd1 = FlowData::new();
        fd1.insert_str("a", "1");
        let mut fd2 = FlowData::new();
        fd2.insert_str("b", "2");
        fd1.merge(&fd2);
        assert_eq!(fd1.get_str("a"), Some("1"));
        assert_eq!(fd1.get_str("b"), Some("2"));
    }

    #[test]
    fn test_flow_data_contains_key() {
        let mut fd = FlowData::new();
        fd.insert_str("key1", "val1");
        assert!(fd.contains_key("key1"));
        assert!(!fd.contains_key("key2"));
    }

    #[test]
    fn test_flow_data_remove() {
        let mut fd = FlowData::new();
        fd.insert_str("key1", "val1");
        let removed = fd.remove("key1");
        assert!(removed.is_some());
        assert!(!fd.contains_key("key1"));
    }

    #[test]
    fn test_flow_data_from_map() {
        let mut map = std::collections::HashMap::new();
        map.insert("k".to_string(), JsonValue::Str("v".to_string()));
        let fd = FlowData::from_map(map);
        assert_eq!(fd.get_str("k"), Some("v"));
    }

    #[test]
    fn test_parse_utf8_chinese_string() {
        // 回归：按字节 `c as char` 会把「上级审批」打成 latin1 乱码
        let val = parse_json(r#"{"value":"上级审批"}"#).unwrap();
        assert_eq!(val.get_str("value"), Some("上级审批"));
        let val2 = parse_json(r#"{"displayName":"简单审批流程","text":{"value":"发起申请"}}"#).unwrap();
        assert_eq!(val2.get_str("displayName"), Some("简单审批流程"));
        assert_eq!(
            val2.get("text").and_then(|t| t.get_str("value")),
            Some("发起申请")
        );
    }

    #[test]
    fn test_parse_invalid_json_error() {
        assert!(parse_json("{invalid}").is_err());
        assert!(parse_json("[1,2,").is_err());
    }

    #[test]
    fn test_json_array_iteration() {
        let json = r#"[1,2,3]"#;
        let val = parse_json(json).unwrap();
        let arr = val.as_array().unwrap();
        let sum: f64 = arr.iter().filter_map(|v| v.as_f64()).sum();
        assert_eq!(sum, 6.0);
    }
}
