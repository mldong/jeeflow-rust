//! m_ 过滤下推共享工具（issues/106）：
//! - [`build_filter_where`]：把 [`QueryFilter`] 列表构造成 SQL WHERE 片段 + 位置绑定值
//!   （sqlx 仓储消费，列白名单由调用方以 `resolve` 闭包提供）。
//! - [`op_matches`]：算子语义的内存版比较（memory 仓储消费），与 SQL 逐算子一致。
//!
//! 算子语义与原 facade `matches_filter`（内存）/ java pushdown（SQL）逐字对齐。

use crate::model::{FilterOp, QueryFilter};

/// 把 QueryFilter 列表构造成 WHERE 片段 + 位置绑定值。
/// - `resolve(alias, column)` 返回 `Some(带别名的 SQL 列名)` 表示在白名单内；
///   `None` 表示跳过该过滤（不是报错）。
/// - 边界（与内存 op_matches 语义一致）：
///   Bt 拆分 !=2 段 → 片段 "0 = 1"（不命中任何行）；
///   In 拆完无非空段 → "0 = 1"；Nin 拆完无非空段 → 不生成片段（恒真）。
/// 返回 (fragments, values)：fragments 每项已含 `?`，values 按出现顺序铺平（供 bind）。
pub fn build_filter_where(
    filters: &[QueryFilter],
    resolve: impl Fn(&str, &str) -> Option<String>,
) -> (Vec<String>, Vec<String>) {
    let mut frags = Vec::new();
    let mut vals = Vec::new();
    for f in filters {
        let col = match resolve(&f.alias, &f.column) {
            Some(c) => c,
            None => continue,
        };
        match f.op {
            FilterOp::Eq => { frags.push(format!("{} = ?", col)); vals.push(f.value.clone()); }
            FilterOp::Ne => { frags.push(format!("{} <> ?", col)); vals.push(f.value.clone()); }
            FilterOp::Gt => { frags.push(format!("{} > ?", col)); vals.push(f.value.clone()); }
            FilterOp::Lt => { frags.push(format!("{} < ?", col)); vals.push(f.value.clone()); }
            FilterOp::Ge => { frags.push(format!("{} >= ?", col)); vals.push(f.value.clone()); }
            FilterOp::Le => { frags.push(format!("{} <= ?", col)); vals.push(f.value.clone()); }
            FilterOp::Like => { frags.push(format!("{} LIKE ?", col)); vals.push(format!("%{}%", f.value)); }
            FilterOp::In => {
                let ps: Vec<String> = f.value.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                if ps.is_empty() {
                    frags.push("0 = 1".into());
                } else {
                    let ph = vec!["?"; ps.len()].join(", ");
                    frags.push(format!("{} IN ({})", col, ph));
                    vals.extend(ps);
                }
            }
            FilterOp::Nin => {
                let ps: Vec<String> = f.value.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                if ps.is_empty() {
                    // 恒真：不 push 片段
                } else {
                    let ph = vec!["?"; ps.len()].join(", ");
                    frags.push(format!("{} NOT IN ({})", col, ph));
                    vals.extend(ps);
                }
            }
            FilterOp::Bt => {
                let ps: Vec<&str> = f.value.split(',').map(|s| s.trim()).collect();
                if ps.len() == 2 {
                    frags.push(format!("{} BETWEEN ? AND ?", col));
                    vals.push(ps[0].to_string());
                    vals.push(ps[1].to_string());
                } else {
                    frags.push("0 = 1".into());
                }
            }
        }
    }
    (frags, vals)
}

/// 字符串字段值 vs 过滤值，算子语义同 SQL。memory 仓储对行字段调用它。
pub fn op_matches(op: &FilterOp, field_val: &str, filter_val: &str) -> bool {
    use FilterOp::*;
    match op {
        Eq => field_val == filter_val,
        Ne => field_val != filter_val,
        Like => field_val.contains(filter_val),
        Gt => field_val > filter_val,
        Lt => field_val < filter_val,
        Ge => field_val >= filter_val,
        Le => field_val <= filter_val,
        In => filter_val.split(',').any(|v| v.trim() == field_val),
        Nin => !filter_val.split(',').any(|v| v.trim() == field_val),
        Bt => {
            let p: Vec<&str> = filter_val.split(',').collect();
            p.len() == 2 && field_val >= p[0].trim() && field_val <= p[1].trim()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::FilterOp;

    fn f(op: FilterOp, column: &str, value: &str) -> QueryFilter {
        QueryFilter { alias: "t".into(), op, column: column.into(), value: value.into() }
    }

    fn resolve_t_state(alias: &str, column: &str) -> Option<String> {
        match (alias, column) {
            ("t", "state") => Some("t.state".into()),
            ("t", "name") => Some("t.name".into()),
            _ => None,
        }
    }

    #[test]
    fn test_build_eq() {
        let (frags, vals) = build_filter_where(&[f(FilterOp::Eq, "state", "1")], resolve_t_state);
        assert_eq!(frags, vec!["t.state = ?".to_string()]);
        assert_eq!(vals, vec!["1".to_string()]);
    }

    #[test]
    fn test_build_like_wraps_value() {
        let (frags, vals) = build_filter_where(&[f(FilterOp::Like, "name", "v")], resolve_t_state);
        assert_eq!(frags, vec!["t.name LIKE ?".to_string()]);
        assert_eq!(vals, vec!["%v%".to_string()]);
    }

    #[test]
    fn test_build_in_expands_placeholders() {
        let (frags, vals) = build_filter_where(&[f(FilterOp::In, "name", "a,b,c")], resolve_t_state);
        assert_eq!(frags, vec!["t.name IN (?, ?, ?)".to_string()]);
        assert_eq!(vals, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    }

    #[test]
    fn test_build_unknown_column_skipped() {
        let (frags, vals) = build_filter_where(&[f(FilterOp::Eq, "hack", "1")], resolve_t_state);
        assert!(frags.is_empty());
        assert!(vals.is_empty());
    }

    #[test]
    fn test_build_between() {
        let (frags, vals) = build_filter_where(&[f(FilterOp::Bt, "state", "x,y")], resolve_t_state);
        assert_eq!(frags, vec!["t.state BETWEEN ? AND ?".to_string()]);
        assert_eq!(vals, vec!["x".to_string(), "y".to_string()]);
    }

    #[test]
    fn test_build_between_single_value_contradiction() {
        let (frags, vals) = build_filter_where(&[f(FilterOp::Bt, "state", "x")], resolve_t_state);
        assert_eq!(frags, vec!["0 = 1".to_string()]);
        assert!(vals.is_empty());
    }

    #[test]
    fn test_build_in_empty_contradiction_and_nin_empty_alwaystrue() {
        let (frags, vals) = build_filter_where(&[f(FilterOp::In, "name", " , ")], resolve_t_state);
        assert_eq!(frags, vec!["0 = 1".to_string()]);
        assert!(vals.is_empty());
        let (frags2, vals2) = build_filter_where(&[f(FilterOp::Nin, "name", " , ")], resolve_t_state);
        assert!(frags2.is_empty());
        assert!(vals2.is_empty());
    }

    #[test]
    fn test_op_matches_all_ops() {
        use FilterOp::*;
        assert!(op_matches(&Eq, "1", "1"));
        assert!(!op_matches(&Eq, "2", "1"));
        assert!(op_matches(&Ne, "2", "1"));
        assert!(op_matches(&Like, "hello", "ell"));
        assert!(!op_matches(&Like, "hello", "xyz"));
        assert!(op_matches(&Gt, "5", "3"));
        assert!(op_matches(&Lt, "3", "5"));
        assert!(op_matches(&Ge, "3", "3"));
        assert!(op_matches(&Le, "3", "3"));
        assert!(op_matches(&In, "b", "a, b ,c"));
        assert!(!op_matches(&In, "d", "a,b"));
        assert!(op_matches(&Nin, "d", "a,b"));
        assert!(!op_matches(&Nin, "a", "a,b"));
        assert!(op_matches(&Bt, "5", " 1 , 9 "));
        // 字符串字典序语义（对齐旧 facade matches_filter）："0" < "1" → 不在 [1,9]
        assert!(!op_matches(&Bt, "0", "1,9"));
        assert!(!op_matches(&Bt, "5", "1"));
    }
}
