use super::*;

impl ApexExecutor {
    pub(in crate::query::executor) fn collect_having_extra_aggs(
        expr: &crate::query::SqlExpr,
        select_cols: &[crate::query::SelectColumn],
    ) -> Vec<(crate::query::AggregateFunc, Option<String>)> {
        use crate::query::{AggregateFunc, SelectColumn, SqlExpr};

        // Build set of already-present aggregate output names
        let existing: Vec<String> = select_cols
            .iter()
            .filter_map(|c| {
                if let SelectColumn::Aggregate {
                    func,
                    column,
                    alias,
                    ..
                } = c
                {
                    let fn_name = match func {
                        AggregateFunc::Count => "COUNT",
                        AggregateFunc::Sum => "SUM",
                        AggregateFunc::Avg => "AVG",
                        AggregateFunc::Min => "MIN",
                        AggregateFunc::Max => "MAX",
                    };
                    Some(alias.clone().unwrap_or_else(|| {
                        format!("{}({})", fn_name, column.as_deref().unwrap_or("*"))
                    }))
                } else {
                    None
                }
            })
            .collect();

        let mut found: Vec<(AggregateFunc, Option<String>)> = Vec::new();
        Self::walk_having_expr(expr, &existing, &mut found);
        found
    }

    pub(in crate::query::executor) fn walk_having_expr(
        expr: &crate::query::SqlExpr,
        existing: &[String],
        out: &mut Vec<(crate::query::AggregateFunc, Option<String>)>,
    ) {
        use crate::query::{AggregateFunc, SqlExpr};
        match expr {
            SqlExpr::Function { name, args } => {
                let agg_func = if name.eq_ignore_ascii_case("COUNT") {
                    Some(AggregateFunc::Count)
                } else if name.eq_ignore_ascii_case("SUM") {
                    Some(AggregateFunc::Sum)
                } else if name.eq_ignore_ascii_case("AVG") {
                    Some(AggregateFunc::Avg)
                } else if name.eq_ignore_ascii_case("MIN") {
                    Some(AggregateFunc::Min)
                } else if name.eq_ignore_ascii_case("MAX") {
                    Some(AggregateFunc::Max)
                } else {
                    None
                };
                if let Some(func) = agg_func {
                    // Determine column argument
                    let col: Option<String> = args.first().and_then(|a| match a {
                        SqlExpr::Column(c) if c == "*" => None,
                        SqlExpr::Column(c) => Some(c.clone()),
                        SqlExpr::Literal(crate::data::Value::String(s)) if s == "*" => None,
                        _ => None,
                    });
                    let fn_name = match func {
                        AggregateFunc::Count => "COUNT",
                        AggregateFunc::Sum => "SUM",
                        AggregateFunc::Avg => "AVG",
                        AggregateFunc::Min => "MIN",
                        AggregateFunc::Max => "MAX",
                    };
                    let key = format!("{}({})", fn_name, col.as_deref().unwrap_or("*"));
                    if !existing.iter().any(|e| e.eq_ignore_ascii_case(&key))
                        && !out.iter().any(|(f, c)| {
                            let fn2 = match f {
                                AggregateFunc::Count => "COUNT",
                                AggregateFunc::Sum => "SUM",
                                AggregateFunc::Avg => "AVG",
                                AggregateFunc::Min => "MIN",
                                AggregateFunc::Max => "MAX",
                            };
                            format!("{}({})", fn2, c.as_deref().unwrap_or("*"))
                                .eq_ignore_ascii_case(&key)
                        })
                    {
                        out.push((func, col));
                    }
                } else {
                    for arg in args {
                        Self::walk_having_expr(arg, existing, out);
                    }
                }
            }
            SqlExpr::BinaryOp { left, right, .. } => {
                Self::walk_having_expr(left, existing, out);
                Self::walk_having_expr(right, existing, out);
            }
            SqlExpr::UnaryOp { expr, .. } | SqlExpr::Paren(expr) | SqlExpr::Cast { expr, .. } => {
                Self::walk_having_expr(expr, existing, out);
            }
            SqlExpr::Case {
                when_then,
                else_expr,
            } => {
                for (c, t) in when_then {
                    Self::walk_having_expr(c, existing, out);
                    Self::walk_having_expr(t, existing, out);
                }
                if let Some(e) = else_expr {
                    Self::walk_having_expr(e, existing, out);
                }
            }
            _ => {}
        }
    }
}
