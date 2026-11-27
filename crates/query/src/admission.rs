//! Conservative shape checks for admission to the reserved query lane.
use datafusion::sql::sqlparser::{
    ast::{
        BinaryOperator, Expr, GroupByExpr, LimitClause, OrderByKind, Query, SelectItem, SetExpr,
        Statement, TableFactor, Value,
    },
    dialect::GenericDialect,
    parser::Parser,
};
use verdigris_core::config::QueryConfig;

/// Classify the SQL the engine will execute, after stripping the DSL wrapper.
/// Keep a file-byte budget for general queries and accept only a simple point
/// lookup as an exception. Ambiguous or unsupported SQL stays in the heavy lane.
pub fn is_cheap_query(
    sql: &str,
    table: &str,
    scan_bytes: u64,
    cfg: &QueryConfig,
    allow_point_lookup: bool,
) -> bool {
    if cfg.cheap_scan_mib == 0 {
        return false;
    }
    let small_scan = scan_bytes <= cfg.cheap_scan_mib.saturating_mul(1024 * 1024);
    if sql.is_empty() {
        return small_scan;
    }
    let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
        return false;
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return false;
    };
    let limit = match &query.limit_clause {
        Some(LimitClause::LimitOffset {
            limit: Some(Expr::Value(value)),
            offset: None,
            limit_by,
        }) if limit_by.is_empty() => match &value.value {
            Value::Number(n, _) => n.parse::<u64>().ok(),
            _ => None,
        },
        _ => None,
    };
    let bounded = cfg.cheap_limit_rows > 0 && limit.is_some_and(|n| n <= cfg.cheap_limit_rows);
    if small_scan {
        return cfg.cheap_limit_rows == 0
            || bounded
            || (query.limit_clause.is_none() && query.fetch.is_none());
    }
    allow_point_lookup && bounded && is_simple_trace_lookup(query, table)
}

fn is_simple_trace_lookup(query: &Query, table: &str) -> bool {
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return false;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    let [source] = select.from.as_slice() else {
        return false;
    };
    let TableFactor::Table {
        name,
        alias: None,
        args: None,
        with_hints,
        version: None,
        with_ordinality: false,
        partitions,
        json_path: None,
        sample: None,
        index_hints,
    } = &source.relation
    else {
        return false;
    };
    if name.0.len() != 1
        || !name.to_string().eq_ignore_ascii_case(table)
        || !source.joins.is_empty()
        || !with_hints.is_empty()
        || !partitions.is_empty()
        || !index_hints.is_empty()
        || select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !matches!(&select.group_by, GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty())
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.select_modifiers.is_some()
        || !select.optimizer_hints.is_empty()
        || !select.projection.iter().all(|item| match item {
            SelectItem::UnnamedExpr(Expr::Identifier(_)) => true,
            SelectItem::Wildcard(_) => item.to_string() == "*",
            _ => false,
        })
    {
        return false;
    }
    if let Some(order) = &query.order_by {
        let OrderByKind::Expressions(exprs) = &order.kind else {
            return false;
        };
        if order.interpolate.is_some()
            || !matches!(exprs.as_slice(), [expr] if expr.with_fill.is_none() && matches!(&expr.expr, Expr::Identifier(id) if id.value.eq_ignore_ascii_case("ts")))
        {
            return false;
        }
    }
    let Some(Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    }) = &select.selection
    else {
        return false;
    };
    matches!((left.as_ref(), right.as_ref()), (Expr::Identifier(id), Expr::Value(value))
        if id.value.eq_ignore_ascii_case("trace_id") && matches!(&value.value, Value::SingleQuotedString(_)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use verdigris_core::{scan, search};

    #[test]
    fn normalized_dsl_and_sql_use_the_same_trace_lane() {
        let cfg = QueryConfig {
            cheap_scan_mib: 8,
            cheap_limit_rows: 1_000,
            ..Default::default()
        };
        let compiled =
            search::to_sql("trace_id:abc", "bench", 0, scan::DEFAULT_QUERY_LIMIT).unwrap();
        for sql in [
            compiled.as_str(),
            "SELECT * FROM bench WHERE trace_id='abc' LIMIT 10",
            "select trace_id from bench where TRACE_ID = 'abc' order by ts desc limit 10",
        ] {
            assert!(is_cheap_query(sql, "bench", 9 << 20, &cfg, true), "{sql}");
            assert!(!is_cheap_query(sql, "bench", 9 << 20, &cfg, false), "{sql}");
        }
    }

    #[test]
    fn only_a_proven_lookup_can_override_the_file_byte_budget() {
        let cfg = QueryConfig {
            cheap_scan_mib: 8,
            cheap_limit_rows: 1_000,
            ..Default::default()
        };
        for sql in [
            "SELECT * FROM bench LIMIT 10",
            "SELECT count(*) FROM bench GROUP BY service LIMIT 1",
            "SELECT count(*) FROM bench WHERE trace_id = 'abc' LIMIT 1",
            "SELECT * FROM bench WHERE trace_id = 'abc' OR level = 'ERROR' LIMIT 10",
            "SELECT * FROM bench WHERE message LIKE '%trace_id =%' LIMIT 10",
            "SELECT * FROM bench WHERE trace_id = 'abc' LIMIT 10 OFFSET 1",
            "SELECT * FROM other WHERE trace_id = 'abc' LIMIT 10",
            "SELECT * FROM bench WHERE trace_id = 'abc' LIMIT 5000",
        ] {
            assert!(!is_cheap_query(sql, "bench", 9 << 20, &cfg, true), "{sql}");
        }
        assert!(is_cheap_query(
            "SELECT count(*) FROM bench",
            "bench",
            1 << 20,
            &cfg,
            false
        ));
        assert!(!is_cheap_query(
            "SELECT * FROM bench LIMIT 5000",
            "bench",
            1 << 20,
            &cfg,
            false
        ));
    }
}
