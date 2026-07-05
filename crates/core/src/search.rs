//! Search-DSL → SQL translator.
//!
//! The frontend search bar speaks a concise, familiar log query language; SQL remains
//! the first-class interface (this just compiles down to it, so portability is
//! preserved). Pure and sans-I/O: `now_millis` is passed in by the caller (from
//! the real clock in the shell, or a fixed value under simulation) so time
//! windows like `last 1h` are deterministic and testable.
//!
//! Grammar (v1):
//!   query      := term* ( '|' command )*
//!   term       := field ':' value      (service:auth, level:error)
//!               | field op value        (status >= 500, status == 200)
//!               | word                  (free text -> message ILIKE '%word%')
//!   op         := == | = | != | >= | <= | > | <
//!   command    := last <duration>       (last 1h, last 30m, last 7d)
//!
//! Known columns: ts, level, service, status, message, trace_id. Any other key
//! is matched inside the `attrs_json` blob.

/// Columns selected for log-search results (must exist in the table schema).
pub const RESULT_COLUMNS: &str = "ts, level, service, status, message, trace_id, attrs_json";

/// Translate a search-DSL `input` into a SQL query over `table`.
pub fn to_sql(
    input: &str,
    table: &str,
    now_millis: i64,
    limit: usize,
) -> Result<String, String> {
    let (filter_part, commands) = split_pipes(input);

    // Glue spaces around operators so `status == 200` tokenizes as one term.
    let filter_part = normalize_operators(&filter_part);

    let mut conditions: Vec<String> = Vec::new();
    for token in filter_part.split_whitespace() {
        conditions.push(translate_term(token)?);
    }

    for cmd in commands {
        let cmd = cmd.trim();
        if let Some(rest) = cmd.strip_prefix("last ").or_else(|| cmd.strip_prefix("last")) {
            let dur_ms = parse_duration(rest.trim())?;
            let from = now_millis.saturating_sub(dur_ms);
            conditions.push(format!("ts >= to_timestamp_millis({from})"));
        } else if !cmd.is_empty() {
            return Err(format!("unknown command: '{cmd}'"));
        }
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    Ok(format!(
        "SELECT {RESULT_COLUMNS} FROM {table}{where_clause} ORDER BY ts DESC LIMIT {limit}"
    ))
}

/// Heuristic: does this look like raw SQL (pass through) rather than the DSL?
pub fn looks_like_sql(input: &str) -> bool {
    let t = input.trim_start().to_ascii_lowercase();
    t.starts_with("select") || t.starts_with("with")
}

/// Extract the file-prunable predicates from a DSL query's filter terms, for
/// metadata-only file skipping shared by the estimator and the executor.
///
/// Two kinds qualify: pure equality on the stat-carrying columns (`service:auth`,
/// `level:error`, `service==x`) and free-text terms (`timeout` → prunable via the
/// per-file `message` trigram set). Ranges (`status>=500`), negations
/// (`level!=info`), and attribute matches are ignored — none can prove a file
/// value-free, so they must not prune. A free-text token containing a SQL LIKE
/// wildcard (`%`/`_`) is also skipped: it reaches the engine as a wildcard, so
/// its literal trigrams aren't a necessary condition for a match.
/// Raw SQL returns empty (we don't parse arbitrary SQL predicates); its files are
/// pruned by tier + time window only, which is always safe.
///
/// `level` values are upper-cased to match how they're stored (`Level::as_str`)
/// and how [`to_sql`] emits them, so the predicate compares against the recorded
/// per-file stats correctly.
pub fn stat_predicates(input: &str) -> Vec<crate::manifest::Predicate> {
    use crate::manifest::Predicate;
    if looks_like_sql(input) {
        return Vec::new();
    }
    let (filter_part, _commands) = split_pipes(input);
    let filter_part = normalize_operators(&filter_part);
    let mut preds = Vec::new();
    for token in filter_part.split_whitespace() {
        if let Some((key, value)) = equality_term(token) {
            match key {
                "service" => preds.push(Predicate::service(value)),
                "level" => preds.push(Predicate::level(value.to_uppercase())),
                _ => {} // other columns have no per-file value stats
            }
        } else if is_free_text(token) && !token.contains(['%', '_']) {
            preds.push(Predicate::message_contains(token));
        }
    }
    preds
}

/// Does `token` translate as a bare free-text word (→ `message ILIKE '%…%'`)?
/// Mirrors [`translate_term`]: anything carrying a comparison operator or a
/// `field:` prefix is not free text.
fn is_free_text(token: &str) -> bool {
    !token.contains(['=', '>', '<', '!', ':'])
}

/// If `token` is a pure equality term (`key:value`, `key=value`, `key==value`),
/// return `(key, value)`. Any inequality/range operator (`!= >= <= > <`) yields
/// `None` — those can't safely skip a file.
fn equality_term(token: &str) -> Option<(&str, &str)> {
    // Reject range/negation operators before matching `=` so `>=` isn't read as `=`.
    for bad in ["!=", ">=", "<=", ">", "<"] {
        if token.contains(bad) {
            return None;
        }
    }
    for op in ["==", "=", ":"] {
        if let Some(idx) = token.find(op) {
            let key = &token[..idx];
            let value = &token[idx + op.len()..];
            if key.is_empty() || value.is_empty() {
                return None;
            }
            return Some((key, value));
        }
    }
    None
}

/// Extract the `[from, to]` time window implied by a `| last <dur>` command, for
/// metadata-only scan pruning. Returns `None` if the query has no time bound.
pub fn time_window(input: &str, now_millis: i64) -> Option<(i64, i64)> {
    let (_filter, commands) = split_pipes(input);
    for cmd in commands {
        if let Some(rest) = cmd.trim().strip_prefix("last") {
            if let Ok(ms) = parse_duration(rest.trim()) {
                return Some((now_millis.saturating_sub(ms), now_millis));
            }
        }
    }
    None
}

/// Remove whitespace immediately around comparison operators, so both
/// `status>=500` and `status >= 500` tokenize as a single term. Two-char
/// operators are glued before one-char ones so `>=` isn't split into `>`.
fn normalize_operators(s: &str) -> String {
    let mut out = s.to_string();
    for op in ["==", ">=", "<=", "!=", ">", "<", "="] {
        out = out
            .replace(&format!(" {op} "), op)
            .replace(&format!(" {op}"), op)
            .replace(&format!("{op} "), op);
    }
    out
}

fn split_pipes(input: &str) -> (String, Vec<String>) {
    let mut parts = input.split('|');
    let head = parts.next().unwrap_or("").to_string();
    let commands = parts.map(|s| s.to_string()).collect();
    (head, commands)
}

const STRING_COLUMNS: &[&str] = &["service", "level", "message", "trace_id"];

fn translate_term(token: &str) -> Result<String, String> {
    // Comparison operators first (2-char before 1-char).
    for op in ["==", ">=", "<=", "!=", ">", "<", "="] {
        if let Some(idx) = token.find(op) {
            let key = &token[..idx];
            let value = &token[idx + op.len()..];
            if key.is_empty() || value.is_empty() {
                return Err(format!("malformed term: '{token}'"));
            }
            return translate_compare(key, normalize_op(op), value);
        }
    }
    // `field:value`
    if let Some((key, value)) = token.split_once(':') {
        if key.is_empty() || value.is_empty() {
            return Err(format!("malformed term: '{token}'"));
        }
        return translate_compare(key, "=", value);
    }
    // Bare word -> free-text search on message.
    Ok(format!("message ILIKE '{}'", like_escape(token)))
}

fn normalize_op(op: &str) -> &'static str {
    match op {
        "==" => "=",
        ">=" => ">=",
        "<=" => "<=",
        "!=" => "!=",
        ">" => ">",
        "<" => "<",
        _ => "=",
    }
}

fn translate_compare(key: &str, op: &str, value: &str) -> Result<String, String> {
    match key {
        "status" => {
            let n: i64 = value
                .parse()
                .map_err(|_| format!("status must be numeric, got '{value}'"))?;
            Ok(format!("status {op} {n}"))
        }
        "level" => Ok(format!("level {op} '{}'", sql_escape(&value.to_uppercase()))),
        k if STRING_COLUMNS.contains(&k) => Ok(format!("{k} {op} '{}'", sql_escape(value))),
        // Unknown key -> match inside the attrs_json blob (equality only).
        other => {
            if op != "=" {
                return Err(format!(
                    "operator '{op}' not supported on attribute '{other}' (only ':' / '==')"
                ));
            }
            // attrs_json stores values as JSON; match the "key":"value" substring.
            Ok(format!(
                "attrs_json LIKE '%{}%'",
                sql_escape(&format!("\"{other}\":\"{value}\""))
            ))
        }
    }
}

fn parse_duration(s: &str) -> Result<i64, String> {
    if s.is_empty() {
        return Err("missing duration after 'last'".to_string());
    }
    let (num, unit) = s.split_at(s.len() - 1);
    let n: i64 = num
        .parse()
        .map_err(|_| format!("bad duration number in '{s}'"))?;
    let ms = match unit {
        "s" => n * 1_000,
        "m" => n * 60_000,
        "h" => n * 3_600_000,
        "d" => n * 86_400_000,
        _ => return Err(format!("bad duration unit in '{s}' (use s/m/h/d)")),
    };
    Ok(ms)
}

fn sql_escape(v: &str) -> String {
    v.replace('\'', "''")
}

fn like_escape(v: &str) -> String {
    // Wrap a bare value for a LIKE '%...%' contains-match, escaping quotes.
    format!("%{}%", v.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_and_status_and_time() {
        let sql = to_sql("service:auth status>=500 | last 1h", "logs", 3_600_000, 200).unwrap();
        assert_eq!(
            sql,
            "SELECT ts, level, service, status, message, trace_id, attrs_json \
             FROM logs WHERE service = 'auth' AND status >= 500 \
             AND ts >= to_timestamp_millis(0) ORDER BY ts DESC LIMIT 200"
        );
    }

    #[test]
    fn equality_and_level_uppercased() {
        let sql = to_sql("service:auth status == 200 level:error", "t", 0, 50).unwrap();
        assert!(sql.contains("service = 'auth'"));
        assert!(sql.contains("status = 200"));
        assert!(sql.contains("level = 'ERROR'"));
    }

    #[test]
    fn free_text_and_attr() {
        let sql = to_sql("timeout region:us-east-1", "t", 0, 10).unwrap();
        assert!(sql.contains("message ILIKE '%timeout%'"));
        assert!(sql.contains(r#"attrs_json LIKE '%"region":"us-east-1"%'"#));
    }

    #[test]
    fn sql_passthrough_detection() {
        assert!(looks_like_sql("SELECT 1"));
        assert!(looks_like_sql("  with x as (..)"));
        assert!(!looks_like_sql("service:auth"));
    }

    #[test]
    fn bad_inputs_error() {
        assert!(to_sql("status>=abc", "t", 0, 10).is_err());
        assert!(to_sql("foo | last 5x", "t", 0, 10).is_err());
    }

    #[test]
    fn stat_predicates_extract_only_prunable_terms() {
        use crate::manifest::Predicate;
        // service/level equality and free text → predicates; level upper-cased
        // to match storage; free text prunes via the message trigram stats.
        let p = stat_predicates("service:auth level:error status>=500 timeout | last 1h");
        assert_eq!(
            p,
            vec![
                Predicate::service("auth"),
                Predicate::level("ERROR"),
                Predicate::message_contains("timeout"),
            ],
            "service/level equality and free text prune; status range and `last` do not"
        );
        // `==` is equality too.
        assert_eq!(stat_predicates("service==billing"), vec![Predicate::service("billing")]);
        // Negation and ranges never prune.
        assert!(stat_predicates("level!=info").is_empty());
        assert!(stat_predicates("status>=500").is_empty());
        // Raw SQL isn't parsed for predicates.
        assert!(stat_predicates("SELECT * FROM logs WHERE service='auth'").is_empty());
        // A column without value stats (trace_id) yields no file-level predicate.
        assert!(stat_predicates("trace_id:4ac9d21").is_empty());
        // A free-text token carrying a LIKE wildcard reaches the engine as a
        // wildcard pattern — its literal trigrams must not prune.
        assert!(stat_predicates("time%out").is_empty());
        assert!(stat_predicates("dead_lock").is_empty());
    }
}
