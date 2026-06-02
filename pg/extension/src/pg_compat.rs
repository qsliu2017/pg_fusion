use postgres::{SimpleQueryMessage, Transaction};

use crate::smoke_tests::{batch_execute_pg_fusion_disabled, smoke_client, smoke_transaction};

const FIXTURES_SQL: &str = include_str!("../pg_compat/fixtures.sql");
const PASSED_SQL: &str = include_str!("../pg_compat/passed.sql");
const LIMITATIONS_SQL: &str = include_str!("../pg_compat/limitations.sql");

#[derive(Clone, Copy)]
enum CompareMode {
    Ordered,
    Multiset,
}

struct CompatCase {
    id: String,
    origin: String,
    sql: String,
    compare: CompareMode,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct QueryRow(Vec<Option<String>>);

pub(crate) fn pg_compat_allowlist() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    tx.batch_execute(
        "\
        SET LOCAL statement_timeout = '10s';
        SET LOCAL max_parallel_workers_per_gather = 0
        ",
    )
    .expect("initialize pg_compat session");
    batch_execute_pg_fusion_disabled(&mut tx, FIXTURES_SQL);

    let cases = parse_cases(PASSED_SQL);
    assert!(
        !cases.is_empty(),
        "pg_compat passed corpus must not be empty"
    );
    let limitation_cases = parse_cases(LIMITATIONS_SQL);
    assert!(
        !limitation_cases.is_empty(),
        "pg_compat limitations corpus must not be empty"
    );
    for case in &cases {
        assert_uses_pg_fusion(&mut tx, case);
        let vanilla = query_rows(&mut tx, case, false);
        let fusion = query_rows(&mut tx, case, true);
        assert_same_result(case, vanilla, fusion);
    }
}

fn parse_cases(source: &str) -> Vec<CompatCase> {
    let mut cases = Vec::new();
    let mut id: Option<String> = None;
    let mut origin: Option<String> = None;
    let mut compare: Option<CompareMode> = None;
    let mut sql_lines = Vec::new();

    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(value) = comment_value(trimmed, "id") {
            assert!(
                sql_lines.is_empty(),
                "pg_compat case {} is missing a terminating semicolon",
                id.as_deref().unwrap_or("<unknown>")
            );
            id = Some(value.to_owned());
            origin = None;
            compare = None;
            continue;
        }
        if let Some(value) = comment_value(trimmed, "origin") {
            origin = Some(value.to_owned());
            continue;
        }
        if let Some(value) = comment_value(trimmed, "compare") {
            compare = Some(parse_compare_mode(value));
            continue;
        }
        if trimmed.starts_with("--") {
            continue;
        }

        assert!(
            id.is_some() && origin.is_some() && compare.is_some(),
            "pg_compat SQL statement is missing id/origin/compare metadata: {line}"
        );
        sql_lines.push(line);
        if trimmed.ends_with(';') {
            let sql = sql_lines.join("\n");
            cases.push(CompatCase {
                id: id.take().expect("case id must be present"),
                origin: origin.take().expect("case origin must be present"),
                compare: compare.take().expect("case compare mode must be present"),
                sql,
            });
            sql_lines.clear();
        }
    }

    assert!(
        sql_lines.is_empty(),
        "pg_compat passed corpus ended inside an unterminated SQL statement"
    );
    cases
}

fn comment_value<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    line.strip_prefix("--")
        .map(str::trim_start)?
        .strip_prefix(key)?
        .strip_prefix(':')
        .map(str::trim)
}

fn parse_compare_mode(value: &str) -> CompareMode {
    match value {
        "ordered" => CompareMode::Ordered,
        "multiset" => CompareMode::Multiset,
        other => panic!("unknown pg_compat compare mode: {other}"),
    }
}

fn assert_uses_pg_fusion(tx: &mut Transaction<'_>, case: &CompatCase) {
    tx.batch_execute("SET LOCAL pg_fusion.enable = on")
        .expect("enable pg_fusion for EXPLAIN");
    let explain = tx
        .simple_query(&format!("EXPLAIN {}", case.sql))
        .unwrap_or_else(|err| {
            let message = err
                .as_db_error()
                .map(|db_error| db_error.message().to_owned())
                .unwrap_or_else(|| err.to_string());
            panic!(
                "compat case {} from {} failed during EXPLAIN:\n{}\nerror: {message}",
                case.id, case.origin, case.sql
            )
        })
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        explain.contains("Custom Scan (PgFusionScan)"),
        "compat case {} from {} bypassed pg_fusion:\n{}\nplan:\n{}",
        case.id,
        case.origin,
        case.sql,
        explain
    );
}

fn query_rows(
    tx: &mut Transaction<'_>,
    case: &CompatCase,
    pg_fusion_enabled: bool,
) -> Vec<QueryRow> {
    let enabled = if pg_fusion_enabled { "on" } else { "off" };
    tx.batch_execute(&format!("SET LOCAL pg_fusion.enable = {enabled}"))
        .unwrap_or_else(|err| panic!("set pg_fusion.enable for {} failed: {err}", case.id));
    tx.simple_query(&case.sql)
        .unwrap_or_else(|err| {
            let message = err
                .as_db_error()
                .map(|db_error| db_error.message().to_owned())
                .unwrap_or_else(|| err.to_string());
            panic!(
                "compat case {} from {} failed with pg_fusion.enable={enabled}:\n{}\nerror: {err}",
                case.id,
                case.origin,
                case.sql,
                err = message,
            )
        })
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(QueryRow(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_owned))
                    .collect(),
            )),
            _ => None,
        })
        .collect()
}

fn assert_same_result(case: &CompatCase, mut vanilla: Vec<QueryRow>, mut fusion: Vec<QueryRow>) {
    if matches!(case.compare, CompareMode::Multiset) {
        vanilla.sort();
        fusion.sort();
    }
    assert_eq!(
        vanilla, fusion,
        "compat case {} from {} returned different rows:\n{}",
        case.id, case.origin, case.sql
    );
}
