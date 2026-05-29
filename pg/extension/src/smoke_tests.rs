use control_transport::{AcquireError, BackendSlotLease};
use postgres::{Client, SimpleQueryMessage, Transaction};
use std::io::Read;
use std::thread;
use std::time::{Duration, Instant};

use crate::shmem::attach_control_region;

const WORKER_START_TIMEOUT: Duration = Duration::from_secs(5);
const SMOKE_TEST_ADVISORY_LOCK: i64 = 0x5047_4655_5349_4f4e;

fn ensure_shared_preload(client: &mut Client) {
    let preload = simple_query_first_column_client(client, "SHOW shared_preload_libraries")
        .expect("SHOW shared_preload_libraries must return one row");
    assert!(
        preload
            .split(',')
            .map(str::trim)
            .any(|lib| lib == "pg_fusion"),
        "pg_fusion must be in shared_preload_libraries, got: {preload}"
    );
}

fn wait_for_worker() {
    let region = attach_control_region();
    let deadline = Instant::now() + WORKER_START_TIMEOUT;
    loop {
        match BackendSlotLease::acquire(&region) {
            Ok(mut lease) => {
                lease.release();
                return;
            }
            Err(AcquireError::WorkerOffline | AcquireError::Empty) => {}
            Err(err) => panic!("control transport readiness probe failed: {err}"),
        }
        assert!(
            Instant::now() < deadline,
            "pg_fusion background worker did not publish an online control generation within {:?}",
            WORKER_START_TIMEOUT
        );
        thread::sleep(Duration::from_millis(50));
    }
}

pub(crate) fn smoke_client() -> Client {
    wait_for_worker();
    let (client, _session_id) = pgrx_tests::client().expect("connect to pgrx test cluster");
    client
}

pub(crate) fn smoke_transaction(client: &mut Client) -> Transaction<'_> {
    ensure_shared_preload(client);
    let mut tx = client.transaction().expect("start smoke-test transaction");
    tx.batch_execute(&format!(
        "\
        SELECT pg_advisory_xact_lock({SMOKE_TEST_ADVISORY_LOCK});
        SET LOCAL pg_fusion.enable = on
        "
    ))
    .expect("initialize pg_fusion smoke-test session state");
    tx
}

pub(crate) fn batch_execute_pg_fusion_disabled(tx: &mut Transaction<'_>, sql: &str) {
    tx.batch_execute("SET LOCAL pg_fusion.enable = off")
        .expect("disable pg_fusion during fixture setup");
    tx.batch_execute(sql)
        .expect("fixture setup with pg_fusion disabled must succeed");
    tx.batch_execute("SET LOCAL pg_fusion.enable = on")
        .expect("re-enable pg_fusion after fixture setup");
}

fn simple_query_first_column_client(client: &mut Client, sql: &str) -> Option<String> {
    client
        .simple_query(sql)
        .expect("simple query must succeed")
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
}

fn simple_query_first_column_tx(tx: &mut Transaction<'_>, sql: &str) -> Option<String> {
    tx.simple_query(sql)
        .expect("simple query must succeed")
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
}

pub(crate) fn simple_query_first_column_rows_tx(
    tx: &mut Transaction<'_>,
    sql: &str,
) -> Vec<String> {
    tx.simple_query(sql)
        .expect("simple query must succeed")
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect()
}

fn simple_query_rows_tx(tx: &mut Transaction<'_>, sql: &str) -> Vec<Vec<Option<String>>> {
    tx.simple_query(sql)
        .expect("simple query must succeed")
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

fn copy_out_to_string_tx(tx: &mut Transaction<'_>, sql: &str) -> String {
    let mut reader = tx.copy_out(sql).expect("COPY TO STDOUT must start");
    let mut output = String::new();
    reader
        .read_to_string(&mut output)
        .expect("COPY TO STDOUT output must be readable");
    output
}

fn fusion_scan_metric_summary_tx(tx: &mut Transaction<'_>) -> (i64, i64, i64) {
    let summary = simple_query_first_column_tx(
        tx,
        "\
        SELECT concat(
            coalesce(max(value) FILTER (WHERE metric = 'scan_rows_encoded_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'backend_rows_returned_total'), 0), ',',
            coalesce(max(reset_epoch), 0)
        )
        FROM pg_fusion_metrics()
        ",
    )
    .expect("pg_fusion metric summary must return one row");
    let parts = summary
        .split(',')
        .map(|part| part.parse::<i64>().expect("metric value must be integer"))
        .collect::<Vec<_>>();
    assert_eq!(parts.len(), 3);
    (parts[0], parts[1], parts[2])
}

pub(crate) fn simple_select_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let one: i64 = simple_query_first_column_tx(&mut tx, "SELECT 1::bigint AS one")
        .expect("simple smoke select must return one row")
        .parse()
        .expect("simple smoke select must return one bigint value");
    assert_eq!(one, 1);
}

pub(crate) fn numeric_special_value_error_smoke() {
    let mut client = smoke_client();
    for sql in [
        "SELECT avg('NaN'::numeric)",
        "SELECT avg(CAST('Infinity' AS numeric))",
        "SELECT avg(CAST('-Infinity' AS decimal(38, 10)))",
        "SELECT avg(x::numeric) FROM (VALUES ('1'), ('Infinity')) AS v(x)",
    ] {
        let mut tx = smoke_transaction(&mut client);
        let err = tx
            .simple_query(sql)
            .expect_err("special numeric query must fail with pg_fusion enabled");
        let message = err
            .as_db_error()
            .map(|db_error| db_error.message().to_owned())
            .unwrap_or_else(|| err.to_string());
        assert!(
            message.contains(
                "pg_fusion Decimal128 avg cannot represent PostgreSQL numeric NaN/Infinity values"
            ),
            "unexpected error for {sql}: {message}"
        );
    }
}

pub(crate) fn float_avg_special_value_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);

    let cases = [
        ("SELECT avg('Infinity'::float8)", "Infinity"),
        ("SELECT avg('-Infinity'::float8)", "-Infinity"),
        ("SELECT avg('NaN'::float8)", "NaN"),
        (
            "SELECT avg(x::float8) FROM (VALUES ('Infinity'), ('-Infinity')) AS v(x)",
            "NaN",
        ),
    ];
    for (sql, expected) in cases {
        let actual = simple_query_first_column_tx(&mut tx, sql)
            .unwrap_or_else(|| panic!("float avg smoke query must return one row: {sql}"));
        assert_eq!(actual, expected, "unexpected result for {sql}");
    }
}

pub(crate) fn explain_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let explain =
        simple_query_first_column_tx(&mut tx, "EXPLAIN (FORMAT JSON) SELECT 1::bigint AS one")
            .expect("smoke EXPLAIN must return one row");
    assert!(
        explain.contains("\"Plan\""),
        "unexpected EXPLAIN JSON: {explain}"
    );
    let verbose_simple =
        simple_query_first_column_rows_tx(&mut tx, "EXPLAIN (VERBOSE) SELECT 1::bigint AS one")
            .join("\n");
    assert!(
        verbose_simple.contains("Custom Scan (PgFusionScan)"),
        "verbose EXPLAIN should render pg_fusion custom scan: {verbose_simple}"
    );

    reset_heap_fixture(&mut tx, "pg_fusion_explain_smoke");
    let text_explain = simple_query_first_column_rows_tx(
        &mut tx,
        "EXPLAIN SELECT * FROM pg_fusion_explain_smoke WHERE id = 1",
    )
    .join("\n");
    assert!(
        !text_explain.contains("pg_fusion:"),
        "text EXPLAIN should not render pg_fusion property label: {text_explain}"
    );
    assert!(
        text_explain.contains("PostgreSQL Scan:"),
        "text EXPLAIN should render the DataFusion leaf on a separate line: {text_explain}"
    );
    assert!(
        !text_explain.contains("PostgreSQL Plan:"),
        "text EXPLAIN should not render a redundant nested plan header: {text_explain}"
    );
    let verbose_heap_explain = simple_query_first_column_rows_tx(
        &mut tx,
        "EXPLAIN (VERBOSE) SELECT * FROM pg_fusion_explain_smoke WHERE id = 1",
    )
    .join("\n");
    assert!(
        verbose_heap_explain.contains("PostgreSQL Scan:")
            && verbose_heap_explain.contains("PgFusion Producers:"),
        "verbose heap EXPLAIN should render pg_fusion scan details: {verbose_heap_explain}"
    );
    assert!(
        verbose_heap_explain.contains("sql=\"SELECT"),
        "verbose heap EXPLAIN should keep compiled scan SQL: {verbose_heap_explain}"
    );
    assert!(
        !verbose_heap_explain.contains("scan_id=")
            && !verbose_heap_explain.contains("table_oid=")
            && !verbose_heap_explain.contains("planner_fetch_hint=")
            && !verbose_heap_explain.contains("output_schema="),
        "verbose heap EXPLAIN should omit internal scan metadata: {verbose_heap_explain}"
    );
    assert!(
        verbose_heap_explain.contains("Output: id, payload"),
        "verbose heap EXPLAIN should keep nested PostgreSQL verbose output: {verbose_heap_explain}"
    );
    assert!(
        verbose_heap_explain.contains("PgFusion Producer 0: leader"),
        "verbose heap EXPLAIN should keep producer diagnostics: {verbose_heap_explain}"
    );
}

pub(crate) fn planner_catalog_bypass_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);

    let catalog_explain = simple_query_first_column_rows_tx(
        &mut tx,
        "EXPLAIN SELECT count(*)::bigint FROM pg_catalog.pg_class",
    )
    .join("\n");
    assert!(
        !catalog_explain.contains("Custom Scan (PgFusionScan)"),
        "catalog query should bypass pg_fusion planner: {catalog_explain}"
    );

    let completion_count: i64 = simple_query_first_column_tx(
        &mut tx,
        "\
        SELECT count(*)::bigint
        FROM pg_catalog.pg_class AS c
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        WHERE c.oid = 'pg_catalog.pg_class'::regclass
          AND pg_catalog.pg_table_is_visible(c.oid)
        ",
    )
    .expect("catalog completion-style query must return one row")
    .parse()
    .expect("catalog completion-style query must return a bigint value");
    assert_eq!(completion_count, 1);

    let settings_completion_explain = simple_query_first_column_rows_tx(
        &mut tx,
        "\
        EXPLAIN
        SELECT pg_catalog.lower(name)
        FROM pg_catalog.pg_settings
        WHERE context IN ('user', 'superuser')
          AND pg_catalog.lower(name) LIKE pg_catalog.lower('pg\\_fu%')
        LIMIT 1000
        ",
    )
    .join("\n");
    assert!(
        !settings_completion_explain.contains("Custom Scan (PgFusionScan)"),
        "pg_settings completion query should bypass pg_fusion planner: {settings_completion_explain}"
    );

    let cte_explain = simple_query_first_column_rows_tx(
        &mut tx,
        "\
        EXPLAIN
        WITH catalog_rel AS (
            SELECT oid FROM pg_catalog.pg_class WHERE relname = 'pg_class'
        )
        SELECT count(*)::bigint FROM catalog_rel
        ",
    )
    .join("\n");
    assert!(
        !cte_explain.contains("Custom Scan (PgFusionScan)"),
        "catalog relation inside CTE should bypass pg_fusion planner: {cte_explain}"
    );

    reset_heap_fixture(&mut tx, "pg_fusion_catalog_bypass_user_table");
    let user_explain = simple_query_first_column_rows_tx(
        &mut tx,
        "EXPLAIN SELECT count(*)::bigint FROM pg_fusion_catalog_bypass_user_table",
    )
    .join("\n");
    assert!(
        user_explain.contains("Custom Scan (PgFusionScan)"),
        "user table query should still use pg_fusion planner: {user_explain}"
    );
}

pub(crate) fn planner_bound_params_bypass_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);

    let row = tx
        .query_one("SELECT $1::bigint", &[&42_i64])
        .expect("parameterized query should bypass pg_fusion and execute with vanilla planner");
    let value: i64 = row.get(0);
    assert_eq!(value, 42);

    let table_name = "pgf_bound_params_bypass_smoke";
    batch_execute_pg_fusion_disabled(
        &mut tx,
        &format!(
            "\
            CREATE TABLE {table_name} (id integer NOT NULL);
            INSERT INTO {table_name} (id) VALUES (1)
            "
        ),
    );
    tx.batch_execute(
        "\
        SET LOCAL pg_fusion.frontend_mode = 2;
        SET LOCAL plan_cache_mode = force_generic_plan;
        PREPARE pgf_param_frontend_bypass(integer) AS
            SELECT $1::integer AS v FROM pgf_bound_params_bypass_smoke WHERE id = 1
        ",
    )
    .expect("generic prepared statement with Param nodes should prepare through vanilla planner");

    let prepared_explain = simple_query_first_column_rows_tx(
        &mut tx,
        "EXPLAIN (VERBOSE) EXECUTE pgf_param_frontend_bypass(42)",
    )
    .join("\n");
    assert!(
        !prepared_explain.contains("Custom Scan (PgFusionScan)"),
        "generic prepared Param query should bypass pg_fusion custom scans: {prepared_explain}"
    );

    let row = tx
        .query_one("EXECUTE pgf_param_frontend_bypass(42)", &[])
        .expect("generic prepared Param query should execute through vanilla planner");
    let value: i32 = row.get(0);
    assert_eq!(value, 42);
}

pub(crate) fn frontend_mode_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pgf_frontend_mode_smoke";
    batch_execute_pg_fusion_disabled(
        &mut tx,
        &format!(
            "\
            CREATE TABLE {table_name} (id integer NOT NULL, payload text NOT NULL);
            INSERT INTO {table_name} (id, payload)
            VALUES (1, 'one'), (2, 'two'), (3, 'three')
            "
        ),
    );
    tx.batch_execute("SET LOCAL pg_fusion.frontend_mode = 2")
        .expect("require pg_frontend planning");

    let frontend_explain = simple_query_first_column_rows_tx(
        &mut tx,
        &format!("EXPLAIN (VERBOSE) SELECT id FROM {table_name} WHERE id = 2"),
    )
    .join("\n");
    assert!(
        frontend_explain.contains("Planning Source: pg_frontend"),
        "supported frontend query should use pg_frontend path: {frontend_explain}"
    );

    let only_explain = simple_query_first_column_rows_tx(
        &mut tx,
        &format!("EXPLAIN (VERBOSE) SELECT id FROM ONLY {table_name} WHERE id = 2"),
    )
    .join("\n");
    assert!(
        !only_explain.contains("Custom Scan (PgFusionScan)"),
        "ONLY scans should bypass pg_fusion custom scans: {only_explain}"
    );

    tx.batch_execute("SET LOCAL pg_fusion.frontend_mode = 0")
        .expect("disable pg_frontend planning");
    let sql_text_explain = simple_query_first_column_rows_tx(
        &mut tx,
        &format!("EXPLAIN (VERBOSE) SELECT id FROM {table_name} WHERE id = 2"),
    )
    .join("\n");
    assert!(
        sql_text_explain.contains("Planning Source: sql_text"),
        "frontend_mode=0 should force legacy SQL-text path: {sql_text_explain}"
    );
}

pub(crate) fn copy_select_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_copy_select_smoke";
    reset_heap_fixture(&mut tx, table_name);

    let reset_epoch: i64 =
        simple_query_first_column_tx(&mut tx, "SELECT pg_fusion_metrics_reset()")
            .expect("metrics reset must return an epoch")
            .parse()
            .expect("metrics reset epoch must be an integer");

    let output = copy_out_to_string_tx(
        &mut tx,
        &format!(
            "COPY (
                SELECT id::bigint, payload
                FROM {table_name}
                WHERE id >= 2
                ORDER BY id
            ) TO STDOUT WITH (FORMAT csv)"
        ),
    );
    assert_eq!(output, "2,two\n3,three\n");

    let (rows_encoded, rows_returned, metrics_epoch) = fusion_scan_metric_summary_tx(&mut tx);
    assert!(
        rows_encoded > 0,
        "COPY (SELECT ...) should execute through pg_fusion scan path"
    );
    assert!(
        rows_returned > 0,
        "COPY (SELECT ...) should return rows through pg_fusion custom scan"
    );
    assert_eq!(metrics_epoch, reset_epoch);
}

pub(crate) fn copy_catalog_bypass_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);

    let reset_epoch: i64 =
        simple_query_first_column_tx(&mut tx, "SELECT pg_fusion_metrics_reset()")
            .expect("metrics reset must return an epoch")
            .parse()
            .expect("metrics reset epoch must be an integer");

    let output = copy_out_to_string_tx(
        &mut tx,
        "\
        COPY (
            SELECT name
            FROM pg_catalog.pg_settings
            WHERE name = 'pg_fusion.enable'
            ORDER BY name
        ) TO STDOUT WITH (FORMAT csv)
        ",
    );
    assert_eq!(output, "pg_fusion.enable\n");

    let (rows_encoded, rows_returned, metrics_epoch) = fusion_scan_metric_summary_tx(&mut tx);
    assert_eq!(
        rows_encoded, 0,
        "catalog COPY query should bypass pg_fusion scans"
    );
    assert_eq!(
        rows_returned, 0,
        "catalog COPY query should bypass pg_fusion custom scan"
    );
    assert_eq!(metrics_epoch, reset_epoch);
}

fn reset_heap_fixture(tx: &mut Transaction<'_>, table_name: &str) {
    tx.batch_execute(&format!(
        "CREATE TEMP TABLE {table_name} (id bigint NOT NULL, payload text NOT NULL)"
    ))
    .expect("create temp heap table must succeed");
    tx.batch_execute(&format!(
        "INSERT INTO {table_name} (id, payload) VALUES (1, 'one'), (2, 'two'), (3, 'three')"
    ))
    .expect("insert temp heap fixture rows must succeed");
}

pub(crate) fn heap_select_single_row_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_heap_single_row_smoke";
    reset_heap_fixture(&mut tx, table_name);

    let id: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!("SELECT id::bigint FROM {table_name} WHERE id = 2"),
    )
    .expect("single-row heap select must return one row")
    .parse()
    .expect("single-row heap select must return one bigint value");
    assert_eq!(id, 2);
}

pub(crate) fn heap_select_filtered_row_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_heap_filtered_row_smoke";
    reset_heap_fixture(&mut tx, table_name);

    let id: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!("SELECT id::bigint FROM {table_name} WHERE id > 2"),
    )
    .expect("filtered heap select must return one row")
    .parse()
    .expect("filtered heap select must return one bigint value");
    assert_eq!(id, 3);
}

pub(crate) fn heap_numeric_scan_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_heap_numeric_scan_smoke";
    batch_execute_pg_fusion_disabled(
        &mut tx,
        &format!(
            "\
            CREATE TEMP TABLE {table_name} (
                id integer NOT NULL,
                fixed numeric(12,3),
                bare numeric
            );
            INSERT INTO {table_name} VALUES
                (1, 12.340, 1.23),
                (2, -0.125, 1234567890123456789012.1234567890123456),
                (3, NULL, NULL)
            "
        ),
    );

    let explain = simple_query_first_column_rows_tx(
        &mut tx,
        &format!("EXPLAIN SELECT id, fixed, bare FROM {table_name} ORDER BY id"),
    )
    .join("\n");
    assert!(
        explain.contains("Custom Scan (PgFusionScan)"),
        "numeric scan should execute through pg_fusion: {explain}"
    );

    let rows = simple_query_rows_tx(
        &mut tx,
        &format!("SELECT id, fixed, bare FROM {table_name} ORDER BY id"),
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Some("1".to_owned()),
                Some("12.340".to_owned()),
                Some("1.23".to_owned())
            ],
            vec![
                Some("2".to_owned()),
                Some("-0.125".to_owned()),
                Some("1234567890123456789012.1234567890123456".to_owned())
            ],
            vec![Some("3".to_owned()), None, None],
        ]
    );

    let filtered = simple_query_first_column_rows_tx(
        &mut tx,
        &format!("SELECT id FROM {table_name} WHERE bare > 2 ORDER BY id"),
    );
    assert_eq!(filtered, vec!["2"]);

    let grouped = simple_query_rows_tx(
        &mut tx,
        &format!(
            "\
            SELECT fixed, count(*)::bigint
            FROM {table_name}
            GROUP BY fixed
            ORDER BY fixed NULLS LAST
            "
        ),
    );
    assert_eq!(
        grouped,
        vec![
            vec![Some("-0.125".to_owned()), Some("1".to_owned())],
            vec![Some("12.340".to_owned()), Some("1".to_owned())],
            vec![None, Some("1".to_owned())],
        ]
    );
}

pub(crate) fn heap_numeric_scan_error_smoke() {
    let mut client = smoke_client();
    for (table_name, insert_sql, expected) in [
        (
            "pg_temp.pgf_heap_numeric_nan_scan_smoke",
            "INSERT INTO pg_temp.pgf_heap_numeric_nan_scan_smoke VALUES (1::numeric), ('NaN'::numeric)",
            "numeric NaN/Infinity",
        ),
        (
            "pg_temp.pgf_heap_numeric_precision_scan_smoke",
            "INSERT INTO pg_temp.pgf_heap_numeric_precision_scan_smoke VALUES (0.12345678901234567::numeric)",
            "Decimal128(38, 16)",
        ),
    ] {
        let mut tx = smoke_transaction(&mut client);
        batch_execute_pg_fusion_disabled(
            &mut tx,
            &format!("CREATE TEMP TABLE {table_name} (n numeric); {insert_sql};"),
        );
        let err = tx
            .simple_query(&format!("SELECT n FROM {table_name}"))
            .expect_err("unsupported numeric scan must fail");
        let message = err
            .as_db_error()
            .map(|db_error| db_error.message().to_owned())
            .unwrap_or_else(|| err.to_string());
        assert!(
            message.contains(expected),
            "unexpected numeric scan error for {table_name}: {message}"
        );
    }
}

pub(crate) fn heap_avg_full_scan_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_heap_avg_full_scan_smoke";
    batch_execute_pg_fusion_disabled(
        &mut tx,
        &format!(
            "CREATE TEMP TABLE {table_name} AS \
         SELECT generate_series(1, 50000)::bigint AS a"
        ),
    );

    let avg =
        simple_query_first_column_tx(&mut tx, &format!("SELECT avg(a)::text FROM {table_name}"))
            .expect("heap avg full scan must return one row");
    assert_eq!(avg, "25000.5000000000000000");

    batch_execute_pg_fusion_disabled(
        &mut tx,
        &format!(
            "TRUNCATE {table_name}; \
             INSERT INTO {table_name} VALUES (9007199254740992), (9007199254740993)"
        ),
    );
    let exact_avg =
        simple_query_first_column_tx(&mut tx, &format!("SELECT avg(a)::text FROM {table_name}"))
            .expect("heap avg exactness check must return one row");
    assert_eq!(exact_avg, "9007199254740992.5000000000000000");
}

pub(crate) fn heap_avg_window_sliding_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_heap_avg_window_sliding_smoke";
    batch_execute_pg_fusion_disabled(
        &mut tx,
        &format!(
            "\
            CREATE TEMP TABLE {table_name} (
                i integer,
                bigint_value bigint
            );
            INSERT INTO {table_name} VALUES
                (1, 1),
                (2, 2),
                (3, NULL),
                (4, NULL)
            "
        ),
    );

    let bigint_rows = simple_query_first_column_rows_tx(
        &mut tx,
        &format!(
            "\
            SELECT i::text || ':' || COALESCE(
                avg(bigint_value) OVER (
                    ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                )::text,
                'NULL'
            )
            FROM {table_name}
            ORDER BY i
            "
        ),
    );
    assert_eq!(
        bigint_rows,
        vec![
            "1:1.5000000000000000",
            "2:2.0000000000000000",
            "3:NULL",
            "4:NULL"
        ]
    );

    let numeric_rows = simple_query_first_column_rows_tx(
        &mut tx,
        "\
            SELECT i::text || ':' || COALESCE(
                avg(v::numeric) OVER (
                    ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                )::text,
                'NULL'
            )
            FROM (VALUES (1, 1.5), (2, 2.5), (3, NULL), (4, NULL)) AS t(i, v)
            ORDER BY i
        ",
    );
    assert_eq!(
        numeric_rows,
        vec![
            "1:2.0000000000000000",
            "2:2.5000000000000000",
            "3:NULL",
            "4:NULL"
        ]
    );
}

pub(crate) fn heap_interval_avg_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_heap_interval_avg_smoke";
    batch_execute_pg_fusion_disabled(
        &mut tx,
        &format!(
            "\
            CREATE TEMP TABLE {table_name} (
                i integer,
                v interval
            );
            INSERT INTO {table_name} VALUES
                (1, interval '1 month'),
                (2, interval '2 months'),
                (3, interval '1 day'),
                (4, NULL)
            "
        ),
    );

    let avg_sql = format!("SELECT avg(v)::text FROM {table_name}");
    tx.batch_execute("SET LOCAL pg_fusion.enable = off")
        .expect("disable pg_fusion for expected interval avg");
    let expected_avg = simple_query_first_column_tx(&mut tx, &avg_sql)
        .expect("vanilla interval avg must return one row");
    tx.batch_execute("SET LOCAL pg_fusion.enable = on")
        .expect("re-enable pg_fusion for interval avg");
    let actual_avg = simple_query_first_column_tx(&mut tx, &avg_sql)
        .expect("pg_fusion interval avg must return one row");
    assert_eq!(actual_avg, expected_avg);

    let window_sql = format!(
        "\
        SELECT i::text || ':' || COALESCE(
            avg(v) OVER (
                ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )::text,
            'NULL'
        )
        FROM {table_name}
        ORDER BY i
        "
    );
    tx.batch_execute("SET LOCAL pg_fusion.enable = off")
        .expect("disable pg_fusion for expected interval window avg");
    let expected_window = simple_query_first_column_rows_tx(&mut tx, &window_sql);
    tx.batch_execute("SET LOCAL pg_fusion.enable = on")
        .expect("re-enable pg_fusion for interval window avg");
    let actual_window = simple_query_first_column_rows_tx(&mut tx, &window_sql);
    assert_eq!(actual_window, expected_window);
}

pub(crate) fn heap_varlena_full_scan_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_heap_varlena_full_scan_smoke";
    batch_execute_pg_fusion_disabled(
        &mut tx,
        &format!(
            "\
        CREATE TEMP TABLE {table_name} AS
        SELECT
            g::bigint AS id,
            repeat(md5(g::text), 16) AS payload
        FROM generate_series(1, 5000) AS g
        "
        ),
    );

    let summary = simple_query_first_column_tx(
        &mut tx,
        &format!("SELECT concat(count(payload), ',', sum(id)) FROM {table_name}"),
    )
    .expect("heap varlena full scan must return one row");
    assert_eq!(summary, "5000,12502500");
}

pub(crate) fn heap_join_two_tables_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let left_table = "pg_temp.pgf_heap_join_left";
    let right_table = "pg_temp.pgf_heap_join_right";

    tx.batch_execute(&format!(
        "\
        CREATE TEMP TABLE {left_table} (
            id bigint NOT NULL,
            payload text NOT NULL
        );
        CREATE TEMP TABLE {right_table} (
            left_id bigint NOT NULL,
            score bigint NOT NULL,
            marker text NOT NULL
        );
        INSERT INTO {left_table} (id, payload)
        VALUES (1, 'one'), (2, 'two'), (3, 'three');
        INSERT INTO {right_table} (left_id, score, marker)
        VALUES (2, 20, 'dos'), (3, 30, 'tres'), (4, 40, 'cuatro');
        "
    ))
    .expect("create and populate temp heap join fixture must succeed");

    let joined_count: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!(
            "SELECT count(*)::bigint \
             FROM {left_table} AS l \
             JOIN {right_table} AS r ON l.id = r.left_id"
        ),
    )
    .expect("heap join count must return one row")
    .parse()
    .expect("heap join count must return one bigint value");
    assert_eq!(joined_count, 2);

    let score: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!(
            "SELECT r.score::bigint \
             FROM {left_table} AS l \
             JOIN {right_table} AS r ON l.id = r.left_id \
             WHERE l.payload = 'two'"
        ),
    )
    .expect("filtered heap join must return one row")
    .parse()
    .expect("filtered heap join must return one bigint value");
    assert_eq!(score, 20);
}

pub(crate) fn heap_multi_use_cte_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    batch_execute_pg_fusion_disabled(
        &mut tx,
        "\
        CREATE TEMP TABLE pgf_cte_lineitem (
            l_suppkey bigint NOT NULL,
            l_extendedprice double precision NOT NULL,
            l_discount double precision NOT NULL,
            l_shipdate text NOT NULL
        );
        CREATE TEMP TABLE pgf_cte_supplier (
            s_suppkey bigint NOT NULL,
            s_name text NOT NULL
        );
        INSERT INTO pgf_cte_supplier VALUES
            (1, 'supplier#1'),
            (2, 'supplier#2');
        INSERT INTO pgf_cte_lineitem VALUES
            (1, 100.0, 0.10, '1996-01-10'),
            (1, 200.0, 0.00, '1996-02-10'),
            (2, 500.0, 0.10, '1996-01-20'),
            (2, 100.0, 0.00, '1996-03-20'),
            (2, 1000.0, 0.00, '1996-06-01')
        ",
    );

    let supplier = simple_query_first_column_tx(
        &mut tx,
        "\
        WITH revenue AS (
            SELECT
                l_suppkey AS supplier_no,
                sum(l_extendedprice * (1.0 - l_discount)) AS total_revenue
            FROM pgf_cte_lineitem
            WHERE l_shipdate >= '1996-01-01'
              AND l_shipdate < '1996-04-01'
            GROUP BY l_suppkey
        )
        SELECT s.s_name
        FROM pgf_cte_supplier s
        JOIN revenue ON s.s_suppkey = revenue.supplier_no
        WHERE revenue.total_revenue = (SELECT max(total_revenue) FROM revenue)
        ORDER BY s.s_suppkey
        ",
    )
    .expect("multi-use CTE query must return one row");
    assert_eq!(supplier, "supplier#2");

    let explain = simple_query_first_column_rows_tx(
        &mut tx,
        "\
        EXPLAIN
        WITH revenue AS (
            SELECT
                l_suppkey AS supplier_no,
                sum(l_extendedprice * (1.0 - l_discount)) AS total_revenue
            FROM pgf_cte_lineitem
            WHERE l_shipdate >= '1996-01-01'
              AND l_shipdate < '1996-04-01'
            GROUP BY l_suppkey
        )
        SELECT s.s_name
        FROM pgf_cte_supplier s
        JOIN revenue ON s.s_suppkey = revenue.supplier_no
        WHERE revenue.total_revenue = (SELECT max(total_revenue) FROM revenue)
        ",
    )
    .join("\n");
    assert!(
        explain.contains("CteScanExec: revenue"),
        "EXPLAIN should expose materialized multi-use CTE reads: {explain}"
    );
}

pub(crate) fn heap_parallel_scan_smoke() {
    let mut client = smoke_client();
    ensure_shared_preload(&mut client);
    let table_name = "public.pgf_parallel_scan_smoke";
    client
        .batch_execute(&format!(
            "\
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} AS
            SELECT g::bigint AS id, (g * 10)::bigint AS payload
            FROM generate_series(1, 20000) AS g;
            ANALYZE {table_name};
            "
        ))
        .expect("create committed heap table for parallel scan smoke");

    let mut tx = smoke_transaction(&mut client);
    tx.batch_execute(
        "\
        SET LOCAL statement_timeout = '20s';
        SET LOCAL max_parallel_workers_per_gather = 2
        ",
    )
    .expect("enable dynamic scan workers");
    let explain = simple_query_first_column_rows_tx(
        &mut tx,
        &format!("EXPLAIN SELECT sum(id)::bigint FROM {table_name} WHERE id BETWEEN 100 AND 20000"),
    )
    .join("\n");
    assert!(
        explain.contains("PgFusion Producers: planned=3 (leader + 2 workers)")
            && explain.contains("strategy=ctid_range"),
        "EXPLAIN should show planned dynamic scan producers: {explain}"
    );
    let verbose_explain = simple_query_first_column_rows_tx(
        &mut tx,
        &format!(
            "EXPLAIN (VERBOSE) SELECT sum(id)::bigint FROM {table_name} WHERE id BETWEEN 100 AND 20000"
        ),
    )
    .join("\n");
    assert!(
        verbose_explain.contains("sql=\"SELECT *")
            && verbose_explain.contains("ctid >= $1::tid")
            && verbose_explain.contains("ctid < $2::tid"),
        "verbose EXPLAIN should show parameterized representative CTID producer SQL: {verbose_explain}"
    );
    assert!(
        verbose_explain.contains("Tid Range Scan") && !verbose_explain.contains("Gather"),
        "verbose EXPLAIN should render representative CTID producer plan: {verbose_explain}"
    );
    assert!(
        !verbose_explain.contains("ctid >= '(") && !verbose_explain.contains("ctid < '("),
        "verbose EXPLAIN should not expose concrete representative CTID bounds: {verbose_explain}"
    );
    let analyze_explain = simple_query_first_column_rows_tx(
        &mut tx,
        &format!(
            "EXPLAIN ANALYZE SELECT sum(id)::bigint FROM {table_name} WHERE id BETWEEN 100 AND 20000"
        ),
    )
    .join("\n");
    assert!(
        analyze_explain.contains(
            "PgFusion Producers: planned=3 (leader + 2 workers), actual=3 (leader + 2 workers)"
        ),
        "EXPLAIN ANALYZE should show actual dynamic scan producers: {analyze_explain}"
    );
    simple_query_first_column_tx(&mut tx, "SELECT pg_fusion_metrics_reset()")
        .expect("reset metrics before parallel scan");
    let sum: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!("SELECT sum(id)::bigint FROM {table_name} WHERE id BETWEEN 100 AND 20000"),
    )
    .expect("parallel heap scan must return one row")
    .parse()
    .expect("parallel heap scan sum must be an integer");
    assert_eq!(sum, 20000 * 20001 / 2 - 99 * 100 / 2);

    let summary = simple_query_first_column_tx(
        &mut tx,
        "\
        SELECT concat(
            coalesce(max(value) FILTER (WHERE metric = 'scan_page_fill_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_fetch_calls_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_pages_sent_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_rows_encoded_total'), 0)
        )
        FROM pg_fusion_metrics()
        ",
    )
    .expect("parallel scan metrics summary must return one row");
    let parts = summary
        .split(',')
        .map(|part| part.parse::<i64>().expect("metric value must be integer"))
        .collect::<Vec<_>>();
    assert_eq!(parts.len(), 4);
    assert!(
        parts[0] > 0 && parts[1] > 0 && parts[2] > 0 && parts[3] > 0,
        "parallel scan metrics must be positive: {summary}"
    );
    tx.commit()
        .expect("commit parallel scan smoke transaction before cleanup");
    client
        .batch_execute(&format!("DROP TABLE IF EXISTS {table_name}"))
        .expect("drop committed heap table for parallel scan smoke");
}

pub(crate) fn heap_parallel_scan_search_path_smoke() {
    let mut client = smoke_client();
    ensure_shared_preload(&mut client);
    let schema_name = "pgf_parallel_search_path_smoke";
    client
        .batch_execute(&format!(
            "\
            DROP SCHEMA IF EXISTS {schema_name} CASCADE;
            CREATE SCHEMA {schema_name};
            CREATE TABLE {schema_name}.items AS
            SELECT g::bigint AS id, (g * 10)::bigint AS payload
            FROM generate_series(1, 20000) AS g;
            ANALYZE {schema_name}.items;
            "
        ))
        .expect("create committed search_path fixture for parallel scan smoke");

    let mut tx = smoke_transaction(&mut client);
    tx.batch_execute(&format!(
        "\
        SET LOCAL statement_timeout = '20s';
        SET LOCAL max_parallel_workers_per_gather = 2;
        SET LOCAL search_path = {schema_name}, public
        "
    ))
    .expect("enable dynamic scan workers with non-public search_path");
    let sum: i64 = simple_query_first_column_tx(
        &mut tx,
        "SELECT sum(id)::bigint FROM items WHERE id BETWEEN 100 AND 20000",
    )
    .expect("parallel heap scan through search_path must return one row")
    .parse()
    .expect("parallel heap scan sum must be an integer");
    assert_eq!(sum, 20000 * 20001 / 2 - 99 * 100 / 2);
    tx.commit()
        .expect("commit parallel search_path scan smoke transaction before cleanup");
    client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        .expect("drop search_path fixture for parallel scan smoke");
}

pub(crate) fn heap_parallel_worker_budget_smoke() {
    let mut client = smoke_client();
    ensure_shared_preload(&mut client);
    let tables = [
        "public.pgf_worker_budget_a",
        "public.pgf_worker_budget_b",
        "public.pgf_worker_budget_c",
        "public.pgf_worker_budget_d",
    ];
    client
        .batch_execute(&format!(
            "\
            DROP TABLE IF EXISTS {a};
            DROP TABLE IF EXISTS {b};
            DROP TABLE IF EXISTS {c};
            DROP TABLE IF EXISTS {d};
            CREATE TABLE {a} AS
            SELECT g::bigint AS id, (g * 10)::bigint AS payload
            FROM generate_series(1, 8000) AS g;
            CREATE TABLE {b} AS SELECT * FROM {a};
            CREATE TABLE {c} AS SELECT * FROM {a};
            CREATE TABLE {d} AS SELECT * FROM {a};
            ANALYZE {a};
            ANALYZE {b};
            ANALYZE {c};
            ANALYZE {d};
            ",
            a = tables[0],
            b = tables[1],
            c = tables[2],
            d = tables[3],
        ))
        .expect("create committed heap tables for worker budget smoke");

    let mut tx = smoke_transaction(&mut client);
    tx.batch_execute(
        "\
        SET LOCAL statement_timeout = '30s';
        SET LOCAL max_parallel_workers_per_gather = 32
        ",
    )
    .expect("request more dynamic scan workers than a small cluster can launch per leaf");
    let count: i64 = simple_query_first_column_tx(
        &mut tx,
        "\
        SELECT count(*)::bigint
        FROM public.pgf_worker_budget_a a
        JOIN public.pgf_worker_budget_b b ON a.id = b.id
        JOIN public.pgf_worker_budget_c c ON a.id = c.id
        JOIN public.pgf_worker_budget_d d ON a.id = d.id
        WHERE a.id BETWEEN 100 AND 8000
        ",
    )
    .expect("multi-scan worker budget query must return one row")
    .parse()
    .expect("multi-scan worker budget count must be an integer");
    assert_eq!(count, 8000 - 99);
    tx.commit()
        .expect("commit worker budget smoke transaction before cleanup");
    client
        .batch_execute(&format!(
            "\
            DROP TABLE IF EXISTS {a};
            DROP TABLE IF EXISTS {b};
            DROP TABLE IF EXISTS {c};
            DROP TABLE IF EXISTS {d};
            ",
            a = tables[0],
            b = tables[1],
            c = tables[2],
            d = tables[3],
        ))
        .expect("drop committed heap tables for worker budget smoke");
}

pub(crate) fn heap_leader_only_scan_smoke() {
    let mut client = smoke_client();
    ensure_shared_preload(&mut client);
    let table_name = "public.pgf_leader_only_scan_smoke";
    client
        .batch_execute(&format!(
            "\
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} AS
            SELECT g::bigint AS id, (g * 10)::bigint AS payload
            FROM generate_series(1, 1000) AS g;
            ANALYZE {table_name};
            "
        ))
        .expect("create committed heap table for leader-only scan smoke");

    let mut tx = smoke_transaction(&mut client);
    tx.batch_execute(
        "\
        SET LOCAL statement_timeout = '20s';
        SET LOCAL max_parallel_workers_per_gather = 0
        ",
    )
    .expect("disable dynamic scan workers through PostgreSQL parallel worker GUC");
    let explain = simple_query_first_column_rows_tx(
        &mut tx,
        &format!("EXPLAIN SELECT sum(id)::bigint FROM {table_name} WHERE id BETWEEN 10 AND 1000"),
    )
    .join("\n");
    assert!(
        explain.contains("PgFusion Producers: planned=1 (leader-only)")
            && explain.contains("reason=worker_budget_zero"),
        "EXPLAIN should show leader-only scan producer reason: {explain}"
    );
    let sum: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!("SELECT sum(id)::bigint FROM {table_name} WHERE id BETWEEN 10 AND 1000"),
    )
    .expect("leader-only heap scan must return one row")
    .parse()
    .expect("leader-only heap scan sum must be an integer");
    assert_eq!(sum, 1000 * 1001 / 2 - 9 * 10 / 2);
    tx.commit()
        .expect("commit leader-only scan smoke transaction before cleanup");
    client
        .batch_execute(&format!("DROP TABLE IF EXISTS {table_name}"))
        .expect("drop committed heap table for leader-only scan smoke");
}

pub(crate) fn metrics_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_metrics_smoke";
    reset_heap_fixture(&mut tx, table_name);
    let worker_memory_limit =
        simple_query_first_column_tx(&mut tx, "SHOW pg_fusion.worker_memory_limit_mb")
            .expect("worker memory limit GUC must be visible");

    let before_epoch: i64 =
        simple_query_first_column_tx(&mut tx, "SELECT pg_fusion_metrics_reset()")
            .expect("metrics reset must return an epoch")
            .parse()
            .expect("metrics reset epoch must be an integer");

    let id: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!("SELECT id::bigint FROM {table_name} WHERE id = 1"),
    )
    .expect("metrics smoke query must return one row")
    .parse()
    .expect("metrics smoke query must return one bigint value");
    assert_eq!(id, 1);

    let summary = simple_query_first_column_tx(
        &mut tx,
        "\
        SELECT concat(
            coalesce(max(value) FILTER (WHERE metric = 'scan_pages_sent_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_bytes_sent_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_fetch_calls_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_rows_encoded_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_page_fill_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_page_prepare_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_page_finish_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_page_retry_total'), 0), ',',
            count(*) FILTER (WHERE metric IN (
                'scan_page_snapshot_ns',
                'scan_slot_drain_ns',
                'scan_overflow_copy_ns',
                'scan_page_retry_ns',
                'scan_fill_pre_drain_ns',
                'scan_fill_post_drain_ns',
                'scan_fill_overflow_encode_ns',
                'scan_fill_emit_ns',
                'scan_fill_unclassified_ns'
            )), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_batch_send_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_batch_delivery_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_idle_sleep_total'), 0), ',',
            count(*) FILTER (WHERE metric IN (
                'scan_batch_send_ns',
                'scan_batch_send_total',
                'scan_batch_delivery_ns',
                'scan_batch_delivery_total',
                'scan_idle_sleep_ns',
                'scan_idle_sleep_total'
            )), ',',
            coalesce(max(value) FILTER (WHERE metric = 'result_pages_read_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'backend_rows_returned_total'), 0), ',',
            count(*) FILTER (WHERE metric IN (
                'worker_spill_count_total',
                'worker_spilled_rows_total',
                'worker_spilled_bytes_total',
                'worker_spill_leaked_files_total',
                'worker_spill_leaked_bytes_total',
                'worker_spill_dirs_created_total',
                'worker_spill_dirs_removed_total',
                'worker_spill_cleanup_errors_total'
            )), ',',
            coalesce(sum(value) FILTER (WHERE metric IN (
                'worker_spill_count_total',
                'worker_spilled_rows_total',
                'worker_spilled_bytes_total',
                'worker_spill_leaked_files_total',
                'worker_spill_leaked_bytes_total',
                'worker_spill_dirs_created_total',
                'worker_spill_dirs_removed_total',
                'worker_spill_cleanup_errors_total'
            )), 0), ',',
            coalesce(max(reset_epoch), 0)
        )
        FROM pg_fusion_metrics()
        ",
    )
    .expect("metrics summary must return one row");
    let parts = summary
        .split(',')
        .map(|part| part.parse::<i64>().expect("metric value must be integer"))
        .collect::<Vec<_>>();
    assert_eq!(parts.len(), 18);
    assert!(
        parts[0] > 0,
        "scan_pages_sent_total must be positive: {summary}"
    );
    assert!(
        parts[1] > 0,
        "scan_bytes_sent_total must be positive: {summary}"
    );
    assert!(
        parts[2] > 0,
        "scan_fetch_calls_total must be positive: {summary}"
    );
    assert!(
        parts[3] > 0,
        "scan_rows_encoded_total must be positive: {summary}"
    );
    assert!(
        parts[4] > 0,
        "scan_page_fill_ns must be positive: {summary}"
    );
    assert!(
        parts[5] > 0,
        "scan_page_prepare_ns must be positive: {summary}"
    );
    assert!(
        parts[6] > 0,
        "scan_page_finish_ns must be positive: {summary}"
    );
    assert_eq!(
        parts[7], 0,
        "scan_page_retry_total must stay zero for a simple one-row scan: {summary}"
    );
    assert_eq!(
        parts[8], 0,
        "removed detailed scan timing metric rows must not be exposed: {summary}"
    );
    assert!(
        parts[9] > 0,
        "scan_batch_send_total must be positive: {summary}"
    );
    assert!(
        parts[10] > 0,
        "scan_batch_delivery_total must be positive: {summary}"
    );
    assert_eq!(
        parts[12], 6,
        "all worker scan-thread metric rows must be present: {summary}"
    );
    assert!(
        parts[13] > 0,
        "result_pages_read_total must be positive: {summary}"
    );
    assert!(
        parts[14] > 0,
        "backend_rows_returned_total must be positive: {summary}"
    );
    assert_eq!(
        parts[15], 8,
        "all worker spill metric rows must be present: {summary}"
    );
    if worker_memory_limit == "0" {
        assert_eq!(
            parts[16], 0,
            "worker spill metrics must stay zero when worker spill is disabled: {summary}"
        );
    }
    assert_eq!(parts[17], before_epoch);

    let after_epoch: i64 =
        simple_query_first_column_tx(&mut tx, "SELECT pg_fusion_metrics_reset()")
            .expect("second metrics reset must return an epoch")
            .parse()
            .expect("second metrics reset epoch must be an integer");
    assert!(after_epoch > before_epoch);
}

pub(crate) fn runtime_filter_uuid_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let build_table = "pg_temp.pgf_runtime_filter_uuid_build";
    let probe_table = "pg_temp.pgf_runtime_filter_uuid_probe";
    batch_execute_pg_fusion_disabled(
        &mut tx,
        &format!(
            "\
            CREATE TEMP TABLE {build_table} AS
            SELECT ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid AS u
            FROM generate_series(1, 3) AS g;

            CREATE TEMP TABLE {probe_table} AS
            SELECT ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid AS u
            FROM generate_series(1, 100000) AS g;
            "
        ),
    );

    let before_epoch: i64 =
        simple_query_first_column_tx(&mut tx, "SELECT pg_fusion_metrics_reset()")
            .expect("runtime filter metrics reset must return an epoch")
            .parse()
            .expect("runtime filter metrics reset epoch must be an integer");

    let count: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!(
            "\
            SELECT count(*)::bigint
            FROM {build_table} AS b
            JOIN {probe_table} AS p ON b.u = p.u
            "
        ),
    )
    .expect("uuid runtime filter join must return one row")
    .parse()
    .expect("uuid runtime filter join count must be an integer");
    assert_eq!(count, 3);

    let summary = simple_query_first_column_tx(
        &mut tx,
        "\
        SELECT concat(
            coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_allocated_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_ready_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_build_rows_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_probe_rows_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_probe_rows_rejected_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_probe_pass_unfiltered_total'), 0), ',',
            coalesce(max(reset_epoch), 0)
        )
        FROM pg_fusion_metrics()
        ",
    )
    .expect("runtime filter metric summary must return one row");
    let parts = summary
        .split(',')
        .map(|part| {
            part.parse::<i64>()
                .expect("runtime filter metric value must be integer")
        })
        .collect::<Vec<_>>();
    assert_eq!(parts.len(), 7);
    assert!(
        parts[0] > 0,
        "runtime filter should be allocated for uuid join: {summary}"
    );
    assert!(
        parts[1] > 0,
        "runtime filter should become ready for uuid join: {summary}"
    );
    assert!(
        parts[2] >= 3,
        "runtime filter should observe uuid build rows: {summary}"
    );
    assert!(
        parts[3] >= 100000,
        "runtime filter should probe uuid rows: {summary}"
    );
    assert!(
        parts[4] > 0,
        "runtime filter should reject non-matching uuid probe rows: {summary}"
    );
    assert_eq!(parts[6], before_epoch);
}

pub(crate) fn runtime_filter_bytea_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let build_table = "pg_temp.pgf_runtime_filter_bytea_build";
    let probe_table = "pg_temp.pgf_runtime_filter_bytea_probe";
    batch_execute_pg_fusion_disabled(
        &mut tx,
        &format!(
            "\
            CREATE TEMP TABLE {build_table} AS
            SELECT decode(lpad(to_hex(g), 32, '0'), 'hex') AS b
            FROM generate_series(1, 3) AS g;

            CREATE TEMP TABLE {probe_table} AS
            SELECT decode(lpad(to_hex(g), 32, '0'), 'hex') AS b
            FROM generate_series(1, 100000) AS g;
            "
        ),
    );

    let before_epoch: i64 =
        simple_query_first_column_tx(&mut tx, "SELECT pg_fusion_metrics_reset()")
            .expect("runtime filter metrics reset must return an epoch")
            .parse()
            .expect("runtime filter metrics reset epoch must be an integer");

    let count: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!(
            "\
            SELECT count(*)::bigint
            FROM {build_table} AS b
            JOIN {probe_table} AS p ON b.b = p.b
            "
        ),
    )
    .expect("bytea runtime filter join must return one row")
    .parse()
    .expect("bytea runtime filter join count must be an integer");
    assert_eq!(count, 3);

    let summary = simple_query_first_column_tx(
        &mut tx,
        "\
        SELECT concat(
            coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_allocated_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_ready_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_build_rows_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_probe_rows_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_probe_rows_rejected_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_probe_pass_unfiltered_total'), 0), ',',
            coalesce(max(reset_epoch), 0)
        )
        FROM pg_fusion_metrics()
        ",
    )
    .expect("runtime filter metric summary must return one row");
    let parts = summary
        .split(',')
        .map(|part| {
            part.parse::<i64>()
                .expect("runtime filter metric value must be integer")
        })
        .collect::<Vec<_>>();
    assert_eq!(parts.len(), 7);
    assert!(
        parts[0] > 0,
        "runtime filter should be allocated for bytea join: {summary}"
    );
    assert!(
        parts[1] > 0,
        "runtime filter should become ready for bytea join: {summary}"
    );
    assert!(
        parts[2] >= 3,
        "runtime filter should observe bytea build rows: {summary}"
    );
    assert!(
        parts[3] >= 100000,
        "runtime filter should probe bytea rows: {summary}"
    );
    assert!(
        parts[4] > 0,
        "runtime filter should reject non-matching bytea probe rows: {summary}"
    );
    assert_eq!(parts[6], before_epoch);
}

pub(crate) fn runtime_filter_temporal_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let cases = [
        (
            "date",
            "(DATE '2024-01-01' + (g::int - 1))::date",
        ),
        (
            "time",
            "(TIME '00:00:00' + (g::text || ' microseconds')::interval)::time",
        ),
        (
            "timestamp",
            "(TIMESTAMP '2024-01-01 00:00:00' + (g::text || ' microseconds')::interval)::timestamp",
        ),
        (
            "timestamptz",
            "(TIMESTAMPTZ '2024-01-01 00:00:00+00' + (g::text || ' microseconds')::interval)::timestamptz",
        ),
        (
            "interval",
            "(make_interval(months => g::int, days => (g % 31)::int) + (g::text || ' microseconds')::interval)",
        ),
    ];

    for (name, expr) in cases {
        let build_table = format!("pg_temp.pgf_runtime_filter_{name}_build");
        let probe_table = format!("pg_temp.pgf_runtime_filter_{name}_probe");
        batch_execute_pg_fusion_disabled(
            &mut tx,
            &format!(
                "\
                CREATE TEMP TABLE {build_table} AS
                SELECT {expr} AS k
                FROM generate_series(1, 3) AS g;

                CREATE TEMP TABLE {probe_table} AS
                SELECT {expr} AS k
                FROM generate_series(1, 100000) AS g;
                "
            ),
        );

        let before_epoch: i64 =
            simple_query_first_column_tx(&mut tx, "SELECT pg_fusion_metrics_reset()")
                .expect("runtime filter metrics reset must return an epoch")
                .parse()
                .expect("runtime filter metrics reset epoch must be an integer");

        let count: i64 = simple_query_first_column_tx(
            &mut tx,
            &format!(
                "\
                SELECT count(*)::bigint
                FROM {build_table} AS b
                JOIN {probe_table} AS p ON b.k = p.k
                "
            ),
        )
        .unwrap_or_else(|| panic!("{name} runtime filter join must return one row"))
        .parse()
        .unwrap_or_else(|err| panic!("{name} runtime filter join count must be an integer: {err}"));
        assert_eq!(count, 3, "{name} runtime filter join count");

        let summary = simple_query_first_column_tx(
            &mut tx,
            "\
            SELECT concat(
                coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_allocated_total'), 0), ',',
                coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_ready_total'), 0), ',',
                coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_build_rows_total'), 0), ',',
                coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_probe_rows_total'), 0), ',',
                coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_probe_rows_rejected_total'), 0), ',',
                coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_probe_pass_unfiltered_total'), 0), ',',
                coalesce(max(reset_epoch), 0)
            )
            FROM pg_fusion_metrics()
            ",
        )
        .expect("runtime filter metric summary must return one row");
        let parts = summary
            .split(',')
            .map(|part| {
                part.parse::<i64>()
                    .expect("runtime filter metric value must be integer")
            })
            .collect::<Vec<_>>();
        assert_eq!(parts.len(), 7);
        assert!(
            parts[0] > 0,
            "runtime filter should be allocated for {name} join: {summary}"
        );
        assert!(
            parts[1] > 0,
            "runtime filter should become ready for {name} join: {summary}"
        );
        assert!(
            parts[2] >= 3,
            "runtime filter should observe {name} build rows: {summary}"
        );
        assert!(
            parts[3] >= 100000,
            "runtime filter should probe {name} rows: {summary}"
        );
        assert!(
            parts[4] > 0,
            "runtime filter should reject non-matching {name} probe rows: {summary}"
        );
        assert_eq!(parts[6], before_epoch);
    }
}

pub(crate) fn runtime_filter_numeric_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let cases = [
        ("numeric_fixed", "(g::numeric(12,3))", "(g::numeric(12,3))"),
        ("numeric_bare", "(g::numeric)", "(g::numeric)"),
        ("numeric_fixed_bare", "(g::numeric(12,3))", "(g::numeric)"),
        (
            "numeric_fraction",
            "((g::numeric / 100)::numeric(12,3))",
            "((g::numeric / 100)::numeric(38,16))",
        ),
    ];

    for (name, build_expr, probe_expr) in cases {
        let build_table = format!("pg_temp.pgf_runtime_filter_{name}_build");
        let probe_table = format!("pg_temp.pgf_runtime_filter_{name}_probe");
        batch_execute_pg_fusion_disabled(
            &mut tx,
            &format!(
                "\
                CREATE TEMP TABLE {build_table} AS
                SELECT {build_expr} AS k
                FROM generate_series(1, 3) AS g;

                CREATE TEMP TABLE {probe_table} AS
                SELECT {probe_expr} AS k
                FROM generate_series(1, 100000) AS g;
                "
            ),
        );

        let before_epoch: i64 =
            simple_query_first_column_tx(&mut tx, "SELECT pg_fusion_metrics_reset()")
                .expect("runtime filter metrics reset must return an epoch")
                .parse()
                .expect("runtime filter metrics reset epoch must be an integer");

        let count: i64 = simple_query_first_column_tx(
            &mut tx,
            &format!(
                "\
                SELECT count(*)::bigint
                FROM {build_table} AS b
                JOIN {probe_table} AS p ON b.k = p.k
                "
            ),
        )
        .unwrap_or_else(|| panic!("{name} runtime filter join must return one row"))
        .parse()
        .unwrap_or_else(|err| panic!("{name} runtime filter join count must be an integer: {err}"));
        assert_eq!(count, 3, "{name} runtime filter join count");

        let summary = simple_query_first_column_tx(
            &mut tx,
            "\
            SELECT concat(
                coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_allocated_total'), 0), ',',
                coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_ready_total'), 0), ',',
                coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_build_rows_total'), 0), ',',
                coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_probe_rows_total'), 0), ',',
                coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_probe_rows_rejected_total'), 0), ',',
                coalesce(max(value) FILTER (WHERE metric = 'runtime_filter_probe_pass_unfiltered_total'), 0), ',',
                coalesce(max(reset_epoch), 0)
            )
            FROM pg_fusion_metrics()
            ",
        )
        .expect("runtime filter metric summary must return one row");
        let parts = summary
            .split(',')
            .map(|part| {
                part.parse::<i64>()
                    .expect("runtime filter metric value must be integer")
            })
            .collect::<Vec<_>>();
        assert_eq!(parts.len(), 7);
        assert!(
            parts[0] > 0,
            "runtime filter should be allocated for {name} join: {summary}"
        );
        assert!(
            parts[1] > 0,
            "runtime filter should become ready for {name} join: {summary}"
        );
        assert!(
            parts[2] >= 3,
            "runtime filter should observe {name} build rows: {summary}"
        );
        assert!(
            parts[3] >= 100000,
            "runtime filter should probe {name} rows: {summary}"
        );
        assert!(
            parts[4] > 0,
            "runtime filter should reject non-matching {name} probe rows: {summary}"
        );
        assert_eq!(parts[6], before_epoch);
    }
}

pub(crate) fn spill_metrics_smoke() {
    if std::env::var("PG_FUSION_SPILL_PG_TEST").as_deref() != Ok("1") {
        return;
    }

    let mut client = smoke_client();
    let memory_limit =
        simple_query_first_column_client(&mut client, "SHOW pg_fusion.worker_memory_limit_mb")
            .expect("worker memory limit GUC must be visible");
    assert_eq!(
        memory_limit, "128",
        "spill metrics smoke requires finite worker memory"
    );

    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_spill_metrics_smoke";
    batch_execute_pg_fusion_disabled(
        &mut tx,
        &format!(
            "CREATE TEMP TABLE {table_name} AS \
             SELECT g::int AS k, repeat(md5(g::text), 8) AS payload \
             FROM generate_series(1, 200000) AS g"
        ),
    );

    let before_epoch: i64 =
        simple_query_first_column_tx(&mut tx, "SELECT pg_fusion_metrics_reset()")
            .expect("spill metrics reset must return an epoch")
            .parse()
            .expect("spill metrics reset epoch must be an integer");

    let max_row_number: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!(
            "SELECT max(rn)::bigint \
             FROM ( \
               SELECT row_number() OVER (ORDER BY payload) AS rn \
               FROM {table_name} \
             ) AS ordered_src"
        ),
    )
    .expect("spill metrics window-sort query must return one row")
    .parse()
    .expect("spill metrics window-sort query must return one bigint value");
    assert_eq!(max_row_number, 200000);

    let summary = simple_query_first_column_tx(
        &mut tx,
        "\
        SELECT concat(
            coalesce(max(value) FILTER (WHERE metric = 'worker_spill_count_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'worker_spilled_rows_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'worker_spilled_bytes_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'worker_spill_dirs_created_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'worker_spill_dirs_removed_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'worker_spill_leaked_files_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'worker_spill_leaked_bytes_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'worker_spill_cleanup_errors_total'), 0), ',',
            coalesce(max(reset_epoch), 0)
        )
        FROM pg_fusion_metrics()
        ",
    )
    .expect("spill metrics summary must return one row");
    let parts = summary
        .split(',')
        .map(|part| {
            part.parse::<i64>()
                .expect("spill metric value must be integer")
        })
        .collect::<Vec<_>>();
    assert_eq!(parts.len(), 9);
    assert!(
        parts[0] > 0,
        "worker spill count must be positive: {summary}"
    );
    assert!(
        parts[1] > 0,
        "worker spilled rows must be positive: {summary}"
    );
    assert!(
        parts[2] > 0,
        "worker spilled bytes must be positive: {summary}"
    );
    assert_eq!(
        parts[3], 1,
        "one execution spill directory must be created: {summary}"
    );
    assert_eq!(
        parts[4], 1,
        "one execution spill directory must be removed: {summary}"
    );
    assert_eq!(parts[5], 0, "spill files must not leak: {summary}");
    assert_eq!(parts[6], 0, "spill bytes must not leak: {summary}");
    assert_eq!(
        parts[7], 0,
        "spill cleanup must not report errors: {summary}"
    );
    assert_eq!(parts[8], before_epoch);
}
