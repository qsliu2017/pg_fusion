use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use comfy_table::{presets::ASCII_FULL, Cell, ContentArrangement, Table as ConsoleTable};
use owo_colors::OwoColorize;
use postgres::{Client, Config, NoTls, SimpleQueryMessage};
use serde::Serialize;
use std::collections::BTreeSet;
use std::env;
use std::fmt::{self, Display};
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tpchgen::csv::{
    CustomerCsv, LineItemCsv, NationCsv, OrderCsv, PartCsv, PartSuppCsv, RegionCsv, SupplierCsv,
};
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};

const DEFAULT_RESULTS_DIR: &str = "benches/tpch/results";
const DEFAULT_SCHEMA: &str = "tpch";
const TABLES: &[&str] = &[
    "region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem",
];

#[derive(Debug, Clone, Parser, Serialize)]
#[command(
    name = "pg_fusion_tpch",
    about = "Generate, load, and compare native TPC-H on PostgreSQL and pg_fusion"
)]
pub struct Args {
    #[arg(long, short = 'd', help = "database name")]
    pub dbname: Option<String>,
    #[arg(long, help = "PostgreSQL host or Unix socket directory")]
    pub host: Option<String>,
    #[arg(long, help = "PostgreSQL port")]
    pub port: Option<u16>,
    #[arg(long, short = 'U', help = "PostgreSQL user")]
    pub user: Option<String>,
    #[arg(long, default_value = DEFAULT_SCHEMA)]
    pub schema: String,
    #[arg(long, short = 's', default_value_t = 0.01)]
    pub scale_factor: f64,
    #[arg(long, default_value = "all", help = "comma list like q01,q06 or 'all'")]
    pub queries: String,
    #[arg(long, default_value_t = 3, help = "measured runs per query/mode")]
    pub runs: usize,
    #[arg(long, default_value_t = 1, help = "warmup runs per query/mode")]
    pub warmup: usize,
    #[arg(long, default_value_t = 120.0, help = "per query timeout in seconds")]
    pub timeout: f64,
    #[arg(long, default_value_t = 2)]
    pub parallel_workers: i32,
    #[arg(long, help = "skip data generation and loading")]
    pub no_prepare: bool,
    #[arg(long, help = "prepare data/schema and exit")]
    pub only_prepare: bool,
    #[arg(
        long,
        help = "accepted for script compatibility; preparation always recreates the schema"
    )]
    pub force_prepare: bool,
    #[arg(long, default_value = DEFAULT_RESULTS_DIR)]
    pub results_dir: PathBuf,
    #[arg(long, help = "disable ANSI colors in the console report")]
    pub no_color: bool,
}

pub fn main() -> Result<()> {
    let args = Args::parse();
    run(args)
}

pub fn run(args: Args) -> Result<()> {
    validate_args(&args)?;
    let selected = select_queries(&args.queries)?;
    fs::create_dir_all(&args.results_dir)
        .with_context(|| format!("create results dir {}", args.results_dir.display()))?;

    let mut client = connect(&args)?;
    let metadata = collect_metadata(&mut client, &args);

    if !args.no_prepare {
        prepare_schema(&mut client, &args)?;
        if args.only_prepare {
            println!(
                "Prepared native TPC-H schema '{}' at scale factor {}",
                args.schema, args.scale_factor
            );
            return Ok(());
        }
    }

    let summaries = run_suite(&mut client, &args, &selected)?;
    write_results(&args, &metadata, &summaries)?;
    print_report(&args, &metadata, &summaries);
    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    if !is_simple_ident(&args.schema) {
        bail!("--schema must be a simple SQL identifier");
    }
    if !args.scale_factor.is_finite() || args.scale_factor <= 0.0 {
        bail!("--scale-factor must be finite and positive");
    }
    if args.runs == 0 {
        bail!("--runs must be positive");
    }
    if !args.timeout.is_finite() || args.timeout <= 0.0 {
        bail!("--timeout must be finite and positive");
    }
    if args.parallel_workers < 0 {
        bail!("--parallel-workers must be non-negative");
    }
    if args.no_prepare && args.only_prepare {
        bail!("--no-prepare and --only-prepare cannot be used together");
    }
    Ok(())
}

fn connect(args: &Args) -> Result<Client> {
    let config = connection_config(args)?;
    config
        .connect(NoTls)
        .with_context(|| "connect to PostgreSQL")
}

fn connection_config(args: &Args) -> Result<Config> {
    let mut config = Config::new();
    let dbname = args.dbname.clone().or_else(|| env::var("PGDATABASE").ok());
    if let Some(dbname) = dbname {
        config.dbname(&dbname);
    }
    let user = args.user.clone().or_else(|| env::var("PGUSER").ok());
    if let Some(user) = user {
        config.user(&user);
    }
    if let Ok(password) = env::var("PGPASSWORD") {
        config.password(password);
    }

    let allow_pgrx_autodetect = args.host.is_none()
        && args.port.is_none()
        && env::var_os("PGHOST").is_none()
        && env::var_os("PGPORT").is_none();
    let detected = allow_pgrx_autodetect.then(detect_pgrx_socket).flatten();
    let host = args
        .host
        .clone()
        .or_else(|| env::var("PGHOST").ok())
        .or_else(|| detected.clone().map(|(host, _)| host));
    if let Some(host) = host {
        config.host(&host);
    }

    let env_port = match env::var("PGPORT") {
        Ok(port) => Some(
            port.parse::<u16>()
                .with_context(|| format!("parse PGPORT={port:?} as a PostgreSQL port"))?,
        ),
        Err(env::VarError::NotPresent) => None,
        Err(err) => return Err(err).context("read PGPORT"),
    };
    let port = args
        .port
        .or(env_port)
        .or_else(|| detected.and_then(|(_, port)| port.parse().ok()));
    if let Some(port) = port {
        config.port(port);
    }
    Ok(config)
}

fn detect_pgrx_socket() -> Option<(String, String)> {
    if env::var_os("PGHOST").is_some() || env::var_os("PGPORT").is_some() {
        return None;
    }
    let home = env::var_os("HOME")?;
    let pgrx_root = PathBuf::from(home).join(".pgrx");
    let entries = fs::read_dir(&pgrx_root).ok()?;
    let sockets = entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with(".s.PGSQL.") && !name.ends_with(".lock"))
        .filter_map(|name| {
            name.rsplit_once('.')
                .and_then(|(_, port)| port.parse::<u16>().ok().map(|_| port.to_string()))
        })
        .collect::<Vec<_>>();
    if sockets.len() == 1 {
        Some((pgrx_root.display().to_string(), sockets[0].clone()))
    } else {
        None
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RunMetadata {
    pub scale_factor: f64,
    pub schema: String,
    pub runs: usize,
    pub warmup: usize,
    pub timeout_seconds: f64,
    pub parallel_workers: i32,
    pub server_version: Option<String>,
    pub shared_preload_libraries: Option<String>,
    pub pg_fusion_extversion: Option<String>,
    pub generated_at_unix_seconds: u64,
}

fn collect_metadata(client: &mut Client, args: &Args) -> RunMetadata {
    let server_version = query_scalar(client, "SHOW server_version");
    let shared_preload_libraries = query_scalar(client, "SHOW shared_preload_libraries");
    if shared_preload_libraries
        .as_deref()
        .is_some_and(|libs| !libs.contains("pg_fusion"))
    {
        eprintln!(
            "warning: shared_preload_libraries does not mention pg_fusion; fusion runs may fail"
        );
    }
    let pg_fusion_extversion = query_scalar(
        client,
        "SELECT extversion FROM pg_extension WHERE extname = 'pg_fusion'",
    );
    RunMetadata {
        scale_factor: args.scale_factor,
        schema: args.schema.clone(),
        runs: args.runs,
        warmup: args.warmup,
        timeout_seconds: args.timeout,
        parallel_workers: args.parallel_workers,
        server_version,
        shared_preload_libraries,
        pg_fusion_extversion,
        generated_at_unix_seconds: unix_seconds(),
    }
}

fn query_scalar(client: &mut Client, sql: &str) -> Option<String> {
    client
        .query_opt(sql, &[])
        .ok()
        .flatten()
        .and_then(|row| row.try_get::<usize, String>(0).ok())
}

fn prepare_schema(client: &mut Client, args: &Args) -> Result<()> {
    println!(
        "Preparing native TPC-H schema '{}' at scale factor {}",
        args.schema, args.scale_factor
    );
    client
        .batch_execute(&render_schema_sql(&args.schema))
        .with_context(|| format!("create schema {}", args.schema))?;

    for table in TABLES {
        let rows = load_table(client, &args.schema, table, args.scale_factor)
            .with_context(|| format!("load table {table}"))?;
        println!("  loaded {table}: {rows} rows");
    }

    for table in TABLES {
        client
            .batch_execute(&format!("ANALYZE {}.{};", args.schema, table))
            .with_context(|| format!("analyze table {table}"))?;
    }
    Ok(())
}

pub fn render_schema_sql(schema: &str) -> String {
    format!(
        r#"
DROP SCHEMA IF EXISTS {schema} CASCADE;
CREATE SCHEMA {schema};
SET search_path TO {schema}, public;

CREATE TABLE region (
    r_regionkey bigint NOT NULL,
    r_name text NOT NULL,
    r_comment text NOT NULL
);

CREATE TABLE nation (
    n_nationkey bigint NOT NULL,
    n_name text NOT NULL,
    n_regionkey bigint NOT NULL,
    n_comment text NOT NULL
);

CREATE TABLE supplier (
    s_suppkey bigint NOT NULL,
    s_name text NOT NULL,
    s_address text NOT NULL,
    s_nationkey bigint NOT NULL,
    s_phone text NOT NULL,
    s_acctbal numeric(15,2) NOT NULL,
    s_comment text NOT NULL
);

CREATE TABLE part (
    p_partkey bigint NOT NULL,
    p_name text NOT NULL,
    p_mfgr text NOT NULL,
    p_brand text NOT NULL,
    p_type text NOT NULL,
    p_size integer NOT NULL,
    p_container text NOT NULL,
    p_retailprice numeric(15,2) NOT NULL,
    p_comment text NOT NULL
);

CREATE TABLE partsupp (
    ps_partkey bigint NOT NULL,
    ps_suppkey bigint NOT NULL,
    ps_availqty integer NOT NULL,
    ps_supplycost numeric(15,2) NOT NULL,
    ps_comment text NOT NULL
);

CREATE TABLE customer (
    c_custkey bigint NOT NULL,
    c_name text NOT NULL,
    c_address text NOT NULL,
    c_nationkey bigint NOT NULL,
    c_phone text NOT NULL,
    c_acctbal numeric(15,2) NOT NULL,
    c_mktsegment text NOT NULL,
    c_comment text NOT NULL
);

CREATE TABLE orders (
    o_orderkey bigint NOT NULL,
    o_custkey bigint NOT NULL,
    o_orderstatus text NOT NULL,
    o_totalprice numeric(15,2) NOT NULL,
    o_orderdate date NOT NULL,
    o_orderpriority text NOT NULL,
    o_clerk text NOT NULL,
    o_shippriority integer NOT NULL,
    o_comment text NOT NULL
);

CREATE TABLE lineitem (
    l_orderkey bigint NOT NULL,
    l_partkey bigint NOT NULL,
    l_suppkey bigint NOT NULL,
    l_linenumber integer NOT NULL,
    l_quantity numeric(15,2) NOT NULL,
    l_extendedprice numeric(15,2) NOT NULL,
    l_discount numeric(15,2) NOT NULL,
    l_tax numeric(15,2) NOT NULL,
    l_returnflag text NOT NULL,
    l_linestatus text NOT NULL,
    l_shipdate date NOT NULL,
    l_commitdate date NOT NULL,
    l_receiptdate date NOT NULL,
    l_shipinstruct text NOT NULL,
    l_shipmode text NOT NULL,
    l_comment text NOT NULL
);
"#
    )
}

fn load_table(client: &mut Client, schema: &str, table: &str, scale_factor: f64) -> Result<u64> {
    let columns = table_columns(table).ok_or_else(|| anyhow!("unknown table {table}"))?;
    let sql =
        format!("COPY {schema}.{table} ({columns}) FROM STDIN WITH (FORMAT csv, HEADER false)");
    let mut writer = client.copy_in(&sql)?;
    match table {
        "region" => write_csv_rows(
            &mut writer,
            RegionGenerator::new(scale_factor, 1, 1)
                .iter()
                .map(RegionCsv::new),
        )?,
        "nation" => write_csv_rows(
            &mut writer,
            NationGenerator::new(scale_factor, 1, 1)
                .iter()
                .map(NationCsv::new),
        )?,
        "supplier" => write_csv_rows(
            &mut writer,
            SupplierGenerator::new(scale_factor, 1, 1)
                .iter()
                .map(SupplierCsv::new),
        )?,
        "part" => write_csv_rows(
            &mut writer,
            PartGenerator::new(scale_factor, 1, 1)
                .iter()
                .map(PartCsv::new),
        )?,
        "partsupp" => write_csv_rows(
            &mut writer,
            PartSuppGenerator::new(scale_factor, 1, 1)
                .iter()
                .map(PartSuppCsv::new),
        )?,
        "customer" => write_csv_rows(
            &mut writer,
            CustomerGenerator::new(scale_factor, 1, 1)
                .iter()
                .map(CustomerCsv::new),
        )?,
        "orders" => write_csv_rows(
            &mut writer,
            OrderGenerator::new(scale_factor, 1, 1)
                .iter()
                .map(OrderCsv::new),
        )?,
        "lineitem" => write_csv_rows(
            &mut writer,
            LineItemGenerator::new(scale_factor, 1, 1)
                .iter()
                .map(LineItemCsv::new),
        )?,
        _ => unreachable!("table is validated above"),
    }
    writer.finish().map_err(Into::into)
}

fn write_csv_rows<I, D>(writer: &mut postgres::CopyInWriter<'_>, rows: I) -> Result<()>
where
    I: IntoIterator<Item = D>,
    D: Display,
{
    for row in rows {
        writeln!(writer, "{row}")?;
    }
    Ok(())
}

fn table_columns(table: &str) -> Option<&'static str> {
    match table {
        "region" => Some(RegionCsv::header()),
        "nation" => Some(NationCsv::header()),
        "supplier" => Some(SupplierCsv::header()),
        "part" => Some(PartCsv::header()),
        "partsupp" => Some(PartSuppCsv::header()),
        "customer" => Some(CustomerCsv::header()),
        "orders" => Some(OrderCsv::header()),
        "lineitem" => Some(LineItemCsv::header()),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BenchQuery {
    pub id: &'static str,
    pub title: &'static str,
    pub sql: &'static str,
}

pub fn all_queries() -> &'static [BenchQuery] {
    &QUERIES
}

pub fn select_queries(selection: &str) -> Result<Vec<BenchQuery>> {
    if selection.eq_ignore_ascii_case("all") {
        return Ok(all_queries().to_vec());
    }
    let mut wanted = BTreeSet::new();
    let mut invalid = Vec::new();
    for item in selection.split(',').map(str::trim) {
        match normalize_query_id(item) {
            Some(id) => {
                wanted.insert(id);
            }
            None => invalid.push(if item.is_empty() { "<empty>" } else { item }),
        }
    }
    if !invalid.is_empty() {
        bail!("unknown query id(s): {}", invalid.join(", "));
    }
    if wanted.is_empty() {
        bail!("--queries did not contain any valid query ids");
    }

    let mut selected = Vec::new();
    for query in all_queries() {
        if wanted.contains(query.id) {
            selected.push(*query);
        }
    }
    let found = selected
        .iter()
        .map(|query| query.id)
        .collect::<BTreeSet<_>>();
    let missing = wanted.difference(&found).copied().collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!("unknown query id(s): {}", missing.join(", "));
    }
    Ok(selected)
}

fn normalize_query_id(item: &str) -> Option<&'static str> {
    let item = item.trim().to_ascii_lowercase();
    if item.is_empty() {
        return None;
    }
    let number = item.strip_prefix('q').unwrap_or(&item).parse::<u8>().ok()?;
    match number {
        1 => Some("q01"),
        2 => Some("q02"),
        3 => Some("q03"),
        4 => Some("q04"),
        5 => Some("q05"),
        6 => Some("q06"),
        7 => Some("q07"),
        8 => Some("q08"),
        9 => Some("q09"),
        10 => Some("q10"),
        11 => Some("q11"),
        12 => Some("q12"),
        13 => Some("q13"),
        14 => Some("q14"),
        15 => Some("q15"),
        16 => Some("q16"),
        17 => Some("q17"),
        18 => Some("q18"),
        19 => Some("q19"),
        20 => Some("q20"),
        21 => Some("q21"),
        22 => Some("q22"),
        _ => None,
    }
}

fn run_suite(
    client: &mut Client,
    args: &Args,
    queries: &[BenchQuery],
) -> Result<Vec<QuerySummary>> {
    let mut summaries = Vec::with_capacity(queries.len());
    for query in queries {
        println!("Running {}", query.id);
        let summary = run_query_pair(client, args, *query);
        summaries.push(summary);
    }
    Ok(summaries)
}

fn run_query_pair(client: &mut Client, args: &Args, query: BenchQuery) -> QuerySummary {
    let mut backend = PostgresQueryBackend { client };
    run_query_pair_with(&mut backend, args, query)
}

fn run_query_pair_with<B: QueryBackend + ?Sized>(
    backend: &mut B,
    args: &Args,
    query: BenchQuery,
) -> QuerySummary {
    let mut pg = QueryRun::default();
    let mut fusion = QueryRun::default();

    if let Err(error) = explain_query(backend, args, query.sql, FusionMode::Off) {
        pg.error = Some(tail(&error.to_string()));
        return summarize_query(query, pg, fusion);
    }

    if let Err(error) = explain_query(backend, args, query.sql, FusionMode::On) {
        fusion.error = Some(tail(&error.to_string()));
        return summarize_query(query, pg, fusion);
    }

    for _ in 0..args.warmup {
        if let Err(error) = backend.execute(args, query.sql, FusionMode::Off) {
            pg.error = Some(tail(&error.to_string()));
            return summarize_query(query, pg, fusion);
        }
        if let Err(error) = backend.execute(args, query.sql, FusionMode::On) {
            fusion.error = Some(tail(&error.to_string()));
            return summarize_query(query, pg, fusion);
        }
    }

    for index in 0..args.runs {
        let order = if index % 2 == 0 {
            [FusionMode::Off, FusionMode::On]
        } else {
            [FusionMode::On, FusionMode::Off]
        };
        for mode in order {
            match backend.execute(args, query.sql, mode) {
                Ok(output) => match mode {
                    FusionMode::Off => pg.push(output),
                    FusionMode::On => fusion.push(output),
                },
                Err(error) => {
                    match mode {
                        FusionMode::Off => pg.error = Some(tail(&error.to_string())),
                        FusionMode::On => fusion.error = Some(tail(&error.to_string())),
                    }
                    return summarize_query(query, pg, fusion);
                }
            }
        }
    }

    summarize_query(query, pg, fusion)
}

trait QueryBackend {
    fn explain(&mut self, args: &Args, sql: &str, mode: FusionMode) -> Result<String>;

    fn execute(&mut self, args: &Args, sql: &str, mode: FusionMode) -> Result<QueryOutput>;
}

struct PostgresQueryBackend<'a> {
    client: &'a mut Client,
}

impl QueryBackend for PostgresQueryBackend<'_> {
    fn explain(&mut self, args: &Args, sql: &str, mode: FusionMode) -> Result<String> {
        explain_query_on_client(self.client, args, sql, mode)
    }

    fn execute(&mut self, args: &Args, sql: &str, mode: FusionMode) -> Result<QueryOutput> {
        execute_query_on_client(self.client, args, sql, mode)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FusionMode {
    Off,
    On,
}

impl FusionMode {
    fn guc(self) -> &'static str {
        match self {
            FusionMode::Off => "off",
            FusionMode::On => "on",
        }
    }
}

fn explain_query<B: QueryBackend + ?Sized>(
    backend: &mut B,
    args: &Args,
    sql: &str,
    mode: FusionMode,
) -> Result<()> {
    let output = backend.explain(args, sql, mode)?;
    if mode == FusionMode::On && !output.contains("Custom Scan (PgFusionScan)") {
        bail!("fusion EXPLAIN did not contain PgFusionScan")
    }
    Ok(())
}

#[derive(Debug, Default, Clone)]
struct QueryRun {
    times_ms: Vec<f64>,
    row_counts: Vec<usize>,
    hashes: Vec<String>,
    output: Option<Vec<u8>>,
    error: Option<String>,
}

impl QueryRun {
    fn push(&mut self, output: QueryOutput) {
        self.times_ms.push(output.elapsed_ms);
        self.row_counts.push(output.row_count);
        self.hashes.push(output.hash);
        self.output = Some(output.bytes);
    }

    fn ok(&self) -> bool {
        self.error.is_none() && !self.times_ms.is_empty()
    }

    fn median_ms(&self) -> Option<f64> {
        if self.ok() {
            median(&self.times_ms)
        } else {
            None
        }
    }

    fn representative_hash(&self) -> Option<&str> {
        self.hashes.last().map(String::as_str)
    }

    fn representative_rows(&self) -> Option<usize> {
        self.row_counts.last().copied()
    }
}

#[derive(Debug)]
struct QueryOutput {
    elapsed_ms: f64,
    row_count: usize,
    hash: String,
    bytes: Vec<u8>,
}

fn explain_query_on_client(
    client: &mut Client,
    args: &Args,
    sql: &str,
    mode: FusionMode,
) -> Result<String> {
    set_runtime_gucs(client, args, mode)?;
    let explain_sql = format!("EXPLAIN {}", strip_semicolon(sql));
    simple_query_text(client, &explain_sql)
}

fn execute_query_on_client(
    client: &mut Client,
    args: &Args,
    sql: &str,
    mode: FusionMode,
) -> Result<QueryOutput> {
    set_runtime_gucs(client, args, mode)?;
    let copy_sql = format!(
        "COPY ({}) TO STDOUT WITH (FORMAT csv, HEADER false)",
        strip_semicolon(sql)
    );
    let start = Instant::now();
    let mut reader = client.copy_out(&copy_sql)?;
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let row_count = count_csv_rows(&bytes);
    let hash = blake3::hash(&bytes).to_hex().to_string();
    Ok(QueryOutput {
        elapsed_ms,
        row_count,
        hash,
        bytes,
    })
}

fn set_runtime_gucs(client: &mut Client, args: &Args, mode: FusionMode) -> Result<()> {
    let timeout_ms = timeout_ms(args.timeout);
    client.batch_execute(&format!(
        r#"
SET search_path TO {}, public;
SET statement_timeout = {};
SET max_parallel_workers_per_gather = {};
SET pg_fusion.enable = {};
"#,
        args.schema,
        timeout_ms,
        args.parallel_workers,
        mode.guc()
    ))?;
    Ok(())
}

fn timeout_ms(timeout_seconds: f64) -> u64 {
    let timeout = Duration::from_secs_f64(timeout_seconds.max(0.001));
    u64::try_from(timeout.as_millis())
        .unwrap_or(u64::MAX)
        .max(1)
}

fn simple_query_text(client: &mut Client, sql: &str) -> Result<String> {
    let mut output = String::new();
    for message in client.simple_query(sql)? {
        match message {
            SimpleQueryMessage::Row(row) => {
                for index in 0..row.len() {
                    if index > 0 {
                        output.push('\t');
                    }
                    output.push_str(row.get(index).unwrap_or(""));
                }
                output.push('\n');
            }
            SimpleQueryMessage::CommandComplete(count) => {
                output.push_str(&count.to_string());
                output.push('\n');
            }
            _ => {}
        }
    }
    Ok(output)
}

fn strip_semicolon(sql: &str) -> &str {
    sql.trim().trim_end_matches(';').trim()
}

fn count_csv_rows(bytes: &[u8]) -> usize {
    if bytes.is_empty() {
        return 0;
    }
    let mut count = 0;
    let mut in_quotes = false;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' if in_quotes && bytes.get(index + 1) == Some(&b'"') => {
                index += 2;
                continue;
            }
            b'"' => {
                in_quotes = !in_quotes;
            }
            b'\n' if !in_quotes => {
                count += 1;
            }
            _ => {}
        }
        index += 1;
    }
    if bytes.last() != Some(&b'\n') {
        count += 1;
    }
    count
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryStatus {
    Ok,
    Mismatch,
    FusionFail,
    PgFail,
}

impl Display for QueryStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            QueryStatus::Ok => "ok",
            QueryStatus::Mismatch => "mismatch",
            QueryStatus::FusionFail => "fusion_fail",
            QueryStatus::PgFail => "pg_fail",
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct QuerySummary {
    pub query: String,
    pub title: String,
    pub status: QueryStatus,
    pub pg_median_ms: Option<f64>,
    pub fusion_median_ms: Option<f64>,
    pub speedup: Option<f64>,
    pub fusion_vs_pg: Option<f64>,
    pub pg_rows: Option<usize>,
    pub fusion_rows: Option<usize>,
    pub result_match: bool,
    pub pg_times_ms: Vec<f64>,
    pub fusion_times_ms: Vec<f64>,
    pub pg_hash: Option<String>,
    pub fusion_hash: Option<String>,
    pub pg_error: Option<String>,
    pub fusion_error: Option<String>,
}

fn summarize_query(query: BenchQuery, pg: QueryRun, fusion: QueryRun) -> QuerySummary {
    let result_match = pg.ok()
        && fusion.ok()
        && outputs_match(
            pg.output.as_deref().unwrap_or_default(),
            fusion.output.as_deref().unwrap_or_default(),
        );
    let status = if pg.error.is_some() {
        QueryStatus::PgFail
    } else if fusion.error.is_some() {
        QueryStatus::FusionFail
    } else if !pg.ok() {
        QueryStatus::PgFail
    } else if !fusion.ok() {
        QueryStatus::FusionFail
    } else if !result_match {
        QueryStatus::Mismatch
    } else {
        QueryStatus::Ok
    };
    let pg_median_ms = pg.median_ms();
    let fusion_median_ms = fusion.median_ms();
    let speedup = pg_median_ms
        .zip(fusion_median_ms)
        .map(|(pg, fusion)| pg / fusion);
    let fusion_vs_pg = pg_median_ms
        .zip(fusion_median_ms)
        .map(|(pg, fusion)| fusion / pg);
    let pg_hash = pg.representative_hash().map(ToOwned::to_owned);
    let fusion_hash = fusion.representative_hash().map(ToOwned::to_owned);
    QuerySummary {
        query: query.id.to_string(),
        title: query.title.to_string(),
        status,
        pg_median_ms,
        fusion_median_ms,
        speedup,
        fusion_vs_pg,
        pg_rows: pg.representative_rows(),
        fusion_rows: fusion.representative_rows(),
        result_match,
        pg_times_ms: pg.times_ms,
        fusion_times_ms: fusion.times_ms,
        pg_hash,
        fusion_hash,
        pg_error: pg.error,
        fusion_error: fusion.error,
    }
}

pub fn outputs_match(left: &[u8], right: &[u8]) -> bool {
    left == right
}

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    if sorted.len() & 1 == 0 {
        Some((sorted[middle - 1] + sorted[middle]) / 2.0)
    } else {
        Some(sorted[middle])
    }
}

#[derive(Debug, Serialize)]
struct ResultsFile<'a> {
    metadata: &'a RunMetadata,
    queries: &'a [QuerySummary],
}

fn write_results(args: &Args, metadata: &RunMetadata, summaries: &[QuerySummary]) -> Result<()> {
    let stamp = metadata.generated_at_unix_seconds;
    let scale = scale_label(args.scale_factor);
    let csv_path = args
        .results_dir
        .join(format!("tpch_sf_{scale}_{stamp}.csv"));
    let json_path = csv_path.with_extension("json");

    let mut writer = csv::Writer::from_path(&csv_path)?;
    writer.write_record([
        "query",
        "status",
        "pg_median_ms",
        "fusion_median_ms",
        "speedup",
        "fusion_vs_pg",
        "pg_rows",
        "fusion_rows",
        "result_match",
        "pg_error",
        "fusion_error",
    ])?;
    for summary in summaries {
        writer.write_record([
            summary.query.as_str(),
            &summary.status.to_string(),
            &format_optional_float(summary.pg_median_ms),
            &format_optional_float(summary.fusion_median_ms),
            &format_optional_float(summary.speedup),
            &format_optional_float(summary.fusion_vs_pg),
            &summary
                .pg_rows
                .map(|value| value.to_string())
                .unwrap_or_default(),
            &summary
                .fusion_rows
                .map(|value| value.to_string())
                .unwrap_or_default(),
            &summary.result_match.to_string(),
            summary.pg_error.as_deref().unwrap_or(""),
            summary.fusion_error.as_deref().unwrap_or(""),
        ])?;
    }
    writer.flush()?;

    let json = serde_json::to_string_pretty(&ResultsFile {
        metadata,
        queries: summaries,
    })?;
    fs::write(&json_path, json + "\n")?;
    println!("Wrote {}", csv_path.display());
    println!("Wrote {}", json_path.display());
    Ok(())
}

fn print_report(args: &Args, metadata: &RunMetadata, summaries: &[QuerySummary]) {
    println!();
    println!(
        "TPC-H native comparison: sf={} schema={} runs={} warmup={} parallel_workers={}",
        metadata.scale_factor,
        metadata.schema,
        metadata.runs,
        metadata.warmup,
        metadata.parallel_workers
    );
    if let Some(version) = &metadata.server_version {
        println!("PostgreSQL: {version}");
    }
    if let Some(version) = &metadata.pg_fusion_extversion {
        println!("pg_fusion: {version}");
    }

    println!();
    println!("{}", render_report_table(args.no_color, summaries));

    let ok = summaries
        .iter()
        .filter(|summary| summary.status == QueryStatus::Ok)
        .count();
    let total = summaries.len();
    let pg_total = summaries
        .iter()
        .filter_map(|summary| summary.pg_median_ms)
        .sum::<f64>();
    let fusion_total = summaries
        .iter()
        .filter_map(|summary| summary.fusion_median_ms)
        .sum::<f64>();
    println!();
    if fusion_total > 0.0 && pg_total > 0.0 {
        println!(
            "Summary: {ok}/{total} ok, PostgreSQL {:.1} ms, pg_fusion {:.1} ms, overall {:.2}x",
            pg_total,
            fusion_total,
            pg_total / fusion_total
        );
    } else {
        println!("Summary: {ok}/{total} ok");
    }

    let failed = summaries
        .iter()
        .filter(|summary| summary.status != QueryStatus::Ok)
        .collect::<Vec<_>>();
    if !failed.is_empty() {
        println!();
        println!("Failures and mismatches:");
        for summary in failed {
            let detail = summary
                .fusion_error
                .as_deref()
                .or(summary.pg_error.as_deref())
                .unwrap_or("result mismatch");
            println!("- {}: {}: {}", summary.query, summary.status, detail);
        }
    }
}

pub fn render_report_table(no_color: bool, summaries: &[QuerySummary]) -> String {
    let mut table = ConsoleTable::new();
    table
        .load_preset(ASCII_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            "query",
            "status",
            "pg ms",
            "fusion ms",
            "speedup",
            "rows",
        ]);
    for summary in summaries {
        let rows = summary
            .fusion_rows
            .or(summary.pg_rows)
            .map(|rows| rows.to_string())
            .unwrap_or_default();
        table.add_row(vec![
            Cell::new(&summary.query),
            Cell::new(format_status(summary.status, no_color)),
            Cell::new(format_optional_float(summary.pg_median_ms)),
            Cell::new(format_optional_float(summary.fusion_median_ms)),
            Cell::new(format_speedup(summary.speedup)),
            Cell::new(rows),
        ]);
    }
    table.to_string()
}

fn format_status(status: QueryStatus, no_color: bool) -> String {
    let text = status.to_string();
    if no_color {
        return text;
    }
    match status {
        QueryStatus::Ok => text.green().to_string(),
        QueryStatus::Mismatch => text.yellow().to_string(),
        QueryStatus::FusionFail | QueryStatus::PgFail => text.red().to_string(),
    }
}

fn format_optional_float(value: Option<f64>) -> String {
    value.map(|value| format!("{value:.3}")).unwrap_or_default()
}

fn format_speedup(value: Option<f64>) -> String {
    match value {
        Some(value) if value >= 1.0 => format!("{value:.2}x"),
        Some(value) if value > 0.0 => format!("{:.2}x slower", 1.0 / value),
        Some(_) => "0.00x".to_string(),
        None => String::new(),
    }
}

fn scale_label(scale_factor: f64) -> String {
    scale_factor.to_string().replace(['.', '-'], "_")
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn is_simple_ident(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn tail(text: &str) -> String {
    const MAX: usize = 500;
    let text = text.trim();
    if text.len() <= MAX {
        return text.to_string();
    }
    format!("...{}", &text[text.len() - MAX..])
}

const QUERIES: [BenchQuery; 22] = [
    BenchQuery {
        id: "q01",
        title: "Pricing Summary Report",
        sql: include_str!("../../queries/q01.sql"),
    },
    BenchQuery {
        id: "q02",
        title: "Minimum Cost Supplier",
        sql: include_str!("../../queries/q02.sql"),
    },
    BenchQuery {
        id: "q03",
        title: "Shipping Priority",
        sql: include_str!("../../queries/q03.sql"),
    },
    BenchQuery {
        id: "q04",
        title: "Order Priority Checking",
        sql: include_str!("../../queries/q04.sql"),
    },
    BenchQuery {
        id: "q05",
        title: "Local Supplier Volume",
        sql: include_str!("../../queries/q05.sql"),
    },
    BenchQuery {
        id: "q06",
        title: "Forecasting Revenue Change",
        sql: include_str!("../../queries/q06.sql"),
    },
    BenchQuery {
        id: "q07",
        title: "Volume Shipping",
        sql: include_str!("../../queries/q07.sql"),
    },
    BenchQuery {
        id: "q08",
        title: "National Market Share",
        sql: include_str!("../../queries/q08.sql"),
    },
    BenchQuery {
        id: "q09",
        title: "Product Type Profit Measure",
        sql: include_str!("../../queries/q09.sql"),
    },
    BenchQuery {
        id: "q10",
        title: "Returned Item Reporting",
        sql: include_str!("../../queries/q10.sql"),
    },
    BenchQuery {
        id: "q11",
        title: "Important Stock Identification",
        sql: include_str!("../../queries/q11.sql"),
    },
    BenchQuery {
        id: "q12",
        title: "Shipping Modes and Order Priority",
        sql: include_str!("../../queries/q12.sql"),
    },
    BenchQuery {
        id: "q13",
        title: "Customer Distribution",
        sql: include_str!("../../queries/q13.sql"),
    },
    BenchQuery {
        id: "q14",
        title: "Promotion Effect",
        sql: include_str!("../../queries/q14.sql"),
    },
    BenchQuery {
        id: "q15",
        title: "Top Supplier",
        sql: include_str!("../../queries/q15.sql"),
    },
    BenchQuery {
        id: "q16",
        title: "Parts/Supplier Relationship",
        sql: include_str!("../../queries/q16.sql"),
    },
    BenchQuery {
        id: "q17",
        title: "Small-Quantity-Order Revenue",
        sql: include_str!("../../queries/q17.sql"),
    },
    BenchQuery {
        id: "q18",
        title: "Large Volume Customer",
        sql: include_str!("../../queries/q18.sql"),
    },
    BenchQuery {
        id: "q19",
        title: "Discounted Revenue",
        sql: include_str!("../../queries/q19.sql"),
    },
    BenchQuery {
        id: "q20",
        title: "Potential Part Promotion",
        sql: include_str!("../../queries/q20.sql"),
    },
    BenchQuery {
        id: "q21",
        title: "Suppliers Who Kept Orders Waiting",
        sql: include_str!("../../queries/q21.sql"),
    },
    BenchQuery {
        id: "q22",
        title: "Global Sales Opportunity",
        sql: include_str!("../../queries/q22.sql"),
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use postgres::config::Host;
    use std::collections::VecDeque;
    use std::ffi::OsString;
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        _lock: MutexGuard<'static, ()>,
        saved: Vec<(&'static str, Option<OsString>)>,
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.saved.iter().rev() {
                if let Some(value) = value {
                    env::set_var(key, value);
                } else {
                    env::remove_var(key);
                }
            }
        }
    }

    fn set_test_env(vars: &[(&'static str, Option<&str>)]) -> EnvGuard {
        let lock = ENV_LOCK.lock().expect("env test lock poisoned");
        let saved = vars
            .iter()
            .map(|(key, _)| (*key, env::var_os(key)))
            .collect::<Vec<_>>();
        for (key, value) in vars {
            if let Some(value) = value {
                env::set_var(key, value);
            } else {
                env::remove_var(key);
            }
        }
        EnvGuard { _lock: lock, saved }
    }

    fn test_args() -> Args {
        Args {
            dbname: None,
            host: None,
            port: None,
            user: None,
            schema: DEFAULT_SCHEMA.to_string(),
            scale_factor: 0.01,
            queries: "all".to_string(),
            runs: 3,
            warmup: 1,
            timeout: 120.0,
            parallel_workers: 2,
            no_prepare: false,
            only_prepare: false,
            force_prepare: false,
            results_dir: PathBuf::from(DEFAULT_RESULTS_DIR),
            no_color: true,
        }
    }

    fn test_query() -> BenchQuery {
        BenchQuery {
            id: "q00",
            title: "Test Query",
            sql: "SELECT 1",
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum FakeCall {
        Explain(FusionMode),
        Execute(FusionMode),
    }

    enum FakeExplain {
        Ok(&'static str),
        Err(&'static str),
    }

    enum FakeExecute {
        Ok(&'static [u8]),
    }

    struct FakeBackend {
        calls: Vec<FakeCall>,
        explains: VecDeque<FakeExplain>,
        executes: VecDeque<FakeExecute>,
    }

    impl FakeBackend {
        fn new(explains: Vec<FakeExplain>, executes: Vec<FakeExecute>) -> Self {
            Self {
                calls: Vec::new(),
                explains: explains.into(),
                executes: executes.into(),
            }
        }
    }

    impl QueryBackend for FakeBackend {
        fn explain(&mut self, _args: &Args, _sql: &str, mode: FusionMode) -> Result<String> {
            self.calls.push(FakeCall::Explain(mode));
            match self
                .explains
                .pop_front()
                .expect("missing fake EXPLAIN result")
            {
                FakeExplain::Ok(output) => Ok(output.to_string()),
                FakeExplain::Err(message) => Err(anyhow::anyhow!(message)),
            }
        }

        fn execute(&mut self, _args: &Args, _sql: &str, mode: FusionMode) -> Result<QueryOutput> {
            self.calls.push(FakeCall::Execute(mode));
            match self
                .executes
                .pop_front()
                .expect("missing fake execute result")
            {
                FakeExecute::Ok(bytes) => {
                    let bytes = bytes.to_vec();
                    let row_count = count_csv_rows(&bytes);
                    let hash = blake3::hash(&bytes).to_hex().to_string();
                    Ok(QueryOutput {
                        elapsed_ms: 1.0,
                        row_count,
                        hash,
                        bytes,
                    })
                }
            }
        }
    }

    fn query_sql(id: &str) -> &'static str {
        all_queries()
            .iter()
            .find(|query| query.id == id)
            .map(|query| query.sql)
            .unwrap()
    }

    #[test]
    fn vanilla_explain_failure_is_pg_fail_without_fusion_attempt() {
        let args = test_args();
        let mut backend = FakeBackend::new(vec![FakeExplain::Err("missing relation")], vec![]);

        let summary = run_query_pair_with(&mut backend, &args, test_query());

        assert_eq!(summary.status, QueryStatus::PgFail);
        assert_eq!(summary.pg_error.as_deref(), Some("missing relation"));
        assert_eq!(summary.fusion_error, None);
        assert_eq!(backend.calls, vec![FakeCall::Explain(FusionMode::Off)]);
    }

    #[test]
    fn fusion_explain_failure_after_vanilla_success_is_fusion_fail() {
        let args = test_args();
        let mut backend = FakeBackend::new(
            vec![
                FakeExplain::Ok("Seq Scan"),
                FakeExplain::Err("fusion planner failed"),
            ],
            vec![],
        );

        let summary = run_query_pair_with(&mut backend, &args, test_query());

        assert_eq!(summary.status, QueryStatus::FusionFail);
        assert_eq!(summary.pg_error, None);
        assert_eq!(
            summary.fusion_error.as_deref(),
            Some("fusion planner failed")
        );
        assert_eq!(
            backend.calls,
            vec![
                FakeCall::Explain(FusionMode::Off),
                FakeCall::Explain(FusionMode::On)
            ]
        );
    }

    #[test]
    fn missing_fusion_custom_scan_after_vanilla_success_is_fusion_fail() {
        let args = test_args();
        let mut backend = FakeBackend::new(
            vec![FakeExplain::Ok("Seq Scan"), FakeExplain::Ok("Seq Scan")],
            vec![],
        );

        let summary = run_query_pair_with(&mut backend, &args, test_query());

        assert_eq!(summary.status, QueryStatus::FusionFail);
        assert_eq!(summary.pg_error, None);
        assert_eq!(
            summary.fusion_error.as_deref(),
            Some("fusion EXPLAIN did not contain PgFusionScan")
        );
        assert_eq!(
            backend.calls,
            vec![
                FakeCall::Explain(FusionMode::Off),
                FakeCall::Explain(FusionMode::On)
            ]
        );
    }

    #[test]
    fn successful_preflights_continue_to_measured_execution() {
        let mut args = test_args();
        args.warmup = 0;
        args.runs = 1;
        let mut backend = FakeBackend::new(
            vec![
                FakeExplain::Ok("Seq Scan"),
                FakeExplain::Ok("Custom Scan (PgFusionScan)"),
            ],
            vec![FakeExecute::Ok(b"1\n"), FakeExecute::Ok(b"1\n")],
        );

        let summary = run_query_pair_with(&mut backend, &args, test_query());

        assert_eq!(summary.status, QueryStatus::Ok);
        assert!(summary.result_match);
        assert_eq!(summary.pg_rows, Some(1));
        assert_eq!(summary.fusion_rows, Some(1));
        assert_eq!(
            backend.calls,
            vec![
                FakeCall::Explain(FusionMode::Off),
                FakeCall::Explain(FusionMode::On),
                FakeCall::Execute(FusionMode::Off),
                FakeCall::Execute(FusionMode::On),
            ]
        );
    }

    #[test]
    fn selects_all_or_specific_queries() {
        assert_eq!(select_queries("all").unwrap().len(), 22);
        let selected = select_queries("q1,06,q22").unwrap();
        let ids = selected.iter().map(|query| query.id).collect::<Vec<_>>();
        assert_eq!(ids, ["q01", "q06", "q22"]);
    }

    #[test]
    fn rejects_invalid_query_ids_without_dropping_them() {
        let error = select_queries("q01,q23").unwrap_err().to_string();
        assert!(error.contains("q23"), "{error}");

        let error = select_queries("q01,").unwrap_err().to_string();
        assert!(error.contains("<empty>"), "{error}");
    }

    #[test]
    fn pg_connection_env_is_used_for_connection_config() {
        let _env = set_test_env(&[
            ("PGDATABASE", Some("pg_fusion")),
            ("PGHOST", Some("localhost")),
            ("PGPORT", Some("5433")),
            ("PGUSER", Some("bench")),
            ("PGPASSWORD", Some("secret")),
        ]);
        let config = connection_config(&test_args()).unwrap();
        assert_eq!(config.get_user(), Some("bench"));
        assert_eq!(config.get_dbname(), Some("pg_fusion"));
        assert_eq!(config.get_hosts(), &[Host::Tcp("localhost".to_string())]);
        assert_eq!(config.get_ports(), &[5433]);
        assert_eq!(config.get_password(), Some(b"secret".as_slice()));
    }

    #[test]
    fn invalid_pgport_is_rejected() {
        let _env = set_test_env(&[
            ("PGDATABASE", None),
            ("PGHOST", None),
            ("PGPORT", Some("not-a-port")),
            ("PGUSER", None),
            ("PGPASSWORD", None),
        ]);
        let error = connection_config(&test_args()).unwrap_err().to_string();
        assert!(error.contains("PGPORT"), "{error}");
    }

    #[test]
    fn pgrx_autodetect_does_not_fill_missing_explicit_connection_parts() {
        let home = tempfile::tempdir().unwrap();
        let pgrx = home.path().join(".pgrx");
        fs::create_dir(&pgrx).unwrap();
        fs::write(pgrx.join(".s.PGSQL.6543"), "").unwrap();
        let home = home.path().to_str().unwrap();
        let _env = set_test_env(&[
            ("HOME", Some(home)),
            ("PGDATABASE", None),
            ("PGHOST", None),
            ("PGPORT", None),
            ("PGUSER", None),
            ("PGPASSWORD", None),
        ]);

        let mut args = test_args();
        args.host = Some("localhost".to_string());
        let config = connection_config(&args).unwrap();
        assert_eq!(config.get_hosts(), &[Host::Tcp("localhost".to_string())]);
        assert!(config.get_ports().is_empty());

        let mut args = test_args();
        args.port = Some(5432);
        let config = connection_config(&args).unwrap();
        assert!(config.get_hosts().is_empty());
        assert_eq!(config.get_ports(), &[5432]);
    }

    #[test]
    fn query_templates_are_fully_substituted() {
        for query in all_queries() {
            assert!(
                !query.sql.contains(":1") && !query.sql.contains(":s"),
                "{} still contains a TPC-H placeholder",
                query.id
            );
            assert!(
                !query.sql.contains("revenue:s"),
                "{} contains the non-PostgreSQL q15 view name",
                query.id
            );
        }
    }

    #[test]
    fn query_templates_are_standalone_sql_files() {
        assert_eq!(all_queries().len(), 22);
        for query in all_queries() {
            assert!(
                query.sql.trim_end().ends_with(';'),
                "{} should be usable directly from psql",
                query.id
            );
        }
    }

    #[test]
    fn top_n_queries_keep_canonical_limits() {
        assert!(query_sql("q02").contains("LIMIT 100"));
        assert!(query_sql("q03").contains("LIMIT 10"));
        assert!(query_sql("q10").contains("LIMIT 20"));
        assert!(query_sql("q18").contains("LIMIT 100"));
    }

    #[test]
    fn q02_keeps_canonical_minimum_cost_subquery() {
        let q02 = query_sql("q02");
        assert!(q02.contains("AND ps.ps_supplycost = ("));
        assert!(q02.contains("SELECT min(ps2.ps_supplycost)"));
        assert!(q02.contains("WHERE ps2.ps_partkey = p.p_partkey"));
    }

    #[test]
    fn rejects_non_finite_numeric_args() {
        let mut args = test_args();
        args.scale_factor = f64::NAN;
        assert!(validate_args(&args)
            .unwrap_err()
            .to_string()
            .contains("--scale-factor"));

        let mut args = test_args();
        args.scale_factor = f64::INFINITY;
        assert!(validate_args(&args)
            .unwrap_err()
            .to_string()
            .contains("--scale-factor"));

        let mut args = test_args();
        args.timeout = f64::INFINITY;
        assert!(validate_args(&args)
            .unwrap_err()
            .to_string()
            .contains("--timeout"));
    }

    #[test]
    fn schema_uses_native_numeric_and_date_types() {
        let schema = render_schema_sql("tpch");
        assert!(schema.contains("s_acctbal numeric(15,2)"));
        assert!(schema.contains("l_discount numeric(15,2)"));
        assert!(schema.contains("o_orderdate date NOT NULL"));
        assert!(schema.contains("l_shipdate date NOT NULL"));
        assert!(!schema.contains("double precision"));
    }

    #[test]
    fn output_comparison_requires_identical_bytes() {
        assert!(outputs_match(b"1.0000000,a\n", b"1.0000000,a\n"));
        assert!(!outputs_match(b"1.0000000,a\n", b"1.0000001,a\n"));
        assert!(!outputs_match(b"1.0,a\n", b"1.00,a\n"));
        assert!(!outputs_match(b"1.0,a\n", b"1.0,b\n"));
    }

    #[test]
    fn csv_row_count_includes_one_column_null_rows() {
        assert_eq!(count_csv_rows(b""), 0);
        assert_eq!(count_csv_rows(b"\n"), 1);
        assert_eq!(count_csv_rows(b"\n\n"), 2);
        assert_eq!(count_csv_rows(b",\n"), 1);
        assert_eq!(count_csv_rows(b"\"line\nbreak\"\n"), 1);
        assert_eq!(count_csv_rows(b"\"escaped \"\" quote\"\n\n"), 2);
    }

    #[test]
    fn median_handles_even_and_odd_runs() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&[4.0, 2.0, 10.0, 8.0]), Some(6.0));
    }

    #[test]
    fn report_table_contains_core_columns() {
        let summary = QuerySummary {
            query: "q01".to_string(),
            title: "Pricing Summary Report".to_string(),
            status: QueryStatus::Ok,
            pg_median_ms: Some(10.0),
            fusion_median_ms: Some(5.0),
            speedup: Some(2.0),
            fusion_vs_pg: Some(0.5),
            pg_rows: Some(4),
            fusion_rows: Some(4),
            result_match: true,
            pg_times_ms: vec![10.0],
            fusion_times_ms: vec![5.0],
            pg_hash: Some("a".to_string()),
            fusion_hash: Some("a".to_string()),
            pg_error: None,
            fusion_error: None,
        };
        let table = render_report_table(true, &[summary]);
        assert!(table.contains("query"));
        assert!(table.contains("q01"));
        assert!(table.contains("2.00x"));
    }
}
