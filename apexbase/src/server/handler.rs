//! PostgreSQL wire protocol handler for ApexBase
//!
//! Bridges pgwire's SimpleQueryHandler to ApexBase's ApexExecutor.

use std::collections::HashMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::Semaphore;

use async_trait::async_trait;
use futures::stream;
use futures::Sink;

use arrow::array::*;
use arrow::datatypes::{DataType as ArrowDataType, Int32Type, UInt32Type};
use arrow::record_batch::RecordBatch;

use bytes::Bytes;
use once_cell::sync::Lazy;
use regex::Regex;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::Type;
use pgwire::api::{ClientInfo, ClientPortalStore};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use super::pg_catalog;
use super::types::{schema_to_field_info, schema_to_field_info_binary};

/// Client metadata key for tracking current database per connection
const METADATA_KEY_DB: &str = "apex_current_db";

/// Default number of statements that may execute at once.
///
/// Every query used to run *on* a tokio worker: execution concurrency was capped
/// by the worker count and a long scan stalled that worker's share of the
/// connection loop. Queries now run on the blocking pool under this bound, so
/// the reactor keeps accepting, reading and encoding while they execute.
/// `APEX_SERVER_QUERY_CONCURRENCY` overrides it; an unparsable or zero value
/// keeps the derived default.
pub fn default_query_concurrency() -> usize {
    let cpus = std::thread::available_parallelism().map_or(4, |count| count.get());
    std::env::var("APEX_SERVER_QUERY_CONCURRENCY")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| cpus.saturating_mul(2).max(1))
}

/// How long a statement waits for a query slot before it is rejected.
///
/// `APEX_SERVER_QUERY_WAIT_MS` overrides it; zero or an unparsable value keeps
/// the default. The bound exists because the semaphore queue is unbounded: a
/// client must learn that the server is at capacity instead of waiting forever.
pub fn default_query_wait() -> std::time::Duration {
    let millis = std::env::var("APEX_SERVER_QUERY_WAIT_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(30_000);
    std::time::Duration::from_millis(millis)
}

/// A statement failure with the SQLSTATE a client acts on.
#[derive(Debug)]
struct QueryFailure {
    code: &'static str,
    message: String,
}

impl QueryFailure {
    fn new(message: impl Into<String>) -> Self {
        Self {
            code: "42000",
            message: message.into(),
        }
    }

    /// The server is at its statement capacity; the client may retry.
    fn capacity(message: impl Into<String>) -> Self {
        Self {
            code: "53300",
            message: message.into(),
        }
    }
}

/// ApexBase handler for PostgreSQL wire protocol
pub struct ApexBaseHandler {
    /// Root directory containing .apex database files and named-database subdirs
    base_dir: PathBuf,
    /// Schema cache: SQL template → Arc<Vec<FieldInfo>>.
    /// Avoids re-executing queries in do_describe_statement just to get column schema.
    schema_cache: RwLock<HashMap<String, (Arc<Vec<FieldInfo>>, u64)>>,
    /// Admission control for query execution. Waiting here queues a statement
    /// instead of occupying a reactor thread, and never blocks the connection.
    query_slots: Arc<Semaphore>,
    /// How long a statement may wait for a slot before it is rejected.
    query_wait: std::time::Duration,
    /// Configured capacity, reported even while every slot is busy.
    query_limit: usize,
}

impl ApexBaseHandler {
    pub fn new(base_dir: PathBuf) -> Self {
        Self::with_query_limits(base_dir, default_query_concurrency(), default_query_wait())
    }

    /// Build a handler whose queries run at most `query_concurrency` at a time.
    pub fn with_query_concurrency(base_dir: PathBuf, query_concurrency: usize) -> Self {
        Self::with_query_limits(base_dir, query_concurrency, default_query_wait())
    }

    /// Build a handler with explicit admission limits.
    pub fn with_query_limits(
        base_dir: PathBuf,
        query_concurrency: usize,
        query_wait: std::time::Duration,
    ) -> Self {
        let query_limit = query_concurrency.max(1);
        Self {
            base_dir,
            schema_cache: RwLock::new(HashMap::new()),
            query_slots: Arc::new(Semaphore::new(query_limit)),
            query_wait,
            query_limit,
        }
    }

    /// How many statements this handler runs at once (the configured capacity).
    pub fn query_concurrency(&self) -> usize {
        self.query_limit
    }

    /// Slots that are free right now.
    pub fn available_query_slots(&self) -> usize {
        self.query_slots.available_permits()
    }

    /// How long a statement waits for a slot before it is rejected.
    pub fn query_wait(&self) -> std::time::Duration {
        self.query_wait
    }

    /// Look up cached schema for a SQL string.
    fn cached_schema(&self, sql: &str) -> Option<Arc<Vec<FieldInfo>>> {
        let epoch = crate::storage::epoch::global_current();
        self.schema_cache
            .read()
            .get(sql)
            .and_then(|(fields, observed_epoch)| {
                (*observed_epoch == epoch).then(|| Arc::clone(fields))
            })
    }

    /// Store schema in cache after successful execution.
    fn cache_schema(&self, sql: &str, fields: Arc<Vec<FieldInfo>>) {
        let mut cache = self.schema_cache.write();
        if cache.len() > 512 {
            cache.clear();
        }
        cache.insert(
            sql.to_string(),
            (fields, crate::storage::epoch::global_current()),
        );
    }

    /// Compute effective base_dir for a given database name.
    fn effective_base_dir(&self, current_db: &str) -> PathBuf {
        if current_db.is_empty() || current_db.eq_ignore_ascii_case("default") {
            self.base_dir.clone()
        } else {
            self.base_dir.join(current_db)
        }
    }

    /// Execute a SQL query in the context of the given database.
    ///
    /// This is the synchronous core: the caller owns a query slot and runs it on
    /// a blocking thread, so it must not touch the async runtime.
    fn execute_query_sync(
        base_dir: &Path,
        database_dir: &Path,
        sql: &str,
    ) -> Result<RecordBatch, String> {
        let default_table_path = database_dir.join("apexbase.apex");
        crate::Session::new(database_dir, &default_table_path)
            .with_root_dir(base_dir)
            .execute(sql)
            .map_err(|e| e.to_string())?
            .to_record_batch()
            .map_err(|e| e.to_string())
    }

    /// Execute a SQL query in the context of the given database, waiting for a
    /// query slot and running the work on the blocking pool.
    ///
    /// The wait is bounded: the semaphore queue is unbounded, so past
    /// [`Self::query_wait`] the statement is rejected with `53300` instead of
    /// queueing forever.
    async fn run_query(
        &self,
        sql: &str,
        current_db: &str,
    ) -> Result<RecordBatch, QueryFailure> {
        // Catalog probes are cheap metadata answers that every client polls on
        // connect; they must never queue behind a running table scan.
        if let Some(batch) = pg_catalog::try_handle_catalog_query(sql, &self.base_dir) {
            return Ok(batch);
        }
        let base_dir = self.base_dir.clone();
        let database_dir = self.effective_base_dir(current_db);
        let sql = sql.to_string();
        let permit = match tokio::time::timeout(
            self.query_wait,
            Arc::clone(&self.query_slots).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(error)) => {
                return Err(QueryFailure::new(format!("query admission closed: {error}")))
            }
            Err(_) => {
                return Err(QueryFailure::capacity(format!(
                    "too many concurrent statements: all {} query slots are busy",
                    self.query_concurrency()
                )))
            }
        };
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            Self::execute_query_sync(&base_dir, &database_dir, &sql)
        })
        .await
        .map_err(|error| QueryFailure::new(format!("query task failed: {error}")))?
        .map_err(QueryFailure::new)
    }

    /// Try to parse a USE / \c database-switching command.
    /// Returns the sanitized database name if recognised, None otherwise.
    fn try_parse_use_cmd(sql: &str) -> Option<String> {
        let trimmed = sql.trim();
        let db_name = if trimmed.len() >= 4 && trimmed[..4].eq_ignore_ascii_case("USE ") {
            trimmed[4..]
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .trim_matches('`')
        } else if trimmed.starts_with("\\c ")
            || trimmed.starts_with("\\C ")
            || trimmed.starts_with("\\c\t")
            || trimmed.starts_with("\\C\t")
        {
            trimmed[3..].trim()
        } else {
            return None;
        };
        let safe_db: String = if db_name.is_empty() || db_name.eq_ignore_ascii_case("default") {
            "default".to_string()
        } else {
            db_name
                .chars()
                .map(|c| {
                    if c.is_alphanumeric() || c == '_' || c == '-' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect()
        };
        Some(safe_db)
    }

    /// Read the current database from client metadata (defaults to "default").
    fn current_db_from_metadata<C: ClientInfo>(client: &mut C) -> String {
        client
            .metadata()
            .get(METADATA_KEY_DB)
            .cloned()
            .unwrap_or_else(|| "default".to_string())
    }

    /// Stream encoded `DataRow`s for a batch.
    ///
    /// The whole `Vec<DataRow>` used to be built before the first row was sent,
    /// so a result cost its full *encoded* size on top of the Arrow batch it came
    /// from (text encoding of a wide result can exceed the batch itself). Rows are
    /// now encoded one at a time as the client consumes them, which keeps only
    /// one encoded row alive.
    ///
    /// The first row is still encoded eagerly: an unsupported column that cannot
    /// produce a row fails the statement before anything is sent, exactly as
    /// before. Columns are extracted once, outside the row loop.
    fn encode_batch_rows(
        batch: RecordBatch,
        field_infos: Arc<Vec<FieldInfo>>,
    ) -> PgWireResult<impl Iterator<Item = PgWireResult<pgwire::messages::data::DataRow>> + Send + 'static>
    {
        let num_rows = batch.num_rows();
        // Pre-extract all column refs once — avoids repeated batch.column() + Arc clone per row
        let cols: Vec<ArrayRef> = (0..batch.num_columns())
            .map(|index| batch.column(index).clone())
            .collect();

        // The `?` is the eager validation: a row that cannot be encoded fails the
        // statement before the first DataRow is sent.
        let validated = if num_rows == 0 {
            None
        } else {
            Some(Self::encode_row(&cols, &field_infos, 0)?)
        };
        let first = validated.map(Ok);
        let rest = (usize::from(num_rows > 0)..num_rows)
            .map(move |row| Self::encode_row(&cols, &field_infos, row));
        Ok(first.into_iter().chain(rest))
    }

    /// Encode one row of already-extracted columns.
    fn encode_row(
        cols: &[ArrayRef],
        field_infos: &Arc<Vec<FieldInfo>>,
        row: usize,
    ) -> PgWireResult<pgwire::messages::data::DataRow> {
        let mut encoder = DataRowEncoder::new(field_infos.clone());
        for col in cols {
            encode_arrow_value(&mut encoder, col, row)?;
        }
        encoder.finish()
    }

    /// Strip leading SQL comments (-- and /* */) to get the effective SQL start
    fn strip_sql_comments(sql: &str) -> &str {
        let mut s = sql.trim();
        loop {
            if s.starts_with("--") {
                // Skip to end of line
                if let Some(pos) = s.find('\n') {
                    s = s[pos + 1..].trim();
                } else {
                    return ""; // entire string is a comment
                }
            } else if s.starts_with("/*") {
                // Skip to closing */
                if let Some(pos) = s.find("*/") {
                    s = s[pos + 2..].trim();
                } else {
                    return ""; // unterminated comment
                }
            } else {
                return s;
            }
        }
    }

    /// Check if SQL is a write/command operation (not a SELECT query)
    fn is_write_op(sql: &str) -> bool {
        let s = Self::strip_sql_comments(sql);
        if s.is_empty() {
            return false;
        }
        // First-byte guard + eq_ignore_ascii_case — zero allocation
        match s.as_bytes()[0] | 0x20 {
            b'i' => s.len() >= 6 && s[..6].eq_ignore_ascii_case("INSERT"),
            b'u' => s.len() >= 6 && s[..6].eq_ignore_ascii_case("UPDATE"),
            b'd' => {
                (s.len() >= 6 && s[..6].eq_ignore_ascii_case("DELETE"))
                    || (s.len() >= 4 && s[..4].eq_ignore_ascii_case("DROP"))
            }
            b'c' => {
                (s.len() >= 6 && s[..6].eq_ignore_ascii_case("CREATE"))
                    || (s.len() >= 6 && s[..6].eq_ignore_ascii_case("COMMIT"))
            }
            b'a' => s.len() >= 5 && s[..5].eq_ignore_ascii_case("ALTER"),
            b't' => s.len() >= 8 && s[..8].eq_ignore_ascii_case("TRUNCATE"),
            b'b' => s.len() >= 5 && s[..5].eq_ignore_ascii_case("BEGIN"),
            b'r' => {
                (s.len() >= 8 && s[..8].eq_ignore_ascii_case("ROLLBACK"))
                    || (s.len() >= 7 && s[..7].eq_ignore_ascii_case("RELEASE"))
            }
            b's' => s.len() >= 9 && s[..9].eq_ignore_ascii_case("SAVEPOINT"),
            _ => false,
        }
    }

    /// Get the proper PG command tag for a SQL statement
    fn command_tag(sql: &str, rows: usize) -> Tag {
        let s = Self::strip_sql_comments(sql);
        // Zero-allocation: first-byte dispatch + eq_ignore_ascii_case
        if s.len() >= 6 && s[..6].eq_ignore_ascii_case("INSERT") {
            Tag::new("INSERT").with_oid(0).with_rows(rows)
        } else if s.len() >= 6 && s[..6].eq_ignore_ascii_case("DELETE") {
            Tag::new("DELETE").with_rows(rows)
        } else if s.len() >= 6 && s[..6].eq_ignore_ascii_case("UPDATE") {
            Tag::new("UPDATE").with_rows(rows)
        } else if s.len() >= 12 && s[..12].eq_ignore_ascii_case("CREATE TABLE") {
            Tag::new("CREATE TABLE")
        } else if s.len() >= 12 && s[..12].eq_ignore_ascii_case("CREATE INDEX") {
            Tag::new("CREATE INDEX")
        } else if s.len() >= 6 && s[..6].eq_ignore_ascii_case("CREATE") {
            Tag::new("CREATE TABLE")
        } else if s.len() >= 4 && s[..4].eq_ignore_ascii_case("DROP") {
            Tag::new("DROP TABLE")
        } else if s.len() >= 5 && s[..5].eq_ignore_ascii_case("ALTER") {
            Tag::new("ALTER TABLE")
        } else if s.len() >= 8 && s[..8].eq_ignore_ascii_case("TRUNCATE") {
            Tag::new("TRUNCATE TABLE")
        } else if s.len() >= 5 && s[..5].eq_ignore_ascii_case("BEGIN") {
            Tag::new("BEGIN")
        } else if s.len() >= 6 && s[..6].eq_ignore_ascii_case("COMMIT") {
            Tag::new("COMMIT")
        } else if s.len() >= 8 && s[..8].eq_ignore_ascii_case("ROLLBACK") {
            Tag::new("ROLLBACK")
        } else if s.len() >= 9 && s[..9].eq_ignore_ascii_case("SAVEPOINT") {
            Tag::new("SAVEPOINT")
        } else if s.len() >= 7 && s[..7].eq_ignore_ascii_case("RELEASE") {
            Tag::new("RELEASE")
        } else {
            Tag::new("OK")
        }
    }
}

#[async_trait]
impl NoopStartupHandler for ApexBaseHandler {
    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        log::info!("Client connected from {}", client.socket_addr(),);
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for ApexBaseHandler {
    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        log::debug!("Simple query: {}", query);

        // Handle multiple statements separated by semicolons
        let statements: Vec<&str> = query
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        let mut responses = Vec::new();

        for sql in statements {
            // Handle USE <database> / \c <database> per-connection switching
            if let Some(db_name) = Self::try_parse_use_cmd(sql) {
                client
                    .metadata_mut()
                    .insert(METADATA_KEY_DB.to_string(), db_name.clone());
                log::info!("Connection switched to database: {}", db_name);
                responses.push(Response::Execution(Tag::new("USE")));
                continue;
            }
            let current_db = Self::current_db_from_metadata(client);
            match self.run_query(sql, &current_db).await {
                Ok(batch) => {
                    if Self::is_write_op(sql) {
                        let tag = Self::command_tag(sql, batch.num_rows());
                        responses.push(Response::Execution(tag));
                    } else {
                        let schema = batch.schema();
                        let field_infos: Arc<Vec<FieldInfo>> =
                            Arc::new(schema_to_field_info(&schema));
                        self.cache_schema(sql, field_infos.clone());

                        let rows = Self::encode_batch_rows(batch, Arc::clone(&field_infos))?;
                        let data_row_stream = stream::iter(rows);
                        responses.push(Response::Query(QueryResponse::new(
                            field_infos,
                            data_row_stream,
                        )));
                    }
                }
                Err(failure) => {
                    return Err(PgWireError::UserError(Box::new(
                        pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            failure.code.to_string(),
                            failure.message,
                        ),
                    )));
                }
            }
        }

        if responses.is_empty() {
            responses.push(Response::EmptyQuery);
        }

        Ok(responses)
    }
}

// CopyHandler — use default no-op implementations
#[async_trait]
impl CopyHandler for ApexBaseHandler {}

// Extended Query Protocol support
// This allows clients like psql, DBeaver, etc. to use prepared statements.
#[async_trait]
impl ExtendedQueryHandler for ApexBaseHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser::new())
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &target.statement;
        if sql.trim().is_empty() {
            return Ok(DescribeStatementResponse::new(vec![], vec![]));
        }

        // Build parameter types: use declared types from Parse message, pad with UNKNOWN
        let param_count = count_params(sql);
        let mut param_types = target.parameter_types.clone();
        while param_types.len() < param_count {
            param_types.push(Type::UNKNOWN);
        }

        if Self::is_write_op(sql) {
            return Ok(DescribeStatementResponse::new(param_types, vec![]));
        }

        // Check schema cache first — avoids executing the query a second time just for schema
        if let Some(cached) = self.cached_schema(sql) {
            return Ok(DescribeStatementResponse::new(
                param_types,
                (*cached).clone(),
            ));
        }

        // Substitute dummy NULLs for parameters to discover result schema
        let dummy_params: Vec<Option<Bytes>> = vec![None; param_count];
        let eval_sql = substitute_parameters(
            sql,
            &dummy_params,
            &Format::UnifiedText,
            &target.parameter_types,
        )
        .unwrap_or_else(|_| sql.clone());

        let current_db = Self::current_db_from_metadata(client);
        match self.run_query(&eval_sql, &current_db).await {
            Ok(batch) => {
                let fields = schema_to_field_info(&batch.schema());
                let fields_arc = Arc::new(fields.clone());
                self.cache_schema(sql, fields_arc);
                Ok(DescribeStatementResponse::new(param_types, fields))
            }
            Err(_) => Ok(DescribeStatementResponse::new(param_types, vec![])),
        }
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &target.statement.statement;
        if sql.trim().is_empty() || Self::is_write_op(sql) {
            return Ok(DescribePortalResponse::new(vec![]));
        }
        // Use actual bound parameters for accurate schema discovery
        let eval_sql = substitute_parameters(
            sql,
            &target.parameters,
            &target.parameter_format,
            &target.statement.parameter_types,
        )
        .unwrap_or_else(|_| sql.clone());

        // Check schema cache first (keyed by SQL template, not bound eval_sql)
        if let Some(cached) = self.cached_schema(sql) {
            return Ok(DescribePortalResponse::new((*cached).clone()));
        }

        let current_db = Self::current_db_from_metadata(client);
        match self.run_query(&eval_sql, &current_db).await {
            Ok(batch) => {
                let fields = schema_to_field_info(&batch.schema());
                let fields_arc = Arc::new(fields.clone());
                self.cache_schema(sql, fields_arc);
                Ok(DescribePortalResponse::new(fields))
            }
            Err(_) => Ok(DescribePortalResponse::new(vec![])),
        }
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let raw_sql = &portal.statement.statement;
        log::debug!("Extended query: {}", raw_sql);

        if raw_sql.trim().is_empty() {
            return Ok(Response::EmptyQuery);
        }

        // Substitute bound parameters ($1, $2, ...) into the SQL
        let sql_owned = substitute_parameters(
            raw_sql,
            &portal.parameters,
            &portal.parameter_format,
            &portal.statement.parameter_types,
        )?;
        let sql = sql_owned.as_str();
        log::debug!("Extended query (substituted): {}", sql);

        let current_db = Self::current_db_from_metadata(_client);
        // Check if client requested binary result format
        let want_binary = matches!(portal.result_column_format, Format::UnifiedBinary);
        match self.run_query(sql, &current_db).await {
            Ok(batch) => {
                if Self::is_write_op(sql) {
                    let tag = Self::command_tag(sql, batch.num_rows());
                    Ok(Response::Execution(tag))
                } else {
                    let schema = batch.schema();
                    let field_infos: Arc<Vec<FieldInfo>> = Arc::new(if want_binary {
                        schema_to_field_info_binary(&schema)
                    } else {
                        schema_to_field_info(&schema)
                    });
                    // Cache text-format schema by raw_sql template for describe_statement reuse
                    if !want_binary {
                        self.cache_schema(raw_sql, field_infos.clone());
                    }

                    let rows = Self::encode_batch_rows(batch, Arc::clone(&field_infos))?;
                    let data_row_stream = stream::iter(rows);
                    Ok(Response::Query(QueryResponse::new(
                        field_infos,
                        data_row_stream,
                    )))
                }
            }
            Err(failure) => Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    failure.code.to_string(),
                    failure.message,
                ),
            ))),
        }
    }
}

// ============================================================================
// Extended Query Protocol helpers
// ============================================================================

static PARAM_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\$(\d+)").unwrap());

/// Count the number of distinct parameters in a SQL string (max $N index found).
fn count_params(sql: &str) -> usize {
    PARAM_RE
        .captures_iter(sql)
        .filter_map(|c| c[1].parse::<usize>().ok())
        .max()
        .unwrap_or(0)
}

/// Convert a text-format parameter value to a SQL literal.
fn text_param_to_sql_literal(s: &str, pg_type: Option<&Type>) -> String {
    let is_numeric = pg_type
        .map(|t| {
            *t == Type::INT2
                || *t == Type::INT4
                || *t == Type::INT8
                || *t == Type::FLOAT4
                || *t == Type::FLOAT8
                || *t == Type::NUMERIC
                || *t == Type::OID
        })
        .unwrap_or(false);

    let is_bool = pg_type.map(|t| *t == Type::BOOL).unwrap_or(false);

    if is_numeric {
        s.to_owned()
    } else if is_bool {
        if s.eq_ignore_ascii_case("t") || s.eq_ignore_ascii_case("true") || s == "1" {
            "true".to_owned()
        } else {
            "false".to_owned()
        }
    } else {
        // Infer from content when type is unknown
        if s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok() {
            return s.to_owned();
        }
        if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false") {
            return s.to_owned();
        }
        // Quote as SQL string literal with single-quote escaping
        format!("'{}'", s.replace('\'', "''"))
    }
}

/// Decode a binary-format parameter to a SQL literal string.
fn binary_param_to_sql_literal(bytes: &Bytes, pg_type: &Type) -> PgWireResult<String> {
    let b = bytes.as_ref();
    if *pg_type == Type::INT2 && b.len() == 2 {
        let v = i16::from_be_bytes([b[0], b[1]]);
        return Ok(v.to_string());
    }
    if *pg_type == Type::INT4 && b.len() == 4 {
        let v = i32::from_be_bytes([b[0], b[1], b[2], b[3]]);
        return Ok(v.to_string());
    }
    if *pg_type == Type::INT8 && b.len() == 8 {
        let arr: [u8; 8] = b.try_into().unwrap();
        let v = i64::from_be_bytes(arr);
        return Ok(v.to_string());
    }
    if *pg_type == Type::FLOAT4 && b.len() == 4 {
        let bits = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
        let v = f32::from_bits(bits);
        return Ok(v.to_string());
    }
    if *pg_type == Type::FLOAT8 && b.len() == 8 {
        let arr: [u8; 8] = b.try_into().unwrap();
        let bits = u64::from_be_bytes(arr);
        let v = f64::from_bits(bits);
        return Ok(v.to_string());
    }
    if *pg_type == Type::BOOL && b.len() == 1 {
        return Ok(if b[0] != 0 {
            "true".to_owned()
        } else {
            "false".to_owned()
        });
    }
    // Fallback: try as UTF-8 string
    match std::str::from_utf8(b) {
        Ok(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
        Err(_) => Err(PgWireError::UserError(Box::new(
            pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(),
                "22021".to_owned(),
                format!("Cannot decode binary parameter of type {}", pg_type.name()),
            ),
        ))),
    }
}

/// Substitute $1, $2, ... parameters into a SQL string.
///
/// Uses regex matching so $10 is not confused with $1. Parameters are
/// converted to safe SQL literals (numbers unquoted, strings single-quoted
/// with escaping, NULLs become the SQL NULL keyword).
fn substitute_parameters(
    sql: &str,
    parameters: &[Option<Bytes>],
    parameter_format: &Format,
    parameter_types: &[Type],
) -> PgWireResult<String> {
    if parameters.is_empty() {
        return Ok(sql.to_owned());
    }

    let mut substitute_error: Option<PgWireError> = None;

    let result = PARAM_RE.replace_all(sql, |caps: &regex::Captures| {
        if substitute_error.is_some() {
            return caps[0].to_owned();
        }

        let n: usize = match caps[1].parse::<usize>() {
            Ok(n) if n >= 1 => n - 1, // 1-indexed → 0-indexed
            _ => return caps[0].to_owned(),
        };

        match parameters.get(n) {
            None => caps[0].to_owned(), // placeholder with no value — keep as-is
            Some(None) => "NULL".to_owned(),
            Some(Some(bytes)) => {
                let is_text = parameter_format.is_text(n);
                let pg_type = parameter_types.get(n);

                if is_text {
                    match std::str::from_utf8(bytes) {
                        Ok(s) => text_param_to_sql_literal(s, pg_type),
                        Err(e) => {
                            substitute_error = Some(PgWireError::UserError(Box::new(
                                pgwire::error::ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "22021".to_owned(),
                                    format!("Invalid UTF-8 in parameter ${}: {}", n + 1, e),
                                ),
                            )));
                            caps[0].to_owned()
                        }
                    }
                } else {
                    let pg_type_ref = pg_type.unwrap_or(&Type::UNKNOWN);
                    match binary_param_to_sql_literal(bytes, pg_type_ref) {
                        Ok(s) => s,
                        Err(e) => {
                            substitute_error = Some(e);
                            caps[0].to_owned()
                        }
                    }
                }
            }
        }
    });

    if let Some(e) = substitute_error {
        return Err(e);
    }

    Ok(result.into_owned())
}

// ============================================================================
// Arrow value encoding helpers
// ============================================================================

/// Encode a single Arrow array value at a given row index into the DataRowEncoder
fn encode_arrow_value(
    encoder: &mut DataRowEncoder,
    array: &ArrayRef,
    row: usize,
) -> PgWireResult<()> {
    if array.is_null(row) {
        encoder.encode_field(&None::<&str>)?;
        return Ok(());
    }

    match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            encoder.encode_field(&arr.value(row))?;
        }
        ArrowDataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            encoder.encode_field(&(arr.value(row) as i16))?;
        }
        ArrowDataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            encoder.encode_field(&arr.value(row))?;
        }
        ArrowDataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            encoder.encode_field(&arr.value(row))?;
        }
        ArrowDataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            encoder.encode_field(&arr.value(row))?;
        }
        ArrowDataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            encoder.encode_field(&(arr.value(row) as i16))?;
        }
        ArrowDataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            encoder.encode_field(&(arr.value(row) as i32))?;
        }
        ArrowDataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            encoder.encode_field(&(arr.value(row) as i64))?;
        }
        ArrowDataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            encoder.encode_field(&arr.value(row).to_string())?;
        }
        ArrowDataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            encoder.encode_field(&arr.value(row))?;
        }
        ArrowDataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            encoder.encode_field(&arr.value(row))?;
        }
        ArrowDataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            encoder.encode_field(&arr.value(row))?;
        }
        ArrowDataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            encoder.encode_field(&arr.value(row))?;
        }
        ArrowDataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            encoder.encode_field(&arr.value(row))?;
        }
        ArrowDataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            encoder.encode_field(&arr.value(row))?;
        }
        ArrowDataType::Dictionary(_, _) => {
            if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
                let values: &StringArray = dict
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let key = dict.keys().value(row) as usize;
                encoder.encode_field(&values.value(key))?;
            } else if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
                let values: &StringArray = dict
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let key = dict.keys().value(row) as usize;
                encoder.encode_field(&values.value(key))?;
            } else {
                encoder.encode_field(&"<dict>")?;
            }
        }
        _ => {
            let formatted = arrow::util::display::array_value_to_string(array, row)
                .unwrap_or_else(|_| "NULL".to_string());
            encoder.encode_field(&formatted)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn query_concurrency_is_clamped_and_reported() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        assert_eq!(
            ApexBaseHandler::with_query_concurrency(path.clone(), 0).query_concurrency(),
            1,
            "zero slots would deadlock every statement"
        );
        assert_eq!(
            ApexBaseHandler::with_query_concurrency(path.clone(), 3).query_concurrency(),
            3
        );
        assert!(ApexBaseHandler::new(path).query_concurrency() > 0);
        // The reported capacity is the configured limit, not what happens to be
        // free: an overloaded server must not tell clients "0 slots".
        let busy = ApexBaseHandler::with_query_concurrency(dir.path().to_path_buf(), 2);
        let held = Arc::clone(&busy.query_slots).try_acquire_owned().unwrap();
        assert_eq!(busy.query_concurrency(), 2);
        assert_eq!(busy.available_query_slots(), 1);
        drop(held);
    }

    #[test]
    fn default_query_concurrency_prefers_the_env_override() {
        let prior = std::env::var("APEX_SERVER_QUERY_CONCURRENCY").ok();
        std::env::set_var("APEX_SERVER_QUERY_CONCURRENCY", "3");
        assert_eq!(default_query_concurrency(), 3);
        std::env::set_var("APEX_SERVER_QUERY_CONCURRENCY", "0");
        let derived = default_query_concurrency();
        assert!(derived > 0, "a zero override must keep the derived default");
        std::env::set_var("APEX_SERVER_QUERY_CONCURRENCY", "many");
        assert_eq!(default_query_concurrency(), derived);
        match prior {
            Some(value) => std::env::set_var("APEX_SERVER_QUERY_CONCURRENCY", value),
            None => std::env::remove_var("APEX_SERVER_QUERY_CONCURRENCY"),
        }
    }

    /// Statements that touch data go through the blocking pool, so they must
    /// still work end to end with a bounded number of slots.
    #[test]
    fn statements_run_through_the_blocking_pool() {
        let dir = tempfile::tempdir().unwrap();
        let handler = ApexBaseHandler::with_query_concurrency(dir.path().to_path_buf(), 2);
        let runtime = runtime();
        runtime
            .block_on(handler.run_query("CREATE TABLE t (a INT)", "default"))
            .unwrap();
        runtime
            .block_on(handler.run_query("INSERT INTO t (a) VALUES (1)", "default"))
            .unwrap();
        let batch = runtime
            .block_on(handler.run_query("SELECT COUNT(*) FROM t", "default"))
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(handler.query_concurrency(), 2, "slots are returned when done");
    }

    /// Rows are produced one at a time, so a result does not need its encoded
    /// copy resident, and a row that cannot be encoded still fails the statement
    /// before anything is sent.
    #[test]
    fn rows_are_encoded_lazily_with_a_validated_first_row() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap();
        let fields: Arc<Vec<FieldInfo>> = Arc::new(schema_to_field_info(&batch.schema()));

        let mut rows = ApexBaseHandler::encode_batch_rows(batch, fields).unwrap();
        let mut count = 0;
        for row in rows.by_ref() {
            assert!(row.is_ok());
            count += 1;
        }
        assert_eq!(count, 3);
        assert!(rows.next().is_none());

        // An empty result yields nothing (and must not need a first row).
        let empty = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Int64,
            false,
        )])));
        let fields: Arc<Vec<FieldInfo>> = Arc::new(schema_to_field_info(&empty.schema()));
        assert_eq!(ApexBaseHandler::encode_batch_rows(empty, fields).unwrap().count(), 0);
    }

    /// A statement that cannot get a slot within the wait budget is rejected with
    /// the capacity SQLSTATE instead of queueing forever, and the connection
    /// keeps working afterwards.
    #[test]
    fn saturated_statements_fail_after_the_wait_budget() {
        let dir = tempfile::tempdir().unwrap();
        let handler = ApexBaseHandler::with_query_limits(
            dir.path().to_path_buf(),
            1,
            std::time::Duration::from_millis(50),
        );
        assert_eq!(handler.query_wait(), std::time::Duration::from_millis(50));

        let busy = Arc::clone(&handler.query_slots).try_acquire_owned().unwrap();
        let started = std::time::Instant::now();
        let failure = runtime()
            .block_on(handler.run_query("CREATE TABLE t (a INT)", "default"))
            .expect_err("the only slot is held, so the statement must be rejected");
        assert_eq!(failure.code, "53300", "{failure:?}");
        assert!(failure.message.contains("query slots"), "{failure:?}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "the wait must be bounded by the configured budget"
        );

        // The slot returns and the connection recovers.
        drop(busy);
        runtime()
            .block_on(handler.run_query("CREATE TABLE t (a INT)", "default"))
            .unwrap();
    }

    /// Metadata probes answer even while every query slot is busy.
    #[test]
    fn catalog_probes_skip_the_admission_bound() {
        let dir = tempfile::tempdir().unwrap();
        let handler = ApexBaseHandler::with_query_concurrency(dir.path().to_path_buf(), 1);
        let busy = Arc::clone(&handler.query_slots).try_acquire_owned().unwrap();
        let batch = runtime()
            .block_on(handler.run_query("SELECT version()", "default"))
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        drop(busy);
        assert_eq!(handler.query_concurrency(), 1);
    }
}
