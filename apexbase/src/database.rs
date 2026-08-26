//! Top-level database façade for coordinating query and storage services.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::data::{DataType, Value};
use crate::query::{ApexExecutor, ApexResult, QuerySignature, SqlStatement};
use crate::storage::{engine, ColumnType, DurabilityLevel, TableStorageBackend};

pub struct Database;

/// Borrowed session façade that owns query-context setup for every entry point.
///
/// The façade is allocation-free: it only borrows paths supplied by the
/// caller, then restores the executor's thread-local context on drop.
pub struct Session<'a> {
    base_dir: &'a Path,
    table_path: &'a Path,
    root_dir: Option<&'a Path>,
    temp_dir: Option<&'a Path>,
}

struct QueryScope {
    previous_root_dir: Option<Option<std::path::PathBuf>>,
    previous_temp_dir: Option<Option<std::path::PathBuf>>,
}

impl Drop for QueryScope {
    fn drop(&mut self) {
        if let Some(previous) = self.previous_temp_dir.take() {
            if let Some(path) = previous {
                crate::query::executor::set_temp_dir(&path);
            } else {
                crate::query::executor::clear_temp_dir();
            }
        }
        if let Some(previous) = self.previous_root_dir.take() {
            if let Some(path) = previous {
                crate::query::executor::set_query_root_dir(&path);
            } else {
                crate::query::executor::clear_query_root_dir();
            }
        }
    }
}

impl<'a> Session<'a> {
    #[inline]
    pub fn new(base_dir: &'a Path, table_path: &'a Path) -> Self {
        Self {
            base_dir,
            table_path,
            root_dir: None,
            temp_dir: None,
        }
    }

    #[inline]
    pub fn with_root_dir(mut self, root_dir: &'a Path) -> Self {
        self.root_dir = Some(root_dir);
        self
    }

    #[inline]
    pub fn with_temp_dir(mut self, temp_dir: &'a Path) -> Self {
        self.temp_dir = Some(temp_dir);
        self
    }

    #[inline]
    fn enter(&self) -> QueryScope {
        let previous_root_dir = self
            .root_dir
            .map(|_| crate::query::executor::get_query_root_dir());
        let previous_temp_dir = self
            .temp_dir
            .map(|_| crate::query::executor::get_temp_dir());
        if let Some(root_dir) = self.root_dir {
            crate::query::executor::set_query_root_dir(root_dir);
        }
        if let Some(temp_dir) = self.temp_dir {
            crate::query::executor::set_temp_dir(temp_dir);
        }
        QueryScope {
            previous_root_dir,
            previous_temp_dir,
        }
    }

    #[inline]
    pub fn execute(&self, sql: &str) -> io::Result<ApexResult> {
        let _scope = self.enter();
        Database::execute(sql, self.base_dir, self.table_path)
    }

    #[inline]
    pub(crate) fn execute_classified(
        &self,
        sql: &str,
        signature: &QuerySignature,
    ) -> io::Result<ApexResult> {
        let _scope = self.enter();
        Database::execute_classified(sql, signature, self.base_dir, self.table_path)
    }

    #[inline]
    pub fn execute_in_txn(&self, txn_id: u64, statement: SqlStatement) -> io::Result<ApexResult> {
        let _scope = self.enter();
        Database::execute_in_txn(txn_id, statement, self.base_dir, self.table_path)
    }

    #[inline]
    pub fn commit_txn(&self, txn_id: u64) -> io::Result<ApexResult> {
        let _scope = self.enter();
        Database::commit_txn(txn_id, self.base_dir, self.table_path)
    }

    #[inline]
    pub fn rollback_txn(&self, txn_id: u64) -> io::Result<ApexResult> {
        let _scope = self.enter();
        Database::rollback_txn(txn_id)
    }

    #[inline]
    pub fn execute_multi_with_txn(
        &self,
        statements: Vec<SqlStatement>,
        initial_txn_id: Option<u64>,
    ) -> io::Result<(ApexResult, Option<u64>)> {
        let _scope = self.enter();
        Database::execute_multi_with_txn(statements, self.base_dir, self.table_path, initial_txn_id)
    }

    #[inline]
    pub(crate) fn copy_import(
        &self,
        table_path: &Path,
        table_name: &str,
        file_path: &str,
        format: &str,
        options: &[(String, String)],
    ) -> io::Result<ApexResult> {
        let _scope = self.enter();
        Database::copy_import(
            table_path,
            table_name,
            file_path,
            format,
            options,
            self.base_dir,
            self.table_path,
        )
    }
}

impl Database {
    #[inline]
    pub(crate) fn cached_backend(table_path: &Path) -> io::Result<Arc<TableStorageBackend>> {
        crate::query::executor::get_cached_backend_pub(table_path)
    }

    #[inline]
    pub(crate) fn cache_backend(table_path: &Path, backend: Arc<TableStorageBackend>) {
        crate::query::executor::cache_backend_pub(table_path, backend);
    }

    #[inline]
    pub(crate) fn open_backend(
        table_path: &Path,
        durability: DurabilityLevel,
    ) -> io::Result<Arc<TableStorageBackend>> {
        if let Some(backend) = engine().memory_backend(table_path) {
            return Ok(backend);
        }
        TableStorageBackend::open_with_durability(table_path, durability).map(Arc::new)
    }

    #[inline]
    pub(crate) fn open_insert_backend(
        table_path: &Path,
        durability: DurabilityLevel,
    ) -> io::Result<Arc<TableStorageBackend>> {
        if let Some(backend) = engine().memory_backend(table_path) {
            return Ok(backend);
        }
        TableStorageBackend::open_for_insert_with_durability(table_path, durability).map(Arc::new)
    }

    #[inline]
    pub(crate) fn open_write_backend(
        table_path: &Path,
        durability: DurabilityLevel,
    ) -> io::Result<Arc<TableStorageBackend>> {
        if let Some(backend) = engine().memory_backend(table_path) {
            return Ok(backend);
        }
        TableStorageBackend::open_for_write_with_durability(table_path, durability).map(Arc::new)
    }

    #[inline]
    pub(crate) fn create_backend(
        table_path: &Path,
        durability: DurabilityLevel,
    ) -> io::Result<Arc<TableStorageBackend>> {
        if let Some(backend) = engine().memory_backend(table_path) {
            return Ok(backend);
        }
        if crate::storage::is_memory_path(table_path) {
            engine().create_table(table_path, durability)?;
            return engine().get_read_backend(table_path);
        }
        crate::storage::table_catalog::materialize_table_backend(table_path, durability)
            .map(Arc::new)
    }

    #[inline]
    pub(crate) fn temp_dir() -> Option<std::path::PathBuf> {
        crate::query::executor::get_temp_dir()
    }

    #[inline]
    pub(crate) fn clear_temp_dir() {
        crate::query::executor::clear_temp_dir();
    }

    #[inline]
    pub(crate) fn invalidate_query_cache(table_path: &Path) {
        crate::query::executor::invalidate_storage_cache(table_path);
    }

    #[cfg(target_os = "windows")]
    #[inline]
    pub(crate) fn invalidate_query_cache_for_dir(base_dir: &Path) {
        crate::query::executor::ApexExecutor::invalidate_cache_for_dir(base_dir);
    }

    #[inline]
    pub(crate) fn wait_fts_backfill(base_dir: &Path, table_name: &str) {
        crate::query::executor::wait_fts_backfill(base_dir, table_name);
    }

    #[inline]
    pub(crate) fn has_fts_backfill(base_dir: &Path, table_name: &str) -> bool {
        crate::query::executor::has_fts_backfill(base_dir, table_name)
    }

    #[inline]
    pub(crate) fn fts_manager(base_dir: &Path) -> Option<Arc<crate::fts::FtsManager>> {
        crate::query::executor::get_fts_manager(base_dir)
    }

    #[inline]
    pub(crate) fn register_fts_manager(base_dir: &Path, manager: Arc<crate::fts::FtsManager>) {
        crate::query::executor::register_fts_manager(base_dir, manager);
    }

    #[inline]
    pub(crate) fn unregister_fts_manager(base_dir: &Path) {
        crate::query::executor::unregister_fts_manager(base_dir);
    }

    #[inline]
    pub fn invalidate(table_path: &Path) {
        engine().invalidate(table_path);
    }

    #[inline]
    pub fn invalidate_dir(dir: &Path) {
        engine().invalidate_dir(dir);
    }

    #[inline]
    pub fn read_backend(table_path: &Path) -> io::Result<Arc<TableStorageBackend>> {
        engine().get_read_backend(table_path)
    }

    #[inline]
    pub fn write_backend(
        table_path: &Path,
        durability: DurabilityLevel,
    ) -> io::Result<Arc<TableStorageBackend>> {
        engine().get_write_backend(table_path, durability)
    }

    #[inline]
    pub fn create_table(table_path: &Path, durability: DurabilityLevel) -> io::Result<()> {
        engine().create_table(table_path, durability)
    }

    #[inline]
    pub fn create_table_with_schema(
        table_path: &Path,
        durability: DurabilityLevel,
        schema: &[(String, ColumnType)],
    ) -> io::Result<()> {
        engine().create_table_with_schema(table_path, durability, schema)
    }

    #[inline]
    pub fn create_table_with_schema_object(
        table_path: &Path,
        durability: DurabilityLevel,
        schema: crate::storage::OnDemandSchema,
    ) -> io::Result<()> {
        engine().create_table_with_schema_object(table_path, durability, schema)
    }

    /// True when a table exists as a file on disk or as a process-local
    /// in-memory table. This is the storage-facade equivalent of
    /// `table_catalog::file_exists_or_registered` that also sees memory tables.
    #[inline]
    pub fn table_exists(table_path: &Path) -> bool {
        engine().table_exists(table_path)
    }

    /// Remove a process-local in-memory table. Returns false when absent.
    #[inline]
    pub fn drop_memory_table(table_path: &Path) -> bool {
        engine().drop_memory_table(table_path)
    }

    /// List table names in a process-local in-memory database directory.
    #[inline]
    pub fn list_memory_tables(base_dir: &Path) -> Vec<String> {
        engine().list_memory_tables(base_dir)
    }

    /// Release every process-local in-memory table under a database directory.
    #[inline]
    pub fn drop_memory_database(base_dir: &Path) {
        engine().drop_memory_database(base_dir)
    }

    #[inline]
    pub fn add_quantized_vector_column(
        table_path: &Path,
        source_column: &str,
        target_column: &str,
        target_dtype: crate::data::DataType,
        durability: DurabilityLevel,
    ) -> io::Result<()> {
        engine().add_quantized_vector_column(
            table_path,
            source_column,
            target_column,
            target_dtype,
            durability,
        )
    }

    #[inline]
    pub fn replace(
        table_path: &Path,
        id: u64,
        fields: &HashMap<String, Value>,
        durability: DurabilityLevel,
    ) -> io::Result<bool> {
        engine().replace(table_path, id, fields, durability)
    }

    #[inline]
    pub fn delete_one(table_path: &Path, id: u64, durability: DurabilityLevel) -> io::Result<bool> {
        engine().delete_one(table_path, id, durability)
    }

    #[inline]
    pub fn delete(
        table_path: &Path,
        ids: &[u64],
        durability: DurabilityLevel,
    ) -> io::Result<usize> {
        engine().delete(table_path, ids, durability)
    }

    #[inline]
    pub fn active_row_count(table_path: &Path) -> io::Result<u64> {
        engine().active_row_count(table_path)
    }

    #[inline]
    pub fn exists(table_path: &Path, id: u64) -> io::Result<bool> {
        engine().exists(table_path, id)
    }

    #[inline]
    pub fn schema(table_path: &Path) -> io::Result<Vec<(String, DataType)>> {
        engine().get_schema(table_path)
    }

    #[inline]
    pub fn columns(table_path: &Path) -> io::Result<Vec<String>> {
        engine().list_columns(table_path)
    }

    #[inline]
    pub fn column_type(table_path: &Path, name: &str) -> io::Result<Option<DataType>> {
        engine().get_column_type(table_path, name)
    }

    #[inline]
    pub fn add_column(
        table_path: &Path,
        name: &str,
        dtype: DataType,
        durability: DurabilityLevel,
    ) -> io::Result<()> {
        engine().add_column(table_path, name, dtype, durability)
    }

    #[inline]
    pub fn drop_column(
        table_path: &Path,
        name: &str,
        durability: DurabilityLevel,
    ) -> io::Result<()> {
        engine().drop_column(table_path, name, durability)
    }

    #[inline]
    pub fn drop_quantized_vector_column(
        table_path: &Path,
        target: &str,
        durability: DurabilityLevel,
    ) -> io::Result<()> {
        engine().drop_quantized_vector_column(table_path, target, durability)
    }

    #[inline]
    pub fn rename_column(
        table_path: &Path,
        old_name: &str,
        new_name: &str,
        durability: DurabilityLevel,
    ) -> io::Result<()> {
        engine().rename_column(table_path, old_name, new_name, durability)
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn write_typed(
        table_path: &Path,
        int_columns: HashMap<String, Vec<i64>>,
        float_columns: HashMap<String, Vec<f64>>,
        string_columns: HashMap<String, Vec<String>>,
        binary_columns: HashMap<String, Vec<Vec<u8>>>,
        fixedlist_columns: HashMap<String, Vec<Vec<u8>>>,
        bool_columns: HashMap<String, Vec<bool>>,
        null_positions: HashMap<String, Vec<bool>>,
        durability: DurabilityLevel,
    ) -> io::Result<Vec<u64>> {
        engine().write_typed(
            table_path,
            int_columns,
            float_columns,
            string_columns,
            binary_columns,
            fixedlist_columns,
            bool_columns,
            null_positions,
            durability,
        )
    }

    /// Write pre-built columnar data (borrowed-buffer path used by the Python
    /// binding). See `StorageEngine::write_typed_columns`.
    #[inline]
    pub fn write_typed_columns(
        table_path: &Path,
        columns: HashMap<String, crate::storage::on_demand::ColumnData>,
        null_positions: HashMap<String, Vec<bool>>,
        durability: DurabilityLevel,
    ) -> io::Result<Vec<u64>> {
        engine().write_typed_columns(table_path, columns, null_positions, durability)
    }

    #[inline]
    pub fn execute(sql: &str, base_dir: &Path, table_path: &Path) -> io::Result<ApexResult> {
        ApexExecutor::execute_with_base_dir(sql, base_dir, table_path)
    }

    #[inline]
    pub fn query(sql: &str, base_dir: &Path, table_path: &Path) -> io::Result<RecordBatch> {
        Self::execute(sql, base_dir, table_path)?.to_record_batch()
    }

    #[inline]
    pub(crate) fn execute_classified(
        sql: &str,
        signature: &QuerySignature,
        base_dir: &Path,
        table_path: &Path,
    ) -> io::Result<ApexResult> {
        ApexExecutor::execute_classified_with_base_dir(sql, signature, base_dir, table_path)
    }

    #[inline]
    pub fn execute_in_txn(
        txn_id: u64,
        statement: SqlStatement,
        base_dir: &Path,
        table_path: &Path,
    ) -> io::Result<ApexResult> {
        ApexExecutor::execute_in_txn(txn_id, statement, base_dir, table_path)
    }

    #[inline]
    pub fn commit_txn(txn_id: u64, base_dir: &Path, table_path: &Path) -> io::Result<ApexResult> {
        ApexExecutor::execute_commit_txn(txn_id, base_dir, table_path)
    }

    #[inline]
    pub fn rollback_txn(txn_id: u64) -> io::Result<ApexResult> {
        ApexExecutor::execute_rollback_txn(txn_id)
    }

    #[inline]
    pub fn execute_multi_with_txn(
        statements: Vec<SqlStatement>,
        base_dir: &Path,
        table_path: &Path,
        initial_txn_id: Option<u64>,
    ) -> io::Result<(ApexResult, Option<u64>)> {
        ApexExecutor::execute_multi_with_txn(statements, base_dir, table_path, initial_txn_id)
    }

    #[inline]
    pub(crate) fn copy_import(
        table_path: &Path,
        table_name: &str,
        file_path: &str,
        format: &str,
        options: &[(String, String)],
        base_dir: &Path,
        default_table_path: &Path,
    ) -> io::Result<ApexResult> {
        ApexExecutor::execute_copy_import(
            table_path,
            table_name,
            file_path,
            format,
            options,
            base_dir,
            default_table_path,
        )
    }

    #[inline]
    pub(crate) fn fts_backfill(
        base_dir: &Path,
        table: &str,
        fields: Option<&[String]>,
        manager: std::sync::Arc<crate::fts::FtsManager>,
    ) -> io::Result<usize> {
        ApexExecutor::fts_backfill_table(base_dir, table, fields, manager)
    }

    pub fn write(
        table_path: &Path,
        rows: &[HashMap<String, Value>],
        durability: DurabilityLevel,
    ) -> io::Result<Vec<u64>> {
        let ids = engine().write(table_path, rows, durability)?;
        Self::notify_indexes_after_write(table_path, &ids);
        Ok(ids)
    }

    #[inline]
    pub fn notify_indexes_after_write(table_path: &Path, ids: &[u64]) {
        ApexExecutor::notify_indexes_after_write(table_path, ids);
    }
}
