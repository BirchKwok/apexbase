use super::*;
use crate::storage::OnDemandStorage;
use std::collections::HashMap;
use tempfile::tempdir;

#[test]
fn commit_contract_invalid_transaction_preserves_error_kind() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("t.apex");
    let error = crate::Session::new(dir.path(), &path).commit_txn(u64::MAX).err().expect("invalid transaction must fail");
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    let detail = error.get_ref().unwrap().downcast_ref::<crate::txn::CommitError>().unwrap();
    assert_eq!(detail.outcome, crate::txn::CommitOutcome::Unknown);
    assert_eq!(detail.txn_id, u64::MAX);
    let txn_id = crate::txn::txn_manager().begin();
    let session = crate::Session::new(dir.path(), &path);
    session.commit_txn(txn_id).unwrap();
    let repeated = session.commit_txn(txn_id).err().expect("finished transaction must fail");
    let detail = repeated.get_ref().unwrap().downcast_ref::<crate::txn::CommitError>().unwrap();
    assert_eq!(detail.outcome, crate::txn::CommitOutcome::Unknown);
}

#[test]
#[cfg(unix)]
fn commit_contract_real_io_failures_and_recovery() {
    use std::os::unix::fs::PermissionsExt;
    use crate::txn::{CommitError, CommitOutcome};
    for (suffix, expected, recovered_rows) in [
        ("wal", CommitOutcome::NotCommitted, 1),
        ("delta", CommitOutcome::Unknown, 2),
        ("wal.meta", CommitOutcome::Committed, 2),
    ] {
        let dir = tempdir().unwrap();
        let path = dir.path().join("t.apex");
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path, crate::storage::DurabilityLevel::Safe,
            &[("value".to_string(), crate::storage::ColumnType::Int64)],
        ).unwrap();
        storage.insert_rows(&[HashMap::from([("value".to_string(), crate::storage::ColumnValue::Int64(0))])]).unwrap();
        storage.save_full().unwrap();
        drop(storage);
        let session = crate::Session::new(dir.path(), &path);
        let mgr = crate::txn::txn_manager();
        let txn_id = mgr.begin();
        session.execute_in_txn(txn_id, SqlParser::parse("INSERT INTO t (value) VALUES (1)").unwrap()).unwrap();
        let epoch = crate::storage::epoch::current(&path);
        let fault = dir.path().join(format!("t.apex.{suffix}"));
        if suffix == "wal.meta" {
            if fault.exists() { std::fs::remove_file(&fault).unwrap(); }
            std::fs::create_dir(&fault).unwrap();
        } else {
            if !fault.exists() { std::fs::write(&fault, []).unwrap(); }
            std::fs::set_permissions(&fault, std::fs::Permissions::from_mode(0o444)).unwrap();
        }
        let result = session.commit_txn(txn_id);
        if suffix == "wal.meta" {
            std::fs::remove_dir(&fault).unwrap();
        } else {
            std::fs::set_permissions(&fault, std::fs::Permissions::from_mode(0o644)).unwrap();
        }
        let error = result.err().expect("real I/O fault must fail the commit");
        let detail = error.get_ref().unwrap().downcast_ref::<CommitError>().unwrap();
        assert_eq!(detail.outcome, expected, "{suffix}: {error}");
        assert!(!mgr.is_active(txn_id));
        if expected == CommitOutcome::Committed {
            assert!(crate::storage::epoch::current(&path) > epoch);
        }
        let reopened = OnDemandStorage::open_with_durability(&path, crate::storage::DurabilityLevel::Safe).unwrap();
        assert_eq!(reopened.row_count(), recovered_rows, "{suffix}");
        drop(reopened);
        let next = mgr.begin();
        session.execute_in_txn(next, SqlParser::parse("INSERT INTO t (value) VALUES (2)").unwrap()).unwrap();
        session.commit_txn(next).unwrap();
        let reopened = OnDemandStorage::open_with_durability(&path, crate::storage::DurabilityLevel::Safe).unwrap();
        assert_eq!(reopened.row_count(), recovered_rows + 1, "{suffix}");
    }
}

#[test]
fn wal_backed_transaction_update_commits_and_survives_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal_update_t.apex");
    let storage = OnDemandStorage::create_with_schema_and_durability(
        &path,
        crate::storage::DurabilityLevel::Safe,
        &[("value".to_string(), crate::storage::ColumnType::Int64)],
    ).unwrap();
    storage.insert_rows(&[HashMap::from([(
        "value".to_string(),
        crate::storage::ColumnValue::Int64(7),
    )])]).unwrap();
    storage.save_full().unwrap();
    drop(storage);

    let wal_path = path.with_extension("apex.wal");
    let wal_len = std::fs::metadata(&wal_path).unwrap().len();
    let session = crate::Session::new(dir.path(), &path);
    let mgr = crate::txn::txn_manager();
    let txn_id = mgr.begin();
    session.execute_in_txn(
        txn_id,
        SqlParser::parse("UPDATE wal_update_t SET value = 9 WHERE _id = 1").unwrap(),
    ).unwrap();

    session.commit_txn(txn_id).unwrap();
    assert!(!mgr.is_active(txn_id));
    assert!(std::fs::metadata(&wal_path).unwrap().len() > wal_len);

    let reopened = crate::Session::new(dir.path(), &path);
    let result = reopened
        .execute("SELECT value FROM wal_update_t WHERE _id = 1")
        .unwrap();
    let batch = result.to_record_batch().unwrap();
    let values = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.value(0), 9);
}

#[test]
fn wal_backed_transaction_update_apply_failure_recovers() {
    use crate::txn::{CommitError, CommitOutcome};

    let dir = tempdir().unwrap();
    let path = dir.path().join("wal_update_recovery_t.apex");
    let storage = OnDemandStorage::create_with_schema_and_durability(
        &path,
        crate::storage::DurabilityLevel::Safe,
        &[
            ("value".to_string(), crate::storage::ColumnType::Int64),
            ("other".to_string(), crate::storage::ColumnType::Int64),
        ],
    )
    .unwrap();
    storage
        .insert_rows(&[HashMap::from([
            (
                "value".to_string(),
                crate::storage::ColumnValue::Int64(7),
            ),
            (
                "other".to_string(),
                crate::storage::ColumnValue::Int64(1),
            ),
        ])])
        .unwrap();
    storage.save_full().unwrap();
    drop(storage);

    let session = crate::Session::new(dir.path(), &path);
    let mgr = crate::txn::txn_manager();
    let txn_id = mgr.begin();
    session
        .execute_in_txn(
            txn_id,
            SqlParser::parse(
                "UPDATE wal_update_recovery_t SET value = 9, other = 2 WHERE _id = 1",
            )
            .unwrap(),
        )
        .unwrap();

    let deltastore_tmp_path = dir
        .path()
        .join("wal_update_recovery_t.apex.deltastore.tmp");
    std::fs::create_dir(&deltastore_tmp_path).unwrap();
    let error = session
        .commit_txn(txn_id)
        .err()
        .expect("post-marker UPDATE apply failure must be reported");
    let detail = error
        .get_ref()
        .unwrap()
        .downcast_ref::<CommitError>()
        .unwrap();
    assert_eq!(detail.outcome, CommitOutcome::Unknown);
    assert!(!mgr.is_active(txn_id));
    std::fs::remove_dir(&deltastore_tmp_path).unwrap();

    let reopened = crate::Session::new(dir.path(), &path);
    let result = reopened
        .execute("SELECT value FROM wal_update_recovery_t WHERE _id = 1")
        .unwrap();
    let batch = result.to_record_batch().unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 9);
}

#[test]
#[cfg(unix)]
fn committed_index_save_failure_falls_back_until_reindex() {
    use crate::txn::{CommitError, CommitOutcome};
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir().unwrap();
    let path = dir.path().join("indexed_recovery_t.apex");
    let storage = OnDemandStorage::create_with_schema_and_durability(
        &path,
        crate::storage::DurabilityLevel::Safe,
        &[("value".to_string(), crate::storage::ColumnType::Int64)],
    )
    .unwrap();
    storage
        .insert_rows(&[HashMap::from([(
            "value".to_string(),
            crate::storage::ColumnValue::Int64(1),
        )])])
        .unwrap();
    storage.save_full().unwrap();
    drop(storage);

    let session = crate::Session::new(dir.path(), &path);
    session
        .execute("CREATE INDEX idx_value ON indexed_recovery_t(value) USING HASH")
        .unwrap();
    let index_path = dir
        .path()
        .join("indexes")
        .join("indexed_recovery_t_idx_value.hashidx");
    assert!(index_path.exists());

    let txn_id = crate::txn::txn_manager().begin();
    session
        .execute_in_txn(
            txn_id,
            SqlParser::parse("INSERT INTO indexed_recovery_t (value) VALUES (999)").unwrap(),
        )
        .unwrap();
    std::fs::set_permissions(&index_path, std::fs::Permissions::from_mode(0o444)).unwrap();
    let result = session.commit_txn(txn_id);
    std::fs::set_permissions(&index_path, std::fs::Permissions::from_mode(0o644)).unwrap();

    let error = result
        .err()
        .expect("index persistence failure after the WAL commit point must be reported");
    let detail = error
        .get_ref()
        .unwrap()
        .downcast_ref::<CommitError>()
        .unwrap();
    assert_eq!(detail.outcome, CommitOutcome::Unknown);
    let stale_path = dir.path().join("indexed_recovery_t.apex.index.stale");
    assert!(stale_path.exists());
    assert!(!ApexExecutor::table_has_index_catalog(
        Some(dir.path()),
        &path
    ));

    // The row is committed even though its posting was not persisted. A new
    // session must see it through the authoritative scan fallback.
    let reopened = crate::Session::new(dir.path(), &path);
    let result = reopened
        .execute("SELECT value FROM indexed_recovery_t WHERE value = 999")
        .unwrap();
    let batch = result.to_record_batch().unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values(), &[999]);

    // REINDEX first materializes sidecars/compaction, then rebuilds from the
    // committed table and only clears the durable stale marker after save.
    reopened.execute("REINDEX indexed_recovery_t").unwrap();
    assert!(!stale_path.exists());
    drop(reopened);

    let reopened = crate::Session::new(dir.path(), &path);
    let result = reopened
        .execute("SELECT value FROM indexed_recovery_t WHERE value = 999")
        .unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(ApexExecutor::table_has_index_catalog(Some(dir.path()), &path));
    let idx_mgr = get_index_manager(dir.path(), "indexed_recovery_t");
    let posting = idx_mgr
        .lock()
        .lookup(
            "value",
            &crate::storage::index::index_manager::PredicateHint::Eq(Value::Int64(999)),
        )
        .unwrap()
        .expect("rebuilt HASH index must serve equality lookup");
    assert_eq!(posting.row_ids.len(), 1);
}

#[test]
fn update_unchanged_unique_index_key_after_manager_reload() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("unique_update_t.apex");
    let storage = OnDemandStorage::create_with_schema_and_durability(
        &path,
        crate::storage::DurabilityLevel::Safe,
        &[
            ("name".to_string(), crate::storage::ColumnType::String),
            ("age".to_string(), crate::storage::ColumnType::Int64),
        ],
    )
    .unwrap();
    storage.save_full().unwrap();
    drop(storage);

    let session = crate::Session::new(dir.path(), &path);
    session
        .execute("CREATE UNIQUE INDEX idx_name ON unique_update_t(name) USING HASH")
        .unwrap();
    session
        .execute("INSERT INTO unique_update_t (name, age) VALUES ('Alice', 25)")
        .unwrap();

    // The insert advances the table epoch, so the UPDATE obtains a freshly
    // loaded manager whose runtime index instance has not been loaded yet.
    session
        .execute("UPDATE unique_update_t SET age = 30 WHERE name = 'Alice'")
        .unwrap();
    let result = session
        .execute("SELECT age FROM unique_update_t WHERE name = 'Alice'")
        .unwrap();
    let batch = result.to_record_batch().unwrap();
    let ages = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ages.values(), &[30]);
}

#[test]
fn fast_transaction_update_remains_supported() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("fast_update_t.apex");
    let storage = OnDemandStorage::create_with_schema_and_durability(
        &path,
        crate::storage::DurabilityLevel::Fast,
        &[("value".to_string(), crate::storage::ColumnType::Int64)],
    ).unwrap();
    storage.insert_rows(&[HashMap::from([(
        "value".to_string(),
        crate::storage::ColumnValue::Int64(7),
    )])]).unwrap();
    storage.save_full().unwrap();
    drop(storage);

    let session = crate::Session::new(dir.path(), &path);
    let txn_id = crate::txn::txn_manager().begin();
    session.execute_in_txn(
        txn_id,
        SqlParser::parse("UPDATE fast_update_t SET value = 9 WHERE _id = 1").unwrap(),
    ).unwrap();
    session.commit_txn(txn_id).unwrap();

    let result = session
        .execute("SELECT value FROM fast_update_t WHERE _id = 1")
        .unwrap();
    let batch = result.to_record_batch().unwrap();
    let values = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.value(0), 9);
}

#[test]
fn cross_table_wal_recovery_converges_per_table_at_marker_boundary() {
    use crate::storage::on_demand::ColumnValue;

    let dir = tempdir().unwrap();
    let first_path = dir.path().join("a_first.apex");
    let second_path = dir.path().join("b_second.apex");
    let schema = &[("value".to_string(), crate::storage::ColumnType::Int64)];

    let first = OnDemandStorage::create_with_schema_and_durability(
        &first_path,
        crate::storage::DurabilityLevel::Safe,
        schema,
    )
    .unwrap();
    let second = OnDemandStorage::create_with_schema_and_durability(
        &second_path,
        crate::storage::DurabilityLevel::Safe,
        schema,
    )
    .unwrap();
    for storage in [&first, &second] {
        storage
            .insert_rows(&[HashMap::from([(
                "value".to_string(),
                ColumnValue::Int64(0),
            )])])
            .unwrap();
        storage.save_full().unwrap();
    }

    // This is the durable state produced by a process stopping between the
    // two sorted per-table commit markers: the first WAL has a complete
    // transaction, while the second has the same DML without its marker.
    let txn_id = 77;
    let inserted = HashMap::from([("value".to_string(), ColumnValue::Int64(1))]);
    first.wal_write_txn_begin(txn_id).unwrap();
    first
        .wal_write_txn_insert(txn_id, 2, inserted.clone())
        .unwrap();
    first.wal_write_txn_commit(txn_id).unwrap();

    second.wal_write_txn_begin(txn_id).unwrap();
    second
        .wal_write_txn_insert(txn_id, 2, inserted)
        .unwrap();
    second.wal_sync().unwrap();
    drop(first);
    drop(second);

    let reopened_first = OnDemandStorage::open_with_durability(
        &first_path,
        crate::storage::DurabilityLevel::Safe,
    )
    .unwrap();
    let reopened_second = OnDemandStorage::open_with_durability(
        &second_path,
        crate::storage::DurabilityLevel::Safe,
    )
    .unwrap();
    assert_eq!(reopened_first.row_count(), 2);
    assert_eq!(reopened_second.row_count(), 1);
    drop(reopened_first);
    drop(reopened_second);

    // Per-table recovery is idempotent; it must not accidentally promote the
    // uncommitted suffix on a later open.
    let reopened_first = OnDemandStorage::open_with_durability(
        &first_path,
        crate::storage::DurabilityLevel::Safe,
    )
    .unwrap();
    let reopened_second = OnDemandStorage::open_with_durability(
        &second_path,
        crate::storage::DurabilityLevel::Safe,
    )
    .unwrap();
    assert_eq!(reopened_first.row_count(), 2);
    assert_eq!(reopened_second.row_count(), 1);
}

#[test]
fn max_transaction_commit_syncs_the_wal() {
    for (name, durability, expected_syncs) in [
        ("t_safe", crate::storage::DurabilityLevel::Safe, 0),
        ("t_max", crate::storage::DurabilityLevel::Max, 1),
    ] {
        let dir = tempdir().unwrap();
        let path = dir.path().join(format!("{name}.apex"));
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            durability,
            &[("value".to_string(), crate::storage::ColumnType::Int64)],
        )
        .unwrap();
        storage.save_full().unwrap();
        drop(storage);

        crate::storage::incremental::take_wal_sync_count();
        let session = crate::Session::new(dir.path(), &path).with_durability(durability);
        let txn_id = crate::txn::txn_manager().begin();
        session
            .execute_in_txn(
                txn_id,
                SqlParser::parse(&format!("INSERT INTO {name} (value) VALUES (1)")).unwrap(),
            )
            .unwrap();
        session.commit_txn(txn_id).unwrap();
        assert_eq!(
            crate::storage::incremental::take_wal_sync_count(),
            expected_syncs,
            "{name} commit used the wrong WAL sync boundary"
        );
    }
}

#[test]
#[cfg(unix)]
fn commit_contract_real_wal_stage_failures() {
    use crate::storage::incremental::{set_wal_io_fault, WalIoFault};
    use crate::txn::{CommitError, CommitOutcome};

    struct FaultReset;
    impl Drop for FaultReset {
        fn drop(&mut self) {
            set_wal_io_fault(None);
        }
    }

    for (name, fault, durability, expected_outcome, recovered_rows, wide_value) in [
        (
            "t_write_fault",
            WalIoFault::Write,
            crate::storage::DurabilityLevel::Safe,
            CommitOutcome::NotCommitted,
            0,
            true,
        ),
        (
            "t_flush_fault",
            WalIoFault::Flush,
            crate::storage::DurabilityLevel::Safe,
            CommitOutcome::Unknown,
            0,
            false,
        ),
        (
            "t_sync_fault",
            WalIoFault::Sync,
            crate::storage::DurabilityLevel::Max,
            CommitOutcome::Unknown,
            1,
            false,
        ),
    ] {
        let _reset = FaultReset;
        let dir = tempdir().unwrap();
        let path = dir.path().join(format!("{name}.apex"));
        let column_type = if wide_value {
            crate::storage::ColumnType::String
        } else {
            crate::storage::ColumnType::Int64
        };
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            durability,
            &[("value".to_string(), column_type)],
        )
        .unwrap();
        storage.save_full().unwrap();
        drop(storage);

        let session = crate::Session::new(dir.path(), &path).with_durability(durability);
        let txn_id = crate::txn::txn_manager().begin();
        let insert = if wide_value {
            format!(
                "INSERT INTO {name} (value) VALUES ('{}')",
                "x".repeat(70 * 1024)
            )
        } else {
            format!("INSERT INTO {name} (value) VALUES (1)")
        };
        session
            .execute_in_txn(txn_id, SqlParser::parse(&insert).unwrap())
            .unwrap();
        set_wal_io_fault(Some(fault));
        let error = session
            .commit_txn(txn_id)
            .err()
            .expect("the armed OS-level WAL fault must fail commit");
        set_wal_io_fault(None);
        let detail = error
            .get_ref()
            .unwrap()
            .downcast_ref::<CommitError>()
            .unwrap();
        assert_eq!(detail.outcome, expected_outcome, "{name}: {error}");
        assert!(!crate::txn::txn_manager().is_active(txn_id));

        let reopened = OnDemandStorage::open_with_durability(&path, durability).unwrap();
        assert_eq!(reopened.row_count(), recovered_rows, "{name}");
        drop(reopened);

        let next_txn = crate::txn::txn_manager().begin();
        let next_insert = if wide_value {
            format!("INSERT INTO {name} (value) VALUES ('next')")
        } else {
            format!("INSERT INTO {name} (value) VALUES (2)")
        };
        session
            .execute_in_txn(next_txn, SqlParser::parse(&next_insert).unwrap())
            .unwrap();
        session.commit_txn(next_txn).unwrap();
        let reopened = OnDemandStorage::open_with_durability(&path, durability).unwrap();
        assert_eq!(reopened.row_count(), recovered_rows + 1, "{name}");
    }
}

fn create_test_storage(path: &Path) {
    let storage = OnDemandStorage::create(path).unwrap();

    let mut int_cols: HashMap<String, Vec<i64>> = HashMap::new();
    let mut float_cols: HashMap<String, Vec<f64>> = HashMap::new();
    let mut string_cols: HashMap<String, Vec<String>> = HashMap::new();

    int_cols.insert("id".to_string(), vec![1, 2, 3, 4, 5]);
    int_cols.insert("age".to_string(), vec![25, 30, 35, 40, 45]);
    float_cols.insert("score".to_string(), vec![85.0, 90.0, 75.0, 88.0, 92.0]);
    string_cols.insert(
        "name".to_string(),
        vec![
            "Alice".to_string(),
            "Bob".to_string(),
            "Charlie".to_string(),
            "Diana".to_string(),
            "Eve".to_string(),
        ],
    );

    storage
        .insert_typed(
            int_cols,
            float_cols,
            string_cols,
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();
}

#[test]
fn test_simple_select() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.apex");
    create_test_storage(&path);

    let result = ApexExecutor::execute("SELECT * FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 5);
}

#[test]
fn unregister_fts_manager_waits_for_background_flush() {
    let dir = tempdir().unwrap();
    let manager = Arc::new(crate::fts::FtsManager::new(
        dir.path().join("fts_indexes"),
        crate::fts::FtsConfig::default(),
    ));
    let engine = manager.get_engine("docs").unwrap();
    engine
        .add_document(
            1,
            HashMap::from([("body".to_string(), "background flush".to_string())]),
        )
        .unwrap();
    engine.flush_async().unwrap();

    register_fts_manager(dir.path(), manager);
    unregister_fts_manager(dir.path());

    assert_eq!(engine.wait_flush().unwrap(), 0);
}

#[test]
fn test_select_with_where() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.apex");
    create_test_storage(&path);

    let result = ApexExecutor::execute("SELECT * FROM default WHERE age > 30", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 3); // age 35, 40, 45
}

#[test]
fn test_select_with_limit() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.apex");
    create_test_storage(&path);

    let result = ApexExecutor::execute("SELECT * FROM default LIMIT 2", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_count_aggregate() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.apex");
    create_test_storage(&path);

    let result = ApexExecutor::execute("SELECT COUNT(*) FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 1);

    let count_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count_array.value(0), 5);
}

#[test]
fn test_sum_aggregate() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.apex");
    create_test_storage(&path);

    let result = ApexExecutor::execute("SELECT SUM(age) FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();

    let sum_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(sum_array.value(0), 175); // 25+30+35+40+45
}

#[test]
fn test_order_by() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.apex");
    create_test_storage(&path);

    let result =
        ApexExecutor::execute("SELECT * FROM default ORDER BY age DESC LIMIT 2", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 2);

    let age_array = batch
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(age_array.value(0), 45);
    assert_eq!(age_array.value(1), 40);
}

#[test]
fn test_is_null_query() {
    use crate::data::Value;
    use crate::storage::backend::TableStorageBackend;
    use std::collections::HashMap;

    let dir = tempdir().unwrap();
    let path = dir.path().join("test_null.apex");

    // Create storage with NULL boolean
    {
        let backend = TableStorageBackend::create(&path).unwrap();

        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), Value::Int64(1));
        row1.insert("flag".to_string(), Value::Bool(true));

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), Value::Int64(2));
        row2.insert("flag".to_string(), Value::Bool(false));

        let mut row3 = HashMap::new();
        row3.insert("id".to_string(), Value::Int64(3));
        row3.insert("flag".to_string(), Value::Null); // NULL boolean

        backend.insert_rows(&[row1, row2, row3]).unwrap();
        backend.save().unwrap();
    }

    // Clear any cached backend
    invalidate_storage_cache(&path);

    // First check SELECT * to verify data is correctly read
    let result_all =
        ApexExecutor::execute("SELECT id, flag FROM test_null ORDER BY id", &path).unwrap();
    let batch_all = result_all.to_record_batch().unwrap();

    println!("SELECT all rows: {} rows", batch_all.num_rows());
    if let Some(flag_col) = batch_all.column_by_name("flag") {
        println!(
            "Flag column null_count in SELECT *: {}",
            flag_col.null_count()
        );
        let bool_arr = flag_col.as_any().downcast_ref::<BooleanArray>().unwrap();
        for i in 0..bool_arr.len() {
            println!(
                "  Row {}: is_null={}, value={:?}",
                i,
                bool_arr.is_null(i),
                if bool_arr.is_null(i) {
                    None
                } else {
                    Some(bool_arr.value(i))
                }
            );
        }
    }

    // Clear cache again before IS NULL query
    invalidate_storage_cache(&path);

    // Test reading just the flag column for WHERE evaluation
    let backend = get_cached_backend(&path).unwrap();
    let where_batch = backend
        .read_columns_to_arrow(Some(&["flag"]), 0, None)
        .unwrap();
    println!("WHERE batch (flag only): {} rows", where_batch.num_rows());
    if let Some(flag_col) = where_batch.column_by_name("flag") {
        println!("  null_count: {}", flag_col.null_count());
        let bool_arr = flag_col.as_any().downcast_ref::<BooleanArray>().unwrap();
        for i in 0..bool_arr.len() {
            println!("    Row {}: is_null={}", i, bool_arr.is_null(i));
        }
        // Test is_null compute
        let is_null_mask = arrow::compute::is_null(flag_col).unwrap();
        println!("  is_null mask: {:?}", is_null_mask);
    }

    // Clear cache again
    invalidate_storage_cache(&path);

    // Manually test the predicate evaluation path
    let backend2 = get_cached_backend(&path).unwrap();
    let full_batch = backend2.read_columns_to_arrow(None, 0, None).unwrap();
    println!("Full batch before filter: {} rows", full_batch.num_rows());

    // Check flag column null status in full batch
    if let Some(flag_col) = full_batch.column_by_name("flag") {
        println!("  flag null_count in full batch: {}", flag_col.null_count());
        // Compute is_null mask
        let is_null_mask = arrow::compute::is_null(flag_col).unwrap();
        println!("  is_null mask: {:?}", is_null_mask);
        let true_count = is_null_mask.iter().filter(|v| *v == Some(true)).count();
        println!("  True count in is_null mask: {}", true_count);

        // Manually apply filter
        let filtered_batch =
            arrow::compute::filter_record_batch(&full_batch, &is_null_mask).unwrap();
        println!(
            "  Manually filtered batch: {} rows",
            filtered_batch.num_rows()
        );
    }

    // Clear cache again
    invalidate_storage_cache(&path);

    // Check what the parser produces for the IS NULL query
    let parsed = SqlParser::parse("SELECT * FROM test_null WHERE flag IS NULL").unwrap();
    if let SqlStatement::Select(stmt) = parsed {
        println!("Parsed statement:");
        println!("  is_select_star: {}", stmt.is_select_star());
        println!("  where_clause: {:?}", stmt.where_clause);
        println!("  where_columns: {:?}", stmt.where_columns());
        println!("  order_by: {:?}", stmt.order_by);
    }

    // Test IS NULL query
    let result =
        ApexExecutor::execute("SELECT * FROM test_null WHERE flag IS NULL", &path).unwrap();
    let batch = result.to_record_batch().unwrap();

    println!("IS NULL query result: {} rows", batch.num_rows());
    println!("Schema: {:?}", batch.schema());

    // Check the flag column for nulls
    if let Some(flag_col) = batch.column_by_name("flag") {
        println!("Flag column null_count: {}", flag_col.null_count());
    }

    assert_eq!(batch.num_rows(), 1, "IS NULL should return 1 row");

    // Also test that SELECT * returns correct data
    let result2 =
        ApexExecutor::execute("SELECT id, flag FROM test_null ORDER BY id", &path).unwrap();
    let batch2 = result2.to_record_batch().unwrap();

    println!("SELECT all rows: {} rows", batch2.num_rows());
    if let Some(flag_col) = batch2.column_by_name("flag") {
        println!(
            "Flag column null_count in SELECT *: {}",
            flag_col.null_count()
        );
        let bool_arr = flag_col.as_any().downcast_ref::<BooleanArray>().unwrap();
        for i in 0..bool_arr.len() {
            println!(
                "  Row {}: is_null={}, value={:?}",
                i,
                bool_arr.is_null(i),
                if bool_arr.is_null(i) {
                    None
                } else {
                    Some(bool_arr.value(i))
                }
            );
        }
    }
}

// ========================================================================
// OLTP Tests: Insert, Point Lookup, Update, Delete, Batch Operations
// ========================================================================

fn create_oltp_storage(path: &Path) {
    let storage = OnDemandStorage::create(path).unwrap();
    let mut int_cols: HashMap<String, Vec<i64>> = HashMap::new();
    let mut float_cols: HashMap<String, Vec<f64>> = HashMap::new();
    let mut string_cols: HashMap<String, Vec<String>> = HashMap::new();
    let mut bool_cols: HashMap<String, Vec<bool>> = HashMap::new();

    let n = 1000;
    int_cols.insert("user_id".to_string(), (1..=n as i64).collect());
    int_cols.insert(
        "age".to_string(),
        (0..n).map(|i| 20 + (i % 50) as i64).collect(),
    );
    float_cols.insert(
        "balance".to_string(),
        (0..n).map(|i| 100.0 + i as f64 * 1.5).collect(),
    );
    string_cols.insert(
        "city".to_string(),
        (0..n)
            .map(|i| {
                ["Beijing", "Shanghai", "Shenzhen", "Guangzhou", "Hangzhou"][i % 5].to_string()
            })
            .collect(),
    );
    bool_cols.insert("active".to_string(), (0..n).map(|i| i % 3 != 0).collect());

    storage
        .insert_typed(int_cols, float_cols, string_cols, HashMap::new(), bool_cols)
        .unwrap();
    storage.save().unwrap();
}

#[test]
fn test_oltp_insert_and_row_count() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oltp_insert.apex");
    create_oltp_storage(&path);

    let result = ApexExecutor::execute("SELECT COUNT(*) FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1000);
}

#[test]
fn test_oltp_point_lookup_by_id() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oltp_point.apex");
    create_oltp_storage(&path);

    // Point lookup by _id (first row)
    let result = ApexExecutor::execute("SELECT * FROM default WHERE _id = 1", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let uid = batch
        .column_by_name("user_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(uid, 1);
}

#[test]
fn test_oltp_batch_insert_incremental() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oltp_batch.apex");
    create_oltp_storage(&path);

    // Insert additional batch — force V4 data load via to_arrow_batch
    invalidate_storage_cache(&path);
    let storage = OnDemandStorage::open(&path).unwrap();
    let _ = storage.to_arrow_batch(None, true); // reads via mmap path
    let mut int_cols: HashMap<String, Vec<i64>> = HashMap::new();
    let mut float_cols: HashMap<String, Vec<f64>> = HashMap::new();
    let mut string_cols: HashMap<String, Vec<String>> = HashMap::new();
    int_cols.insert("user_id".to_string(), vec![1001, 1002, 1003]);
    int_cols.insert("age".to_string(), vec![28, 35, 42]);
    float_cols.insert("balance".to_string(), vec![5000.0, 6000.0, 7000.0]);
    string_cols.insert(
        "city".to_string(),
        vec![
            "Chengdu".to_string(),
            "Wuhan".to_string(),
            "Nanjing".to_string(),
        ],
    );
    storage
        .insert_typed(
            int_cols,
            float_cols,
            string_cols,
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();

    invalidate_storage_cache(&path);
    let result = ApexExecutor::execute("SELECT COUNT(*) FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1003);
}

#[test]
fn test_oltp_update_single_row() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oltp_update.apex");
    create_oltp_storage(&path);

    // Update age for user_id = 1
    let result =
        ApexExecutor::execute("UPDATE default SET age = 99 WHERE user_id = 1", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert!(batch.num_rows() >= 1);

    invalidate_storage_cache(&path);
    // After UPDATE, verify updated value exists (UPDATE may soft-delete + re-insert)
    let result2 =
        ApexExecutor::execute("SELECT age FROM default WHERE user_id = 1", &path).unwrap();
    let batch2 = result2.to_record_batch().unwrap();
    assert!(
        batch2.num_rows() >= 1,
        "Should find at least 1 row with user_id=1"
    );
    // Check that at least one row has the updated age value
    let age_arr = batch2
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let has_updated = (0..age_arr.len()).any(|i| age_arr.value(i) == 99);
    assert!(
        has_updated,
        "At least one row should have age=99 after update"
    );
}

#[test]
fn test_oltp_delete_single_row() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oltp_delete.apex");
    create_oltp_storage(&path);

    let result = ApexExecutor::execute("DELETE FROM default WHERE user_id = 1", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert!(batch.num_rows() >= 1);

    invalidate_storage_cache(&path);
    let result2 =
        ApexExecutor::execute("SELECT COUNT(*) FROM default WHERE user_id = 1", &path).unwrap();
    let batch2 = result2.to_record_batch().unwrap();
    let count = batch2
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 0);
}

#[test]
fn test_oltp_delete_then_count() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oltp_del_cnt.apex");
    create_oltp_storage(&path);

    ApexExecutor::execute("DELETE FROM default WHERE city = 'Beijing'", &path).unwrap();
    invalidate_storage_cache(&path);
    let result = ApexExecutor::execute("SELECT COUNT(*) FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    // Beijing is every 5th row → 200 deleted from 1000
    assert_eq!(count, 800);
}

#[test]
fn test_oltp_update_multiple_rows() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oltp_upd_multi.apex");
    create_oltp_storage(&path);

    ApexExecutor::execute(
        "UPDATE default SET balance = 0.0 WHERE city = 'Shanghai'",
        &path,
    )
    .unwrap();
    invalidate_storage_cache(&path);
    let result =
        ApexExecutor::execute("SELECT COUNT(*) FROM default WHERE balance = 0.0", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    // Shanghai is every 5th row starting at index 1 → 200 rows
    assert_eq!(count, 200);
}

#[test]
fn test_oltp_string_equality_filter() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oltp_str_eq.apex");
    create_oltp_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT * FROM default WHERE city = 'Shenzhen' LIMIT 10",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 10);

    // Verify all returned rows have city = 'Shenzhen'
    if let Some(city_col) = batch.column_by_name("city") {
        let str_arr = city_col.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..str_arr.len() {
            assert_eq!(str_arr.value(i), "Shenzhen");
        }
    }
}

#[test]
fn test_oltp_numeric_range_filter() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oltp_range.apex");
    create_oltp_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT * FROM default WHERE age BETWEEN 30 AND 35 LIMIT 50",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    assert!(batch.num_rows() > 0 && batch.num_rows() <= 50);

    let age_arr = batch
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..age_arr.len() {
        let v = age_arr.value(i);
        assert!(v >= 30 && v <= 35, "age {} out of range [30,35]", v);
    }
}

#[test]
fn test_oltp_insert_then_immediate_read() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oltp_ins_read.apex");

    let storage = OnDemandStorage::create(&path).unwrap();
    let mut int_cols: HashMap<String, Vec<i64>> = HashMap::new();
    int_cols.insert("val".to_string(), vec![42]);
    let ids = storage
        .insert_typed(
            int_cols,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();

    // Immediately read back without cache invalidation
    let result = ApexExecutor::execute("SELECT val FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let val = batch
        .column_by_name("val")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(val, 42);
    assert_eq!(ids.len(), 1);
}

// ========================================================================
// OLAP Tests: Full Scan, Aggregation, GROUP BY, ORDER BY, LIMIT, Subquery
// ========================================================================

fn create_olap_storage(path: &Path) {
    let storage = OnDemandStorage::create(path).unwrap();
    let n = 5000;
    let cities = [
        "Beijing",
        "Shanghai",
        "Shenzhen",
        "Guangzhou",
        "Hangzhou",
        "Chengdu",
        "Wuhan",
        "Nanjing",
        "Tianjin",
        "Xian",
    ];
    let depts = ["Engineering", "Sales", "Marketing", "HR", "Finance"];

    let mut int_cols: HashMap<String, Vec<i64>> = HashMap::new();
    let mut float_cols: HashMap<String, Vec<f64>> = HashMap::new();
    let mut string_cols: HashMap<String, Vec<String>> = HashMap::new();
    let mut bool_cols: HashMap<String, Vec<bool>> = HashMap::new();

    int_cols.insert("emp_id".to_string(), (1..=n as i64).collect());
    int_cols.insert(
        "age".to_string(),
        (0..n).map(|i| 22 + (i % 40) as i64).collect(),
    );
    int_cols.insert(
        "years".to_string(),
        (0..n).map(|i| (i % 20) as i64).collect(),
    );
    float_cols.insert(
        "salary".to_string(),
        (0..n)
            .map(|i| 50000.0 + (i % 100) as f64 * 1000.0)
            .collect(),
    );
    string_cols.insert(
        "city".to_string(),
        (0..n).map(|i| cities[i % 10].to_string()).collect(),
    );
    string_cols.insert(
        "dept".to_string(),
        (0..n).map(|i| depts[i % 5].to_string()).collect(),
    );
    string_cols.insert(
        "team".to_string(),
        (0..n).map(|i| format!("Team{}", i % 3)).collect(),
    );
    bool_cols.insert(
        "is_manager".to_string(),
        (0..n).map(|i| i % 10 == 0).collect(),
    );

    storage
        .insert_typed(int_cols, float_cols, string_cols, HashMap::new(), bool_cols)
        .unwrap();
    storage.save().unwrap();
}

#[test]
fn test_olap_full_scan() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_scan.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute("SELECT * FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 5000);
    // Verify all expected columns exist
    assert!(batch.column_by_name("emp_id").is_some());
    assert!(batch.column_by_name("salary").is_some());
    assert!(batch.column_by_name("city").is_some());
    assert!(batch.column_by_name("dept").is_some());
}


#[test]
fn test_olap_count_star() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_count.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute("SELECT COUNT(*) FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5000
    );
}

#[test]
fn test_olap_sum_avg_min_max() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_aggs.apex");
    create_olap_storage(&path);

    // SUM
    let result = ApexExecutor::execute("SELECT SUM(salary) FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    let sum_col = batch.column(0);
    // salary = 50000 + (i%100)*1000 for i=0..5000
    // Each cycle of 100: sum = 100*50000 + (0+1+...+99)*1000 = 5000000+4950000 = 9950000
    // 50 cycles: 50*9950000 = 497500000
    let sum_val = if let Some(arr) = sum_col.as_any().downcast_ref::<Float64Array>() {
        arr.value(0)
    } else {
        sum_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0) as f64
    };
    assert!(
        (sum_val - 497_500_000.0).abs() < 1.0,
        "SUM(salary) = {}",
        sum_val
    );

    invalidate_storage_cache(&path);
    // AVG
    let result = ApexExecutor::execute("SELECT AVG(salary) FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    let avg_val = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert!((avg_val - 99500.0).abs() < 1.0, "AVG(salary) = {}", avg_val);

    invalidate_storage_cache(&path);
    // MIN
    let result = ApexExecutor::execute("SELECT MIN(salary) FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    let min_col = batch.column(0);
    let min_val = if let Some(arr) = min_col.as_any().downcast_ref::<Float64Array>() {
        arr.value(0)
    } else {
        min_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0) as f64
    };
    assert!((min_val - 50000.0).abs() < 1.0, "MIN(salary) = {}", min_val);

    invalidate_storage_cache(&path);
    // MAX
    let result = ApexExecutor::execute("SELECT MAX(salary) FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    let max_col = batch.column(0);
    let max_val = if let Some(arr) = max_col.as_any().downcast_ref::<Float64Array>() {
        arr.value(0)
    } else {
        max_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0) as f64
    };
    assert!(
        (max_val - 149000.0).abs() < 1.0,
        "MAX(salary) = {}",
        max_val
    );
}

#[test]
fn test_olap_group_by_single_col() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_gb1.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT dept, COUNT(*) as cnt FROM default GROUP BY dept ORDER BY cnt DESC",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    // 5 departments
    assert_eq!(batch.num_rows(), 5);
    // Each dept has 1000 rows (5000/5)
    let cnt_col = batch
        .column_by_name("cnt")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..cnt_col.len() {
        assert_eq!(cnt_col.value(i), 1000, "dept group {} count", i);
    }
}

#[test]
fn test_olap_group_by_two_cols() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_gb2.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
            "SELECT city, dept, COUNT(*) as cnt FROM default GROUP BY city, dept ORDER BY cnt DESC LIMIT 10", &path
        ).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 10);
    // Verify all groups have positive counts and sum makes sense
    let cnt_col = batch
        .column_by_name("cnt")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..cnt_col.len() {
        assert!(cnt_col.value(i) > 0, "group {} has zero count", i);
    }
    // Verify descending order
    for i in 1..cnt_col.len() {
        assert!(
            cnt_col.value(i - 1) >= cnt_col.value(i),
            "not DESC order at {}",
            i
        );
    }
}

#[test]
fn test_olap_group_by_two_cols_full_aggregate_family() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_gb2_full_stats.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT city, years, COUNT(*) AS n, SUM(age) AS total, AVG(salary) AS av, \
         MIN(salary) AS lo, MAX(salary) AS hi FROM default \
         GROUP BY city, years ORDER BY city, years",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 20);
    let counts = batch
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!((0..counts.len()).map(|row| counts.value(row)).sum::<i64>(), 5000);
    assert!((0..counts.len()).all(|row| counts.value(row) == 250));
    let lows = batch
        .column_by_name("lo")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let highs = batch
        .column_by_name("hi")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((0..batch.num_rows()).all(|row| lows.value(row) <= highs.value(row)));
}

#[test]
fn test_olap_cached_numeric_equality_and_prefix_aggregates() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_cached_filtered_aggregates.apex");
    create_olap_storage(&path);

    let numeric = ApexExecutor::execute(
        "SELECT COUNT(*) AS n, AVG(salary) AS av FROM default WHERE age=25",
        &path,
    )
    .unwrap()
    .to_record_batch()
    .unwrap();
    assert_eq!(
        numeric
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        125
    );
    assert_eq!(
        numeric
            .column_by_name("av")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        93000.0
    );

    let prefix = ApexExecutor::execute(
        "SELECT COUNT(*) AS n, AVG(salary) AS av FROM default WHERE city LIKE 'Bei%'",
        &path,
    )
    .unwrap()
    .to_record_batch()
    .unwrap();
    assert_eq!(
        prefix
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        500
    );
    assert_eq!(
        prefix
            .column_by_name("av")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        95000.0
    );
}

#[test]
fn test_olap_cached_numeric_analytics_shapes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_cached_numeric_shapes.apex");
    let storage = OnDemandStorage::create(&path).unwrap();
    let mut ints = HashMap::new();
    let mut strings = HashMap::new();
    ints.insert("x".to_string(), (0..200).collect());
    ints.insert("d".to_string(), (0..200).map(|value| value % 10).collect());
    ints.insert("n".to_string(), (0..200).map(|value| value * 2).collect());
    strings.insert(
        "g".to_string(),
        (0..200)
            .map(|value| if value % 2 == 0 { "A" } else { "B" }.to_string())
            .collect(),
    );
    storage
        .insert_typed(ints, HashMap::new(), strings, HashMap::new(), HashMap::new())
        .unwrap();
    storage.save().unwrap();

    let grouped = ApexExecutor::execute(
        "SELECT d, COUNT(*) AS c, SUM(n) AS s, AVG(n) AS a, MIN(n) AS lo, MAX(n) AS hi \
         FROM default GROUP BY d ORDER BY d",
        &path,
    )
    .unwrap()
    .to_record_batch()
    .unwrap();
    assert_eq!(grouped.num_rows(), 10);
    let counts = grouped
        .column_by_name("c")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!((0..10).all(|row| counts.value(row) == 20));

    let ratios = ApexExecutor::execute(
        "SELECT g, AVG(n/(d+1.0)) AS ratio FROM default GROUP BY g ORDER BY g",
        &path,
    )
    .unwrap()
    .to_record_batch()
    .unwrap();
    assert_eq!(ratios.num_rows(), 2);
    let ratio_values = ratios
        .column_by_name("ratio")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for group in 0..2 {
        let expected = (0..200)
            .filter(|value| value % 2 == group)
            .map(|value| (value * 2) as f64 / ((value % 10) as f64 + 1.0))
            .sum::<f64>()
            / 100.0;
        assert!((ratio_values.value(group) - expected).abs() < 1e-9);
    }

    let filtered = ApexExecutor::execute(
        "SELECT COUNT(*) AS c, AVG(n) AS a, SUM(x) AS s FROM default \
         WHERE d=3 AND x BETWEEN 20 AND 150",
        &path,
    )
    .unwrap()
    .to_record_batch()
    .unwrap();
    let expected = (20..=150)
        .filter(|value| value % 10 == 3)
        .collect::<Vec<i64>>();
    assert_eq!(
        filtered
            .column_by_name("c")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        expected.len() as i64
    );
    assert_eq!(
        filtered
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        expected.iter().sum::<i64>()
    );

    let topk = ApexExecutor::execute(
        "SELECT x, d, n FROM default WHERE d>=3 AND d<=6 \
         ORDER BY n DESC, x, d LIMIT 10",
        &path,
    )
    .unwrap()
    .to_record_batch()
    .unwrap();
    let xs = topk
        .column_by_name("x")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let expected_top = (0..200)
        .rev()
        .filter(|value| (3..=6).contains(&(value % 10)))
        .take(10)
        .collect::<Vec<i64>>();
    assert_eq!(
        (0..10).map(|row| xs.value(row)).collect::<Vec<_>>(),
        expected_top
    );
}

#[test]
fn test_nested_subquery_from_chained_ctes_uses_general_executor() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("orders.apex");
    let storage = OnDemandStorage::create(&path).unwrap();
    let mut ints = HashMap::new();
    let mut strings = HashMap::new();
    ints.insert("amount".to_string(), vec![100, 200, 300, 50, 400, 80]);
    strings.insert(
        "customer".to_string(),
        ["Alice", "Alice", "Bob", "Bob", "Carol", "Carol"]
            .into_iter()
            .map(str::to_string)
            .collect(),
    );
    storage
        .insert_typed(ints, HashMap::new(), strings, HashMap::new(), HashMap::new())
        .unwrap();
    storage.save().unwrap();

    let result = ApexExecutor::execute(
        "WITH totals AS (\
             SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer\
         ), big AS (\
             SELECT customer, total FROM totals WHERE total > 200\
         ) SELECT customer FROM big ORDER BY total DESC",
        &path,
    )
    .unwrap()
    .to_record_batch()
    .unwrap();
    let customers = result
        .column_by_name("customer")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        (0..customers.len()).map(|row| customers.value(row)).collect::<Vec<_>>(),
        vec!["Carol", "Bob", "Alice"]
    );
}

#[test]
fn test_olap_not_null_numeric_topk_late_materialization() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_not_null_topk.apex");
    let storage = OnDemandStorage::create(&path).unwrap();
    let mut ints = HashMap::new();
    let mut floats = HashMap::new();
    ints.insert("row_id".to_string(), (0..5000).collect());
    floats.insert(
        "score".to_string(),
        (0..5000).map(|value| value as f64 + 0.25).collect(),
    );
    storage
        .insert_typed(ints, floats, HashMap::new(), HashMap::new(), HashMap::new())
        .unwrap();
    storage.save().unwrap();

    let result = ApexExecutor::execute(
        "SELECT row_id, score FROM default WHERE score IS NOT NULL \
         ORDER BY score DESC, row_id LIMIT 25",
        &path,
    )
    .unwrap()
    .to_record_batch()
    .unwrap();
    assert_eq!(result.num_rows(), 25);
    assert_eq!(
        result
            .column_by_name("row_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        4999
    );
    assert_eq!(
        result
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(24),
        4975.25
    );
}

#[test]
fn test_olap_group_by_with_having() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_having.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT city, AVG(salary) as avg_sal FROM default GROUP BY city HAVING AVG(salary) > 99000",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    // All cities have similar salary distribution, so avg ~99500
    assert!(batch.num_rows() > 0, "HAVING should return some rows");
    let avg_col = batch
        .column_by_name("avg_sal")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for i in 0..avg_col.len() {
        assert!(
            avg_col.value(i) > 99000.0,
            "avg_sal {} <= 99000",
            avg_col.value(i)
        );
    }
}

#[test]
fn test_scan_group_pipeline_applies_having_before_topk_and_reads_delta() {
    use crate::data::Value;
    use crate::storage::backend::TableStorageBackend;

    let dir = tempdir().unwrap();
    let path = dir.path().join("scan_group_delta.apex");
    let storage = OnDemandStorage::create(&path).unwrap();
    storage
        .insert_typed(
            HashMap::from([("age".to_string(), vec![20, 25, 30, 28, 35])]),
            HashMap::from([("score".to_string(), vec![99.0, 50.0, 60.0, 70.0, 80.0])]),
            HashMap::from([(
                "city".to_string(),
                vec!["A", "A", "A", "B", "C"]
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
            )]),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();

    let query = "SELECT city, COUNT(*) AS n, AVG(score) AS av FROM default \
                 WHERE age > 20 AND age <= 35 AND score >= 40 \
                 GROUP BY city HAVING COUNT(*) > 1 \
                 ORDER BY n DESC, city LIMIT 2";
    let base = ApexExecutor::execute(query, &path)
        .unwrap()
        .to_record_batch()
        .unwrap();
    assert_eq!(base.num_rows(), 1);
    assert_eq!(
        base.column_by_name("city")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "A"
    );
    assert_eq!(
        base.column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );

    let backend = TableStorageBackend::open(&path).unwrap();
    let delta_rows = [
        HashMap::from([
            ("age".to_string(), Value::Int64(32)),
            ("score".to_string(), Value::Float64(90.0)),
            ("city".to_string(), Value::String("B".to_string())),
        ]),
        HashMap::from([
            ("age".to_string(), Value::Int64(33)),
            ("score".to_string(), Value::Float64(100.0)),
            ("city".to_string(), Value::String("B".to_string())),
        ]),
    ];
    backend.insert_rows_to_delta(&delta_rows).unwrap();
    drop(backend);
    invalidate_storage_cache(&path);

    let with_delta = ApexExecutor::execute(query, &path)
        .unwrap()
        .to_record_batch()
        .unwrap();
    assert_eq!(with_delta.num_rows(), 2);
    let cities = with_delta
        .column_by_name("city")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = with_delta
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!((cities.value(0), counts.value(0)), ("B", 3));
    assert_eq!((cities.value(1), counts.value(1)), ("A", 2));
}

#[test]
fn test_olap_order_by_desc_limit() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_order.apex");
    create_olap_storage(&path);

    let result =
        ApexExecutor::execute("SELECT * FROM default ORDER BY salary DESC LIMIT 10", &path)
            .unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 10);

    let sal_arr = batch
        .column_by_name("salary")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    // Verify descending order
    for i in 1..sal_arr.len() {
        assert!(
            sal_arr.value(i - 1) >= sal_arr.value(i),
            "salary[{}]={} < salary[{}]={}",
            i - 1,
            sal_arr.value(i - 1),
            i,
            sal_arr.value(i)
        );
    }
    // Top salary should be 149000
    assert!((sal_arr.value(0) - 149000.0).abs() < 1.0);
}

#[test]
fn test_olap_order_by_asc_limit() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_order_asc.apex");
    create_olap_storage(&path);

    let result =
        ApexExecutor::execute("SELECT * FROM default ORDER BY age ASC LIMIT 5", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 5);

    let age_arr = batch
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..age_arr.len() {
        assert_eq!(age_arr.value(i), 22, "min age should be 22");
    }
}

#[test]
fn test_olap_where_between() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_between.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT * FROM default WHERE age BETWEEN 30 AND 35 LIMIT 100",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    assert!(batch.num_rows() > 0 && batch.num_rows() <= 100);

    let age_arr = batch
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..age_arr.len() {
        let v = age_arr.value(i);
        assert!(v >= 30 && v <= 35, "age {} not in [30,35]", v);
    }
}

#[test]
fn test_olap_where_string_eq_no_limit() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_str_nolim.apex");
    create_olap_storage(&path);

    let result =
        ApexExecutor::execute("SELECT * FROM default WHERE city = 'Beijing'", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    // Beijing = every 10th row → 500
    assert_eq!(batch.num_rows(), 500);
}

#[test]
fn test_olap_complex_filter_group_order() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_complex.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT dept, COUNT(*) as cnt, AVG(salary) as avg_sal FROM default \
             WHERE city = 'Beijing' GROUP BY dept ORDER BY avg_sal DESC",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    // Beijing rows across departments — verify groups exist
    assert!(
        batch.num_rows() >= 1,
        "Should have at least 1 dept group for Beijing"
    );
    let cnt_col = batch
        .column_by_name("cnt")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let total: i64 = (0..cnt_col.len()).map(|i| cnt_col.value(i)).sum();
    assert_eq!(
        total, 500,
        "Total Beijing rows across all depts should be 500"
    );
}

#[test]
fn test_olap_count_distinct() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_cdist.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute("SELECT COUNT(DISTINCT city) FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 10);

    // The second execution exercises the backend-owned validated cardinality
    // summary rather than repeating global dictionary/file validation.
    let warm = ApexExecutor::execute("SELECT COUNT(DISTINCT city) FROM default", &path).unwrap();
    let warm = warm.to_record_batch().unwrap();
    assert_eq!(
        warm.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        10
    );
}

#[test]
fn test_olap_multiple_count_distinct_dictionary_columns() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_multi_count_distinct.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT COUNT(DISTINCT city) AS cities, COUNT(DISTINCT dept) AS depts, \
         COUNT(DISTINCT team) AS teams, COUNT(DISTINCT years) AS years FROM default",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    let value = |name| {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!(value("cities"), 10);
    assert_eq!(value("depts"), 5);
    assert_eq!(value("teams"), 3);
    assert_eq!(value("years"), 20);
}

#[test]
fn test_olap_numeric_group_full_aggregate_family() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_numeric_group.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT years, COUNT(*) AS n, SUM(age) AS total, AVG(age) AS av, \
         MIN(age) AS lo, MAX(age) AS hi FROM default \
         GROUP BY years HAVING COUNT(*) > 100 ORDER BY years",
        &path,
    )
    .unwrap();
    let rows = result.to_record_batch().unwrap();
    assert_eq!(rows.num_rows(), 20);
    assert_eq!(
        rows.column_by_name("years")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    assert_eq!(
        rows.column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        250
    );
    assert_eq!(
        rows.column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        8000
    );
    assert_eq!(
        rows.column_by_name("av")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        32.0
    );
}

#[test]
fn test_olap_numeric_mod_group() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_numeric_mod_group.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT MOD(emp_id, 5) AS fold, COUNT(*) AS n, AVG(age) AS av \
         FROM default GROUP BY MOD(emp_id, 5) ORDER BY fold",
        &path,
    )
    .unwrap();
    let rows = result.to_record_batch().unwrap();
    assert_eq!(rows.num_rows(), 5);
    let folds = rows
        .column_by_name("fold")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = rows
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for row in 0..5 {
        assert_eq!(folds.value(row), row as i64);
        assert_eq!(counts.value(row), 1000);
    }
}

#[test]
fn test_olap_high_cardinality_substr_group() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_high_card_substr_group.apex");
    let storage = OnDemandStorage::create(&path).unwrap();
    let mut ints = HashMap::new();
    let mut floats = HashMap::new();
    let mut strings = HashMap::new();
    ints.insert("id".to_string(), (0..5000).collect());
    floats.insert(
        "value".to_string(),
        (0..5000).map(|value| value as f64).collect(),
    );
    strings.insert(
        "event_time".to_string(),
        (0..5000)
            .map(|value| format!("2026-08-27 {:02}:{:02}:{:02}", value % 24, value % 60, value % 60))
            .collect(),
    );
    storage
        .insert_typed(ints, floats, strings, HashMap::new(), HashMap::new())
        .unwrap();
    storage.save().unwrap();

    let result = ApexExecutor::execute(
        "SELECT SUBSTR(event_time, 12, 2) AS hour, COUNT(*) AS n, AVG(value) AS av \
         FROM default GROUP BY SUBSTR(event_time, 12, 2) ORDER BY hour",
        &path,
    )
    .unwrap();
    let rows = result.to_record_batch().unwrap();
    assert_eq!(rows.num_rows(), 24);
    assert_eq!(
        rows.column_by_name("hour")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "00"
    );
}

#[test]
fn test_olap_derived_numeric_case_bucket_group() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_derived_case_bucket_group.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT band, COUNT(*) AS n, AVG(salary) AS av FROM \
         (SELECT CASE WHEN age < 30 THEN 'young' WHEN age < 50 THEN 'mid' \
          ELSE 'senior' END AS band, salary FROM default) s \
         GROUP BY band ORDER BY band",
        &path,
    )
    .unwrap();
    let rows = result.to_record_batch().unwrap();
    assert_eq!(rows.num_rows(), 3);
    let bands = rows
        .column_by_name("band")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = rows
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let actual: Vec<(&str, i64)> = (0..rows.num_rows())
        .map(|row| (bands.value(row), counts.value(row)))
        .collect();
    assert_eq!(actual, vec![("mid", 2500), ("senior", 1500), ("young", 1000)]);
}

#[test]
fn test_olap_string_group_numeric_count_distinct() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_group_numeric_distinct.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT city, COUNT(*) AS n, COUNT(DISTINCT age) AS ages \
         FROM default GROUP BY city ORDER BY city",
        &path,
    )
    .unwrap();
    let rows = result.to_record_batch().unwrap();
    assert_eq!(rows.num_rows(), 10);
    let ages = rows
        .column_by_name("ages")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let actual: Vec<i64> = (0..ages.len()).map(|row| ages.value(row)).collect();
    assert!(
        actual.iter().all(|&value| value == 4),
        "expected four distinct ages per city, got {actual:?}"
    );
}

#[test]
fn test_olap_scalar_numeric_case_counts() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_numeric_case.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT SUM(CASE WHEN age = 22 THEN 1 ELSE 0 END) AS age_22, \
         SUM(CASE WHEN years BETWEEN 5 AND 9 THEN 1 ELSE 0 END) AS years_5_9 \
         FROM default",
        &path,
    )
    .unwrap();
    let row = result.to_record_batch().unwrap();
    let value = |name| {
        row.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!(value("age_22"), 125.0);
    assert_eq!(value("years_5_9"), 1250.0);
}

#[test]
fn test_olap_cached_numeric_case_group() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_cached_numeric_case_group.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT city, SUM(CASE WHEN age > 40 THEN 1 ELSE 0 END) AS older, \
         SUM(CASE WHEN years > 10 THEN 1 ELSE 0 END) AS experienced, \
         AVG(salary) AS av FROM default GROUP BY city ORDER BY city",
        &path,
    )
    .unwrap();
    let rows = result.to_record_batch().unwrap();
    assert_eq!(rows.num_rows(), 10);
    let older = rows
        .column_by_name("older")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let experienced = rows
        .column_by_name("experienced")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!((0..10).map(|row| older.value(row)).sum::<f64>(), 2625.0);
    assert_eq!(
        (0..10).map(|row| experienced.value(row)).sum::<f64>(),
        2250.0
    );
}

#[test]
fn test_olap_multi_column_distinct_dictionary_projection() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_multi_distinct.apex");
    create_olap_storage(&path);

    // Both columns remain dictionary encoded through the raw DISTINCT path.
    // There are ten repeating (city, dept) pairs across the 5,000 source rows.
    let result = ApexExecutor::execute("SELECT DISTINCT city, dept FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 10);

    // The same generic path supports more than two dictionary columns too.
    // The independently-cycled third key expands the result to 30 combinations.
    invalidate_storage_cache(&path);
    let result =
        ApexExecutor::execute("SELECT DISTINCT city, dept, team FROM default", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 30);
}

#[test]
fn test_olap_mixed_cached_distinct_projection() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_mixed_cached_distinct.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT DISTINCT city, years, dept FROM default ORDER BY city, years, dept",
        &path,
    )
    .unwrap();
    let rows = result.to_record_batch().unwrap();
    assert_eq!(rows.num_rows(), 20);
    assert_eq!(
        rows.column_by_name("years")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
}

#[test]
fn test_olap_limit_on_filter() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_limit.apex");
    create_olap_storage(&path);

    // LIMIT on string filter
    let result = ApexExecutor::execute(
        "SELECT * FROM default WHERE city = 'Beijing' LIMIT 10",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 10);

    // LIMIT on full scan
    invalidate_storage_cache(&path);
    let result2 = ApexExecutor::execute("SELECT * FROM default LIMIT 20", &path).unwrap();
    let batch2 = result2.to_record_batch().unwrap();
    assert_eq!(batch2.num_rows(), 20);
}

#[test]
fn test_olap_in_list() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_in.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT * FROM default WHERE city IN ('Beijing', 'Shanghai')",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    // Beijing + Shanghai = 500 + 500 = 1000
    assert_eq!(batch.num_rows(), 1000);
}

#[test]
fn test_olap_like_filter() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_like.apex");
    create_olap_storage(&path);

    let result =
        ApexExecutor::execute("SELECT * FROM default WHERE city LIKE 'Sh%'", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    // Shanghai + Shenzhen = 500 + 500 = 1000
    assert_eq!(batch.num_rows(), 1000);
}

#[test]
fn test_olap_multi_condition_and() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_and.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT * FROM default WHERE city = 'Beijing' AND age > 50",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    // Beijing every 10th, age = 22 + (i%40), age > 50 means i%40 > 28 → i%40 in [29..39] = 11 values
    // Among Beijing rows (i%10==0), i%40 distribution: need i%10==0 AND i%40>28
    // This is a subset check — just verify all returned rows satisfy both conditions
    assert!(batch.num_rows() > 0);
    if let Some(city_col) = batch.column_by_name("city") {
        let str_arr = city_col.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..str_arr.len() {
            assert_eq!(str_arr.value(i), "Beijing");
        }
    }
    let age_arr = batch
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..age_arr.len() {
        assert!(age_arr.value(i) > 50);
    }
}

#[test]
fn test_olap_multi_condition_or() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_or.apex");
    create_olap_storage(&path);

    let result =
        ApexExecutor::execute("SELECT * FROM default WHERE age < 23 OR age > 60", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert!(batch.num_rows() > 0);
    let age_arr = batch
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..age_arr.len() {
        let v = age_arr.value(i);
        assert!(v < 23 || v > 60, "age {} not < 23 and not > 60", v);
    }
}

#[test]
fn test_olap_column_projection() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_proj.apex");
    create_olap_storage(&path);

    let result =
        ApexExecutor::execute("SELECT emp_id, salary FROM default LIMIT 10", &path).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 10);
    assert_eq!(batch.num_columns(), 2);
    assert!(batch.column_by_name("emp_id").is_some());
    assert!(batch.column_by_name("salary").is_some());
}

#[test]
fn test_olap_expression_in_select() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_expr.apex");
    create_olap_storage(&path);

    // Use CAST expression which is supported by the SQL parser
    let result = ApexExecutor::execute(
        "SELECT emp_id, CAST(salary AS INT) as salary_int FROM default LIMIT 5",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 5);
    assert!(batch.column_by_name("salary_int").is_some());
}

#[test]
fn test_olap_group_by_with_sum() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_gb_sum.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT dept, SUM(salary) as total_sal FROM default GROUP BY dept ORDER BY total_sal DESC",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 5);
    let sum_col = batch
        .column_by_name("total_sal")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    // Verify descending order
    for i in 1..sum_col.len() {
        assert!(sum_col.value(i - 1) >= sum_col.value(i));
    }
}

#[test]
fn test_olap_empty_result() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_empty.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT * FROM default WHERE city = 'NonExistentCity'",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_olap_boolean_filter() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("olap_bool.apex");
    create_olap_storage(&path);

    let result = ApexExecutor::execute(
        "SELECT COUNT(*) FROM default WHERE is_manager = true",
        &path,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    // is_manager = true when i%10==0 → 500 rows
    assert_eq!(count, 500);
}

// ========== P0-5: Constraint Tests ==========

/// Helper: parse + execute SQL via multi-table path with a base dir
fn exec_multi(sql: &str, base_dir: &Path) -> io::Result<ApexResult> {
    let default_path = base_dir.join("default.apex");
    ApexExecutor::execute_with_base_dir(sql, base_dir, &default_path)
}

/// Helper: assert that a Result is an error containing expected substring
fn assert_err_contains(result: io::Result<ApexResult>, expected: &str) {
    match result {
        Ok(_) => panic!("Expected error containing '{}', but got Ok", expected),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains(expected),
                "Expected error containing '{}', got: {}",
                expected,
                msg
            );
        }
    }
}

#[test]
fn test_constraint_not_null_reject() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE t1 (id INT NOT NULL, name TEXT)", base).unwrap();
    exec_multi("INSERT INTO t1 (id, name) VALUES (1, 'Alice')", base).unwrap();
    assert_err_contains(
        exec_multi("INSERT INTO t1 (id, name) VALUES (NULL, 'Bob')", base),
        "NOT NULL",
    );
}

#[test]
fn test_constraint_unique_reject() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE t2 (id INT, email TEXT UNIQUE)", base).unwrap();
    exec_multi("INSERT INTO t2 (id, email) VALUES (1, 'a@b.com')", base).unwrap();
    assert_err_contains(
        exec_multi("INSERT INTO t2 (id, email) VALUES (2, 'a@b.com')", base),
        "UNIQUE",
    );
}

#[test]
fn test_constraint_unique_allows_multiple_nulls() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE t3 (id INT, email TEXT UNIQUE)", base).unwrap();
    exec_multi("INSERT INTO t3 (id, email) VALUES (1, NULL)", base).unwrap();
    exec_multi("INSERT INTO t3 (id, email) VALUES (2, NULL)", base).unwrap();
    let result = exec_multi("SELECT COUNT(*) FROM t3", base).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
}

#[test]
fn test_constraint_primary_key_reject() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE t4 (uid INT PRIMARY KEY, name TEXT)", base).unwrap();
    exec_multi("INSERT INTO t4 (uid, name) VALUES (1, 'Alice')", base).unwrap();
    assert_err_contains(
        exec_multi("INSERT INTO t4 (uid, name) VALUES (1, 'Bob')", base),
        "PRIMARY KEY",
    );
}

#[test]
fn test_constraint_primary_key_implies_not_null() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE t5 (uid INT PRIMARY KEY, name TEXT)", base).unwrap();
    assert_err_contains(
        exec_multi("INSERT INTO t5 (uid, name) VALUES (NULL, 'Alice')", base),
        "NOT NULL",
    );
}

#[test]
fn test_constraint_default_value_fill() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi(
        "CREATE TABLE t6 (id INT NOT NULL, score INT DEFAULT 100)",
        base,
    )
    .unwrap();
    exec_multi("INSERT INTO t6 (id) VALUES (1)", base).unwrap();
    let result = exec_multi("SELECT score FROM t6 WHERE id = 1", base).unwrap();
    let batch = result.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let score = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(score, 100);
}

#[test]
fn test_constraint_default_time_functions() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    exec_multi(
        "CREATE TABLE t_time (id INT NOT NULL, created TEXT DEFAULT CURRENT_DATE, ts BIGINT DEFAULT UNIX_TIMESTAMP())",
        base,
    )
    .unwrap();
    exec_multi("INSERT INTO t_time (id) VALUES (1)", base).unwrap();
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let result = exec_multi("SELECT created, ts FROM t_time WHERE id = 1", base).unwrap();
    let batch = result.to_record_batch().unwrap();
    let created = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    let ts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(created.len(), 10);
    assert_eq!(&created[4..5], "-");
    assert_eq!(&created[7..8], "-");
    assert!(before <= ts && ts <= after);
}

#[test]
fn test_constraint_default_expressions_and_insert_default() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi(
        "CREATE TABLE t_default_expr (
            id INT DEFAULT 7,
            ttl INT DEFAULT (60 * 60),
            status TEXT DEFAULT LOWER('ACTIVE'),
            created TEXT DEFAULT CAST('2026-01-02' AS DATE)
        )",
        base,
    )
    .unwrap();
    exec_multi("INSERT INTO t_default_expr DEFAULT VALUES", base).unwrap();
    exec_multi(
        "INSERT INTO t_default_expr (id, ttl, status, created) VALUES (8, DEFAULT, DEFAULT, DEFAULT)",
        base,
    )
    .unwrap();
    let result = exec_multi(
        "SELECT id, ttl, status, created FROM t_default_expr ORDER BY id",
        base,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    let id = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ttl = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let status = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let created = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(id.value(0), 7);
    assert_eq!(id.value(1), 8);
    assert_eq!(ttl.value(0), 3600);
    assert_eq!(ttl.value(1), 3600);
    assert_eq!(status.value(0), "active");
    assert_eq!(status.value(1), "active");
    assert_eq!(created.value(0), "2026-01-02");
    assert_eq!(created.value(1), "2026-01-02");
    assert_err_contains(
        exec_multi(
            "CREATE TABLE t_bad_default (a INT, b INT DEFAULT a + 1)",
            base,
        ),
        "cannot reference column",
    );
}

#[test]
fn test_constraint_update_not_null_reject() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi(
        "CREATE TABLE t7 (id INT NOT NULL, name TEXT NOT NULL)",
        base,
    )
    .unwrap();
    exec_multi("INSERT INTO t7 (id, name) VALUES (1, 'Alice')", base).unwrap();
    assert_err_contains(
        exec_multi("UPDATE t7 SET name = NULL WHERE id = 1", base),
        "NOT NULL",
    );
}

#[test]
fn test_constraint_update_unique_reject() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE t8 (id INT, email TEXT UNIQUE)", base).unwrap();
    exec_multi("INSERT INTO t8 (id, email) VALUES (1, 'a@b.com')", base).unwrap();
    exec_multi("INSERT INTO t8 (id, email) VALUES (2, 'c@d.com')", base).unwrap();
    assert_err_contains(
        exec_multi("UPDATE t8 SET email = 'a@b.com' WHERE id = 2", base),
        "UNIQUE",
    );
}

#[test]
fn test_constraint_batch_insert_duplicate_in_batch() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE t9 (id INT, email TEXT UNIQUE)", base).unwrap();
    assert_err_contains(
        exec_multi(
            "INSERT INTO t9 (id, email) VALUES (1, 'x@y.com'), (2, 'x@y.com')",
            base,
        ),
        "UNIQUE",
    );
}

// ========== P1: CTAS Tests ==========

#[test]
fn test_ctas_basic() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE src (id INT, name TEXT)", base).unwrap();
    exec_multi(
        "INSERT INTO src (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
        base,
    )
    .unwrap();
    let result = exec_multi(
        "CREATE TABLE dst AS SELECT id, name FROM src WHERE id > 1",
        base,
    )
    .unwrap();
    // Should return number of inserted rows
    if let ApexResult::Scalar(n) = result {
        assert_eq!(n, 2);
    }
    // Verify the new table has the right data
    let q = exec_multi("SELECT COUNT(*) FROM dst", base).unwrap();
    let batch = q.to_record_batch().unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
}

#[test]
fn test_ctas_if_not_exists() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE src2 (id INT)", base).unwrap();
    exec_multi("INSERT INTO src2 (id) VALUES (1)", base).unwrap();
    exec_multi("CREATE TABLE dst2 AS SELECT id FROM src2", base).unwrap();
    // Second CTAS without IF NOT EXISTS should error
    assert_err_contains(
        exec_multi("CREATE TABLE dst2 AS SELECT id FROM src2", base),
        "already exists",
    );
    // With IF NOT EXISTS should succeed silently
    let result = exec_multi(
        "CREATE TABLE IF NOT EXISTS dst2 AS SELECT id FROM src2",
        base,
    )
    .unwrap();
    if let ApexResult::Scalar(n) = result {
        assert_eq!(n, 0);
    }
}

#[test]
fn test_ctas_existing_table_keeps_registered_file() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE src4 (id INT)", base).unwrap();
    exec_multi("INSERT INTO src4 (id) VALUES (1), (2), (3)", base).unwrap();
    exec_multi("CREATE TABLE dst4 AS SELECT id FROM src4", base).unwrap();
    let dst_file = base.join("dst4.apex");
    assert!(dst_file.exists());

    // A CTAS that only returns AlreadyExists must not touch the live file.
    assert_err_contains(
        exec_multi("CREATE TABLE dst4 AS SELECT id FROM src4", base),
        "already exists",
    );
    assert!(
        dst_file.exists(),
        "failed CTAS must not delete the registered table file"
    );
    let q = exec_multi("SELECT COUNT(*) FROM dst4", base).unwrap();
    let batch = q.to_record_batch().unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
}

#[test]
fn test_ctas_reaps_orphan_file_for_unregistered_name() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE seed (id INT)", base).unwrap();
    exec_multi("INSERT INTO seed (id) VALUES (7)", base).unwrap();
    exec_multi(
        "CREATE TABLE ghost AS SELECT id FROM seed WHERE id > 999",
        base,
    )
    .unwrap();
    let ghost_file = base.join("ghost.apex");
    assert!(ghost_file.exists());

    // Unregister the name to simulate a crash orphan: file on disk,
    // absent from the registry.
    {
        let lock = crate::storage::table_catalog::lock(base).unwrap();
        assert!(lock.remove("ghost").unwrap().is_some());
    }
    let result = exec_multi("CREATE TABLE ghost AS SELECT id FROM seed", base).unwrap();
    if let ApexResult::Scalar(n) = result {
        assert_eq!(n, 1);
    }
    let q = exec_multi("SELECT id FROM ghost ORDER BY id", base).unwrap();
    let batch = q.to_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
}

#[test]
fn test_ctas_empty_result() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE src3 (id INT, val TEXT)", base).unwrap();
    exec_multi("INSERT INTO src3 (id, val) VALUES (1, 'x')", base).unwrap();
    let result = exec_multi(
        "CREATE TABLE dst3 AS SELECT id, val FROM src3 WHERE id > 999",
        base,
    )
    .unwrap();
    if let ApexResult::Scalar(n) = result {
        assert_eq!(n, 0);
    }
    let q = exec_multi("SELECT COUNT(*) FROM dst3", base).unwrap();
    let batch = q.to_record_batch().unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
}

// ========== P1: RIGHT / FULL OUTER / CROSS JOIN Tests ==========

#[test]
fn test_preaggregated_dimension_join_group() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE facts (code INT, value REAL)", base).unwrap();
    exec_multi("CREATE TABLE dims (code INT, label TEXT)", base).unwrap();
    exec_multi(
        "INSERT INTO facts (code, value) VALUES \
         (1, 10.0), (1, 20.0), (2, 30.0), (2, 50.0), (3, 70.0), (9, 99.0)",
        base,
    )
    .unwrap();
    exec_multi(
        "INSERT INTO dims (code, label) VALUES (1, 'A'), (2, 'B'), (3, 'B')",
        base,
    )
    .unwrap();
    let result = exec_multi(
        "SELECT d.label, COUNT(*) AS n, AVG(f.value) AS av \
         FROM facts f JOIN dims d ON f.code=d.code \
         GROUP BY d.label ORDER BY d.label",
        base,
    )
    .unwrap();
    let rows = result.to_record_batch().unwrap();
    assert_eq!(rows.num_rows(), 2);
    let labels = rows
        .column_by_name("label")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = rows
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let averages = rows
        .column_by_name("av")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(labels.value(0), "A");
    assert_eq!(counts.value(0), 2);
    assert_eq!(averages.value(0), 15.0);
    assert_eq!(labels.value(1), "B");
    assert_eq!(counts.value(1), 3);
    assert_eq!(averages.value(1), 50.0);
}

#[test]
fn test_right_join() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE left_t (id INT, lval TEXT)", base).unwrap();
    exec_multi("CREATE TABLE right_t (id INT, rval TEXT)", base).unwrap();
    exec_multi(
        "INSERT INTO left_t (id, lval) VALUES (1, 'a'), (2, 'b')",
        base,
    )
    .unwrap();
    exec_multi(
        "INSERT INTO right_t (id, rval) VALUES (2, 'x'), (3, 'y')",
        base,
    )
    .unwrap();
    let result = exec_multi(
        "SELECT * FROM left_t RIGHT JOIN right_t ON left_t.id = right_t.id",
        base,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    // RIGHT JOIN: all right rows preserved. id=2 matches, id=3 has NULL left.
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_right_join_qualified_key_projection() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE left_t (id INT, lval TEXT)", base).unwrap();
    exec_multi("CREATE TABLE right_t (id INT, rval TEXT)", base).unwrap();
    exec_multi(
        "INSERT INTO left_t (id, lval) VALUES (1, 'a'), (2, 'b')",
        base,
    )
    .unwrap();
    exec_multi(
        "INSERT INTO right_t (id, rval) VALUES (2, 'x'), (3, 'y')",
        base,
    )
    .unwrap();

    let result = exec_multi(
        "SELECT left_t.id AS lid, right_t.id AS rid, left_t.lval, right_t.rval \
         FROM left_t RIGHT JOIN right_t ON left_t.id = right_t.id \
         ORDER BY right_t.rval",
        base,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();

    let lid = batch
        .column_by_name("lid")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let rid = batch
        .column_by_name("rid")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let lval = batch
        .column_by_name("lval")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let rval = batch
        .column_by_name("rval")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(lid.value(0), 2);
    assert!(lid.is_null(1));
    assert_eq!(rid.value(0), 2);
    assert_eq!(rid.value(1), 3);
    assert_eq!(lval.value(0), "b");
    assert!(lval.is_null(1));
    assert_eq!(rval.value(0), "x");
    assert_eq!(rval.value(1), "y");
}

#[test]
fn test_full_outer_join() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE fl (id INT, lv TEXT)", base).unwrap();
    exec_multi("CREATE TABLE fr (id INT, rv TEXT)", base).unwrap();
    exec_multi("INSERT INTO fl (id, lv) VALUES (1, 'a'), (2, 'b')", base).unwrap();
    exec_multi("INSERT INTO fr (id, rv) VALUES (2, 'x'), (3, 'y')", base).unwrap();
    let result = exec_multi("SELECT * FROM fl FULL OUTER JOIN fr ON fl.id = fr.id", base).unwrap();
    let batch = result.to_record_batch().unwrap();
    // FULL OUTER: id=1 (left only), id=2 (both), id=3 (right only) = 3 rows
    assert_eq!(batch.num_rows(), 3);
}

#[test]
fn test_full_outer_join_qualified_key_projection() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE fl (id INT, lv TEXT)", base).unwrap();
    exec_multi("CREATE TABLE fr (id INT, rv TEXT)", base).unwrap();
    exec_multi("INSERT INTO fl (id, lv) VALUES (1, 'a'), (2, 'b')", base).unwrap();
    exec_multi("INSERT INTO fr (id, rv) VALUES (2, 'x'), (3, 'y')", base).unwrap();

    let result = exec_multi(
        "SELECT fl.id AS lid, fr.id AS rid, fl.lv, fr.rv \
         FROM fl FULL OUTER JOIN fr ON fl.id = fr.id \
         ORDER BY COALESCE(fl.id, fr.id)",
        base,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();

    let lid = batch
        .column_by_name("lid")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let rid = batch
        .column_by_name("rid")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let lv = batch
        .column_by_name("lv")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let rv = batch
        .column_by_name("rv")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(lid.value(0), 1);
    assert!(rid.is_null(0));
    assert_eq!(lid.value(1), 2);
    assert_eq!(rid.value(1), 2);
    assert!(lid.is_null(2));
    assert_eq!(rid.value(2), 3);
    assert_eq!(lv.value(0), "a");
    assert_eq!(lv.value(1), "b");
    assert!(lv.is_null(2));
    assert!(rv.is_null(0));
    assert_eq!(rv.value(1), "x");
    assert_eq!(rv.value(2), "y");
}

#[test]
fn test_cross_join() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE ca (id INT)", base).unwrap();
    exec_multi("CREATE TABLE cb (id INT)", base).unwrap();
    exec_multi("INSERT INTO ca (id) VALUES (1), (2)", base).unwrap();
    exec_multi("INSERT INTO cb (id) VALUES (10), (20), (30)", base).unwrap();
    let result = exec_multi("SELECT * FROM ca CROSS JOIN cb", base).unwrap();
    let batch = result.to_record_batch().unwrap();
    // CROSS JOIN: 2 × 3 = 6 rows
    assert_eq!(batch.num_rows(), 6);
}

#[test]
fn test_persistent_view_across_execute_calls() {
    let dir = tempdir().unwrap();
    let base = dir.path();

    exec_multi("CREATE TABLE src (id INT, city TEXT)", base).unwrap();
    exec_multi(
        "INSERT INTO src (id, city) VALUES (1, 'Beijing'), (2, 'Shanghai')",
        base,
    )
    .unwrap();
    exec_multi(
        "CREATE VIEW v_src AS SELECT id, city FROM src WHERE id >= 2",
        base,
    )
    .unwrap();

    let result = exec_multi("SELECT city FROM v_src", base).unwrap();
    let batch = result.to_record_batch().unwrap();
    let city = batch
        .column_by_name("city")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(city.value(0), "Shanghai");

    let reopened = exec_multi("SELECT COUNT(*) FROM v_src", base).unwrap();
    let count_batch = reopened.to_record_batch().unwrap();
    assert_eq!(
        count_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );

    exec_multi("DROP VIEW v_src", base).unwrap();
    assert!(exec_multi("SELECT * FROM v_src", base).is_err());
}

#[test]
fn test_persistent_view_over_default_table_without_default_path() {
    let dir = tempdir().unwrap();
    let base = dir.path();

    exec_multi("CREATE TABLE default (a INT)", base).unwrap();
    exec_multi("INSERT INTO default (a) VALUES (1), (2), (3)", base).unwrap();
    exec_multi(
        "CREATE VIEW v_default AS SELECT a FROM default WHERE a >= 2",
        base,
    )
    .unwrap();

    let result =
        ApexExecutor::execute_with_base_dir("SELECT * FROM v_default ORDER BY a", base, base)
            .unwrap();
    let batch = result.to_record_batch().unwrap();
    let values = batch
        .column_by_name("a")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(values.value(0), 2);
    assert_eq!(values.value(1), 3);
}

#[test]
fn test_copy_to_csv_and_json_export() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    let csv_path = base.join("export.csv");
    let json_path = base.join("export.jsonl");

    exec_multi("CREATE TABLE export_t (id INT, name TEXT)", base).unwrap();
    exec_multi(
        "INSERT INTO export_t (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
        base,
    )
    .unwrap();

    exec_multi(
        &format!("COPY export_t TO '{}'", csv_path.to_string_lossy()),
        base,
    )
    .unwrap();
    exec_multi(
        &format!("COPY export_t TO '{}'", json_path.to_string_lossy()),
        base,
    )
    .unwrap();

    let csv = std::fs::read_to_string(&csv_path).unwrap();
    assert!(csv.contains("id,name"));
    assert!(csv.contains("Alice"));

    let json = std::fs::read_to_string(&json_path).unwrap();
    assert!(json.contains("\"name\":\"Alice\""));
    assert!(json.contains("\"name\":\"Bob\""));
}

#[test]
fn test_json_mutation_functions() {
    let dir = tempdir().unwrap();
    let base = dir.path();

    let result = exec_multi(
        "SELECT \
            JSON_SET('{\"a\":1}', '$.b', 2) AS set_v, \
            JSON_INSERT('{\"a\":1}', '$.c', 3) AS ins_v, \
            JSON_REPLACE('{\"a\":1}', '$.a', 9) AS rep_v, \
            JSON_REMOVE('{\"a\":1,\"b\":2}', '$.b') AS rem_v",
        base,
    )
    .unwrap();
    let batch = result.to_record_batch().unwrap();
    let set_v = batch
        .column_by_name("set_v")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ins_v = batch
        .column_by_name("ins_v")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let rep_v = batch
        .column_by_name("rep_v")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let rem_v = batch
        .column_by_name("rem_v")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(set_v.value(0), "{\"a\":1,\"b\":2}");
    assert_eq!(ins_v.value(0), "{\"a\":1,\"c\":3}");
    assert_eq!(rep_v.value(0), "{\"a\":9}");
    assert_eq!(rem_v.value(0), "{\"a\":1}");
}

// ========== P1: Per-RG Zone Maps Tests ==========

#[test]
fn test_zone_maps_persisted_in_footer() {
    use crate::storage::on_demand::OnDemandStorage;
    let dir = tempdir().unwrap();
    let base = dir.path();
    exec_multi("CREATE TABLE zm (id INT, score INT, name TEXT)", base).unwrap();
    exec_multi(
        "INSERT INTO zm (id, score, name) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')",
        base,
    )
    .unwrap();

    // Open storage and check footer has zone maps
    let path = base.join("zm.apex");
    let storage = OnDemandStorage::open(&path).unwrap();
    if let Some(footer) = storage.get_or_load_footer().unwrap() {
        assert!(
            !footer.zone_maps.is_empty(),
            "Zone maps should be populated"
        );
        // First RG should have zone maps for Int64 columns (id and score)
        let rg0_zmaps = &footer.zone_maps[0];
        assert!(
            rg0_zmaps.len() >= 2,
            "Should have zone maps for at least 2 Int64 columns"
        );
        // Check zone map values for id column (min=1, max=3)
        let id_zm = &rg0_zmaps[0];
        assert_eq!(id_zm.min_bits, 1);
        assert_eq!(id_zm.max_bits, 3);
        assert!(!id_zm.is_float);
        // Check zone map values for score column (min=10, max=30)
        let score_zm = &rg0_zmaps[1];
        assert_eq!(score_zm.min_bits, 10);
        assert_eq!(score_zm.max_bits, 30);
    } else {
        panic!("Expected V4 footer");
    }
}

#[test]
fn test_zone_map_pruning_logic() {
    use crate::storage::on_demand::RgColumnZoneMap;
    let zm = RgColumnZoneMap {
        col_idx: 0,
        min_bits: 10,
        max_bits: 100,
        has_nulls: false,
        is_float: false,
    };
    // Value 50 is in range [10,100]
    assert!(zm.may_contain_int("=", 50));
    // Value 200 is NOT in range [10,100]
    assert!(!zm.may_contain_int("=", 200));
    // All values > 5 — max=100 > 5
    assert!(zm.may_contain_int(">", 5));
    // All values > 100 — max=100 NOT > 100
    assert!(!zm.may_contain_int(">", 100));
    // BETWEEN 50..150 overlaps [10,100]
    assert!(zm.may_overlap_int_range(50, 150));
    // BETWEEN 200..300 does NOT overlap [10,100]
    assert!(!zm.may_overlap_int_range(200, 300));
}

// ============================================================================
// R3: serial batched Filter -> GROUP BY -> HAVING -> TopK pipeline
// ============================================================================

/// Serializes tests that toggle APEX_BATCH_SCAN / APEX_PARALLEL_SCAN
/// (process-wide env state); pipeline selection and the path detail
/// depend on both switches, so both are guarded by this one lock.
static BATCH_SCAN_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn with_batch_scan<T>(enabled: bool, f: impl FnOnce() -> T) -> T {
    let _guard = BATCH_SCAN_ENV_LOCK.lock().unwrap();
    std::env::set_var("APEX_BATCH_SCAN", if enabled { "1" } else { "0" });
    let result = f();
    std::env::remove_var("APEX_BATCH_SCAN");
    result
}

fn run_with_batch_scan(enabled: bool, path: &Path, sql: &str) -> RecordBatch {
    with_batch_scan(enabled, || {
        ApexExecutor::execute(sql, path)
            .unwrap()
            .to_record_batch()
            .unwrap()
    })
}

/// Wide rows (110-byte pad) force the adaptive 32768-row RG size, so the
/// 70000-row fixture spans three row groups and exercises the multi-batch
/// pipeline. Score/amount carry NULLs; values stay exactly representable in
/// f64 so bit-equal aggregate parity is expected.
fn create_batch_scan_fixture(path: &Path) {
    const ROWS: usize = 70_000;
    let storage = OnDemandStorage::create(path).unwrap();
    let mut cities = Vec::with_capacity(ROWS);
    let mut codes = Vec::with_capacity(ROWS);
    let mut flags = Vec::with_capacity(ROWS);
    let mut scores = Vec::with_capacity(ROWS);
    let mut amounts = Vec::with_capacity(ROWS);
    let mut pads = Vec::with_capacity(ROWS);
    let mut score_nulls = vec![false; ROWS];
    let mut amount_nulls = vec![false; ROWS];
    for i in 0..ROWS {
        cities.push(format!("city{}", i % 13));
        codes.push(i as i64 % 7);
        flags.push(i % 2 == 0);
        scores.push((i % 97) as f64 * 0.5);
        score_nulls[i] = i % 23 == 5;
        amounts.push((i % 500) as i64 - 250);
        amount_nulls[i] = i % 11 == 10;
        pads.push("p".repeat(110));
    }
    storage
        .insert_typed_with_nulls(
            HashMap::from([
                ("code".to_string(), codes),
                ("amount".to_string(), amounts),
            ]),
            HashMap::from([("score".to_string(), scores)]),
            HashMap::from([
                ("city".to_string(), cities),
                ("pad".to_string(), pads),
            ]),
            HashMap::new(),
            HashMap::from([("flag".to_string(), flags)]),
            HashMap::from([
                ("score".to_string(), score_nulls),
                ("amount".to_string(), amount_nulls),
            ]),
        )
        .unwrap();
    storage.save().unwrap();
}

fn normalized_column_values(column: &ArrayRef) -> Vec<String> {
    use arrow::array::{DictionaryArray, LargeStringArray};
    use arrow::datatypes::UInt32Type;

    if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    "null".to_string()
                } else {
                    arr.value(i).to_string()
                }
            })
            .collect()
    } else if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    "null".to_string()
                } else {
                    arr.value(i).to_bits().to_string()
                }
            })
            .collect()
    } else if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    "null".to_string()
                } else {
                    arr.value(i).to_string()
                }
            })
            .collect()
    } else if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    "null".to_string()
                } else {
                    arr.value(i).to_string()
                }
            })
            .collect()
    } else if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    "null".to_string()
                } else {
                    arr.value(i).to_string()
                }
            })
            .collect()
    } else if let Some(arr) = column.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
        let values = arr
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    "null".to_string()
                } else {
                    values.value(arr.keys().value(i) as usize).to_string()
                }
            })
            .collect()
    } else {
        panic!(
            "unsupported column type in batch-scan parity test: {}",
            column.data_type()
        )
    }
}

fn assert_batches_logically_equal(a: &RecordBatch, b: &RecordBatch, sql: &str) {
    assert_eq!(a.num_columns(), b.num_columns(), "{sql}: column count");
    assert_eq!(a.num_rows(), b.num_rows(), "{sql}: row count");
    for i in 0..a.num_columns() {
        let name = a.schema().field(i).name().to_string();
        assert_eq!(
            name,
            b.schema().field(i).name().to_string(),
            "{sql}: column {i} name"
        );
        assert_eq!(
            normalized_column_values(a.column(i)),
            normalized_column_values(b.column(i)),
            "{sql}: column {name} values"
        );
    }
}

#[test]
fn batch_group_pipeline_executes_gated_shapes_and_falls_back_outside_gate() {
    use crate::query::sql_parser::SqlStatement;

    let dir = tempdir().unwrap();
    let path = dir.path().join("batch_pipeline_gate.apex");
    create_batch_scan_fixture(&path);

    let parse = |sql: &str| -> SelectStatement {
        match SqlParser::parse(sql).unwrap() {
            SqlStatement::Select(stmt) => stmt,
            other => panic!("expected SELECT, got {other:?}"),
        }
    };

    with_batch_scan(true, || {
        let backend = TableStorageBackend::open(&path).unwrap();

        // Gated shape: two plain keys, typed WHERE, one aggregate source.
        let sql = "SELECT city, code, COUNT(*) AS n, SUM(score) AS s                    FROM default WHERE score >= 20 AND code IN (1, 3, 5)                    GROUP BY city, code";
        let stmt = parse(sql);
        let predicate =
            ApexExecutor::build_scan_predicate(stmt.where_clause.as_ref().unwrap()).unwrap();
        let result =
            ApexExecutor::try_batch_group_pipeline(&backend, &stmt, &predicate, &path.to_string_lossy()).unwrap();
        assert!(
            result.is_some(),
            "gated shape must be served by the batch pipeline"
        );

        // Outside the gate: three group keys fall back to the single-batch path.
        let wide_sql = "SELECT city, code, flag, COUNT(*) AS n                         FROM default WHERE score > 0                         GROUP BY city, code, flag";
        let wide_stmt = parse(wide_sql);
        let wide_predicate =
            ApexExecutor::build_scan_predicate(wide_stmt.where_clause.as_ref().unwrap()).unwrap();
        assert!(
            ApexExecutor::try_batch_group_pipeline(&backend, &wide_stmt, &wide_predicate, &path.to_string_lossy())
                .unwrap()
                .is_none(),
            "three-key GROUP BY must fall back"
        );
    });

    // Outside the gate: delta state forces the single-shot fallback.
    let delta_backend = TableStorageBackend::open(&path).unwrap();
    delta_backend
        .insert_rows_to_delta(&[HashMap::from([
            ("city".to_string(), Value::String("city0".to_string())),
            ("code".to_string(), Value::Int64(1)),
            ("flag".to_string(), Value::Bool(true)),
            ("score".to_string(), Value::Float64(1.0)),
            ("amount".to_string(), Value::Int64(1)),
            ("pad".to_string(), Value::String("z".repeat(110))),
        ])])
        .unwrap();
    let wide_sql = "SELECT city, code, COUNT(*) AS n                     FROM default WHERE score >= 20                     GROUP BY city, code";
    let stmt = parse(wide_sql);
    let predicate =
        ApexExecutor::build_scan_predicate(stmt.where_clause.as_ref().unwrap()).unwrap();
    let result = with_batch_scan(true, || {
        ApexExecutor::try_batch_group_pipeline(&delta_backend, &stmt, &predicate, &path.to_string_lossy()).unwrap()
    });
    assert!(
        result.is_none(),
        "delta state must disable the batch pipeline"
    );
}

#[test]
fn batch_group_pipeline_honors_cancellation_token() {
    use crate::query::sql_parser::SqlStatement;

    let dir = tempdir().unwrap();
    let path = dir.path().join("batch_cancel.apex");
    create_batch_scan_fixture(&path);

    let sql = "SELECT city, code, COUNT(*) AS n                 FROM default WHERE score >= 20                 GROUP BY city, code";
    let stmt = match SqlParser::parse(sql).unwrap() {
        SqlStatement::Select(stmt) => stmt,
        other => panic!("expected SELECT, got {other:?}"),
    };
    let predicate =
        ApexExecutor::build_scan_predicate(stmt.where_clause.as_ref().unwrap()).unwrap();
    let backend = TableStorageBackend::open(&path).unwrap();

    with_batch_scan(true, || {
        // No token: the pipeline completes normally.
        assert!(!crate::query::executor::query_cancelled());
        assert!(
            ApexExecutor::try_batch_group_pipeline(&backend, &stmt, &predicate, &path.to_string_lossy())
                .unwrap()
                .is_some(),
            "batch pipeline must complete without a cancellation token"
        );

        // Pre-set token: the first batch boundary aborts with Interrupted.
        let token = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        crate::query::executor::set_query_cancel_token(Some(std::sync::Arc::clone(&token)));
        let outcome = ApexExecutor::try_batch_group_pipeline(&backend, &stmt, &predicate, &path.to_string_lossy());
        crate::query::executor::set_query_cancel_token(None);
        let err = match outcome {
            Err(err) => err,
            Ok(_) => panic!("cancellation surfaces as an error"),
        };
        assert_eq!(err.kind(), std::io::ErrorKind::Interrupted);
        assert_eq!(err.to_string(), "query cancelled");

        // Token cleared again: the pipeline completes.
        assert!(
            ApexExecutor::try_batch_group_pipeline(&backend, &stmt, &predicate, &path.to_string_lossy())
                .unwrap()
                .is_some(),
            "batch pipeline must complete after the token is cleared"
        );
    });
}

#[test]
fn batch_scan_pipeline_matches_single_batch_pipeline() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("batch_scan_ab.apex");
    create_batch_scan_fixture(&path);

    let queries = [
        // Int key, int source, HAVING over a SELECT aggregate.
        "SELECT code, COUNT(*) AS n, SUM(amount) AS s, MIN(amount) AS mn, MAX(amount) AS mx          FROM default WHERE amount IS NOT NULL AND code >= 1          GROUP BY code HAVING SUM(amount) > -5000 ORDER BY n DESC, code LIMIT 10",
        // Two keys (string + int), float source, range + IN.
        "SELECT city, code, COUNT(*) AS n, AVG(score) AS av, MIN(score) AS mn, MAX(score) AS mx, SUM(score) AS s          FROM default WHERE score >= 20 AND score <= 40 AND code IN (1, 3, 5)          GROUP BY city, code HAVING COUNT(*) > 2 ORDER BY av DESC, city, code LIMIT 20",
        // IS NULL predicate, no HAVING.
        "SELECT city, COUNT(*) AS n FROM default WHERE score IS NULL GROUP BY city          ORDER BY n DESC, city LIMIT 5",
        // OR tree with BETWEEN + IN, OFFSET.
        "SELECT city, COUNT(*) AS n, AVG(amount) AS av FROM default          WHERE amount BETWEEN -100 AND 100 OR city IN ('city1', 'city7')          GROUP BY city HAVING COUNT(*) > 10 ORDER BY av DESC, city LIMIT 7 OFFSET 1",
        // Bool group key.
        "SELECT flag, COUNT(*) AS n FROM default WHERE amount > 0 GROUP BY flag          HAVING COUNT(*) > 100 ORDER BY n DESC, flag LIMIT 5",
        // Float group key.
        "SELECT score, COUNT(*) AS n FROM default WHERE score IS NOT NULL AND score >= 30          GROUP BY score ORDER BY n DESC, score LIMIT 5",
        // Predicate matching no rows: empty results must match too.
        "SELECT city, COUNT(*) AS n FROM default WHERE code = 99 GROUP BY city ORDER BY n DESC, city LIMIT 3",
        // Three-key GROUP BY is outside the batch gate: env on must still
        // match the single-batch result through the fallback wiring.
        "SELECT city, code, flag, COUNT(*) AS n FROM default          WHERE amount > 0 AND code <= 4 GROUP BY city, code, flag          ORDER BY n DESC, city, code, flag LIMIT 5",
    ];

    for sql in queries {
        let off = run_with_batch_scan(false, &path, sql);
        let on = run_with_batch_scan(true, &path, sql);
        assert_batches_logically_equal(&off, &on, sql);
    }

    // Delta state: the batch pipeline must fall back and still match.
    let backend = TableStorageBackend::open(&path).unwrap();
    backend
        .insert_rows_to_delta(&[
            HashMap::from([
                ("city".to_string(), Value::String("city3".to_string())),
                ("code".to_string(), Value::Int64(3)),
                ("flag".to_string(), Value::Bool(true)),
                ("score".to_string(), Value::Float64(25.5)),
                ("amount".to_string(), Value::Int64(10)),
                ("pad".to_string(), Value::String("d".repeat(110))),
            ]),
            HashMap::from([
                ("city".to_string(), Value::String("city5".to_string())),
                ("code".to_string(), Value::Int64(5)),
                ("flag".to_string(), Value::Bool(false)),
                ("score".to_string(), Value::Float64(35.0)),
                ("amount".to_string(), Value::Int64(-40)),
                ("pad".to_string(), Value::String("d".repeat(110))),
            ]),
        ])
        .unwrap();
    drop(backend);
    invalidate_storage_cache(&path);

    let sql = queries[1];
    let off = run_with_batch_scan(false, &path, sql);
    let on = run_with_batch_scan(true, &path, sql);
    assert_batches_logically_equal(&off, &on, sql);
}

// ============================================================================
// R5.7: parallel (morsel) fold of the batch pipeline, opt-in
// ============================================================================

fn with_parallel_scan<T>(threads: Option<usize>, f: impl FnOnce() -> T) -> T {
    let _guard = BATCH_SCAN_ENV_LOCK.lock().unwrap();
    match threads {
        Some(count) => std::env::set_var("APEX_PARALLEL_SCAN", count.to_string()),
        None => std::env::remove_var("APEX_PARALLEL_SCAN"),
    }
    let result = f();
    std::env::remove_var("APEX_PARALLEL_SCAN");
    result
}

fn run_with_parallel_scan(threads: Option<usize>, path: &Path, sql: &str) -> RecordBatch {
    with_parallel_scan(threads, || {
        ApexExecutor::execute(sql, path)
            .unwrap()
            .to_record_batch()
            .unwrap()
    })
}

#[test]
fn parallel_batch_scan_matches_serial_pipeline() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("parallel_batch_scan.apex");
    create_batch_scan_fixture(&path);

    // The same shapes as the serial parity test: values are exactly
    // representable in f64, so the deterministic chunk-order merge is
    // bit-equal to the serial fold at any thread count.
    let queries = [
        "SELECT code, COUNT(*) AS n, SUM(amount) AS s, MIN(amount) AS mn, MAX(amount) AS mx          FROM default WHERE amount IS NOT NULL AND code >= 1          GROUP BY code HAVING SUM(amount) > -5000 ORDER BY n DESC, code LIMIT 10",
        "SELECT city, code, COUNT(*) AS n, AVG(score) AS av, MIN(score) AS mn, MAX(score) AS mx, SUM(score) AS s          FROM default WHERE score >= 20 AND score <= 40 AND code IN (1, 3, 5)          GROUP BY city, code HAVING COUNT(*) > 2 ORDER BY av DESC, city, code LIMIT 20",
        "SELECT city, COUNT(*) AS n FROM default WHERE score IS NULL GROUP BY city          ORDER BY n DESC, city LIMIT 5",
        "SELECT city, COUNT(*) AS n, AVG(amount) AS av FROM default          WHERE amount BETWEEN -100 AND 100 OR city IN ('city1', 'city7')          GROUP BY city HAVING COUNT(*) > 10 ORDER BY av DESC, city LIMIT 7 OFFSET 1",
        "SELECT flag, COUNT(*) AS n FROM default WHERE amount > 0 GROUP BY flag          HAVING COUNT(*) > 100 ORDER BY n DESC, flag LIMIT 5",
        "SELECT score, COUNT(*) AS n FROM default WHERE score IS NOT NULL AND score >= 30          GROUP BY score ORDER BY n DESC, score LIMIT 5",
        "SELECT city, COUNT(*) AS n FROM default WHERE code = 99 GROUP BY city ORDER BY n DESC, city LIMIT 3",
        "SELECT city, code, flag, COUNT(*) AS n FROM default          WHERE amount > 0 AND code <= 4 GROUP BY city, code, flag          ORDER BY n DESC, city, code, flag LIMIT 5",
    ];

    let serial = queries
        .iter()
        .map(|sql| run_with_parallel_scan(None, &path, sql))
        .collect::<Vec<_>>();
    // 8 requests more workers than the process budget grants; the fused
    // path must still match serial at every granted count.
    for &threads in &[2usize, 4, 8] {
        for (sql, expected) in queries.iter().zip(serial.iter()) {
            let parallel = run_with_parallel_scan(Some(threads), &path, sql);
            assert_batches_logically_equal(expected, &parallel, sql);
        }
    }
}

#[test]
fn parallel_batch_scan_falls_back_when_tokens_exhausted() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("parallel_tokens.apex");
    create_batch_scan_fixture(&path);
    let sql = "SELECT city, code, COUNT(*) AS n, SUM(score) AS s          FROM default WHERE score >= 20 AND score <= 40 AND code IN (1, 3, 5)          GROUP BY city, code HAVING COUNT(*) > 2 ORDER BY n DESC, city, code LIMIT 20";

    let serial = run_with_parallel_scan(None, &path, sql);
    // Hold every in-flight token while the env switch is held under
    // the shared env lock: the parallel request must fall back to the
    // serial fold instead of oversubscribing (the token pool is also
    // process-wide, so the exhausted span must not overlap other
    // parallel tests).
    let _guard = BATCH_SCAN_ENV_LOCK.lock().unwrap();
    std::env::set_var("APEX_PARALLEL_SCAN", "4");
    let _exhausted = crate::query::executor::exhaust_parallel_tokens_for_test();
    let parallel = ApexExecutor::execute(sql, &path)
        .unwrap()
        .to_record_batch()
        .unwrap();
    std::env::remove_var("APEX_PARALLEL_SCAN");
    assert_batches_logically_equal(&serial, &parallel, sql);
}

#[test]
fn parallel_batch_scan_single_morsel_stays_serial() {
    // 3000 narrow rows fit in one row group (default 65536): a single
    // morsel has nothing to parallelize.
    let dir = tempdir().unwrap();
    let path = dir.path().join("parallel_single_rg.apex");
    let storage = OnDemandStorage::create(&path).unwrap();
    let rows: usize = 3000;
    storage
        .insert_typed(
            HashMap::from([
                ("code".to_string(), (0..rows).map(|i| (i % 5) as i64).collect()),
                ("amount".to_string(), (0..rows).map(|i| (i % 50) as i64).collect()),
            ]),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();

    let sql = "SELECT code, COUNT(*) AS n, SUM(amount) AS s          FROM default WHERE amount >= 10          GROUP BY code ORDER BY n DESC, code";
    let serial = run_with_parallel_scan(None, &path, sql);
    let parallel = run_with_parallel_scan(Some(4), &path, sql);
    assert_batches_logically_equal(&serial, &parallel, sql);
}

#[test]
fn parallel_batch_scan_falls_back_with_delta_state() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("parallel_delta.apex");
    create_batch_scan_fixture(&path);
    let backend = TableStorageBackend::open(&path).unwrap();
    backend
        .insert_rows_to_delta(&[HashMap::from([
            ("city".to_string(), Value::String("city3".to_string())),
            ("code".to_string(), Value::Int64(3)),
            ("flag".to_string(), Value::Bool(true)),
            ("score".to_string(), Value::Float64(25.5)),
            ("amount".to_string(), Value::Int64(10)),
            ("pad".to_string(), Value::String("d".repeat(110))),
        ])])
        .unwrap();
    drop(backend);
    invalidate_storage_cache(&path);

    let sql = "SELECT city, code, COUNT(*) AS n, AVG(score) AS av, MIN(score) AS mn, MAX(score) AS mx, SUM(score) AS s          FROM default WHERE score >= 20 AND score <= 40 AND code IN (1, 3, 5)          GROUP BY city, code HAVING COUNT(*) > 2 ORDER BY av DESC, city, code LIMIT 20";
    let serial = run_with_parallel_scan(None, &path, sql);
    let parallel = run_with_parallel_scan(Some(4), &path, sql);
    assert_batches_logically_equal(&serial, &parallel, sql);
}

#[test]
fn parallel_batch_scan_honors_cancellation_token() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("parallel_cancel.apex");
    create_batch_scan_fixture(&path);
    let sql = "SELECT city, code, COUNT(*) AS n          FROM default WHERE score >= 20          GROUP BY city, code ORDER BY n DESC, city, code";

    // Pre-set token: the collect loop aborts at the first batch boundary.
    with_parallel_scan(Some(2), || {
        let token = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        crate::query::executor::set_query_cancel_token(Some(std::sync::Arc::clone(&token)));
        let outcome = ApexExecutor::execute(sql, &path);
        crate::query::executor::set_query_cancel_token(None);
        let err = match outcome {
            Err(err) => err,
            Ok(_) => panic!("cancellation must surface from the parallel collect"),
        };
        assert_eq!(err.kind(), std::io::ErrorKind::Interrupted);
    });

    // Cleared token: the parallel fold completes and matches serial.
    let parallel = run_with_parallel_scan(Some(2), &path, sql);
    let serial = run_with_parallel_scan(None, &path, sql);
    assert_batches_logically_equal(&serial, &parallel, sql);
}

#[test]
fn fused_parallel_scan_ranges_partition_row_groups() {
    // 70K wide rows force the adaptive 32768-row RG size (three RGs);
    // any requested range count must partition the row-group space
    // exactly once (no gaps, no overlaps) at the storage level.
    use arrow::array::Int64Array;
    let dir = tempdir().unwrap();
    let path = dir.path().join("fused_ranges.apex");
    create_batch_scan_fixture(&path);

    let backend = TableStorageBackend::open(&path).unwrap();
    let projection: &[&str] = &["_id", "code"];
    let request = crate::storage::ScanRequest {
        projection: Some(projection),
        predicate: None,
    };
    for range_count in [1usize, 2, 3, 5, 8] {
        let mut ranges = backend
            .scan_batches_ranges(&request, range_count)
            .unwrap()
            .expect("range streams");
        let mut ids: Vec<i64> = Vec::new();
        for stream in &mut ranges {
            for outcome in stream {
                let batch = match outcome {
                    Ok(crate::storage::BatchMorselOutcome::Morsel(morsel)) => {
                        morsel.into_record_batch().unwrap()
                    }
                    Ok(crate::storage::BatchMorselOutcome::Unsupported) => {
                        panic!("typed columns must stay supported")
                    }
                    Err(err) => panic!("stream error: {err}"),
                };
                let id_array = batch
                    .column_by_name("_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..id_array.len() {
                    ids.push(id_array.value(i));
                }
            }
        }
        ids.sort_unstable();
        let expected: Vec<i64> = (1..=70_000).collect();
        assert_eq!(
            ids, expected,
            "range_count={range_count}: ranges must cover every row exactly once"
        );
    }
}

#[test]
fn fused_parallel_scan_reports_worker_count_in_path_detail() {
    // The fused path must report the granted worker count in the EXPLAIN
    // ANALYZE path detail; the serial default must not.
    let dir = tempdir().unwrap();
    let path = dir.path().join("fused_path.apex");
    create_batch_scan_fixture(&path);

    let sql = "EXPLAIN ANALYZE SELECT city, COUNT(*) AS n FROM default WHERE amount > 0 GROUP BY city ORDER BY n DESC, city LIMIT 5";

    let serial_plan = with_parallel_scan(None, || explain_analyze_plan(&path, sql));
    assert!(
        serial_plan.contains("batched_scan_pipeline(batches="),
        "{serial_plan}"
    );
    assert!(!serial_plan.contains("parallel="), "{serial_plan}");

    // Two requested workers: the budget (min(hw-1, 4)) always has at
    // least two free tokens while the env lock is held, and the 3-RG
    // fixture yields two ranges, so the detail must say parallel=2.
    let parallel_plan = with_parallel_scan(Some(2), || explain_analyze_plan(&path, sql));
    assert!(
        parallel_plan.contains("batched_scan_pipeline(batches="),
        "{parallel_plan}"
    );
    assert!(parallel_plan.contains(", parallel=2)"), "{parallel_plan}");
}

// ============================================================================
// R5.12: cost-based auto-enable of the parallel batch scan
// ============================================================================

const AUTO_SQL: &str = "SELECT city, COUNT(*) AS n FROM default WHERE amount > 0 GROUP BY city ORDER BY n DESC, city LIMIT 5";
const AUTO_EXPLAIN_SQL: &str = "EXPLAIN ANALYZE SELECT city, COUNT(*) AS n FROM default WHERE amount > 0 GROUP BY city ORDER BY n DESC, city LIMIT 5";

/// One synthetic R5.3 calibration sample for the serial cost class.
fn record_scan_feedback(path: &Path, sql: &str, time_us: f64) {
    record_plan_feedback(
        &path.to_string_lossy(),
        &parsed_select(sql),
        &ExecutionStrategy::OlapAggregation,
        0.0,
        0.0,
        ExecutedCostClass::Scan,
        0.0,
        time_us,
    );
}

fn record_parallel_feedback(path: &Path, sql: &str, time_us: f64) {
    record_plan_feedback(
        &path.to_string_lossy(),
        &parsed_select(sql),
        &ExecutionStrategy::OlapAggregation,
        0.0,
        0.0,
        ExecutedCostClass::ParallelScan,
        0.0,
        time_us,
    );
}

#[test]
fn auto_parallel_enables_from_calibrated_threshold() {
    let dir = tempdir().unwrap();

    // The first EXPLAIN ANALYZE of a shape runs serial: no calibrated
    // prediction exists yet, so the default behavior is unchanged.
    let pre_path = dir.path().join("auto_pre.apex");
    create_batch_scan_fixture(&pre_path);
    let pre_plan = with_parallel_scan(None, || explain_analyze_plan(&pre_path, AUTO_EXPLAIN_SQL));
    assert!(pre_plan.contains("batched_scan_pipeline(batches="), "{pre_plan}");
    assert!(!pre_plan.contains("parallel="), "{pre_plan}");

    // Calibrated serial prediction above the 2 ms threshold: the next run
    // of the same shape auto-enables the fused parallel scan, and results
    // must match the forced-serial run.
    let enable_path = dir.path().join("auto_enable.apex");
    create_batch_scan_fixture(&enable_path);
    record_scan_feedback(&enable_path, AUTO_SQL, 2500.0);
    let auto_plan = with_parallel_scan(None, || explain_analyze_plan(&enable_path, AUTO_EXPLAIN_SQL));
    assert!(auto_plan.contains("batched_scan_pipeline(batches="), "{auto_plan}");
    assert!(auto_plan.contains(", parallel="), "{auto_plan}");

    let _guard = BATCH_SCAN_ENV_LOCK.lock().unwrap();
    std::env::set_var("APEX_PARALLEL_SCAN", "0");
    let serial = ApexExecutor::execute(AUTO_SQL, &enable_path)
        .unwrap()
        .to_record_batch()
        .unwrap();
    std::env::remove_var("APEX_PARALLEL_SCAN");
    let auto = ApexExecutor::execute(AUTO_SQL, &enable_path)
        .unwrap()
        .to_record_batch()
        .unwrap();
    drop(_guard);
    assert_batches_logically_equal(&serial, &auto, AUTO_SQL);
}

#[test]
fn auto_parallel_flip_back_when_measured_slower() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("auto_flip.apex");
    create_batch_scan_fixture(&path);

    // Calibration above the threshold, but the measured parallel history
    // is slower than the serial prediction: the same closed loop flips
    // the shape back to serial (R5.12).
    record_scan_feedback(&path, AUTO_SQL, 2500.0);
    record_parallel_feedback(&path, AUTO_SQL, 3000.0);
    let flipped_plan = with_parallel_scan(None, || explain_analyze_plan(&path, AUTO_EXPLAIN_SQL));
    assert!(!flipped_plan.contains("parallel="), "{flipped_plan}");

    // A fast enough parallel history re-enables the same shape.
    record_parallel_feedback(&path, AUTO_SQL, 1000.0);
    let reenabled_plan = with_parallel_scan(None, || explain_analyze_plan(&path, AUTO_EXPLAIN_SQL));
    assert!(reenabled_plan.contains(", parallel="), "{reenabled_plan}");
}

#[test]
fn explicit_env_overrides_auto_parallel_decision() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("auto_override.apex");
    create_batch_scan_fixture(&path);

    // Flip state: the cost-based default would stay serial.
    record_scan_feedback(&path, AUTO_SQL, 2500.0);
    record_parallel_feedback(&path, AUTO_SQL, 3000.0);
    let forced = with_parallel_scan(Some(2), || explain_analyze_plan(&path, AUTO_EXPLAIN_SQL));
    assert!(forced.contains(", parallel=2)"), "{forced}");

    // Explicit 0 forces serial despite the calibration above threshold.
    let _guard = BATCH_SCAN_ENV_LOCK.lock().unwrap();
    std::env::set_var("APEX_PARALLEL_SCAN", "0");
    let serial_plan = explain_analyze_plan(&path, AUTO_EXPLAIN_SQL);
    std::env::remove_var("APEX_PARALLEL_SCAN");
    drop(_guard);
    assert!(!serial_plan.contains("parallel="), "{serial_plan}");
}

#[test]
fn two_key_string_int_group_by_keeps_min_max_columns() {
    // Regression: the string-dict + int-range fast kernel only accumulates
    // COUNT/SUM/AVG; MIN/MAX must fall through to the full incremental kernel
    // instead of being silently dropped from the result.
    let dir = tempdir().unwrap();
    let path = dir.path().join("min_max_two_keys.apex");
    let storage = OnDemandStorage::create(&path).unwrap();
    storage
        .insert_typed_with_nulls(
            HashMap::from([("code".to_string(), vec![1, 1, 2, 1, 1, 3])]),
            HashMap::from([
                ("score".to_string(), vec![1.0, 3.0, 0.0, 2.0, 2.0, 5.0]),
                ("pad".to_string(), vec![0.0; 6]),
            ]),
            HashMap::from([
                (
                    "city".to_string(),
                    vec!["A", "A", "A", "B", "B", "C"]
                        .into_iter()
                        .map(str::to_string)
                        .collect(),
                ),
                (
                    "filler".to_string(),
                    vec![0u64; 6]
                        .iter()
                        .map(|_| "f".repeat(120))
                        .collect(),
                ),
            ]),
            HashMap::new(),
            HashMap::new(),
            HashMap::from([("score".to_string(), vec![
                false, false, true, false, false, false,
            ])]),
        )
        .unwrap();
    storage.save().unwrap();

    let sql = "SELECT city, code, COUNT(*) AS n, MIN(score) AS mn, MAX(score) AS mx, AVG(score) AS av, SUM(score) AS s                FROM default WHERE score IS NOT NULL OR code = 2                GROUP BY city, code ORDER BY city, code";
    let batch = with_batch_scan(true, || {
        ApexExecutor::execute(sql, &path).unwrap().to_record_batch().unwrap()
    });

    assert_eq!(batch.num_columns(), 7, "all requested columns must be present");
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["city", "code", "n", "mn", "mx", "av", "s"]
    );
    assert_eq!(batch.num_rows(), 4);

    let cities = batch.column_by_name("city").unwrap();
    let cities = cities.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        (0..4)
            .map(|i| cities.value(i))
            .collect::<Vec<_>>(),
        vec!["A", "A", "B", "C"]
    );

    let n = batch.column_by_name("n").unwrap();
    let n = n.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(n.values(), &[2, 1, 2, 1]);

    let mn = batch.column_by_name("mn").unwrap();
    let mn = mn.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(mn.is_null(1), "all-NULL group must yield NULL MIN");
    assert_eq!(
        [mn.value(0), mn.value(2), mn.value(3)],
        [1.0, 2.0, 5.0]
    );

    let mx = batch.column_by_name("mx").unwrap();
    let mx = mx.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(mx.is_null(1));
    assert_eq!([mx.value(0), mx.value(2), mx.value(3)], [3.0, 2.0, 5.0]);

    let av = batch.column_by_name("av").unwrap();
    let av = av.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(
        [av.value(0), av.value(1), av.value(2), av.value(3)],
        [2.0, 0.0, 2.0, 5.0]
    );

    let s = batch.column_by_name("s").unwrap();
    let s = s.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!([s.value(0), s.value(1), s.value(2), s.value(3)], [4.0, 0.0, 4.0, 5.0]);
}

#[test]
fn three_key_group_by_keeps_alias_and_sorts_deterministically() {
    // Regression: the 3+ key fast path used to drop the aggregate alias
    // (breaking ORDER BY over it) and its comparator could not order bool
    // columns, so repeated runs of the same query diverged.
    let dir = tempdir().unwrap();
    let path = dir.path().join("three_key_alias.apex");
    let storage = OnDemandStorage::create(&path).unwrap();
    const ROWS: usize = 3000;
    let mut cities = Vec::with_capacity(ROWS);
    let mut codes = Vec::with_capacity(ROWS);
    let mut flags = Vec::with_capacity(ROWS);
    for i in 0..ROWS {
        cities.push(format!("c{}", i % 5));
        codes.push(i as i64 % 3);
        flags.push(i % 4 < 2);
    }
    storage
        .insert_typed(
            HashMap::from([("code".to_string(), codes)]),
            HashMap::new(),
            HashMap::from([("city".to_string(), cities)]),
            HashMap::new(),
            HashMap::from([("flag".to_string(), flags)]),
        )
        .unwrap();
    storage.save().unwrap();

    let sql = "SELECT city, code, flag, COUNT(*) AS n FROM default GROUP BY city, code, flag ORDER BY n DESC, city, code, flag LIMIT 4";
    let batch = |sql: &str| -> (Vec<String>, Vec<i64>) {
        let rb = with_batch_scan(true, || {
            ApexExecutor::execute(sql, &path).unwrap().to_record_batch().unwrap()
        });
        let n = rb
            .column_by_name("n")
            .expect("aggregate alias column must be present")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let city_cast = arrow::compute::cast(
            rb.column_by_name("city").unwrap(),
            &arrow::datatypes::DataType::Utf8,
        )
        .unwrap();
        let cities = city_cast.as_any().downcast_ref::<StringArray>().unwrap();
        (
            (0..rb.num_rows())
                .map(|i| cities.value(i).to_string())
                .collect(),
            (0..rb.num_rows()).map(|i| n.value(i)).collect(),
        )
    };

    let (cities, counts) = batch(sql);
    assert_eq!(cities.len(), 4);
    // ORDER BY n DESC must hold.
    for w in counts.windows(2) {
        assert!(w[0] >= w[1], "ORDER BY n DESC violated: {counts:?}");
    }
    // The first 4 groups are the 5x3x2=30 groups with the highest counts;
    // 3000/30 == 100 exactly, so the top rows are a 4-way tie that the
    // (city, code, flag) keys fully resolve: c0/0/false comes first.
    assert_eq!(cities[0], "c0");

    // Repeated executions must be byte-stable.
    for _ in 0..4 {
        assert_eq!(batch(sql), (cities.clone(), counts.clone()));
    }
}

#[test]
fn negative_bound_predicates_stay_in_typed_scan_protocol() {
    use crate::query::sql_parser::{SqlExpr, SqlStatement};
    use crate::storage::{ScanBound, ScanComparison, ScanPredicate, ScanPredicateExpr, ScanValue};

    let parse_where = |sql: &str| -> SqlExpr {
        match SqlParser::parse(sql).unwrap() {
            SqlStatement::Select(stmt) => stmt.where_clause.unwrap(),
            other => panic!("expected SELECT, got {other:?}"),
        }
    };

    // Regression: negative literals parse as UnaryOp(Minus, literal) and used
    // to defeat the typed scan protocol, silently demoting range queries with
    // negative bounds to the generic path.
    let pred = ApexExecutor::build_scan_predicate(&parse_where(
        "SELECT * FROM default WHERE amount >= -100",
    ))
    .unwrap();
    assert_eq!(
        pred,
        ScanPredicateExpr::Predicate(ScanPredicate::Compare {
            column: "amount".to_string(),
            op: ScanComparison::Ge,
            value: ScanValue::Int(-100),
        })
    );

    let pred = ApexExecutor::build_scan_predicate(&parse_where(
        "SELECT * FROM default WHERE amount BETWEEN -100 AND 100",
    ))
    .unwrap();
    assert_eq!(
        pred,
        ScanPredicateExpr::Predicate(ScanPredicate::Between {
            column: "amount".to_string(),
            lower: Some(ScanBound::inclusive(ScanValue::Int(-100))),
            upper: Some(ScanBound::inclusive(ScanValue::Int(100))),
        })
    );

    let pred = ApexExecutor::build_scan_predicate(&parse_where(
        "SELECT * FROM default WHERE score > -1.5",
    ))
    .unwrap();
    assert_eq!(
        pred,
        ScanPredicateExpr::Predicate(ScanPredicate::Compare {
            column: "score".to_string(),
            op: ScanComparison::Gt,
            value: ScanValue::Float(-1.5),
        })
    );

    // Negation applied to a non-literal stays conservative (no fold).
    assert!(
        ApexExecutor::build_scan_predicate(&parse_where(
            "SELECT * FROM default WHERE -amount > 5"
        ))
        .is_none()
    );
}

#[test]
fn negative_bound_group_by_keeps_exact_results_in_both_env_states() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("negative_bounds.apex");
    let storage = OnDemandStorage::create(&path).unwrap();
    storage
        .insert_typed(
            HashMap::from([
                ("code".to_string(), vec![1_i64, 1, 2, 2, 3]),
                ("amount".to_string(), vec![-150_i64, -50, 10, 250, -10]),
            ]),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();

    let sql =
        "SELECT code, COUNT(*) AS n FROM default WHERE amount >= -100 GROUP BY code ORDER BY code";
    for enabled in [false, true] {
        let rb = with_batch_scan(enabled, || {
            ApexExecutor::execute(sql, &path).unwrap().to_record_batch().unwrap()
        });
        let codes = rb
            .column_by_name("code")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = rb
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let got: Vec<(i64, i64)> = (0..rb.num_rows())
            .map(|i| (codes.value(i), counts.value(i)))
            .collect();
        assert_eq!(got, vec![(1, 1), (2, 2), (3, 1)]);
    }

    let sql = "SELECT code, COUNT(*) AS n FROM default WHERE amount BETWEEN -100 AND 100 GROUP BY code ORDER BY code";
    let rb = with_batch_scan(true, || {
        ApexExecutor::execute(sql, &path).unwrap().to_record_batch().unwrap()
    });
    let counts = rb
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        (0..rb.num_rows()).map(|i| counts.value(i)).collect::<Vec<_>>(),
        vec![1, 1, 1]
    );
}

// ============================================================================
// R5.1: EXPLAIN ANALYZE reports the physical path actually taken
// ============================================================================

fn explain_analyze_plan(path: &Path, sql: &str) -> String {
    let rb = ApexExecutor::execute(sql, path)
        .unwrap()
        .to_record_batch()
        .unwrap();
    let arr = rb
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("plan column");
    arr.value(0).to_string()
}

fn actual_path(plan: &str) -> &str {
    plan.lines()
        .find_map(|line| line.trim().strip_prefix("Actual Path:").map(str::trim))
        .expect("plan must contain an Actual Path line")
}

#[test]
fn path_trace_first_record_wins_and_is_off_by_default() {
    crate::query::executor::begin_path_trace();
    crate::query::executor::record_path("route_a");
    crate::query::executor::record_path("route_b");
    crate::query::executor::record_path_detail_f(format_args!("(batches=3)"));
    let trace = crate::query::executor::finish_path_trace().unwrap();
    assert_eq!(trace, "route_a(batches=3)");

    // Tracing off by default: records are dropped, no state is left behind.
    crate::query::executor::record_path("route_off");
    crate::query::executor::record_path_detail_f(format_args!("(x=1)"));
    assert!(crate::query::executor::finish_path_trace().is_none());
}

#[test]
fn explain_analyze_reports_batched_scan_pipeline_path() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("explain_batch_path.apex");
    create_batch_scan_fixture(&path);

    with_batch_scan(true, || {
        let plan = explain_analyze_plan(
            &path,
            "EXPLAIN ANALYZE SELECT city, code, COUNT(*) AS n
             FROM default WHERE score >= 20
             GROUP BY city, code",
        );
        let actual = actual_path(&plan);
        let Some(rest) = actual.strip_prefix("batched_scan_pipeline(batches=") else {
            panic!("expected the batched scan pipeline path, got: {actual}");
        };
        // R5.7 may append the opt-in parallel suffix to the detail
        // (`(batches=N, parallel=T)`); the batch count is the first
        // field in either format.
        let batches_field = rest.split(',').next().unwrap_or(rest);
        let batches: u64 = batches_field.trim_end_matches(')').parse().unwrap();
        assert!(
            batches >= 2,
            "the multi-row-group fixture must consume multiple batches: {actual}"
        );
    });
}

#[test]
fn explain_analyze_reports_metadata_and_point_lookup_paths() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("explain_preparse.apex");
    create_batch_scan_fixture(&path);

    // COUNT(*) is served by the row-count metadata read.
    let plan = explain_analyze_plan(&path, "EXPLAIN ANALYZE SELECT COUNT(*) FROM default");
    assert_eq!(actual_path(&plan), "count_star_metadata");

    // Columnar scan with LIMIT runs through the generic executor route.
    let plan = explain_analyze_plan(&path, "EXPLAIN ANALYZE SELECT city FROM default LIMIT 3");
    assert_eq!(actual_path(&plan), "generic_executor");

    // O(1) point lookup on _id.
    let plan = explain_analyze_plan(
        &path,
        "EXPLAIN ANALYZE SELECT city, code FROM default WHERE _id = 7",
    );
    assert_eq!(actual_path(&plan), "id_point_lookup");
}

#[test]
fn explain_analyze_reports_generic_executor_path() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("explain_generic.apex");
    create_test_storage(&path);

    // A projection + filter shape outside every fast path falls to the
    // generic executor route.
    let plan = explain_analyze_plan(
        &path,
        "EXPLAIN ANALYZE SELECT name, age FROM default WHERE score > 80 AND age < 40 ORDER BY age",
    );
    assert_eq!(actual_path(&plan), "generic_executor");
}

// ============================================================================
// R5.2: plan-driven physical access and plan/execution divergence
// ============================================================================

fn create_index_divergence_fixture(path: &Path) {
    // 1000 rows; `heavy` covers 50% of the rows and the remaining half is
    // spread over 7 tail values (city NDV = 8).
    const ROWS: usize = 1_000;
    let storage = OnDemandStorage::create(path).unwrap();
    let mut cities = Vec::with_capacity(ROWS);
    let mut scores = Vec::with_capacity(ROWS);
    for i in 0..ROWS {
        cities.push(if i % 2 == 0 {
            "heavy".to_string()
        } else {
            format!("t{}", i % 7)
        });
        scores.push((i % 97) as f64 * 0.5);
    }
    storage
        .insert_typed(
            HashMap::new(),
            HashMap::from([("score".to_string(), scores)]),
            HashMap::from([("city".to_string(), cities)]),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();

    ApexExecutor::execute("CREATE INDEX idx_city ON default(city)", path).unwrap();
    ApexExecutor::execute("ANALYZE default", path).unwrap();
}

#[test]
fn plan_divergence_first_record_wins_and_is_off_by_default() {
    crate::query::executor::begin_path_trace();
    crate::query::executor::record_plan_divergence("first");
    crate::query::executor::record_plan_divergence("second");
    assert_eq!(
        crate::query::executor::finish_plan_divergence(),
        Some("first")
    );
    crate::query::executor::finish_path_trace();

    // Tracing off by default: notes are dropped, no state is left behind.
    crate::query::executor::record_plan_divergence("off");
    assert_eq!(crate::query::executor::finish_plan_divergence(), None);

    // begin_path_trace resets the divergence slot.
    crate::query::executor::begin_path_trace();
    crate::query::executor::finish_path_trace();
    assert_eq!(crate::query::executor::finish_plan_divergence(), None);
}

#[test]
fn explain_analyze_reports_index_plan_divergence() {
    let dir = tempdir().unwrap();
    // The file stem must match the table name used in the statements.
    let path = dir.path().join("default.apex");
    create_index_divergence_fixture(&path);

    // The planner prices the skewed `heavy` value at 1/NDV (0.125) and
    // chooses the index candidate; at execution the MCV-based selectivity
    // (0.5) makes the full scan cheaper, so the index route is skipped and
    // the plan/execution divergence is reported.
    let plan = explain_analyze_plan(
        &path,
        "EXPLAIN ANALYZE SELECT * FROM default WHERE city = 'heavy'",
    );
    assert!(
        plan.contains("Chosen Plan: OltpIndexLookup"),
        "planner must choose the index candidate, got:\n{plan}"
    );
    assert!(
        plan.contains(
            "Plan Divergence: plan chose index access; index route unavailable at execution; fell back to scan"
        ),
        "divergence must be reported, got:\n{plan}"
    );
    assert_ne!(actual_path(&plan), "index_accelerated_read");

    // Control: a rare value keeps both cost models on the index route, so
    // the plan is followed and no divergence is reported.
    let plan = explain_analyze_plan(
        &path,
        "EXPLAIN ANALYZE SELECT * FROM default WHERE city = 't1'",
    );
    assert!(plan.contains("Chosen Plan: OltpIndexLookup"));
    assert_eq!(actual_path(&plan), "index_accelerated_read");
    assert!(!plan.contains("Plan Divergence"));
}

// ============================================================================
// R5.3: time-dimension cost calibration (EXPLAIN ANALYZE feedback loop)
// ============================================================================

use crate::query::planner::{
    ExecutionStrategy, PlannerContext, QueryPlan, is_index_cost_class, record_plan_feedback,
    ExecutedCostClass,
};
use crate::query::sql_parser::{SqlParser, SqlStatement};

fn planned_select(path: &Path, sql: &str) -> QueryPlan {
    let select = match SqlParser::parse(sql).unwrap() {
        SqlStatement::Select(select) => select,
        _ => panic!("expected SELECT"),
    };
    let (base_dir, table_name) = base_dir_and_table(path);
    let index_mgr = get_index_manager(&base_dir, &table_name);
    let index_guard = index_mgr.lock();
    crate::query::planner::QueryPlanner::plan_select_details(
        &select,
        Some(&*index_guard),
        &path.to_string_lossy(),
        PlannerContext::default(),
    )
}

#[test]
fn time_calibration_flips_index_to_scan() {
    let dir = tempdir().unwrap();
    // The file stem must match the table name used in the statements.
    let path = dir.path().join("default.apex");
    create_index_divergence_fixture(&path);
    let sql = "SELECT * FROM default WHERE city = 'heavy'";

    // The model prices the skewed value at 1/NDV and chooses the index.
    let first = planned_select(&path, sql);
    assert!(
        matches!(first.strategy, ExecutionStrategy::OltpIndexLookup { .. }),
        "model must choose the index candidate before feedback:\n{first:?}"
    );
    assert!(!first.feedback_applied);

    // Simulate a measured run where the index route actually executed but
    // turned out far slower than the model expected.
    let index_cost = first
        .candidates
        .iter()
        .find(|candidate| is_index_cost_class(&candidate.strategy))
        .expect("index candidate")
        .cost
        .total;
    let select = match SqlParser::parse(sql).unwrap() {
        SqlStatement::Select(select) => select,
        _ => panic!("expected SELECT"),
    };
    record_plan_feedback(
        &path.to_string_lossy(),
        &select,
        &first.strategy,
        first.cost.output_rows,
        500.0,
        ExecutedCostClass::Index,
        index_cost,
        1_000_000.0,
    );

    let second = planned_select(&path, sql);
    assert!(
        !is_index_cost_class(&second.strategy),
        "measured index time must flip the plan to the scan class:\n{second:?}"
    );
    assert!(second.feedback_applied);
}

fn create_index_time_calibration_fixture(path: &Path) {
    // 1000 rows split 50/50 over two city values (NDV = 2), indexed.  The
    // model then prices the index candidate (1/NDV selectivity) above the
    // plain scan, so the first plan chooses the scan.
    const ROWS: usize = 1_000;
    let storage = OnDemandStorage::create(path).unwrap();
    let mut cities = Vec::with_capacity(ROWS);
    let mut scores = Vec::with_capacity(ROWS);
    for i in 0..ROWS {
        cities.push(if i % 2 == 0 {
            "a".to_string()
        } else {
            "b".to_string()
        });
        scores.push((i % 97) as f64 * 0.5);
    }
    storage
        .insert_typed(
            HashMap::new(),
            HashMap::from([("score".to_string(), scores)]),
            HashMap::from([("city".to_string(), cities)]),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();

    ApexExecutor::execute("CREATE INDEX idx_city ON default(city)", path).unwrap();
    ApexExecutor::execute("ANALYZE default", path).unwrap();
}

#[test]
fn time_calibration_flips_scan_to_index() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("default.apex");
    create_index_time_calibration_fixture(&path);
    let sql = "SELECT * FROM default WHERE city = 'a'";

    // The model prices the low-NDV index candidate above the scan.
    let first = planned_select(&path, sql);
    assert!(
        !is_index_cost_class(&first.strategy),
        "model must choose the scan class before feedback:\n{first:?}"
    );
    assert!(!first.feedback_applied);

    // Simulate a measured scan that is far slower than the model expected,
    // making the (relatively cheap) index route worthwhile.
    let scan_cost = first
        .candidates
        .iter()
        .find(|candidate| !is_index_cost_class(&candidate.strategy))
        .expect("scan candidate")
        .cost
        .total;
    let select = match SqlParser::parse(sql).unwrap() {
        SqlStatement::Select(select) => select,
        _ => panic!("expected SELECT"),
    };
    record_plan_feedback(
        &path.to_string_lossy(),
        &select,
        &first.strategy,
        first.cost.output_rows,
        500.0,
        ExecutedCostClass::Scan,
        scan_cost,
        1_000_000.0,
    );

    let second = planned_select(&path, sql);
    assert!(
        is_index_cost_class(&second.strategy),
        "measured scan time must flip the plan to the index class:\n{second:?}"
    );
    assert!(second.feedback_applied);
}

#[test]
fn time_calibration_ignores_zero_cost_samples() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("default.apex");
    create_index_divergence_fixture(&path);
    let sql = "SELECT * FROM default WHERE city = 'heavy'";

    let first = planned_select(&path, sql);
    assert!(
        matches!(first.strategy, ExecutionStrategy::OltpIndexLookup { .. }),
        "model must choose the index candidate before feedback:\n{first:?}"
    );

    // Degraded samples (zero cost, zero time) must not rescale candidates
    // or corrupt their costs.
    let select = match SqlParser::parse(sql).unwrap() {
        SqlStatement::Select(select) => select,
        _ => panic!("expected SELECT"),
    };
    record_plan_feedback(
        &path.to_string_lossy(),
        &select,
        &first.strategy,
        first.cost.output_rows,
        first.cost.output_rows,
        ExecutedCostClass::Index,
        0.0,
        0.0,
    );

    let second = planned_select(&path, sql);
    assert!(second.feedback_applied);
    assert!(
        matches!(second.strategy, ExecutionStrategy::OltpIndexLookup { .. }),
        "zero samples must not flip the plan:\n{second:?}"
    );
    let index_cost = |plan: &QueryPlan| {
        plan.candidates
            .iter()
            .find(|candidate| is_index_cost_class(&candidate.strategy))
            .expect("index candidate")
            .cost
            .total
    };
    assert_eq!(index_cost(&second), index_cost(&first));
}


// ============================================================================
// R5.8: cross-session persistence of plan feedback
// ============================================================================

use crate::query::planner::{feedback_lookup_for_tests, feedback_reset_table_for_tests};

fn parsed_select(sql: &str) -> crate::query::SelectStatement {
    match SqlParser::parse(sql).unwrap() {
        SqlStatement::Select(select) => select,
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn plan_feedback_persists_to_sidecar_and_reloads() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("default.apex");
    let key = path.to_string_lossy().to_string();
    feedback_reset_table_for_tests(&key);
    let select = parsed_select("SELECT * FROM default WHERE city = 'heavy'");

    record_plan_feedback(
        &key,
        &select,
        &ExecutionStrategy::OlapAggregation,
        100.0,
        90.0,
        ExecutedCostClass::Scan,
        50.0,
        1234.0,
    );

    // The sidecar lives next to the table file, and the entry is in memory.
    let sidecar = dir.path().join("default.apex.plan_feedback");
    assert!(sidecar.exists());
    let in_memory = feedback_lookup_for_tests(&key, &select).unwrap();
    assert_eq!(in_memory.samples, 1);
    assert_eq!(in_memory.actual_rows, 90.0);
    assert_eq!(in_memory.scan_time_avg_us, 1234.0);
    assert_eq!(in_memory.scan_samples, 1);

    // Simulate a process exit and restart for this table only: the entry
    // comes back from the sidecar on the first lookup.
    feedback_reset_table_for_tests(&key);
    let reloaded = feedback_lookup_for_tests(&key, &select).unwrap();
    assert_eq!(reloaded.samples, 1);
    assert_eq!(reloaded.actual_rows, 90.0);
    assert_eq!(reloaded.scan_time_avg_us, 1234.0);
    assert!(matches!(reloaded.strategy, ExecutionStrategy::OlapAggregation));

    // A second record in the "new" process appends to the persisted state
    // and the file keeps the merged sliding averages.
    record_plan_feedback(
        &key,
        &select,
        &ExecutionStrategy::OlapAggregation,
        100.0,
        80.0,
        ExecutedCostClass::Scan,
        50.0,
        1300.0,
    );
    feedback_reset_table_for_tests(&key);
    let reloaded = feedback_lookup_for_tests(&key, &select).unwrap();
    assert_eq!(reloaded.samples, 2);
    assert!((reloaded.actual_rows - 85.0).abs() < 1e-9);
    assert!((reloaded.scan_time_avg_us - 1267.0).abs() < 1e-9);
}

#[test]
fn plan_feedback_ignores_unreadable_sidecar() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("default.apex");
    let key = path.to_string_lossy().to_string();
    feedback_reset_table_for_tests(&key);
    std::fs::write(
        dir.path().join("default.apex.plan_feedback"),
        b"not a feedback file",
    )
    .unwrap();
    let select = parsed_select("SELECT * FROM default WHERE city = 't1'");

    // A corrupt sidecar counts as "no persisted feedback"; a subsequent
    // record repairs the file with a valid one.
    assert!(feedback_lookup_for_tests(&key, &select).is_none());
    record_plan_feedback(
        &key,
        &select,
        &ExecutionStrategy::OlapFullScan,
        10.0,
        8.0,
        ExecutedCostClass::Scan,
        4.0,
        100.0,
    );
    feedback_reset_table_for_tests(&key);
    let reloaded = feedback_lookup_for_tests(&key, &select).unwrap();
    assert_eq!(reloaded.samples, 1);
    assert!(matches!(reloaded.strategy, ExecutionStrategy::OlapFullScan));
}


// ============================================================================
// R5.5: route labels for JOIN and CTE execution paths
// ============================================================================

fn join_cte_fixture(base: &Path) {
    // The default table path must exist for the executor entry point.
    exec_multi("CREATE TABLE default (id INT)", base).unwrap();
    exec_multi(
        "CREATE TABLE orders (id INT, user_id INT, amount INT)",
        base,
    )
    .unwrap();
    exec_multi("CREATE TABLE users (id INT, city TEXT)", base).unwrap();
    exec_multi(
        "INSERT INTO orders (id, user_id, amount) VALUES
         (1, 1, 10), (2, 2, 20), (3, 1, 30), (4, 3, 40), (5, 2, 50)",
        base,
    )
    .unwrap();
    exec_multi(
        "INSERT INTO users (id, city) VALUES
         (1, 'a'), (2, 'b'), (3, 'c'), (9, 'x')",
        base,
    )
    .unwrap();
}

#[test]
fn explain_analyze_reports_join_route_labels() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    let default_path = base.join("default.apex");
    join_cte_fixture(base);

    // General hash-join route (no fast path matches).
    let plan = explain_analyze_plan(
        &default_path,
        "EXPLAIN ANALYZE SELECT orders.amount, users.city
         FROM orders JOIN users ON orders.user_id = users.id",
    );
    assert_eq!(actual_path(&plan), "hash_join");

    // COUNT(*) over a plain inner join takes the count fast path.
    let plan = explain_analyze_plan(
        &default_path,
        "EXPLAIN ANALYZE SELECT COUNT(*)
         FROM orders JOIN users ON orders.user_id = users.id",
    );
    assert_eq!(actual_path(&plan), "join_count_fast_path");
}

#[test]
fn explain_analyze_reports_cte_route_labels() {
    let dir = tempdir().unwrap();
    let base = dir.path();
    let default_path = base.join("default.apex");
    join_cte_fixture(base);

    // Single-use CTE is inlined into the main query (no materialization).
    let plan = explain_analyze_plan(
        &default_path,
        "EXPLAIN ANALYZE WITH top AS
         (SELECT amount FROM orders WHERE amount > 25) SELECT * FROM top",
    );
    assert_eq!(actual_path(&plan), "cte_inline");

    // Multi-reference CTE is materialized into the shared batch cache.
    let plan = explain_analyze_plan(
        &default_path,
        "EXPLAIN ANALYZE WITH top AS
         (SELECT amount FROM orders WHERE amount > 25)
         SELECT (SELECT COUNT(*) FROM top) AS n,
                (SELECT MAX(amount) FROM top) AS m",
    );
    assert_eq!(actual_path(&plan), "cte_materialize");

    // Recursive CTE runs the iterative fixpoint loop.
    let plan = explain_analyze_plan(
        &default_path,
        "EXPLAIN ANALYZE WITH RECURSIVE fact(n) AS
         (SELECT 1 UNION ALL SELECT n + 1 FROM fact WHERE n < 5)
         SELECT n FROM fact",
    );
    assert_eq!(actual_path(&plan), "cte_recursive");
}

// ============================================================================
// R5.6: plan-carried index execution spec
// ============================================================================

use crate::data::Value;
use crate::storage::index::index_manager::PredicateHint;

fn index_spec_for(path: &Path, sql: &str) -> Option<crate::query::planner::IndexExecutionSpec> {
    planned_select(path, sql)
        .candidates
        .iter()
        .find(|candidate| candidate.execution.is_some())
        .and_then(|candidate| candidate.execution.clone())
}

fn create_index_spec_range_fixture(path: &Path) {
    // 1000 rows; `heavy` covers 50% of the rows and the remaining half is
    // spread over 7 tail values (city NDV = 8); `score` is indexed for the
    // range and mixed-disjunction shapes.
    const ROWS: usize = 1_000;
    let storage = OnDemandStorage::create(path).unwrap();
    let mut cities = Vec::with_capacity(ROWS);
    let mut scores = Vec::with_capacity(ROWS);
    for i in 0..ROWS {
        cities.push(if i % 2 == 0 {
            "heavy".to_string()
        } else {
            format!("t{}", i % 7)
        });
        scores.push((i % 97) as f64 * 0.5);
    }
    storage
        .insert_typed(
            HashMap::new(),
            HashMap::from([("score".to_string(), scores)]),
            HashMap::from([("city".to_string(), cities)]),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();

    ApexExecutor::execute("CREATE INDEX idx_city ON default(city)", path).unwrap();
    // BTree: the default HASH type cannot serve range lookups.
    ApexExecutor::execute(
        "CREATE INDEX idx_score ON default(score) USING BTREE",
        path,
    )
    .unwrap();
    ApexExecutor::execute("ANALYZE default", path).unwrap();
}

fn result_shape(result: &Option<ApexResult>) -> String {
    match result {
        None => "None".to_string(),
        Some(ApexResult::Data(batch)) => {
            format!("Data({}x{})", batch.num_rows(), batch.num_columns())
        }
        Some(ApexResult::Empty(schema)) => format!("Empty({} cols)", schema.fields().len()),
        Some(ApexResult::Scalar(value)) => format!("Scalar({value})"),
    }
}

fn assert_index_results_equal(
    with_spec: &Option<ApexResult>,
    without_spec: &Option<ApexResult>,
    sql: &str,
) {
    assert_eq!(
        result_shape(with_spec),
        result_shape(without_spec),
        "{sql}: spec-driven and legacy routes must agree"
    );
    match (with_spec.as_ref(), without_spec.as_ref()) {
        (Some(ApexResult::Data(a)), Some(ApexResult::Data(b))) => {
            assert_batches_logically_equal(a, b, sql)
        }
        _ => {}
    }
}

#[test]
fn index_execution_spec_materializes_at_planning() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("default.apex");
    create_index_divergence_fixture(&path);

    // Single equality: the spec carries the extracted predicate and no
    // disjunction.  There is no composite index, so the covering attempt is
    // allowed but the residual filter must stay.
    let spec = index_spec_for(&path, "SELECT * FROM default WHERE city = 'heavy'")
        .expect("equality candidate must carry the execution spec");
    assert!(
        spec.predicates.iter().any(|(column, hint)| {
            column == "city"
                && matches!(
                    hint,
                    PredicateHint::Eq(value)
                        if value == &Value::String("heavy".to_string())
                )
        }),
        "spec must carry the city predicate, got: {spec:?}"
    );
    assert!(spec.disjunction.is_none());
    assert!(!spec.skip_residual_filter);
    assert!(spec.try_covering_scan);
    assert!(spec.composite_columns.is_none());

    // OR shape: the union candidate carries the disjunction flag and no
    // extracted predicates (AND flattening does not descend into OR).
    let spec = index_spec_for(&path, "SELECT * FROM default WHERE city = 'heavy' OR city = 't1'")
        .expect("union candidate must carry the execution spec");
    assert!(spec.disjunction.is_some());
    assert!(spec.predicates.is_empty());
    assert!(!spec.try_covering_scan);
    assert!(!spec.skip_residual_filter);

    // Low-NDV indexed table: the chosen candidate is a scan and the plan
    // carries no execution spec.
    let dir = tempdir().unwrap();
    let path = dir.path().join("default.apex");
    create_index_time_calibration_fixture(&path);
    assert!(
        planned_select(&path, "SELECT * FROM default WHERE city = 'a'")
            .execution
            .is_none()
    );
}

#[test]
fn index_execution_spec_matches_legacy_execution() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("default.apex");
    create_index_spec_range_fixture(&path);
    let (base_dir, _) = base_dir_and_table(&path);
    let backend = get_cached_backend(&path).unwrap();

    let shapes = [
        // Rare equality: the index route executes under both worlds.
        "SELECT * FROM default WHERE city = 't1'",
        // Skewed equality: both CBOs fall back to the scan (Ok(None)).
        "SELECT * FROM default WHERE city = 'heavy'",
        // Range on the BTree index.
        "SELECT * FROM default WHERE score BETWEEN 10 AND 19",
        // Pure OR: the union candidate carries the disjunction flag; the
        // empty extracted predicate list makes both worlds fall back.
        "SELECT * FROM default WHERE city = 't1' OR city = 't2'",
        // Mixed disjunction: the spec carries both the extracted score
        // predicate and the disjunction flag (the executor resolves the OR
        // branch through the index union and intersects the range).
        "SELECT * FROM default WHERE (city = 't1' OR city = 't2') AND score > 40",
        // Covering projection: the index-only scan executes under both.
        "SELECT city FROM default WHERE city = 't5'",
    ];
    for sql in shapes {
        let select = match SqlParser::parse(sql).unwrap() {
            SqlStatement::Select(select) => select,
            _ => panic!("expected SELECT"),
        };
        let where_clause = select.where_clause.as_ref().unwrap();
        let plan = planned_select(&path, sql);
        let with_spec = ApexExecutor::try_index_accelerated_read(
            &backend,
            &select,
            where_clause,
            plan.execution.as_ref(),
            &base_dir,
            &path,
        )
        .unwrap();
        let without_spec = ApexExecutor::try_index_accelerated_read(
            &backend,
            &select,
            where_clause,
            None,
            &base_dir,
            &path,
        )
        .unwrap();
        assert_index_results_equal(&with_spec, &without_spec, sql);
    }

    // Every index-routed shape must carry the spec into the executor.
    for sql in [
        "SELECT * FROM default WHERE city = 't1'",
        "SELECT * FROM default WHERE score BETWEEN 10 AND 19",
        "SELECT * FROM default WHERE city = 't1' OR city = 't2'",
        "SELECT * FROM default WHERE (city = 't1' OR city = 't2') AND score > 40",
        "SELECT city FROM default WHERE city = 't5'",
    ] {
        assert!(
            planned_select(&path, sql).execution.is_some(),
            "{sql}: chosen index candidate must carry the spec"
        );
    }
}

#[test]
fn stale_index_execution_spec_falls_back_to_scan() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("default.apex");
    create_index_divergence_fixture(&path);
    let sql = "SELECT * FROM default WHERE city = 't1'";
    let select = match SqlParser::parse(sql).unwrap() {
        SqlStatement::Select(select) => select,
        _ => panic!("expected SELECT"),
    };
    let spec = index_spec_for(&path, sql).expect("index spec must exist");

    // Drop the index after planning: the spec is stale, so the index route
    // must fall back to the scan instead of trusting the planning-time state.
    ApexExecutor::execute("DROP INDEX idx_city ON default", &path).unwrap();
    let (base_dir, _) = base_dir_and_table(&path);
    let backend = get_cached_backend(&path).unwrap();
    let result = ApexExecutor::try_index_accelerated_read(
        &backend,
        &select,
        select.where_clause.as_ref().unwrap(),
        Some(&spec),
        &base_dir,
        &path,
    )
    .unwrap();
    assert!(
        result.is_none(),
        "stale spec must fall back to the scan, got: {:?}",
        result.is_some()
    );
}
