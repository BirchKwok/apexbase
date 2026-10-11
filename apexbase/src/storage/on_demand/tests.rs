use super::*;
use tempfile::tempdir;

fn insert_pending_delete(
    path: &std::path::Path,
    dev_id: u64,
    ino_id: u64,
    footer_offset: u64,
    footer: &[u8],
) {
    // Record the snapshot the deferred state describes, exactly as the delete
    // path does: file identity plus the length/mtime the footer belongs to.
    let (len, modified) = std::fs::metadata(path)
        .map(|meta| {
            (
                meta.len(),
                meta.modified()
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|since| since.as_nanos() as u64)
                    .unwrap_or(0),
            )
        })
        .unwrap_or((0, 0));
    let mut buf = Vec::with_capacity(36 + 12 + footer.len());
    buf.extend_from_slice(b"APXP");
    buf.extend_from_slice(&dev_id.to_le_bytes());
    buf.extend_from_slice(&ino_id.to_le_bytes());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&modified.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // zero row-group rewrites
    buf.extend_from_slice(&footer_offset.to_le_bytes());
    buf.extend_from_slice(&(footer.len() as u32).to_le_bytes());
    buf.extend_from_slice(footer);
    global_pending_deletes()
        .write()
        .unwrap()
        .as_mut()
        .map(|m| m.insert(path.to_path_buf(), buf));
}

#[cfg(unix)]
#[test]
fn pending_delete_identity_matches_current_file() {
    use std::os::unix::fs::MetadataExt;

    let dir = tempdir().unwrap();
    let table_path = dir.path().join("identity.apex");
    std::fs::write(&table_path, []).unwrap();
    let metadata = std::fs::metadata(&table_path).unwrap();

    assert!(pending_delete_matches_file(
        &table_path,
        metadata.dev(),
        metadata.ino()
    ));
    assert!(!pending_delete_matches_file(
        &table_path,
        metadata.dev(),
        metadata.ino().wrapping_add(1)
    ));
}

#[cfg(not(unix))]
#[test]
fn pending_delete_identity_is_rejected_without_unix_file_ids() {
    assert!(!pending_delete_matches_file(
        std::path::Path::new("unused.apex"),
        1,
        1
    ));
}

#[test]
fn apply_pending_deletes_discards_state_for_recreated_file() {
    let dir = tempdir().unwrap();
    let table_path = dir.path().join("recreated.apex");
    std::fs::write(&table_path, vec![0u8; 100]).unwrap();

    // Deferred state recorded for a different file incarnation (identity does
    // not match the current file); applying it would corrupt the fresh file.
    let footer = vec![0u8; 64];
    insert_pending_delete(&table_path, 0, 0, 4096, &footer);
    assert!(
        global_pending_deletes()
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .contains_key(&table_path)
    );

    apply_pending_deletes(&table_path).unwrap();

    assert!(
        !global_pending_deletes()
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .contains_key(&table_path)
    );
    // The fresh file must be untouched.
    assert_eq!(std::fs::metadata(&table_path).unwrap().len(), 100);
    assert_eq!(std::fs::read(&table_path).unwrap(), vec![0u8; 100]);
}

#[test]
fn apply_pending_deletes_discards_a_snapshot_overtaken_by_a_rewrite() {
    let dir = tempdir().unwrap();
    let table_path = dir.path().join("overtaken.apex");
    std::fs::write(&table_path, vec![0u8; 100]).unwrap();

    // Deferred state recorded for the current file, then the file changes size
    // (an append publishes a new footer at a new offset). Applying the recorded
    // footer would resurrect rows the snapshot still describes and orphan the
    // rows appended since, so the state must be dropped instead.
    let footer = vec![0u8; 64];
    insert_pending_delete(&table_path, 0, 0, 4096, &footer);
    std::fs::write(&table_path, vec![0u8; 120]).unwrap();

    apply_pending_deletes(&table_path).unwrap();

    assert!(
        !global_pending_deletes()
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .contains_key(&table_path),
        "the stale snapshot must be dropped"
    );
    let bytes = std::fs::read(&table_path).unwrap();
    assert_eq!(bytes.len(), 120, "the newer file must be left untouched");
    assert!(bytes.iter().all(|&byte| byte == 0));
}

#[test]
fn clear_pending_deletes_for_dir_removes_only_matching_entries() {
    let dir = tempdir().unwrap();
    let outside_dir = tempdir().unwrap();
    let inside = dir.path().join("inside.apex");
    let outside = outside_dir.path().join("outside.apex");
    insert_pending_delete(&inside, 1, 1, 100, &[0u8; 32]);
    insert_pending_delete(&outside, 1, 1, 100, &[0u8; 32]);

    clear_pending_deletes_for_dir(dir.path());

    let pending = global_pending_deletes().read().unwrap();
    let map = pending.as_ref().unwrap();
    assert!(!map.contains_key(&inside));
    assert!(map.contains_key(&outside));
}

#[test]
fn delta_row_count_cache_survives_unrelated_table_epoch() {
    use std::io::Write;

    let dir = tempdir().unwrap();
    let table_path = dir.path().join("epoch_cache.apex");
    let delta_path = OnDemandStorage::delta_path(&table_path);
    std::fs::write(&delta_path, [0u8; 8]).unwrap();

    let before_meta = std::fs::metadata(&delta_path).unwrap();
    let before_len = before_meta.len();
    let before_modified = before_meta.modified().unwrap();
    DELTA_ROW_COUNT_CACHE.write().insert(
        delta_path.clone(),
        (
            before_len,
            before_modified,
            5,
            crate::storage::epoch::current(&table_path),
        ),
    );

    crate::storage::epoch::bump(&table_path);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&delta_path)
        .unwrap()
        .write_all(&[1u8; 8])
        .unwrap();

    OnDemandStorage::refresh_delta_insert_caches(
        &table_path,
        &delta_path,
        Some((before_len, before_modified)),
        &[10, 11],
        &HashMap::new(),
    );

    let cache = DELTA_ROW_COUNT_CACHE.read();
    let entry = cache.get(&delta_path).unwrap();
    assert_eq!(entry.0, before_len + 8);
    assert_eq!(entry.2, 7);
    assert_eq!(entry.3, crate::storage::epoch::current(&table_path));
    drop(cache);
    DELTA_ROW_COUNT_CACHE.write().remove(&delta_path);
    DELTA_STRING_INDEX_CACHE.write().remove(&delta_path);
}

#[test]
fn delta_row_count_scan_stops_at_metadata_snapshot() {
    let dir = tempdir().unwrap();
    let table_path = dir.path().join("delta_snapshot.apex");
    let storage = OnDemandStorage::create(&table_path).unwrap();

    let mut first = HashMap::new();
    first.insert("value".to_string(), vec![1]);
    storage
        .insert_typed_to_delta(
            first,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();

    let delta_path = OnDemandStorage::delta_path(&table_path);
    let first_len = std::fs::metadata(&delta_path).unwrap().len();

    let mut second = HashMap::new();
    second.insert("value".to_string(), vec![2]);
    storage
        .insert_typed_to_delta(
            second,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();

    let mut file = File::open(&delta_path).unwrap();
    assert_eq!(
        OnDemandStorage::scan_delta_row_count(&mut file, first_len, 0),
        1
    );
}

#[test]
fn test_create_and_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.apex");

    // Create and insert
    {
        let storage = OnDemandStorage::create(&path).unwrap();

        let mut int_cols = HashMap::new();
        int_cols.insert("age".to_string(), vec![25, 30, 35, 40, 45]);

        let mut string_cols = HashMap::new();
        string_cols.insert(
            "name".to_string(),
            vec![
                "Alice".to_string(),
                "Bob".to_string(),
                "Charlie".to_string(),
                "David".to_string(),
                "Eve".to_string(),
            ],
        );

        let ids = storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                string_cols,
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();

        assert_eq!(ids.len(), 5);
        storage.save().unwrap();
    }

    // Reopen and verify
    {
        let storage = OnDemandStorage::open(&path).unwrap();
        assert_eq!(storage.row_count(), 5);
        assert_eq!(storage.column_names().len(), 3); // _id, age, name
    }
}

#[test]
fn cached_group_agg_skips_saved_deletion_vectors() {
    // Uncompressed tables aggregate through the streaming loop, compressed ones
    // through the fallback; both have to skip the saved deletion vectors.
    for compression in [None, Some(CompressionType::Zstd)] {
        let dir = tempdir().unwrap();
        let path = dir.path().join("agg_delete_vectors.apex");
        let n = 1000i64;
        let label = format!("{compression:?}");

        {
            let storage = OnDemandStorage::create(&path).unwrap();
            if let Some(compression) = compression {
                storage.set_compression(compression).unwrap();
            }
            storage
                .insert_typed(
                    HashMap::from([("user_id".to_string(), (1..=n).collect::<Vec<i64>>())]),
                    HashMap::new(),
                    HashMap::from([(
                        "city".to_string(),
                        (0..n)
                            .map(|i| {
                                ["Beijing", "Shanghai", "Shenzhen", "Guangzhou", "Hangzhou"]
                                    [i as usize % 5]
                                    .to_string()
                            })
                            .collect::<Vec<String>>(),
                    )]),
                    HashMap::new(),
                    HashMap::new(),
                )
                .unwrap();
            // Beijing's `user_id`s are 1, 6, 11, ... (every fifth row).
            for id in (1..=n as u64).filter(|id| (id - 1) % 5 == 0) {
                assert!(storage.delete(id), "{label}: row {id} must exist");
            }
            storage.save().unwrap();
        }

        // Read the aggregate back from disk: the vectors are the only record of the
        // deletion, so an aggregate that never reads a deletion bitmap -- a
        // `COUNT(*)`-only group-by has no columns to scan -- counts all 1000 rows.
        let reader = OnDemandStorage::open(&path).unwrap();
        let (labels, group_ids) = reader
            .build_string_dict_cache("city")
            .unwrap()
            .expect("dictionary");
        let aggregated = reader
            .execute_group_agg_cached(&labels, &group_ids, &[("*", true)])
            .unwrap()
            .expect("aggregate");
        let counts: HashMap<&str, i64> = aggregated
            .iter()
            .map(|(label, values)| (label.as_str(), values[0].1))
            .collect();
        assert_eq!(
            counts.get("Beijing").copied().unwrap_or(0),
            0,
            "{label}: {counts:?}"
        );
        for city in ["Shanghai", "Shenzhen", "Guangzhou", "Hangzhou"] {
            assert_eq!(counts.get(city).copied().unwrap_or(0), 200, "{label}: {counts:?}");
        }
    }
}

#[test]
fn streaming_compact_merges_delta_and_deltastore_bounded_memory() {
    use crate::data::Value;

    let dir = tempdir().unwrap();
    let path = dir.path().join("stream_compact.apex");

    // Base table: 5 rows, persisted as compressed V4 Row Groups.
    let base_ids;
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        storage.set_compression(CompressionType::Zstd).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("age".to_string(), vec![25i64, 30, 35, 40, 45]);
        let mut string_cols = HashMap::new();
        string_cols.insert(
            "name".to_string(),
            vec![
                "Alice".to_string(),
                "Bob".to_string(),
                "Charlie".to_string(),
                "David".to_string(),
                "Eve".to_string(),
            ],
        );
        base_ids = storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                string_cols,
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        assert_eq!(base_ids.len(), 5);
        storage.save().unwrap();
    }

    // Reopen mmap-only, append two delta rows, then update/delete via DeltaStore.
    let storage = OnDemandStorage::open(&path).unwrap();
    assert!(!storage.v4_base_loaded.load(Ordering::SeqCst));

    let delta_rows = vec![
        {
            let mut m = HashMap::new();
            m.insert("age".to_string(), Value::Int64(50));
            m.insert("name".to_string(), Value::String("Frank".to_string()));
            m
        },
        {
            let mut m = HashMap::new();
            m.insert("age".to_string(), Value::Int64(55));
            m.insert("name".to_string(), Value::String("Grace".to_string()));
            m
        },
    ];
    let delta_ids = storage.insert_value_rows_to_delta(&delta_rows).unwrap();
    assert_eq!(delta_ids.len(), 2);

    // Update one base row and one delta row; delete one base row.
    storage
        .delta_store
        .write()
        .update_cell(base_ids[0], "age", Value::Int64(99));
    storage
        .delta_store
        .write()
        .update_cell(delta_ids[0], "age", Value::Int64(51));
    storage.delta_store.write().delete_row(base_ids[2]);
    storage.save_delta_store().unwrap();

    storage.compact_streaming_v4().unwrap();

    // Delta artifacts are gone and the instance is back in mmap-only mode.
    assert!(!OnDemandStorage::delta_path(&path).exists());
    assert!(!std::path::Path::new(&format!("{}.deltastore", path.display())).exists());
    assert!(!storage.v4_base_loaded.load(Ordering::SeqCst));
    assert_eq!(storage.active_row_count(), 6);

    // Reopen and verify merged data: one deleted row dropped, updates baked in.
    drop(storage);
    let reopened = OnDemandStorage::open(&path).unwrap();
    assert_eq!(reopened.row_count(), 6);
    assert!(std::path::Path::new(&format!("{}.stats", path.display())).exists());
    let stats = reopened
        .execute_simple_agg(&["age", "name"])
        .unwrap()
        .unwrap();
    assert_eq!(stats[0], (6, 320.0, 30.0, 99.0, true));
    assert_eq!(stats[1].0, 6);
    let result = reopened
        .read_columns(Some(&["age", "name"]), 0, None)
        .unwrap();
    match &result["age"] {
        ColumnData::Int64(vals) => assert_eq!(vals, &[99, 30, 40, 45, 51, 55]),
        other => panic!("unexpected age column: {other:?}"),
    }
    match &result["name"] {
        ColumnData::String { offsets, data } => {
            let names: Vec<String> = (0..offsets.len() - 1)
                .map(|i| {
                    String::from_utf8_lossy(&data[offsets[i] as usize..offsets[i + 1] as usize])
                        .to_string()
                })
                .collect();
            assert_eq!(names, ["Alice", "Bob", "David", "Eve", "Frank", "Grace"]);
        }
        other => panic!("unexpected name column: {other:?}"),
    }
}

#[test]
fn append_chunks_large_batch_into_multiple_row_groups() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("chunked_append.apex");

    // Seed a single-row V4 base.
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("id".to_string(), vec![1i64]);
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    // Reopen mmap-only, then append a batch larger than one Row Group.
    let storage = OnDemandStorage::open(&path).unwrap();
    let mut int_cols = HashMap::new();
    int_cols.insert(
        "id".to_string(),
        (2..=DEFAULT_ROW_GROUP_SIZE as i64 + 1001).collect(),
    );
    let ids = storage
        .insert_typed(
            int_cols,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    assert_eq!(ids.len(), DEFAULT_ROW_GROUP_SIZE as usize + 1000);
    storage.save().unwrap();

    // The append must be split into multiple Row Groups, not one giant RG.
    let footer = storage.get_or_load_footer().unwrap().unwrap();
    assert!(
        footer.row_groups.len() >= 3,
        "expected at least 3 Row Groups after chunked append, got {}",
        footer.row_groups.len()
    );
    let max_rg = footer
        .row_groups
        .iter()
        .map(|rg| rg.row_count)
        .max()
        .unwrap_or(0);
    assert!(
        max_rg <= DEFAULT_ROW_GROUP_SIZE,
        "Row Group larger than target size: {max_rg}"
    );

    drop(storage);
    let reopened = OnDemandStorage::open(&path).unwrap();
    assert_eq!(reopened.row_count(), DEFAULT_ROW_GROUP_SIZE as u64 + 1001);
    let result = reopened.read_columns(Some(&["id"]), 0, None).unwrap();
    match &result["id"] {
        ColumnData::Int64(vals) => {
            assert_eq!(vals.len(), DEFAULT_ROW_GROUP_SIZE as usize + 1001);
            assert_eq!(vals[0], 1);
            assert_eq!(*vals.last().unwrap(), DEFAULT_ROW_GROUP_SIZE as i64 + 1001);
        }
        other => panic!("unexpected id column: {other:?}"),
    }
}

#[test]
fn persisted_string_null_detection_does_not_confuse_values_with_bitmap() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("string_null_detection.apex");
    let storage = OnDemandStorage::create(&path).unwrap();
    storage.set_compression(CompressionType::Zstd).unwrap();
    let mut strings = HashMap::new();
    strings.insert(
        "name".to_string(),
        (0..DEFAULT_ROW_GROUP_SIZE as usize + 1)
            .map(|index| format!("value_{}", index % 10))
            .collect(),
    );
    storage
        .insert_typed(
            HashMap::new(),
            HashMap::new(),
            strings,
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save_v4().unwrap();
    drop(storage);

    let reopened = OnDemandStorage::open(&path).unwrap();
    assert!(!reopened.column_has_nulls("name"));
}

#[test]
fn two_column_group_cache_rejects_unvalidated_out_of_range_ids() {
    let dir = tempdir().unwrap();
    let storage = OnDemandStorage::create(&dir.path().join("invalid_group_ids.apex")).unwrap();
    let dict1 = vec!["city".to_string()];
    let dict2 = vec!["category".to_string()];
    let result = storage
        .execute_group_agg_2col_cached(&dict1, &[1], &dict2, &[0], &[("*", true)], false)
        .unwrap();
    assert!(result.is_none());
}

#[test]
fn test_column_projection() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_proj.apex");

    // Create with multiple columns
    let storage = OnDemandStorage::create(&path).unwrap();

    let mut int_cols = HashMap::new();
    int_cols.insert("a".to_string(), vec![1, 2, 3, 4, 5]);
    int_cols.insert("b".to_string(), vec![10, 20, 30, 40, 50]);
    int_cols.insert("c".to_string(), vec![100, 200, 300, 400, 500]);

    storage
        .insert_typed(
            int_cols,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();

    // Reopen
    let storage = OnDemandStorage::open(&path).unwrap();

    // Read only column "b"
    let result = storage.read_columns(Some(&["b"]), 0, None).unwrap();
    assert_eq!(result.len(), 1);
    assert!(result.contains_key("b"));

    if let ColumnData::Int64(vals) = &result["b"] {
        assert_eq!(vals, &[10, 20, 30, 40, 50]);
    } else {
        panic!("Expected Int64 column");
    }
}

#[test]
fn test_row_range() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_range.apex");

    let storage = OnDemandStorage::create(&path).unwrap();

    let mut int_cols = HashMap::new();
    int_cols.insert("val".to_string(), (0..100).collect());

    storage
        .insert_typed(
            int_cols,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();

    let storage = OnDemandStorage::open(&path).unwrap();

    // Read rows 10-19 (10 rows starting at row 10)
    let result = storage.read_columns(Some(&["val"]), 10, Some(10)).unwrap();

    if let ColumnData::Int64(vals) = &result["val"] {
        assert_eq!(vals.len(), 10);
        assert_eq!(vals[0], 10);
        assert_eq!(vals[9], 19);
    } else {
        panic!("Expected Int64 column");
    }
}

#[test]
fn test_string_column() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_string.apex");

    let storage = OnDemandStorage::create(&path).unwrap();

    let mut string_cols = HashMap::new();
    string_cols.insert(
        "text".to_string(),
        vec![
            "hello".to_string(),
            "world".to_string(),
            "foo".to_string(),
            "bar".to_string(),
        ],
    );

    storage
        .insert_typed(
            HashMap::new(),
            HashMap::new(),
            string_cols,
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();

    let storage = OnDemandStorage::open(&path).unwrap();

    // Read middle 2 rows
    let result = storage.read_columns(Some(&["text"]), 1, Some(2)).unwrap();

    if let ColumnData::String { offsets, data } = &result["text"] {
        assert_eq!(offsets.len(), 3); // 2 strings + 1 trailing offset
        let s0 = std::str::from_utf8(&data[offsets[0] as usize..offsets[1] as usize]).unwrap();
        let s1 = std::str::from_utf8(&data[offsets[1] as usize..offsets[2] as usize]).unwrap();
        assert_eq!(s0, "world");
        assert_eq!(s1, "foo");
    } else {
        panic!("Expected String column");
    }
}

#[test]
fn test_insert_rows_compatibility() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_compat.apex");

    let storage = OnDemandStorage::create(&path).unwrap();

    // Use insert_rows API (ColumnarStorage compatible)
    let mut rows = Vec::new();
    for i in 0..10 {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int64(i));
        row.insert(
            "name".to_string(),
            ColumnValue::String(format!("user_{}", i)),
        );
        row.insert("score".to_string(), ColumnValue::Float64(i as f64 * 1.5));
        rows.push(row);
    }

    let ids = storage.insert_rows(&rows).unwrap();
    assert_eq!(ids.len(), 10);

    storage.save().unwrap();

    // Reopen and verify
    let storage = OnDemandStorage::open(&path).unwrap();
    assert_eq!(storage.row_count(), 10);

    let result = storage
        .read_columns(Some(&["id", "score"]), 0, None)
        .unwrap();
    assert_eq!(result.len(), 2);

    if let ColumnData::Int64(vals) = &result["id"] {
        assert_eq!(vals, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }
}

#[test]
fn test_bool_null_bitmap() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_bool_null.apex");

    let storage = OnDemandStorage::create(&path).unwrap();

    // Insert with NULL boolean
    let mut rows = Vec::new();

    let mut row1 = HashMap::new();
    row1.insert("id".to_string(), ColumnValue::Int64(1));
    row1.insert("flag".to_string(), ColumnValue::Bool(true));
    rows.push(row1);

    let mut row2 = HashMap::new();
    row2.insert("id".to_string(), ColumnValue::Int64(2));
    row2.insert("flag".to_string(), ColumnValue::Bool(false));
    rows.push(row2);

    let mut row3 = HashMap::new();
    row3.insert("id".to_string(), ColumnValue::Int64(3));
    row3.insert("flag".to_string(), ColumnValue::Null); // NULL boolean
    rows.push(row3);

    storage.insert_rows(&rows).unwrap();

    // Check null bitmap in memory BEFORE save
    {
        let nulls = storage.nulls.read();
        let schema = storage.schema.read();
        let flag_idx = schema.get_index("flag").unwrap();
        println!("Flag column index: {}", flag_idx);
        println!("Nulls len: {}", nulls.len());
        if flag_idx < nulls.len() {
            println!("Null bitmap for flag: {:?}", nulls[flag_idx]);
            // Row 3 (index 2) should be marked as NULL
            // Byte 0, bit 2 should be set
            assert!(
                !nulls[flag_idx].is_empty(),
                "Null bitmap should not be empty"
            );
            assert_eq!(
                nulls[flag_idx][0] & (1 << 2),
                1 << 2,
                "Row 2 should be marked as NULL"
            );
        }
    }

    storage.save().unwrap();

    // Reopen and verify null bitmap is persisted
    let storage2 = OnDemandStorage::open(&path).unwrap();

    // Check null mask via get_null_mask
    let null_mask = storage2.get_null_mask("flag", 0, 3);
    println!("Null mask after reopen: {:?}", null_mask);
    assert_eq!(null_mask, vec![false, false, true], "Row 2 should be NULL");
}

#[test]
fn test_append_delta_compatibility() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_delta.apex");

    let storage = OnDemandStorage::create(&path).unwrap();

    // First batch
    let mut rows = Vec::new();
    for i in 0..5 {
        let mut row = HashMap::new();
        row.insert("val".to_string(), ColumnValue::Int64(i));
        rows.push(row);
    }
    storage.append_delta(&rows).unwrap();

    // Second batch
    let mut rows2 = Vec::new();
    for i in 5..10 {
        let mut row = HashMap::new();
        row.insert("val".to_string(), ColumnValue::Int64(i));
        rows2.push(row);
    }
    storage.append_delta(&rows2).unwrap();

    // Verify
    let storage = OnDemandStorage::open(&path).unwrap();
    assert_eq!(storage.row_count(), 10);

    let result = storage.read_columns(Some(&["val"]), 0, None).unwrap();
    if let ColumnData::Int64(vals) = &result["val"] {
        assert_eq!(vals, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }
}

#[test]
fn test_v4_save_and_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_v4.apex");

    // Create, insert, save as V4
    {
        let storage = OnDemandStorage::create(&path).unwrap();

        let mut int_cols = HashMap::new();
        int_cols.insert("age".to_string(), vec![25, 30, 35, 40, 45]);

        let mut string_cols = HashMap::new();
        string_cols.insert(
            "name".to_string(),
            vec![
                "Alice".to_string(),
                "Bob".to_string(),
                "Charlie".to_string(),
                "David".to_string(),
                "Eve".to_string(),
            ],
        );

        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                string_cols,
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();

        // Save as V4 Row Group format
        storage.save_v4().unwrap();

        // Verify header has V4 version
        let header = storage.header.read();
        assert_eq!(header.version, FORMAT_VERSION_V4);
        assert!(header.footer_offset > 0);
        assert!(header.row_group_count >= 1);
    }

    // Reopen V4 file and load data
    {
        let storage = OnDemandStorage::open(&path).unwrap();
        // Header should indicate V4
        let header = storage.header.read();
        assert_eq!(header.version, FORMAT_VERSION_V4);
        assert_eq!(header.row_count, 5);
        drop(header);

        // Load V4 data
        storage.open_v4_data().unwrap();
        assert_eq!(storage.ids.read().len(), 5);

        let columns = storage.columns.read();
        // User columns only (age, name) — _id stored separately in self.ids
        assert_eq!(columns.len(), 2);

        // Verify age column
        let schema = storage.schema.read();
        let age_idx = schema.get_index("age").unwrap();
        if let ColumnData::Int64(vals) = &columns[age_idx] {
            assert_eq!(vals, &[25, 30, 35, 40, 45]);
        } else {
            panic!("Expected Int64 for age column");
        }

        // Verify name column
        let name_idx = schema.get_index("name").unwrap();
        if let ColumnData::String { offsets, data } = &columns[name_idx] {
            assert_eq!(offsets.len(), 6); // 5 rows + 1
            let names: Vec<&str> = (0..5)
                .map(|i| {
                    let s = offsets[i] as usize;
                    let e = offsets[i + 1] as usize;
                    std::str::from_utf8(&data[s..e]).unwrap()
                })
                .collect();
            assert_eq!(names, vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
        } else {
            panic!("Expected String for name column");
        }
    }
}

/// Loading the base must not lower the ID high-water mark below the delta's.
///
/// The open path folds the delta sidecar's maximum ID into `next_id`, and the
/// base loader used to store its base-only value on top: after `delete` plus a
/// delta append, the next SQL `INSERT` was handed the delta row's ID and
/// overwrote it instead of appending.
#[test]
fn open_v4_data_keeps_the_delta_id_high_water_mark() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("delta_high_water.apex");
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("n".to_string(), vec![0i64, 1, 2, 3]);
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    // Append a row through the delta sidecar (the single-row store route).
    let storage = OnDemandStorage::open(&path).unwrap();
    let mut row = HashMap::new();
    row.insert("n".to_string(), crate::data::Value::Int64(4));
    let ids = storage.insert_value_rows_to_delta(&[row]).unwrap();
    assert_eq!(ids, vec![5]);
    storage.save().unwrap();
    drop(storage);

    // A full load (what a write opens with) must keep next_id above the delta row.
    let storage = OnDemandStorage::open(&path).unwrap();
    assert_eq!(storage.read_ids(0, None).unwrap(), vec![1, 2, 3, 4, 5]);
    storage.open_v4_data().unwrap();
    assert_eq!(
        storage.next_id_value(),
        6,
        "the base loader must not lower the delta-aware ID high-water mark"
    );
    let assigned = storage
        .insert_value_rows_to_delta(&[{
            let mut row = HashMap::new();
            row.insert("n".to_string(), crate::data::Value::Int64(99));
            row
        }])
        .unwrap();
    assert_eq!(assigned, vec![6], "the appended row must get a fresh ID");
}

/// Compaction must publish the delta rows it merged, not just the deletions.
///
/// `merge_delta_file` inserted the delta rows without recording them as pending,
/// so the following `save()` saw "no new rows" and took the deletion-vector-only
/// path — and `compact()` then removed the delta file, losing those rows.
#[test]
fn compact_after_delete_keeps_the_appended_delta_row() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("compact_keeps_delta.apex");
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("n".to_string(), vec![0i64, 1, 2, 3]);
        let mut string_cols = HashMap::new();
        string_cols.insert(
            "k".to_string(),
            vec!["k0".to_string(), "k1".to_string(), "k2".to_string(), "k3".to_string()],
        );
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                string_cols,
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    let mut row = HashMap::new();
    row.insert("k".to_string(), crate::data::Value::String("k4".to_string()));
    row.insert("n".to_string(), crate::data::Value::Int64(4));
    assert_eq!(storage.insert_value_rows_to_delta(&[row]).unwrap(), vec![5]);
    storage.save().unwrap();
    drop(storage);

    // Delete row 4 through the DeltaStore overlay, exactly as the API delete does.
    let storage = OnDemandStorage::open(&path).unwrap();
    assert!(storage.delta_delete_row(4).unwrap());
    storage.save_delta_store().unwrap();
    drop(storage);

    // A write opens the table and compacts the delta before writing.
    let storage = OnDemandStorage::open(&path).unwrap();
    assert_eq!(storage.next_id_value(), 6);
    storage.compact().unwrap();
    drop(storage);

    let storage = OnDemandStorage::open(&path).unwrap();
    assert_eq!(
        storage.read_ids(0, None).unwrap(),
        vec![1, 2, 3, 5],
        "row 4 is deleted and the delta row 5 must survive the compaction"
    );
    assert_eq!(
        storage.next_id_value(),
        6,
        "the surviving delta row keeps its ID reserved"
    );
    let cols = storage.read_columns(Some(&["k"]), 0, None).unwrap();
    match &cols["k"] {
        ColumnData::String { offsets, data } => {
            let values: Vec<&str> = (0..4)
                .map(|i| {
                    std::str::from_utf8(&data[offsets[i] as usize..offsets[i + 1] as usize])
                        .unwrap()
                })
                .collect();
            assert_eq!(values, vec!["k0", "k1", "k2", "k4"]);
        }
        other => panic!("unexpected k column: {other:?}"),
    }
}

#[test]
fn test_v4_append_row_group() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_v4_append.apex");

    // Create and save V4 with initial data
    let storage = OnDemandStorage::create(&path).unwrap();
    let mut int_cols = HashMap::new();
    int_cols.insert("val".to_string(), vec![1, 2, 3]);
    storage
        .insert_typed(
            int_cols,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save_v4().unwrap();

    // Verify initial state
    assert_eq!(storage.header.read().row_count, 3);
    assert_eq!(storage.header.read().row_group_count, 1);

    // Append a new Row Group without rewriting
    let new_ids = vec![100, 101, 102];
    let schema = storage.schema.read();
    let col_count = schema.column_count();
    let mut new_columns: Vec<ColumnData> = Vec::new();
    for (name, col_type) in &schema.columns {
        if name == "_id" {
            // _id is handled via new_ids, not as a column
            new_columns.push(ColumnData::Int64(vec![100, 101, 102]));
        } else {
            match col_type {
                ColumnType::Int64 => new_columns.push(ColumnData::Int64(vec![10, 20, 30])),
                _ => new_columns.push(ColumnData::new(*col_type)),
            }
        }
    }
    drop(schema);
    let new_nulls: Vec<Vec<u8>> = vec![Vec::new(); col_count];

    // A reused insert backend may still hold tombstones for older row groups.
    // Brand-new append inputs must never inherit those deletion bits.
    *storage.deleted.write() = vec![0b0000_0111];
    storage
        .append_row_group(&new_ids, &new_columns, &new_nulls)
        .unwrap();

    // Appends invalidate the old sidecar instead of synchronously rebuilding
    // it on the write hot path. The first scalar scan recreates current stats.
    let stats_path = PathBuf::from(format!("{}.stats", path.display()));
    assert!(!stats_path.exists());
    let stats_storage = OnDemandStorage::open(&path).unwrap();
    let stats = stats_storage
        .execute_simple_agg(&["val"])
        .unwrap()
        .unwrap();
    assert_eq!(stats, vec![(6, 66.0, 1.0, 30.0, true)]);
    assert!(stats_path.exists());
    drop(stats_storage);

    // Verify updated header
    let header = storage.header.read();
    assert_eq!(header.row_count, 6); // 3 + 3
    assert_eq!(header.row_group_count, 2);
    drop(header);

    // Loading only the ID index must not make a metadata-only save append the
    // already-persisted base a second time.
    let storage2 = OnDemandStorage::open(&path).unwrap();
    storage2.ensure_ids_loaded_v4().unwrap();
    assert_eq!(storage2.ids.read().len(), 6);
    storage2.save().unwrap();
    drop(storage2);

    // Reopen and load all data to verify
    let storage2 = OnDemandStorage::open(&path).unwrap();
    storage2.open_v4_data().unwrap();
    assert_eq!(storage2.ids.read().len(), 6);
}

#[test]
fn test_v4_multiple_row_groups() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_v4_multi_rg.apex");

    // Create a table with more rows than DEFAULT_ROW_GROUP_SIZE
    // Use a small RG to test splitting (we'll insert 10 rows, RG size is 65536)
    let storage = OnDemandStorage::create(&path).unwrap();

    let mut int_cols = HashMap::new();
    int_cols.insert("val".to_string(), (0..100).collect::<Vec<i64>>());
    storage
        .insert_typed(
            int_cols,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save_v4().unwrap();

    // With 100 rows and RG size 65536, should be 1 RG
    assert_eq!(storage.header.read().row_group_count, 1);

    // Reopen and verify data integrity
    let storage2 = OnDemandStorage::open(&path).unwrap();
    storage2.open_v4_data().unwrap();

    let columns = storage2.columns.read();
    let schema = storage2.schema.read();
    let val_idx = schema.get_index("val").unwrap();
    if let ColumnData::Int64(vals) = &columns[val_idx] {
        assert_eq!(vals.len(), 100);
        assert_eq!(vals[0], 0);
        assert_eq!(vals[99], 99);
    } else {
        panic!("Expected Int64 for val column");
    }
}

#[test]
fn test_v4_read_columns_integration() {
    // Tests the executor read path: open V4 file → read_columns() auto-loads data
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_v4_read.apex");

    // Create, insert, save as V4
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("age".to_string(), vec![10, 20, 30]);
        let mut string_cols = HashMap::new();
        string_cols.insert(
            "name".to_string(),
            vec![
                "Alice".to_string(),
                "Bob".to_string(),
                "Charlie".to_string(),
            ],
        );
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                string_cols,
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save_v4().unwrap();
    }

    // Reopen and use read_columns() (executor path) — no explicit open_v4_data()
    let storage = OnDemandStorage::open(&path).unwrap();
    let result = storage
        .read_columns(Some(&["age", "name"]), 0, None)
        .unwrap();

    // Verify age
    if let ColumnData::Int64(vals) = &result["age"] {
        assert_eq!(vals, &[10, 20, 30]);
    } else {
        panic!("Expected Int64 for age");
    }

    // Verify name
    if let ColumnData::String { offsets, data } = &result["name"] {
        let names: Vec<&str> = (0..3)
            .map(|i| {
                let s = offsets[i] as usize;
                let e = offsets[i + 1] as usize;
                std::str::from_utf8(&data[s..e]).unwrap()
            })
            .collect();
        assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);
    } else {
        panic!("Expected String for name");
    }

    // Test partial read (start_row, row_count)
    let partial = storage.read_columns(Some(&["age"]), 1, Some(2)).unwrap();
    if let ColumnData::Int64(vals) = &partial["age"] {
        assert_eq!(vals, &[20, 30]);
    } else {
        panic!("Expected Int64 for partial age read");
    }
}

// ====== LZ4 Compression Tests ======

#[test]
fn test_lz4_compression_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("lz4_test.apex");

    // Insert enough data to trigger LZ4 (body > 512 bytes)
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        let ids: Vec<i64> = (0..200).collect();
        int_cols.insert("id".to_string(), ids);
        let mut str_cols = HashMap::new();
        let names: Vec<String> = (0..200).map(|i| format!("name_{:04}", i)).collect();
        str_cols.insert("name".to_string(), names);
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                str_cols,
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save_v4().unwrap();
    }

    // Reopen and verify data survives LZ4 compression
    {
        let storage = OnDemandStorage::open(&path).unwrap();
        storage.open_v4_data().unwrap();
        let data = storage
            .read_columns(Some(&["id", "name"]), 0, None)
            .unwrap();
        if let ColumnData::Int64(vals) = &data["id"] {
            assert_eq!(vals.len(), 200);
            assert_eq!(vals[0], 0);
            assert_eq!(vals[199], 199);
        } else {
            panic!("Expected Int64 for id");
        }
        if let ColumnData::String {
            offsets,
            data: sdata,
        } = &data["name"]
        {
            let first =
                std::str::from_utf8(&sdata[offsets[0] as usize..offsets[1] as usize]).unwrap();
            assert_eq!(first, "name_0000");
            let last_start = offsets[199] as usize;
            let last_end = offsets[200] as usize;
            let last = std::str::from_utf8(&sdata[last_start..last_end]).unwrap();
            assert_eq!(last, "name_0199");
        } else {
            panic!("Expected String for name");
        }
    }
}

#[test]
fn test_lz4_small_data_no_compression() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("small.apex");

    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("a".to_string(), vec![1i64, 2, 3]);
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save_v4().unwrap();
    }
    {
        let storage = OnDemandStorage::open(&path).unwrap();
        storage.open_v4_data().unwrap();
        let data = storage.read_columns(Some(&["a"]), 0, None).unwrap();
        if let ColumnData::Int64(vals) = &data["a"] {
            assert_eq!(vals, &[1, 2, 3]);
        } else {
            panic!("Expected Int64");
        }
    }
}

#[test]
fn test_lz4_mixed_types() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("mixed.apex");

    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let n = 200;
        let mut int_cols = HashMap::new();
        int_cols.insert("i".to_string(), (0..n).map(|x| x as i64).collect());
        let mut float_cols = HashMap::new();
        float_cols.insert("f".to_string(), (0..n).map(|x| x as f64 * 0.5).collect());
        let mut str_cols = HashMap::new();
        str_cols.insert("s".to_string(), (0..n).map(|x| format!("v{}", x)).collect());
        let mut bool_cols = HashMap::new();
        bool_cols.insert("b".to_string(), (0..n).map(|x| x % 2 == 0).collect());
        storage
            .insert_typed(int_cols, float_cols, str_cols, HashMap::new(), bool_cols)
            .unwrap();
        storage.save_v4().unwrap();
    }
    {
        let storage = OnDemandStorage::open(&path).unwrap();
        storage.open_v4_data().unwrap();
        let data = storage.read_columns(None, 0, None).unwrap();
        if let ColumnData::Int64(vals) = &data["i"] {
            assert_eq!(vals.len(), 200);
            assert_eq!(vals[100], 100);
        } else {
            panic!("Expected Int64 for i");
        }
        if let ColumnData::Float64(vals) = &data["f"] {
            assert!((vals[100] - 50.0).abs() < 0.01);
        } else {
            panic!("Expected Float64 for f");
        }
    }
}

// ====== Constraint Serialization Tests ======

#[test]
fn test_check_constraint_serialization() {
    let mut schema = OnDemandSchema::new();
    schema.add_column_with_constraints(
        "age",
        ColumnType::Int64,
        ColumnConstraints {
            not_null: false,
            primary_key: false,
            unique: false,
            default_value: None,
            check_expr_sql: Some("age > 0".to_string()),
            foreign_key: None,
            autoincrement: false,
        },
    );

    let bytes = schema.to_bytes();
    let restored = OnDemandSchema::from_bytes(&bytes).unwrap();
    assert_eq!(restored.columns.len(), 1);
    assert_eq!(
        restored.constraints[0].check_expr_sql,
        Some("age > 0".to_string())
    );
}

#[test]
fn test_fk_constraint_serialization() {
    let mut schema = OnDemandSchema::new();
    schema.add_column_with_constraints(
        "dept_id",
        ColumnType::Int64,
        ColumnConstraints {
            not_null: false,
            primary_key: false,
            unique: false,
            default_value: None,
            check_expr_sql: None,
            foreign_key: Some(("departments".to_string(), "id".to_string())),
            autoincrement: false,
        },
    );

    let bytes = schema.to_bytes();
    let restored = OnDemandSchema::from_bytes(&bytes).unwrap();
    assert_eq!(
        restored.constraints[0].foreign_key,
        Some(("departments".to_string(), "id".to_string()))
    );
}

#[test]
fn test_all_constraints_serialization() {
    let mut schema = OnDemandSchema::new();
    schema.add_column_with_constraints(
        "id",
        ColumnType::Int64,
        ColumnConstraints {
            not_null: true,
            primary_key: true,
            unique: false,
            default_value: None,
            check_expr_sql: None,
            foreign_key: None,
            autoincrement: false,
        },
    );
    schema.add_column_with_constraints(
        "val",
        ColumnType::Int64,
        ColumnConstraints {
            not_null: true,
            primary_key: false,
            unique: true,
            default_value: Some(DefaultValue::Int64(0)),
            check_expr_sql: Some("val >= 0".to_string()),
            foreign_key: Some(("other".to_string(), "val".to_string())),
            autoincrement: false,
        },
    );

    let bytes = schema.to_bytes();
    let restored = OnDemandSchema::from_bytes(&bytes).unwrap();

    // Column 0: id
    assert!(restored.constraints[0].primary_key);
    assert!(restored.constraints[0].not_null);
    assert!(restored.constraints[0].check_expr_sql.is_none());
    assert!(restored.constraints[0].foreign_key.is_none());

    // Column 1: val
    assert!(restored.constraints[1].not_null);
    assert!(restored.constraints[1].unique);
    assert_eq!(
        restored.constraints[1].default_value,
        Some(DefaultValue::Int64(0))
    );
    assert_eq!(
        restored.constraints[1].check_expr_sql,
        Some("val >= 0".to_string())
    );
    assert_eq!(
        restored.constraints[1].foreign_key,
        Some(("other".to_string(), "val".to_string()))
    );
}

#[test]
fn test_constraint_persisted_through_save_v4() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("cons.apex");

    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("val".to_string(), vec![1i64, 2, 3]);
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.set_column_constraints(
            "val",
            ColumnConstraints {
                not_null: true,
                primary_key: false,
                unique: false,
                default_value: None,
                check_expr_sql: Some("val > 0".to_string()),
                foreign_key: Some(("parent".to_string(), "id".to_string())),
                autoincrement: false,
            },
        );
        storage.save_v4().unwrap();
    }
    {
        let storage = OnDemandStorage::open(&path).unwrap();
        let cons = storage.get_column_constraints("val");
        assert!(cons.not_null);
        assert_eq!(cons.check_expr_sql, Some("val > 0".to_string()));
        assert_eq!(
            cons.foreign_key,
            Some(("parent".to_string(), "id".to_string()))
        );
    }
}

#[test]
fn test_blob_storage_modes_roundtrip() {
    use arrow::array::{Array, LargeBinaryArray};

    let dir = tempdir().unwrap();
    let path = dir.path().join("blob_modes.apex");
    let small = b"small-blob".to_vec();
    let packed = vec![7u8; 70 * 1024];
    let dedicated = vec![9u8; 4 * 1024 * 1024 + 123];
    let empty = Vec::new();

    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            super::super::DurabilityLevel::Fast,
            &[("payload".to_string(), ColumnType::Blob)],
        )
        .unwrap();

        let rows = vec![
            HashMap::from([("payload".to_string(), ColumnValue::Blob(small.clone()))]),
            HashMap::from([("payload".to_string(), ColumnValue::Blob(packed.clone()))]),
            HashMap::from([("payload".to_string(), ColumnValue::Blob(dedicated.clone()))]),
            HashMap::from([("payload".to_string(), ColumnValue::Null)]),
            HashMap::from([("payload".to_string(), ColumnValue::Blob(empty.clone()))]),
        ];
        storage.insert_rows(&rows).unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    let descriptors = storage.read_columns(Some(&["payload"]), 0, None).unwrap();
    let ColumnData::Binary { offsets, data } = &descriptors["payload"] else {
        panic!("expected blob descriptor column");
    };
    assert_eq!(
        OnDemandStorage::blob_descriptor_mode(&data[offsets[0] as usize..offsets[1] as usize]),
        Some(BlobStorageMode::Inline)
    );
    assert_eq!(
        OnDemandStorage::blob_descriptor_mode(&data[offsets[1] as usize..offsets[2] as usize]),
        Some(BlobStorageMode::Packed)
    );
    assert_eq!(
        OnDemandStorage::blob_descriptor_mode(&data[offsets[2] as usize..offsets[3] as usize]),
        Some(BlobStorageMode::Dedicated)
    );
    assert_eq!(
        OnDemandStorage::blob_descriptor_mode(&data[offsets[3] as usize..offsets[4] as usize]),
        None
    );
    assert_eq!(
        OnDemandStorage::blob_descriptor_mode(&data[offsets[4] as usize..offsets[5] as usize]),
        Some(BlobStorageMode::Inline)
    );
    assert!(
        OnDemandStorage::blob_descriptor_info(&data[offsets[0] as usize..offsets[1] as usize])
            .is_some()
    );

    let batch = storage.to_arrow_batch(Some(&["payload"]), false).unwrap();
    let array = batch
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(array.value(0), small.as_slice());
    assert_eq!(array.value(1), packed.as_slice());
    assert_eq!(array.value(2), dedicated.as_slice());
    assert!(array.is_null(3));
    assert_eq!(array.value(4), empty.as_slice());

    assert_eq!(
        storage.read_blob_by_id("payload", 1).unwrap().as_deref(),
        Some(small.as_slice())
    );
    assert_eq!(
        storage.read_blob_by_id("payload", 2).unwrap().as_deref(),
        Some(packed.as_slice())
    );
    assert_eq!(
        storage.read_blob_by_id("payload", 3).unwrap().as_deref(),
        Some(dedicated.as_slice())
    );
    assert!(storage.read_blob_by_id("payload", 4).unwrap().is_none());
    assert_eq!(
        storage.read_blob_by_id("payload", 5).unwrap().as_deref(),
        Some(empty.as_slice())
    );
    let batch_blobs = storage
        .read_blobs_by_ids("payload", &[1, 2, 3, 4, 5, 99])
        .unwrap();
    assert_eq!(batch_blobs[0].as_deref(), Some(small.as_slice()));
    assert_eq!(batch_blobs[1].as_deref(), Some(packed.as_slice()));
    assert_eq!(batch_blobs[2].as_deref(), Some(dedicated.as_slice()));
    assert!(batch_blobs[3].is_none());
    assert_eq!(batch_blobs[4].as_deref(), Some(empty.as_slice()));
    assert!(batch_blobs[5].is_none());

    let packed_range = storage
        .read_blob_range_by_id("payload", 2, 1024, Some(4096))
        .unwrap()
        .unwrap();
    assert_eq!(packed_range, packed[1024..1024 + 4096]);
    let dedicated_range = storage
        .read_blob_range_by_id("payload", 3, 4 * 1024 * 1024, Some(64))
        .unwrap()
        .unwrap();
    assert_eq!(
        dedicated_range,
        dedicated[4 * 1024 * 1024..4 * 1024 * 1024 + 64]
    );
    assert_eq!(
        storage
            .read_blob_range_by_id("payload", 3, dedicated.len() as u64 + 1, Some(64))
            .unwrap()
            .unwrap(),
        Vec::<u8>::new()
    );
    assert!(storage
        .read_blob_range_by_id("payload", 4, 0, Some(8))
        .unwrap()
        .is_none());
    assert_eq!(
        storage
            .read_blob_range_by_id("payload", 5, 0, Some(8))
            .unwrap()
            .unwrap(),
        Vec::<u8>::new()
    );
    let batch_ranges = storage
        .read_blob_ranges_by_ids(
            "payload",
            &[1, 2, 3, 4, 5],
            &[2, 1024, 4 * 1024 * 1024, 0, 0],
            Some(8),
        )
        .unwrap();
    assert_eq!(batch_ranges[0].as_deref(), Some(&small[2..small.len()]));
    assert_eq!(batch_ranges[1].as_deref(), Some(&packed[1024..1024 + 8]));
    assert_eq!(
        batch_ranges[2].as_deref(),
        Some(&dedicated[4 * 1024 * 1024..4 * 1024 * 1024 + 8])
    );
    assert!(batch_ranges[3].is_none());
    assert_eq!(batch_ranges[4].as_deref(), Some(empty.as_slice()));

    let small_info = storage
        .read_blob_descriptor_info_by_id("payload", 1)
        .unwrap()
        .unwrap();
    assert_eq!(small_info.mode, BlobStorageMode::Inline);
    assert_eq!(small_info.len, small.len() as u64);
    let packed_info = storage
        .read_blob_descriptor_info_by_id("payload", 2)
        .unwrap()
        .unwrap();
    assert_eq!(packed_info.mode, BlobStorageMode::Packed);
    assert_eq!(packed_info.len, packed.len() as u64);
    let dedicated_info = storage
        .read_blob_descriptor_info_by_id("payload", 3)
        .unwrap()
        .unwrap();
    assert_eq!(dedicated_info.mode, BlobStorageMode::Dedicated);
    assert_eq!(dedicated_info.len, dedicated.len() as u64);
    let batch_infos = storage
        .read_blob_descriptor_infos_by_ids("payload", &[1, 2, 3, 4, 5])
        .unwrap();
    assert_eq!(
        batch_infos[0].as_ref().unwrap().mode,
        BlobStorageMode::Inline
    );
    assert_eq!(
        batch_infos[1].as_ref().unwrap().mode,
        BlobStorageMode::Packed
    );
    assert_eq!(
        batch_infos[2].as_ref().unwrap().mode,
        BlobStorageMode::Dedicated
    );
    assert!(batch_infos[3].is_none());
    assert_eq!(
        batch_infos[4].as_ref().unwrap().mode,
        BlobStorageMode::Inline
    );
    assert_eq!(batch_infos[4].as_ref().unwrap().len, 0);
    assert!(storage.read_blob_by_id("payload", 99).unwrap().is_none());
    assert!(storage.read_blob_by_id("missing", 1).unwrap().is_none());

    assert!(storage.blob_sidecar_dir().join("packed.blob").exists());
    assert!(storage.blob_sidecar_dir().join("objects").exists());
}

#[test]
fn large_arrow_offset_boundary_uses_64_bit_offsets() {
    assert!(!super::requires_large_arrow_offsets(i32::MAX as usize));
    assert!(super::requires_large_arrow_offsets(i32::MAX as usize + 1));
}

#[test]
fn footer_only_add_column_does_not_load_base_and_synthesizes_nulls() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("footer_add_col.apex");

    // Seed a V4 base with two columns.
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("id".to_string(), vec![1i64, 2, 3]);
        let mut string_cols = HashMap::new();
        string_cols.insert(
            "name".to_string(),
            vec![
                "Alice".to_string(),
                "Bob".to_string(),
                "Carol".to_string(),
            ],
        );
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                string_cols,
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    // Reopen mmap-only: add_column must not load the base into memory.
    let storage = OnDemandStorage::open(&path).unwrap();
    storage
        .add_column_with_padding("extra", crate::data::DataType::Int64)
        .unwrap();
    assert!(!storage.v4_base_loaded.load(Ordering::SeqCst));
    storage.update_v4_footer_schema().unwrap();

    // Existing rows read back with the new column all-NULL.
    let result = storage
        .read_columns(Some(&["id", "extra"]), 0, None)
        .unwrap();
    assert_eq!(result["id"].len(), 3);
    assert_eq!(result["extra"].len(), 3);
    assert!(storage.column_has_nulls("extra"));

    // Append new rows that include the new column: old rows stay NULL,
    // new rows carry real values.
    let mut int_cols = HashMap::new();
    int_cols.insert("id".to_string(), vec![4i64, 5]);
    int_cols.insert("extra".to_string(), vec![40i64, 50]);
    let mut string_cols = HashMap::new();
    string_cols.insert(
        "name".to_string(),
        vec!["Dave".to_string(), "Erin".to_string()],
    );
    storage
        .insert_typed(
            int_cols,
            HashMap::new(),
            string_cols,
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();
    assert_eq!(storage.row_count(), 5);

    // Streaming compact keeps the semantics (old rows NULL, new rows valued).
    storage.compact_streaming_v4().unwrap();
    drop(storage);

    let reopened = OnDemandStorage::open(&path).unwrap();
    assert_eq!(reopened.row_count(), 5);
    assert!(reopened.column_has_nulls("extra"));
    let result = reopened
        .read_columns(Some(&["id", "extra"]), 0, None)
        .unwrap();
    match &result["extra"] {
        ColumnData::Int64(vals) => {
            assert_eq!(vals, &[0, 0, 0, 40, 50]);
        }
        other => panic!("unexpected extra column: {other:?}"),
    }
}

#[test]
fn streaming_drop_column_rewrites_without_loading_base() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("stream_drop_col.apex");

    // Seed V4 base: name, age, city, active.
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("age".to_string(), vec![25i64, 30]);
        int_cols.insert("active".to_string(), vec![1i64, 0]);
        let mut string_cols = HashMap::new();
        string_cols.insert(
            "name".to_string(),
            vec!["Alice".to_string(), "Bob".to_string()],
        );
        string_cols.insert(
            "city".to_string(),
            vec!["NYC".to_string(), "LA".to_string()],
        );
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                string_cols,
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    // Reopen mmap-only; drop the middle column (city).
    let storage = OnDemandStorage::open(&path).unwrap();
    storage.drop_column("city").unwrap();
    assert!(!storage.v4_base_loaded.load(Ordering::SeqCst));
    storage.rewrite_v4_drop_columns().unwrap();
    drop(storage);

    let reopened = OnDemandStorage::open(&path).unwrap();
    assert_eq!(reopened.row_count(), 2);
    let col_names = reopened.column_names();
    let names = col_names
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, ["_id", "active", "age", "name"]);
    let result = reopened
        .read_columns(Some(&["name", "age", "active"]), 0, None)
        .unwrap();
    match &result["name"] {
        ColumnData::String { offsets, data } => {
            let vals: Vec<String> = (0..offsets.len() - 1)
                .map(|i| {
                    String::from_utf8_lossy(&data[offsets[i] as usize..offsets[i + 1] as usize])
                        .to_string()
                })
                .collect();
            assert_eq!(vals, ["Alice", "Bob"]);
        }
        other => panic!("unexpected name column: {other:?}"),
    }
    match &result["age"] {
        ColumnData::Int64(vals) => assert_eq!(vals, &[25, 30]),
        other => panic!("unexpected age column: {other:?}"),
    }
    match &result["active"] {
        ColumnData::Int64(vals) => assert_eq!(vals, &[1, 0]),
        other => panic!("unexpected active column: {other:?}"),
    }
}

#[test]
fn compressed_delete_uses_streaming_rewrite_not_full_load() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("compressed_delete.apex");

    {
        let storage = OnDemandStorage::create(&path).unwrap();
        storage.set_compression(CompressionType::Zstd).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert(
            "id".to_string(),
            (1..=200).collect::<Vec<i64>>(),
        );
        // Compressible filler: with implicit contiguous IDs the ID section is
        // zero-length, so a single narrow integer column no longer pushes the
        // Row Group body past COMPRESS_MIN_BODY_SIZE. A wide, all-distinct
        // string column keeps the body above the threshold (dict encoding
        // cannot shrink it) while zstd still compresses it (asserted below).
        let mut string_cols = HashMap::new();
        string_cols.insert(
            "label".to_string(),
            (0..200)
                .map(|i| format!("row_number_{i:04}"))
                .collect::<Vec<String>>(),
        );
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                string_cols,
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    // Precondition: this test only means something when the Row Group is really
    // compressed, otherwise the delete takes the uncompressed in-place
    // deletion-vector path instead of the streaming rewrite.
    let bytes = std::fs::read(&path).unwrap();
    assert_ne!(
        bytes[256 + 28],
        RG_COMPRESS_NONE,
        "fixture Row Group must be compressed for this test to exercise the streaming rewrite"
    );

    let storage = OnDemandStorage::open(&path).unwrap();
    assert!(!storage.v4_base_loaded.load(Ordering::SeqCst));
    assert!(storage.delete(50));
    storage.save_delete_only().unwrap();
    assert!(!storage.v4_base_loaded.load(Ordering::SeqCst));
    assert_eq!(storage.row_count(), 199);
    drop(storage);

    let reopened = OnDemandStorage::open(&path).unwrap();
    assert_eq!(reopened.row_count(), 199);
    let result = reopened.read_columns(Some(&["id"]), 0, None).unwrap();
    match &result["id"] {
        ColumnData::Int64(vals) => {
            assert_eq!(vals.len(), 199);
            assert_eq!(vals[0], 1);
            assert_eq!(vals[48], 49);
            assert_eq!(vals[49], 51);
            assert_eq!(*vals.last().unwrap(), 200);
        }
        other => panic!("unexpected id column: {other:?}"),
    }
}

#[test]
fn numeric_range_inplace_delete_handles_implicit_id_row_groups() {
    // A path carrying an `.apex_tmp` component writes the implicit ID encoding,
    // so the Row Group body is `[deletion vector][columns...]` with no ID
    // section. The in-place numeric-range delete must place the deletion vector
    // at `32 + rg_id_section_len(..)`; a hard-coded `row_count * 8` offset lands
    // inside column data (or past EOF, growing the file) and the delete never
    // takes effect.
    let dir = tempdir().unwrap();
    let tmp_root = dir.path().join(".apex_tmp");
    std::fs::create_dir_all(&tmp_root).unwrap();
    let path = tmp_root.join("range_delete.apex");

    // Wide-range values keep the target column out of the bit-packed encoding,
    // which the in-place delete does not support.
    let scale = 9_000_000_000_000i64;
    let values: Vec<i64> = (0..2000).map(|i| (i - 1000) * scale).collect();
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("id".to_string(), values.clone());
        int_cols.insert(
            "v".to_string(),
            (0..2000).map(|i| i % 17).collect::<Vec<i64>>(),
        );
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    // Precondition: this fixture must really exercise the implicit encoding.
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(
        bytes[256 + 30],
        RG_IDS_IMPLICIT_CONTIGUOUS,
        "temp-path Row Groups must use the implicit ID encoding"
    );
    let size_before = bytes.len();

    let storage = OnDemandStorage::open(&path).unwrap();
    let deleted = storage
        .delete_where_numeric_range_inplace("id", f64::NEG_INFINITY, -0.5)
        .unwrap()
        .expect("an uncompressed V4 row group must take the in-place delete path");
    assert_eq!(deleted, 1000);
    drop(storage);

    // On Unix the delete only records pending state; it is applied when the
    // table is next opened, which is where a wrong deletion-vector offset
    // surfaces (the bytes land in column data or past the footer).
    let reopened = OnDemandStorage::open(&path).unwrap();
    assert_eq!(reopened.active_row_count(), 1000);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len() as usize,
        size_before,
        "the in-place delete must not grow the file"
    );
    // Deleted rows are tombstoned, not removed: the raw column scan still holds
    // all 2000 physical rows, and its bytes must be untouched.
    let result = reopened.read_columns(Some(&["id", "v"]), 0, None).unwrap();
    match &result["id"] {
        ColumnData::Int64(vals) => assert_eq!(vals, &values),
        other => panic!("unexpected id column: {other:?}"),
    }
    match &result["v"] {
        ColumnData::Int64(vals) => {
            assert_eq!(vals.len(), 2000);
            assert!(vals.iter().enumerate().all(|(i, &v)| v == (i % 17) as i64));
        }
        other => panic!("unexpected v column: {other:?}"),
    }
    let batch = reopened.to_arrow_batch(None, false).unwrap();
    assert_eq!(batch.num_rows(), 1000);
}

#[test]
fn uncompressed_inplace_delete_keeps_raw_row_order() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("inplace_delete.apex");
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("id".to_string(), (1..=200).collect::<Vec<i64>>());
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }
    let storage = OnDemandStorage::open(&path).unwrap();
    assert!(storage.delete(50));
    storage.save_delete_only().unwrap();
    assert_eq!(storage.active_row_count(), 199);
    drop(storage);

    let reopened = OnDemandStorage::open(&path).unwrap();
    assert_eq!(reopened.active_row_count(), 199);
    // raw column scan still contains all 200 physical rows (deleted row is
    // tombstoned, not removed) — verify via to_arrow_batch instead.
    let batch = reopened.to_arrow_batch(None, false).unwrap();
    assert_eq!(batch.num_rows(), 199);
}

#[test]
fn streaming_compact_scales_across_many_row_groups() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("many_rgs.apex");
    let n = 200_000usize;

    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("id".to_string(), (1..=n as i64).collect());
        int_cols.insert(
            "score".to_string(),
            (0..n as i64).map(|i| i % 1000).collect(),
        );
        let mut string_cols = HashMap::new();
        string_cols.insert(
            "label".to_string(),
            (0..n).map(|i| format!("row_{}", i % 97)).collect(),
        );
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                string_cols,
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    // Reopen mmap-only, append delta rows, apply updates + deletes, then
    // streaming-compact. Memory stays bounded by a Row Group + delta payloads.
    let storage = OnDemandStorage::open(&path).unwrap();
    let delta_rows: Vec<HashMap<String, crate::data::Value>> = (0..500)
        .map(|i| {
            let mut m = HashMap::new();
            m.insert("id".to_string(), crate::data::Value::Int64((n + 1 + i) as i64));
            m.insert("score".to_string(), crate::data::Value::Int64(i as i64));
            m.insert(
                "label".to_string(),
                crate::data::Value::String(format!("delta_{i}")),
            );
            m
        })
        .collect();
    let delta_ids = storage.insert_value_rows_to_delta(&delta_rows).unwrap();
    assert_eq!(delta_ids.len(), 500);
    storage
        .delta_store
        .write()
        .update_cell(10, "score", crate::data::Value::Int64(999_999));
    storage
        .delta_store
        .write()
        .update_cell(delta_ids[0], "score", crate::data::Value::Int64(888_888));
    storage.delta_store.write().delete_row(20);
    storage.save_delta_store().unwrap();

    storage.compact_streaming_v4().unwrap();
    assert!(!OnDemandStorage::delta_path(&path).exists());
    assert!(!storage.v4_base_loaded.load(Ordering::SeqCst));
    assert_eq!(storage.active_row_count(), n as u64 + 499);
    drop(storage);

    let reopened = OnDemandStorage::open(&path).unwrap();
    assert_eq!(reopened.active_row_count(), n as u64 + 499);
    let result = reopened
        .read_columns(Some(&["id", "score"]), 0, None)
        .unwrap();
    match &result["score"] {
        ColumnData::Int64(vals) => {
            assert_eq!(vals.len(), n + 499);
            assert_eq!(vals[9], 999_999, "base row 10 updated");
            assert_eq!(vals[19], 20, "row 20 deleted -> next kept row id 21 has score 20");
            assert_eq!(vals[n - 1], 888_888, "delta row 0 updated");
            assert_eq!(vals[n - 1 + 499], 499);
        }
        other => panic!("unexpected score column: {other:?}"),
    }
}

#[test]
fn dict_encoded_string_column_roundtrips_via_arrow() {
    use arrow::array::Array;
    let dir = tempdir().unwrap();
    let path = dir.path().join("dict_arrow.apex");
    let n = 5000usize;
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
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut string_cols = HashMap::new();
        string_cols.insert(
            "city".to_string(),
            (0..n).map(|i| cities[i % 10].to_string()).collect(),
        );
        storage
            .insert_typed(
                HashMap::new(),
                HashMap::new(),
                string_cols,
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }
    let storage = OnDemandStorage::open(&path).unwrap();
    let batch = storage.to_arrow_batch_dict(Some(&["city"]), false).unwrap();
    assert_eq!(batch.num_rows(), n);
    let col = batch.column_by_name("city").unwrap();
    let mut uniq = std::collections::HashSet::new();
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
        for i in 0..arr.len() {
            uniq.insert(arr.value(i).to_string());
        }
    } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::LargeStringArray>() {
        for i in 0..arr.len() {
            uniq.insert(arr.value(i).to_string());
        }
    } else {
        let casted = arrow::compute::cast(col, &arrow::datatypes::DataType::Utf8).unwrap();
        let arr = casted
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            uniq.insert(arr.value(i).to_string());
        }
    }
    assert_eq!(uniq.len(), 10, "distinct cities = {uniq:?}");
}

#[test]
fn quantized_vector_column_data_roundtrips_and_rejects_truncation() {
    use crate::compute::vector_quantization::{encode_vector, VectorCodec};
    let values = [0.25, -0.5, 1.0, 2.0, -3.0];
    for (column_type, codec) in [
        (ColumnType::BFloat16List, VectorCodec::BFloat16),
        (ColumnType::Int8Vector, VectorCodec::Int8),
        (ColumnType::UInt8Vector, VectorCodec::UInt8),
        (ColumnType::Bit1Vector, VectorCodec::Bit1),
        (ColumnType::TurboQuant2Vector, VectorCodec::TurboQuant2),
        (ColumnType::TurboQuant3Vector, VectorCodec::TurboQuant3),
        (ColumnType::TurboQuant4Vector, VectorCodec::TurboQuant4),
    ] {
        let mut data = Vec::new();
        encode_vector(codec, &values, &mut data).unwrap();
        encode_vector(codec, &values, &mut data).unwrap();
        let column = ColumnData::QuantizedList { data, dim: values.len() as u32, codec };
        let bytes = column.to_bytes();
        let (decoded, consumed) = ColumnData::from_bytes_typed(&bytes, column_type).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.column_type(), column_type);
        assert!(ColumnData::from_bytes_typed(&bytes[..bytes.len() - 1], column_type).is_err());
    }
}

#[test]
fn vector_derivation_schema_roundtrips_and_validates_dependencies() {
    let mut schema = OnDemandSchema::new();
    schema.add_column("embedding", ColumnType::FixedList);
    schema.add_column("embedding_tq4", ColumnType::TurboQuant4Vector);
    schema
        .add_vector_derivation("embedding", "embedding_tq4", 1)
        .unwrap();

    let restored = OnDemandSchema::from_bytes(&schema.to_bytes()).unwrap();
    assert_eq!(restored.vector_derivations.len(), 1);
    assert_eq!(restored.vector_derivations[0].source, "embedding");
    assert_eq!(restored.vector_derivations[0].target, "embedding_tq4");
    assert_eq!(restored.vector_derivations[0].codec_version, 1);
    assert_eq!(restored.vector_dependents("embedding"), ["embedding_tq4"]);

    assert!(schema
        .add_vector_derivation("embedding", "embedding_tq4", 1)
        .is_err());
    assert!(schema
        .add_vector_derivation("embedding_tq4", "embedding", 1)
        .is_err());
    assert!(schema.remove_vector_derivation("embedding_tq4"));
    assert!(schema.vector_derivations.is_empty());
}

#[test]
fn filtered_aggregation_parallelism_requires_enough_rows_per_worker() {
    assert!(!should_parallel_filtered_agg(1_000_000, 16, 10));
    assert!(should_parallel_filtered_agg(4_000_000, 16, 10));
    assert!(!should_parallel_filtered_agg(4_000_000, 1, 10));
    assert!(!should_parallel_filtered_agg(4_000_000, 16, 1));
}

// ====== Fused predicate-lane group aggregation tests ======

fn fused_test_setup(path: &std::path::Path) -> (Vec<String>, Vec<u16>) {
    use std::collections::HashMap;

    let n = 200usize;
    let cities: Vec<String> = (0..n).map(|i| ["A", "B", "C", "D"][i % 4].to_string()).collect();
    let ages: Vec<i64> = (0..n).map(|i| (i % 10) as i64).collect();
    let scores: Vec<f64> = (0..n).map(|i| (i % 7) as f64 * 0.5).collect();

    let mut int_cols = HashMap::new();
    int_cols.insert("age".to_string(), ages);
    let mut float_cols = HashMap::new();
    float_cols.insert("score".to_string(), scores);
    let mut string_cols = HashMap::new();
    string_cols.insert("city".to_string(), cities);

    let storage = OnDemandStorage::create(path).unwrap();
    storage
        .insert_typed(int_cols, float_cols, string_cols, HashMap::new(), HashMap::new())
        .unwrap();
    storage.save_v4().unwrap();

    let dict_strings = vec!["A", "B", "C", "D"].iter().map(|s| s.to_string()).collect();
    let group_ids: Vec<u16> = (0..n).map(|i| (i % 4) as u16).collect();
    (dict_strings, group_ids)
}

fn fused_reference<F: Fn(usize) -> bool>(n: usize, predicate: F) -> [i64; 4] {
    let mut counts = [0i64; 4];
    for i in 0..n {
        if predicate(i) {
            counts[i % 4] += 1;
        }
    }
    counts
}

#[test]
fn fused_group_agg_declines_a_value_aggregate_without_a_lane() {
    // A HAVING clause may inject an extra aggregate the SELECT list never
    // asked for. When no value lane was resolved for it, the LUT scatter loop
    // read the aggregate value through a null pointer and the process
    // segfaulted; the kernel must decline and let the caller fall back.
    use crate::storage::on_demand::{FusedLeaf, FusedLaneSpec, FusedPredicate};
    let dir = tempdir().unwrap();
    let path = dir.path().join("fused_no_lane.apex");
    let (dict_strings, group_ids) = fused_test_setup(&path);

    let storage = OnDemandStorage::open(&path).unwrap();
    let lanes = vec![FusedLaneSpec {
        col: "age".to_string(),
        is_int: true,
    }];
    let predicate = FusedPredicate::Leaf(FusedLeaf::InI64 {
        lane: 0,
        values: vec![0, 1, 2],
    });
    assert!(
        storage
            .execute_fused_group_agg_mmap(&dict_strings, &group_ids, &lanes, &predicate, None, 2)
            .unwrap()
            .is_none(),
        "a value aggregate without a value lane must decline the fused kernel"
    );
    // The same mask with a resolved lane still runs.
    assert!(
        storage
            .execute_fused_group_agg_mmap(&dict_strings, &group_ids, &lanes, &predicate, Some(0), 2)
            .unwrap()
            .is_some(),
        "a value aggregate with a lane keeps using the kernel"
    );
}

#[test]
fn test_fused_group_agg_boolean_tree_matches_reference() {
    use crate::storage::on_demand::{FusedLeaf, FusedLaneSpec, FusedPredicate};
    let dir = tempdir().unwrap();
    let path = dir.path().join("fused_bool.apex");
    let (dict_strings, group_ids) = fused_test_setup(&path);

    let storage = OnDemandStorage::open(&path).unwrap();
    let lanes = vec![
        FusedLaneSpec {
            col: "age".to_string(),
            is_int: true,
        },
        FusedLaneSpec {
            col: "score".to_string(),
            is_int: false,
        },
    ];
    // (city IN {A, B} OR age IN {0, 1, 2}) AND score >= 1.0
    let city_flags: Vec<u8> = vec![1, 1, 0, 0];
    let predicate = FusedPredicate::And(
        Box::new(FusedPredicate::Or(
            Box::new(FusedPredicate::Leaf(FusedLeaf::DictIn { flags: city_flags })),
            Box::new(FusedPredicate::Leaf(FusedLeaf::InI64 {
                lane: 0,
                values: vec![0, 1, 2],
            })),
        )),
        Box::new(FusedPredicate::Leaf(FusedLeaf::RangeF64 {
            lane: 1,
            lo: 1.0,
            hi: f64::INFINITY,
        })),
    );

    let result = storage
        .execute_fused_group_agg_mmap(&dict_strings, &group_ids, &lanes, &predicate, Some(1), 7)
        .unwrap()
        .expect("kernel should run");

    let expected = fused_reference(200, |i| {
        let city_ok = i % 4 == 0 || i % 4 == 1;
        let age_ok = (i % 10) as i64 <= 2;
        let score_ok = (i % 7) as f64 * 0.5 >= 1.0;
        (city_ok || age_ok) && score_ok
    });
    for slot in 0..4 {
        assert_eq!(result[slot].count, expected[slot], "slot {slot}");
    }
    // Sum cross-check for slot 0 (city A, i % 4 == 0)
    let mut sum = 0.0f64;
    for i in (0..200).step_by(4) {
        let age_ok = (i % 10) as i64 <= 2;
        let score_ok = (i % 7) as f64 * 0.5 >= 1.0;
        if (true || age_ok) && score_ok {
            sum += (i % 7) as f64 * 0.5;
        }
    }
    assert!((result[0].sum - sum).abs() < 1e-9);
    assert_eq!(result[0].min, 1.0);
    assert_eq!(result[0].max, 3.0);
}

#[test]
fn test_fused_group_agg_not_and_sentinel_dict_eq() {
    use crate::storage::on_demand::{FusedLeaf, FusedLaneSpec, FusedPredicate};
    let dir = tempdir().unwrap();
    let path = dir.path().join("fused_not.apex");
    let (dict_strings, group_ids) = fused_test_setup(&path);

    let storage = OnDemandStorage::open(&path).unwrap();
    let lanes = vec![FusedLaneSpec {
        col: "age".to_string(),
        is_int: true,
    }];
    // NOT (age IN {0, 1}) AND city = 'B'  → only slot 1
    let predicate = FusedPredicate::And(
        Box::new(FusedPredicate::Not(Box::new(FusedPredicate::Leaf(
            FusedLeaf::InI64 {
                lane: 0,
                values: vec![0, 1],
            },
        )))),
        Box::new(FusedPredicate::Leaf(FusedLeaf::DictEq { key: 1 })),
    );
    let result = storage
        .execute_fused_group_agg_mmap(&dict_strings, &group_ids, &lanes, &predicate, None, 0)
        .unwrap()
        .expect("kernel should run");
    let expected = fused_reference(200, |i| (i % 10) as i64 > 1 && i % 4 == 1);
    for slot in 0..4 {
        assert_eq!(result[slot].count, expected[slot], "slot {slot}");
    }

    // DictEq with the sentinel key (value absent from the dictionary) never
    // matches; NOT of it matches every row.
    let none_pred = FusedPredicate::Leaf(FusedLeaf::DictEq { key: u16::MAX });
    let result = storage
        .execute_fused_group_agg_mmap(&dict_strings, &group_ids, &lanes, &none_pred, None, 0)
        .unwrap()
        .expect("kernel should run");
    assert!(result.iter().all(|agg| agg.count == 0));

    let all_pred = FusedPredicate::Not(Box::new(none_pred));
    let result = storage
        .execute_fused_group_agg_mmap(&dict_strings, &group_ids, &lanes, &all_pred, None, 0)
        .unwrap()
        .expect("kernel should run");
    for slot in 0..4 {
        assert_eq!(result[slot].count, 50, "slot {slot}");
    }
}

#[test]
fn test_fused_group_agg_strict_integer_inequality_is_exact() {
    use crate::storage::on_demand::{FusedLeaf, FusedLaneSpec, FusedPredicate};
    let dir = tempdir().unwrap();
    let path = dir.path().join("fused_strict.apex");
    let (dict_strings, group_ids) = fused_test_setup(&path);

    let storage = OnDemandStorage::open(&path).unwrap();
    let lanes = vec![FusedLaneSpec {
        col: "age".to_string(),
        is_int: true,
    }];
    // age > 4  →  [5, i64::MAX]
    let predicate = FusedPredicate::Leaf(FusedLeaf::RangeI64 {
        lane: 0,
        lo: 5,
        hi: i64::MAX,
    });
    let result = storage
        .execute_fused_group_agg_mmap(&dict_strings, &group_ids, &lanes, &predicate, None, 0)
        .unwrap()
        .expect("kernel should run");
    let expected = fused_reference(200, |i| (i % 10) as i64 > 4);
    for slot in 0..4 {
        assert_eq!(result[slot].count, expected[slot], "slot {slot}");
    }
}

#[test]
fn test_fused_plan_compiles_per_slot_flat_shapes() {
    use crate::storage::on_demand::{
        compile_fused_plan, ExtraKind, FusedLeaf, FusedPredicate, FuseMode, SlotPlan,
    };

    // And(InI64, DictIn): the pure side becomes the common part; the
    // dictionary side becomes per-slot All/None extras (OneMask).
    let predicate = FusedPredicate::And(
        Box::new(FusedPredicate::Leaf(FusedLeaf::InI64 {
            lane: 0,
            values: vec![3],
        })),
        Box::new(FusedPredicate::Leaf(FusedLeaf::DictIn {
            flags: vec![1, 0, 1, 0],
        })),
    );
    let plan = compile_fused_plan(&predicate, 4).expect("compile");
    let in_id = plan
        .leaves
        .iter()
        .position(|l| matches!(l, FusedLeaf::InI64 { .. }))
        .unwrap() as u8;
    assert_eq!(plan.mode, FuseMode::AndCommon);
    assert!(matches!(plan.common, SlotPlan::One(l) if l == in_id));
    assert_eq!(plan.extra_kind, ExtraKind::OneMask);
    assert_eq!(plan.extra_mask, vec![0, 0xFF, 0, 0xFF]);

    // Pure numeric And of two lanes: one flat common part for every slot.
    let predicate = FusedPredicate::And(
        Box::new(FusedPredicate::Leaf(FusedLeaf::RangeI64 {
            lane: 0,
            lo: 1,
            hi: 9,
        })),
        Box::new(FusedPredicate::Leaf(FusedLeaf::RangeF64 {
            lane: 1,
            lo: 0.5,
            hi: 2.0,
        })),
    );
    let plan = compile_fused_plan(&predicate, 4).expect("compile");
    assert_eq!(plan.mode, FuseMode::AndCommon);
    assert!(matches!(plan.common, SlotPlan::And(_, _)));
    assert_eq!(plan.extra_kind, ExtraKind::All);

    // And(Or(leaf, leaf), leaf): the common 3-leaf shape stays flat.
    let leaf = |lane: usize| {
        FusedPredicate::Leaf(FusedLeaf::RangeI64 {
            lane,
            lo: 0,
            hi: 9,
        })
    };
    let predicate = FusedPredicate::And(
        Box::new(FusedPredicate::Or(Box::new(leaf(0)), Box::new(leaf(1)))),
        Box::new(leaf(2)),
    );
    let plan = compile_fused_plan(&predicate, 2).expect("compile");
    assert_eq!(plan.mode, FuseMode::AndCommon);
    assert!(matches!(plan.common, SlotPlan::Bin3 { .. }));

    // And(Or, Or) with four leaves: deeper trees keep the Pure slot table.
    let predicate = FusedPredicate::And(
        Box::new(FusedPredicate::Or(Box::new(leaf(0)), Box::new(leaf(1)))),
        Box::new(FusedPredicate::Or(Box::new(leaf(0)), Box::new(leaf(2)))),
    );
    let plan = compile_fused_plan(&predicate, 2).expect("compile");
    assert_eq!(plan.mode, FuseMode::Pure);
    assert!(plan
        .slot_plans
        .iter()
        .all(|p| matches!(p, SlotPlan::Tree { .. })));

    // Or(DictIn, RangeF64): the numeric side becomes the common part of an
    // OrCommon plan with All/None extras.
    let predicate = FusedPredicate::Or(
        Box::new(FusedPredicate::Leaf(FusedLeaf::DictIn {
            flags: vec![1, 0],
        })),
        Box::new(FusedPredicate::Leaf(FusedLeaf::RangeF64 {
            lane: 0,
            lo: 1.0,
            hi: 2.0,
        })),
    );
    let plan = compile_fused_plan(&predicate, 2).expect("compile");
    assert_eq!(plan.mode, FuseMode::OrCommon);
    assert!(matches!(plan.common, SlotPlan::One(_)));
    assert_eq!(plan.extra_kind, ExtraKind::OneMask);
    assert_eq!(plan.extra_mask, vec![0, 0xFF]);
}

#[test]
fn test_lut_program_truth_table_matches_reference() {
    use crate::storage::on_demand::{
        try_build_lut_program, FusedLeaf, FusedPredicate, LcMode,
    };
    // (city IN {A, C} OR age IN {1, 3}) AND score >= 1.0
    let predicate = FusedPredicate::And(
        Box::new(FusedPredicate::Or(
            Box::new(FusedPredicate::Leaf(FusedLeaf::DictIn {
                flags: vec![1, 0, 1, 0],
            })),
            Box::new(FusedPredicate::Leaf(FusedLeaf::InI64 {
                lane: 0,
                values: vec![1, 3],
            })),
        )),
        Box::new(FusedPredicate::Leaf(FusedLeaf::RangeF64 {
            lane: 1,
            lo: 1.0,
            hi: f64::INFINITY,
        })),
    );
    let prog = try_build_lut_program(&predicate, 4).expect("build");
    assert_eq!(prog.k, 2);
    assert_eq!(prog.mode[0], LcMode::InI64);
    assert_eq!(prog.mode[1], LcMode::RangeF64);
    assert!(!prog.all_none);
    for gid in 0..4usize {
        for cb in 0..4u8 {
            let age_ok = ((cb >> 0) & 1) != 0;
            let score_ok = ((cb >> 1) & 1) != 0;
            let expect = ((gid == 0 || gid == 2) || age_ok) && score_ok;
            assert_eq!(
                prog.lut[(cb as usize) << prog.stride_shift | gid],
                expect as u8,
                "gid={gid} cb={cb}"
            );
        }
    }
}

#[test]
fn test_lut_program_rejects_over_budget() {
    use crate::storage::on_demand::{try_build_lut_program, FusedLeaf, FusedPredicate};
    let range = |lane: usize| {
        FusedPredicate::Leaf(FusedLeaf::RangeI64 {
            lane,
            lo: 0,
            hi: 9,
        })
    };
    // Four distinct lane leaves exceed the budget: generic path.
    let predicate = FusedPredicate::And(
        Box::new(FusedPredicate::And(Box::new(range(0)), Box::new(range(1)))),
        Box::new(FusedPredicate::And(Box::new(range(2)), Box::new(range(3)))),
    );
    assert!(try_build_lut_program(&predicate, 4).is_none());
    // IN sets above the budget stay on the generic path.
    let big_in = FusedPredicate::Leaf(FusedLeaf::InI64 {
        lane: 0,
        values: (0..9).collect(),
    });
    assert!(try_build_lut_program(&big_in, 4).is_none());
    // An all-None predicate is still a valid LUT program with all_none set.
    let none = FusedPredicate::Leaf(FusedLeaf::DictIn { flags: vec![0, 0] });
    let prog = try_build_lut_program(&none, 2).expect("build");
    assert_eq!(prog.k, 0);
    assert!(prog.all_none);
}

#[test]
fn test_fused_group_agg_k3_lut_matches_reference() {
    use crate::storage::on_demand::{FusedLeaf, FusedLaneSpec, FusedPredicate};
    let dir = tempdir().unwrap();
    let path = dir.path().join("fused_k3.apex");
    let (dict_strings, group_ids) = fused_test_setup(&path);

    let storage = OnDemandStorage::open(&path).unwrap();
    let lanes = vec![
        FusedLaneSpec {
            col: "age".to_string(),
            is_int: true,
        },
        FusedLaneSpec {
            col: "score".to_string(),
            is_int: false,
        },
    ];
    // (age IN {1, 3} OR score IN {0.5, 2.5}) AND age BETWEEN 0 AND 8:
    // three distinct lane leaves, so the k=3 LUT loop runs.
    let predicate = FusedPredicate::And(
        Box::new(FusedPredicate::Or(
            Box::new(FusedPredicate::Leaf(FusedLeaf::InI64 {
                lane: 0,
                values: vec![1, 3],
            })),
            Box::new(FusedPredicate::Leaf(FusedLeaf::InF64 {
                lane: 1,
                values: vec![0.5, 2.5],
            })),
        )),
        Box::new(FusedPredicate::Leaf(FusedLeaf::RangeI64 {
            lane: 0,
            lo: 0,
            hi: 8,
        })),
    );

    let result = storage
        .execute_fused_group_agg_mmap(
            &dict_strings, &group_ids, &lanes, &predicate, Some(0), 7,
        )
        .unwrap()
        .expect("kernel should run");

    let matches = |i: usize| {
        let age = (i % 10) as i64;
        let score = (i % 7) as f64 * 0.5;
        (age == 1 || age == 3 || score == 0.5 || score == 2.5) && age <= 8
    };
    let expected = fused_reference(200, matches);
    for slot in 0..4 {
        assert_eq!(result[slot].count, expected[slot], "slot {slot}");
    }
    // Value aggregate over the int lane (age) for slot 1.
    let mut sum = 0.0f64;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for i in (1..200).step_by(4) {
        if matches(i) {
            let age = (i % 10) as i64 as f64;
            sum += age;
            min = min.min(age);
            max = max.max(age);
        }
    }
    assert!((result[1].sum - sum).abs() < 1e-9);
    assert_eq!(result[1].min, min);
    assert_eq!(result[1].max, max);
}

#[test]
fn test_bitpack_fill_matches_reference() {
    fn ref_pack(values: &[i64], bw: usize) -> Vec<u8> {
        let bits = values.len() * bw;
        let mut packed = vec![0u8; (bits + 7) / 8];
        for (i, &v) in values.iter().enumerate() {
            let bits_u = v as u64;
            for b in 0..bw {
                if (bits_u >> b) & 1 == 1 {
                    let g = i * bw + b;
                    packed[g / 8] |= 1 << (g % 8);
                }
            }
        }
        packed
    }
    fn ref_unpack(packed: &[u8], count: usize, bw: usize, min_val: i64) -> Vec<i64> {
        (0..count)
            .map(|i| {
                let bit_offset = i * bw;
                let mut delta: u64 = 0;
                for b in 0..bw {
                    let g = bit_offset + b;
                    if (packed[g / 8] >> (g % 8)) & 1 == 1 {
                        delta |= 1u64 << b;
                    }
                }
                min_val.wrapping_add(delta as i64)
            })
            .collect()
    }
    let mut state: u64 = 0x9E3779B97F4A7C15;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) as u64
    };
    for bw in [1usize, 3, 5, 6, 7, 8, 15, 31, 47] {
        for count in [1usize, 2, 7, 8, 15, 16, 63, 64, 65, 1000, 62_500] {
            let min_val = -500i64;
            let span = if bw == 47 { 1i64 << 40 } else { (1i64 << bw) - 1 };
            let values: Vec<i64> = (0..count)
                .map(|_| min_val + (next() % (span as u64 + 1)) as i64)
                .collect();
            let packed = ref_pack(&values, bw);
            let mut out = vec![0i64; count];
            bitpack_fill(&packed, count, bw, min_val, &mut out);
            let expected = ref_unpack(&packed, count, bw, min_val);
            assert_eq!(out, expected, "bw={bw} count={count}");
            if bw <= 8 {
                let mut compact = vec![0u8; count];
                bitpack_fill_u8(&packed, count, bw, &mut compact);
                let restored: Vec<i64> = compact
                    .into_iter()
                    .map(|delta| min_val.wrapping_add(delta as i64))
                    .collect();
                assert_eq!(restored, expected, "compact bw={bw} count={count}");
            }
        }
    }
}

#[test]
fn test_bitpack_roundtrip_encode_decode() {
    let mut state: u64 = 0xDEADBEEF_CAFEBABE;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) as u32
    };
    let base = 1_000_000i64;
    for count in [16usize, 123, 1000, 65_536] {
        let values: Vec<i64> = (0..count)
            .map(|_| base + (next() % 4096) as i64)
            .collect();
        let encoded = bitpack_encode_i64(&values).expect("should bitpack");
        let (decoded, consumed) = bitpack_decode_i64(&encoded).unwrap();
        assert_eq!(decoded, values, "count={count}");
        assert_eq!(consumed, encoded.len());
    }
}

// ============================================================================
// Crash recovery (WAL commit marker + delta repair / reconstruction)
// ============================================================================

fn recovery_schema() -> Vec<(String, ColumnType)> {
    vec![
        ("name".to_string(), ColumnType::String),
        ("value".to_string(), ColumnType::Int64),
    ]
}

fn row(name: &str, value: i64) -> HashMap<String, ColumnValue> {
    let mut data = HashMap::new();
    data.insert("name".to_string(), ColumnValue::String(name.to_string()));
    data.insert("value".to_string(), ColumnValue::Int64(value));
    data
}

fn append_wal_records(path: &std::path::Path, records: &[crate::storage::incremental::WalRecord]) {
    let wal_path = OnDemandStorage::wal_path(path);
    let mut writer = crate::storage::incremental::WalWriter::open(&wal_path).unwrap();
    for record in records {
        writer.append(record).unwrap();
    }
    writer.flush().unwrap();
}

#[test]
fn torn_delta_tail_is_truncated_on_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("torn.apex");
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Safe,
            &recovery_schema(),
        )
        .unwrap();
        storage.insert_rows(&[row("seed", 0)]).unwrap();

        storage.save_full().unwrap();
        // A second open/appends a complete delta batch...
        drop(storage);
        let storage = OnDemandStorage::open_with_durability(&path, crate::storage::DurabilityLevel::Safe)
            .unwrap();
        storage
            .insert_rows_to_delta(&[row("live", 1)])
            .unwrap();
        drop(storage);
    }
    // ...then simulate a kill mid-append with a torn trailing fragment.
    {
        use std::io::Write;
        let delta_path = OnDemandStorage::delta_path(&path);
        let len_before = std::fs::metadata(&delta_path).unwrap().len();
        let mut file = std::fs::OpenOptions::new().append(true).open(&delta_path).unwrap();
        file.write_all(&[0u8; 13]).unwrap();
        assert!(std::fs::metadata(&delta_path).unwrap().len() > len_before);
        let storage =
            OnDemandStorage::open_with_durability(&path, crate::storage::DurabilityLevel::Safe)
                .unwrap();
        assert_eq!(
            storage.row_count(),
            2,
            "base={} delta={}",
            storage.base_row_count(),
            storage.delta_row_count()
        );
        let len = std::fs::metadata(&delta_path).unwrap().len();
        assert_eq!(len, len_before, "torn tail must be truncated");
    }
}

#[test]
fn uncommitted_txn_delta_rows_are_rolled_back_on_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("uncommitted.apex");
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Safe,
            &recovery_schema(),
        )
        .unwrap();
        storage.insert_rows(&[row("seed", 0)]).unwrap();

        storage.save_full().unwrap();
        drop(storage);
        // Simulate a commit that applied its rows to the delta, then died
        // before the WAL commit marker became durable.
        let storage = OnDemandStorage::open_with_durability(&path, crate::storage::DurabilityLevel::Safe)
            .unwrap();
        storage
            .insert_rows_to_delta(&[row("ghost", 1)])
            .unwrap();
        drop(storage);
    }
    append_wal_records(
        &path,
        &[
            crate::storage::incremental::WalRecord::TxnBegin { txn_id: 99 },
            crate::storage::incremental::WalRecord::Insert {
                id: 2,
                data: row("ghost", 1),
                txn_id: 99,
            },
        ],
    );
    let storage =
        OnDemandStorage::open_with_durability(&path, crate::storage::DurabilityLevel::Safe).unwrap();
    assert_eq!(storage.row_count(), 1, "uncommitted rows must not survive");
    assert!(!storage.exists(2));
}

#[test]
fn committed_txn_insert_missing_from_delta_is_reconstructed() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("reconstruct.apex");
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Safe,
            &recovery_schema(),
        )
        .unwrap();
        storage.insert_rows(&[row("seed", 0)]).unwrap();

        storage.save_full().unwrap();
        drop(storage);
    }
    // The commit marker was durable but the rows never reached the delta.
    append_wal_records(
        &path,
        &[
            crate::storage::incremental::WalRecord::TxnBegin { txn_id: 7 },
            crate::storage::incremental::WalRecord::Insert {
                id: 2,
                data: row("committed", 42),
                txn_id: 7,
            },
            crate::storage::incremental::WalRecord::TxnCommit { txn_id: 7 },
        ],
    );
    let storage =
        OnDemandStorage::open_with_durability(&path, crate::storage::DurabilityLevel::Safe).unwrap();
    assert_eq!(storage.row_count(), 2, "committed rows must be reconstructed");
    assert!(storage.exists(2));
    // Id allocation must continue past the reconstructed rows.
    let ids = storage.insert_rows_to_delta(&[row("after", 3)]).unwrap();
    assert_eq!(ids, vec![3]);
}

#[test]
fn committed_txn_insert_already_in_delta_is_not_duplicated() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("nodup.apex");
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Safe,
            &recovery_schema(),
        )
        .unwrap();
        storage.insert_rows(&[row("seed", 0)]).unwrap();

        storage.save_full().unwrap();
        drop(storage);
        // Normal commit: rows applied to delta AND WAL-logged with a marker.
        let storage = OnDemandStorage::open_with_durability(&path, crate::storage::DurabilityLevel::Safe)
            .unwrap();
        storage
            .insert_rows_to_delta(&[row("applied", 1)])
            .unwrap();
        drop(storage);
    }
    append_wal_records(
        &path,
        &[
            crate::storage::incremental::WalRecord::TxnBegin { txn_id: 5 },
            crate::storage::incremental::WalRecord::Insert {
                id: 2,
                data: row("applied", 1),
                txn_id: 5,
            },
            crate::storage::incremental::WalRecord::TxnCommit { txn_id: 5 },
        ],
    );
    let storage =
        OnDemandStorage::open_with_durability(&path, crate::storage::DurabilityLevel::Safe).unwrap();
    assert_eq!(storage.row_count(), 2, "recovery must not duplicate applied rows");
}

#[test]
fn committed_txn_delete_is_reapplied_on_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("delete_replay.apex");
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Safe,
            &recovery_schema(),
        )
        .unwrap();
        storage.insert_rows(&[row("victim", 0)]).unwrap();
        storage.save_full().unwrap();
        drop(storage);
    }
    // Sanity: the victim row (id 1, FIRST_ROW_ID) is visible before recovery.
    {
        let storage =
            OnDemandStorage::open_with_durability(&path, crate::storage::DurabilityLevel::Safe)
                .unwrap();
        assert!(storage.exists(1), "victim row must exist before recovery");
        drop(storage);
    }
    // Commit marker durable, but the process died before applying the delete.
    append_wal_records(
        &path,
        &[
            crate::storage::incremental::WalRecord::TxnBegin { txn_id: 3 },
            crate::storage::incremental::WalRecord::Delete { id: 1, txn_id: 3 },
            crate::storage::incremental::WalRecord::TxnCommit { txn_id: 3 },
        ],
    );
    let storage =
        OnDemandStorage::open_with_durability(&path, crate::storage::DurabilityLevel::Safe).unwrap();
    assert!(!storage.exists(1), "committed delete must be re-applied");
}

#[test]
fn committed_txn_update_recovery_respects_applied_wal_offset() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("update_replay.apex");
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Safe,
            &[("value".to_string(), ColumnType::Int64)],
        )
        .unwrap();
        storage
            .insert_rows(&[HashMap::from([(
                "value".to_string(),
                ColumnValue::Int64(7),
            )])])
            .unwrap();
        storage.save_full().unwrap();
    }

    // The commit marker reached the WAL, but the update did not reach the
    // DeltaStore. Opening must replay the unapplied suffix.
    append_wal_records(
        &path,
        &[
            crate::storage::incremental::WalRecord::TxnBegin { txn_id: 31 },
            crate::storage::incremental::WalRecord::Update {
                id: 1,
                data: HashMap::from([("value".to_string(), crate::data::Value::Int64(9))]),
                txn_id: 31,
            },
            crate::storage::incremental::WalRecord::TxnCommit { txn_id: 31 },
        ],
    );

    let assert_overlay = |storage: &OnDemandStorage, expected| {
        assert_eq!(
            storage
                .delta_store
                .read()
                .get_updated_value(1, "value"),
            Some(&crate::data::Value::Int64(expected)),
        );
    };
    {
        let storage = OnDemandStorage::open_with_durability(
            &path,
            crate::storage::DurabilityLevel::Safe,
        )
        .unwrap();
        assert_overlay(&storage, 9);
    }
    {
        let storage = OnDemandStorage::open_with_durability(
            &path,
            crate::storage::DurabilityLevel::Safe,
        )
        .unwrap();
        assert_overlay(&storage, 9);

        // A later ordinary update must not be overwritten by an older WAL
        // UPDATE once the applied offset covers that record.
        storage.delta_update_row(
            1,
            &HashMap::from([("value".to_string(), crate::data::Value::Int64(11))]),
        );
        storage.save_delta_store().unwrap();
    }
    {
        let storage = OnDemandStorage::open_with_durability(
            &path,
            crate::storage::DurabilityLevel::Safe,
        )
        .unwrap();
        assert_overlay(&storage, 11);
        storage.compact().unwrap();
    }
    let storage = OnDemandStorage::open_with_durability(
        &path,
        crate::storage::DurabilityLevel::Safe,
    )
    .unwrap();
    let columns = storage.read_columns(Some(&["value"]), 0, None).unwrap();
    match &columns["value"] {
        ColumnData::Int64(values) => assert_eq!(values, &[11]),
        other => panic!("expected int64 column, got {other:?}"),
    }
    drop(storage);

    // Transaction ids restart after a process restart. An old Commit for the
    // same id must not make a new, uncommitted UPDATE replayable.
    append_wal_records(
        &path,
        &[
            crate::storage::incremental::WalRecord::TxnBegin { txn_id: 31 },
            crate::storage::incremental::WalRecord::Update {
                id: 1,
                data: HashMap::from([("value".to_string(), crate::data::Value::Int64(13))]),
                txn_id: 31,
            },
        ],
    );
    let storage = OnDemandStorage::open_with_durability(
        &path,
        crate::storage::DurabilityLevel::Safe,
    )
    .unwrap();
    assert_eq!(
        storage.delta_store.read().get_updated_value(1, "value"),
        None,
    );
    let columns = storage.read_columns(Some(&["value"]), 0, None).unwrap();
    match &columns["value"] {
        ColumnData::Int64(values) => assert_eq!(values, &[11]),
        other => panic!("expected int64 column, got {other:?}"),
    }
}

#[test]
fn auto_commit_insert_missing_from_delta_is_reconstructed() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("autocommit.apex");
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Safe,
            &recovery_schema(),
        )
        .unwrap();
        storage.insert_rows(&[row("seed", 0)]).unwrap();

        storage.save_full().unwrap();
        drop(storage);
    }
    append_wal_records(
        &path,
        &[crate::storage::incremental::WalRecord::Insert {
            id: 2,
            data: row("auto", 9),
            txn_id: 0,
        }],
    );
    let storage =
        OnDemandStorage::open_with_durability(&path, crate::storage::DurabilityLevel::Safe).unwrap();
    assert_eq!(storage.row_count(), 2);
    assert!(storage.exists(2));
}


#[test]
fn torn_delta_tail_with_committed_marker_is_repaired_and_reconstructed() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("torn_committed.apex");
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Safe,
            &recovery_schema(),
        )
        .unwrap();
        storage.insert_rows(&[row("seed", 0)]).unwrap();
        storage.save_full().unwrap();
        drop(storage);
        // A commit that applied its batch, then died mid-way through the
        // next append (torn tail), with the commit marker durable.
        let storage = OnDemandStorage::open_with_durability(&path, crate::storage::DurabilityLevel::Safe)
            .unwrap();
        let ids = storage.insert_rows_to_delta(&[row("committed", 1)]).unwrap();
        assert_eq!(ids, vec![2]);
        drop(storage);
    }
    let delta_path = OnDemandStorage::delta_path(&path);
    let len_before = std::fs::metadata(&delta_path).unwrap().len();
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new().append(true).open(&delta_path).unwrap();
        file.write_all(&[0u8; 9]).unwrap();
    }
    append_wal_records(
        &path,
        &[
            crate::storage::incremental::WalRecord::TxnBegin { txn_id: 11 },
            crate::storage::incremental::WalRecord::Insert {
                id: 2,
                data: row("committed", 1),
                txn_id: 11,
            },
            crate::storage::incremental::WalRecord::TxnCommit { txn_id: 11 },
        ],
    );
    let storage =
        OnDemandStorage::open_with_durability(&path, crate::storage::DurabilityLevel::Safe).unwrap();
    assert_eq!(storage.row_count(), 2, "torn tail repaired, committed row intact");
    assert!(storage.exists(2));
    assert_eq!(
        std::fs::metadata(&delta_path).unwrap().len(),
        len_before,
        "torn tail must be truncated"
    );
}


#[test]
fn delta_batch_cache_tracks_appends() {
    let dir = tempdir().unwrap();
    let table_path = dir.path().join("delta_batch_cache.apex");
    let storage = OnDemandStorage::create(&table_path).unwrap();

    let mut first = HashMap::new();
    first.insert("value".to_string(), vec![1]);
    storage
        .insert_typed_to_delta(
            first,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();

    let delta_path = OnDemandStorage::delta_path(&table_path);
    let (batches_before, _) =
        OnDemandStorage::delta_complete_batches(&delta_path).unwrap();
    assert_eq!(batches_before.len(), 1);

    // Cache hit must return the same boundaries.
    let (cached, _) = OnDemandStorage::delta_complete_batches(&delta_path).unwrap();
    assert_eq!(cached, batches_before);

    // Append a second batch: the memoized state must pick it up.
    let mut second = HashMap::new();
    second.insert("value".to_string(), vec![2]);
    storage
        .insert_typed_to_delta(
            second,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();

    let (after, _) = OnDemandStorage::delta_complete_batches(&delta_path).unwrap();
    assert_eq!(after.len(), 2);
    assert_eq!(after[0], batches_before[0]);

    // Subsequent reads serve the updated boundary.
    let (again, _) = OnDemandStorage::delta_complete_batches(&delta_path).unwrap();
    assert_eq!(again, after);
}

/// The production staleness threshold the open-path sweep runs with.
fn scratch_stale_after_seconds() -> u64 {
    SCRATCH_STALE_AFTER.as_secs()
}

#[test]
fn scratch_sweep_removes_only_stale_rewrite_files() {
    let dir = tempdir().unwrap();
    let table_path = dir.path().join("t.apex");
    let scratch = |stem: &str, pid: u32, seq: u64| {
        dir.path().join(format!("{stem}.apex.{pid}.{seq}.tmp"))
    };
    let stale = scratch("t", std::process::id() as u32, 1);
    let other_table = scratch("other", 4242, 7);
    // Neither the legacy fixed-name scratch nor an unrelated `.tmp` file matches
    // the rewrite pattern, so the sweep must leave them alone.
    let legacy = dir.path().join("t.apex.tmp");
    let unrelated = dir.path().join("notes.tmp");
    let not_numeric = dir.path().join("t.apex.x.y.tmp");
    for path in [&stale, &other_table, &legacy, &unrelated, &not_numeric] {
        std::fs::write(path, b"x").unwrap();
    }

    // Zero minimum age makes every rewrite scratch file count as leftover.
    sweep_scratch_files(&table_path, std::time::Duration::ZERO);
    assert!(!stale.exists(), "a stale scratch file is leftover garbage");
    assert!(
        !other_table.exists(),
        "one sweep clears the directory, not just one table"
    );
    assert!(legacy.exists(), "the fixed legacy name is not a rewrite scratch");
    assert!(unrelated.exists(), "an unrelated .tmp file is untouched");
    assert!(
        not_numeric.exists(),
        "only <stem>.apex.<pid>.<seq>.tmp is a rewrite scratch"
    );

    // A file younger than the threshold is a publish in flight elsewhere.
    std::fs::write(&stale, b"x").unwrap();
    sweep_scratch_files(&table_path, std::time::Duration::from_secs(3600));
    assert!(stale.exists(), "a fresh scratch file belongs to a live writer");
    assert_eq!(scratch_stale_after_seconds(), 60);

    // The open path sweeps each directory once per process.
    let memo_dir = tempdir().unwrap();
    let memo_table = memo_dir.path().join("m.apex");
    let leftover = memo_dir
        .path()
        .join(format!("m.apex.{}.9.tmp", std::process::id()));
    std::fs::write(&leftover, b"x").unwrap();
    sweep_scratch_files(&memo_table, std::time::Duration::ZERO);
    assert!(!leftover.exists());
    std::fs::write(&leftover, b"x").unwrap();
    reap_stale_scratch_files(&memo_table);
    assert!(
        leftover.exists(),
        "the open path sweeps a directory at most once per process"
    );
}

#[test]
fn point_read_page_cache_respects_its_byte_budget() {
    // The pread point-read path caches one 4 KiB page per touched offset, so an
    // unbudgeted cache grows with every distinct page a workload reads. Bound
    // it: the underlying bytes must still be correct after eviction.
    let dir = tempdir().unwrap();
    let path = dir.path().join("page_cache_budget.apex");
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let rows = 20_000usize;
        let mut int_cols = HashMap::new();
        int_cols.insert("a".to_string(), (0..rows as i64).collect::<Vec<i64>>());
        int_cols.insert("b".to_string(), (0..rows as i64).map(|i| i * 3).collect());
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let file_bytes = std::fs::read(&path).unwrap();
    assert!(
        file_bytes.len() > 12 * 4096,
        "fixture must span more pages than the test budget"
    );

    let storage = OnDemandStorage::open(&path).unwrap();
    storage.set_page_cache_budget_bytes(4 * 4096);
    let mut probe = [0u8; 64];
    for page in 0..12u64 {
        storage.read_cached_bytes(page * 4096, &mut probe).unwrap();
        let start = (page * 4096) as usize;
        assert_eq!(
            &probe[..],
            &file_bytes[start..start + 64],
            "page {page} must be read correctly through the cache"
        );
        assert!(
            storage.page_cache_len() <= 4,
            "page cache must stay within its 4-page budget, got {}",
            storage.page_cache_len()
        );
    }

    // A repeated read of a cached page still returns the right bytes.
    storage.read_cached_bytes(0, &mut probe).unwrap();
    assert_eq!(&probe[..], &file_bytes[..64]);
}

#[test]
fn point_read_page_cache_hits_and_invalidation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("page_cache_hits.apex");
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("a".to_string(), (0..4000).collect::<Vec<i64>>());
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let file_bytes = std::fs::read(&path).unwrap();
    let storage = OnDemandStorage::open(&path).unwrap();
    assert_eq!(storage.page_cache_len(), 0);
    let mut first = [0u8; 32];
    storage.read_cached_bytes(64, &mut first).unwrap();
    assert_eq!(storage.page_cache_len(), 1, "a miss caches exactly one page");
    assert_eq!(&first[..], &file_bytes[64..96]);

    // Same page, different slice: served from the cache, no new entry.
    let mut second = [0u8; 32];
    storage.read_cached_bytes(128, &mut second).unwrap();
    assert_eq!(storage.page_cache_len(), 1);
    assert_eq!(&second[..], &file_bytes[128..160]);

    // Writes invalidate the cache (the file may have moved under it).
    storage.invalidate_page_cache();
    assert_eq!(storage.page_cache_len(), 0);
}

#[test]
fn mmap_batch_overlays_delta_updates_with_and_without_id_projection() {
    // `build_arrow_batch` needs the physical row IDs to place DeltaStore
    // updates. Those IDs come from the `_id` column when it is projected and
    // from the retained accumulator otherwise; both branches must overlay the
    // update on the right row, and a scan without any overlay must stay clean.
    let dir = tempdir().unwrap();
    let path = dir.path().join("delta_overlay.apex");
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("value".to_string(), (0..8i64).collect::<Vec<i64>>());
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    // Row 5 (the sixth physical row) gets a cell-level update through the
    // DeltaStore, exactly like `UPDATE ... WHERE _id = 6`.
    storage.delta_store.write().update_cell(
        6,
        "value",
        crate::data::Value::Int64(900),
    );

    let expected: Vec<i64> = vec![0, 1, 2, 3, 4, 900, 6, 7];

    let without_id = storage.to_arrow_batch_mmap(None, false, None, false).unwrap().unwrap();
    let values = without_id
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert!(without_id.column_by_name("_id").is_none());
    assert_eq!(
        (0..values.len()).map(|i| values.value(i)).collect::<Vec<_>>(),
        expected,
    );

    let with_id = storage.to_arrow_batch_mmap(None, true, None, false).unwrap().unwrap();
    let values = with_id
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let ids = with_id
        .column_by_name("_id")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(
        (0..values.len()).map(|i| values.value(i)).collect::<Vec<_>>(),
        expected,
    );
    assert_eq!(
        (0..ids.len()).map(|i| ids.value(i)).collect::<Vec<_>>(),
        (1..=8).collect::<Vec<i64>>(),
    );
}

#[test]
fn parallel_numeric_range_scan_prunes_row_groups_by_zone_map() {
    // Clustered values give every row group a tight zone, so a range confined to
    // one group proves the others empty. The parallel fast path (no limit, many
    // uncompressed row groups) must prune them exactly like the sequential
    // fallback, and must still return the right rows across group boundaries.
    let dir = tempdir().unwrap();
    let path = dir.path().join("numeric_zone_prune.apex");
    let rows = 200_000usize;
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("ts".to_string(), (0..rows as i64).collect::<Vec<i64>>());
        int_cols.insert(
            "v".to_string(),
            (0..rows as i64).map(|i| i % 7).collect::<Vec<i64>>(),
        );
        let mut float_cols = HashMap::new();
        float_cols.insert("f".to_string(), (0..rows).map(|i| i as f64).collect());
        storage
            .insert_typed(
                int_cols,
                float_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    let row_groups = storage
        .get_or_load_footer()
        .unwrap()
        .expect("footer")
        .row_groups
        .len();
    assert!(row_groups >= 3, "fixture needs multiple row groups");

    let scan = |col: &str, low: f64, high: f64| {
        storage
            .scan_numeric_range_mmap(col, low, high, None)
            .unwrap()
            .expect("uncompressed row groups must take the range-scan path")
    };

    // Confined to one row group: only that group may contribute rows, and the
    // pruning counter must prove the other groups were skipped rather than
    // merely returning no matches.
    let before = zone_pruned_row_groups();
    let confined = scan("ts", 150_000.0, 150_009.0);
    assert_eq!(confined, (150_000..=150_009).collect::<Vec<usize>>());
    assert_eq!(
        zone_pruned_row_groups() - before,
        row_groups - 1,
        "every row group outside the requested range must be pruned"
    );

    // Straddling a row-group boundary: both neighbours must be scanned.
    let before = zone_pruned_row_groups();
    let boundary = scan("ts", 65_530.0, 65_545.0);
    assert_eq!(boundary, (65_530..=65_545).collect::<Vec<usize>>());
    assert_eq!(
        zone_pruned_row_groups() - before,
        row_groups - 2
    );

    // A range wider than every zone prunes nothing and still returns every row.
    let before = zone_pruned_row_groups();
    let all = scan("ts", -1.0, rows as f64 + 1.0);
    assert_eq!(all.len(), rows);
    assert_eq!(
        zone_pruned_row_groups() - before,
        0
    );

    // Float zones use the float comparison path.
    let floats = scan("f", 120_000.0, 120_004.0);
    assert_eq!(floats, (120_000..=120_004).collect::<Vec<usize>>());

    // ... and a float range strictly between two groups stays empty.
    assert!(scan("f", rows as f64 + 10.0, rows as f64 + 20.0).is_empty());
}

#[test]
fn column_value_at_maps_every_supported_column_kind() {
    // One shared converter serves the compressed and uncompressed point-read
    // fallbacks. It used to be two divergent matches, and the compressed one
    // returned NULL for dictionary strings and booleans.
    use crate::data::Value;

    let dict = ColumnData::StringDict {
        indices: vec![1, 2, 0],
        dict_offsets: vec![0, 3, 6],
        dict_data: b"abcxyz".to_vec(),
    };
    assert_eq!(column_value_at(&dict, 0), Value::String("abc".to_string()));
    assert_eq!(column_value_at(&dict, 1), Value::String("xyz".to_string()));
    assert_eq!(column_value_at(&dict, 2), Value::Null, "index 0 is the NULL slot");
    assert_eq!(column_value_at(&dict, 3), Value::Null, "out of range");

    let bools = ColumnData::Bool {
        data: vec![0b0000_0101],
        len: 3,
    };
    assert_eq!(column_value_at(&bools, 0), Value::Bool(true));
    assert_eq!(column_value_at(&bools, 1), Value::Bool(false));
    assert_eq!(column_value_at(&bools, 2), Value::Bool(true));
    assert_eq!(column_value_at(&bools, 3), Value::Null);

    let binary = ColumnData::Binary {
        offsets: vec![0, 2, 5],
        data: b"abxyz".to_vec(),
    };
    assert_eq!(column_value_at(&binary, 0), Value::Binary(b"ab".to_vec()));
    assert_eq!(column_value_at(&binary, 1), Value::Binary(b"xyz".to_vec()));
    assert_eq!(column_value_at(&binary, 2), Value::Null);

    let floats = ColumnData::Float64(vec![1.5, 2.5]);
    assert_eq!(column_value_at(&floats, 1), Value::Float64(2.5));
    assert_eq!(column_value_at(&floats, 2), Value::Null);

    // Vector columns read back as their raw row bytes, matching the Python
    // binding's `ColumnValue::FixedList` -> bytes conversion.
    // f32 rows: `dim` elements of four bytes each.
    let fixed = ColumnData::FixedList {
        data: (0..24).map(|i| i as u8).collect(),
        dim: 3,
    };
    assert_eq!(
        column_value_at(&fixed, 0),
        Value::FixedList((0..12).collect())
    );
    assert_eq!(
        column_value_at(&fixed, 1),
        Value::FixedList((12..24).collect())
    );
    assert_eq!(column_value_at(&fixed, 2), Value::Null, "out of range");
    assert_eq!(
        column_value_at(
            &ColumnData::FixedList {
                data: vec![0; 4],
                dim: 0
            },
            0
        ),
        Value::Null,
        "an unknown dimension has no rows"
    );

    let f16 = ColumnData::Float16List {
        data: (0..12).map(|i| i as u8).collect(),
        dim: 2,
    };
    assert_eq!(
        column_value_at(&f16, 2),
        Value::FixedList(vec![8, 9, 10, 11])
    );
    assert_eq!(column_value_at(&f16, 3), Value::Null, "out of range");
}

#[test]
fn filtered_numeric_agg_skips_aggregate_column_nulls_across_row_groups() {
    // The fused filtered-aggregation lane reads values straight from the row
    // group bodies. Its parallel branch (one aggregate column, several row
    // groups) used the filter column's NULL bitmap but not the aggregate
    // column's own, so a matching row with a NULL value still entered
    // COUNT(col)/SUM/MIN/MAX. The sequential single-row-group branch did check
    // it, so the defect only appeared once a table had several row groups.
    let dir = tempdir().unwrap();
    let path = dir.path().join("agg_nulls.apex");
    let schema = [
        ("a".to_string(), ColumnType::Int64),
        ("b".to_string(), ColumnType::Int64),
    ];
    let rows = 300_000usize;
    let nulls_every = 3usize;
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Fast,
            &schema,
        )
        .unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("a".to_string(), (0..rows as i64).collect::<Vec<i64>>());
        int_cols.insert("b".to_string(), (0..rows as i64).collect::<Vec<i64>>());
        let mut null_positions = HashMap::new();
        null_positions.insert(
            "b".to_string(),
            (0..rows).map(|i| i % nulls_every == 0).collect::<Vec<bool>>(),
        );
        storage
            .insert_typed_with_nulls_full(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                null_positions,
            )
            .unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    let footer = storage.get_or_load_footer().unwrap().expect("footer");
    assert!(
        footer.row_groups.len() >= 2,
        "the parallel branch needs several row groups, got {}",
        footer.row_groups.len()
    );

    let stats = storage
        .execute_filtered_numeric_agg_mmap("a", 0.0, f64::INFINITY, &["*", "b"])
        .unwrap()
        .expect("fused aggregation");
    let non_null = rows - rows.div_ceil(nulls_every);
    assert_eq!(stats[0].0, rows as i64, "COUNT(*) keeps every matching row");
    assert_eq!(stats[1].0, non_null as i64, "COUNT(b) skips NULL cells");
    assert_eq!(stats[1].1, (0..rows as i64).filter(|v| v % nulls_every as i64 != 0).sum::<i64>() as f64, "SUM(b)");
    assert_eq!(stats[1].2, 1.0, "MIN(b)");
    assert_eq!(stats[1].3, (rows - 1) as f64, "MAX(b)");
}

#[test]
fn scan_buffer_budget_env_parsing() {
    // The vector TopK lanes hold a whole-column buffer; `APEX_SCAN_BUFFER_MB` decides how
    // large that may get before the lane stands down and reads a windowed batch instead
    // (bounded RSS = the out-of-core contract). The parser is pure so the cases below do
    // not race with other tests over the process environment.
    use crate::storage::on_demand::parse_scan_buffer_budget;
    const MB: usize = 1024 * 1024;
    assert_eq!(
        parse_scan_buffer_budget(None),
        usize::MAX,
        "unlimited by default: a default cap made ordinary vector TopK stand down (+1479% \
         on Batch quantized TopK L2 (float16) in the full gate)"
    );
    assert_eq!(parse_scan_buffer_budget(Some("64")), 64 * MB);
    assert_eq!(parse_scan_buffer_budget(Some(" 8 ")), 8 * MB, "whitespace tolerated");
    assert_eq!(parse_scan_buffer_budget(Some("0")), usize::MAX, "0 = unlimited");
    assert_eq!(
        parse_scan_buffer_budget(Some("nonsense")),
        usize::MAX,
        "an unparsable value keeps the default (unlimited) rather than inventing a cap"
    );
    assert_eq!(
        parse_scan_buffer_budget(Some(&usize::MAX.to_string())),
        usize::MAX.saturating_mul(MB),
        "huge values saturate instead of wrapping"
    );
}

#[test]
fn deletion_state_lets_the_cached_dictionary_be_compacted() {
    // The cached string dictionary describes *physical* rows, so the dict-read path
    // (`read_cached_dict_columns`) compacts its codes onto the surviving rows instead of
    // standing down. That compaction needs exactly two numbers: the persisted physical base
    // and a bitmap whose set bits are the deleted rows. This pins both, because losing
    // either one silently sends every string GROUP BY back to plain strings (1M rows,
    // numeric+string key: 9.3 -> 38.3 ms, 4.13x).
    let dir = tempdir().unwrap();
    let path = dir.path().join("dict_compaction_state.apex");
    let schema = [
        ("k".to_string(), ColumnType::Int64),
        ("s".to_string(), ColumnType::String),
    ];
    let rows = 50_000usize;
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Fast,
            &schema,
        )
        .unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("k".to_string(), (0..rows as i64).collect::<Vec<i64>>());
        let mut string_cols = HashMap::new();
        string_cols.insert(
            "s".to_string(),
            (0..rows).map(|i| format!("c{}", i % 100)).collect::<Vec<String>>(),
        );
        storage
            .insert_typed(HashMap::new(), HashMap::new(), string_cols, HashMap::new(), HashMap::new())
            .unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    assert!(
        storage.persisted_deletion_state().unwrap().is_none(),
        "a table without deletions needs no compaction"
    );

    assert!(storage.delete(5_000));
    assert!(storage.delete(50_000));
    storage.save_delete_only().unwrap();

    let (physical_base, bitmap) = storage
        .persisted_deletion_state()
        .unwrap()
        .expect("a persisted deletion vector must expose its physical base");
    assert_eq!(physical_base, rows, "the physical base is the pre-delete row count");
    let deleted_bits: usize = bitmap.iter().map(|byte| byte.count_ones() as usize).sum();
    assert_eq!(deleted_bits, 2, "exactly the two deleted ids are marked");
    // What the dict path publishes: physical base minus the marked rows.
    assert_eq!(physical_base - deleted_bits, rows - 2);
    assert_eq!(storage.active_row_count() as usize, rows - 2);

    // The dictionary cache itself stays physical-space, which is why compaction is needed.
    if let Some((dict_strings, group_ids)) = storage.build_string_dict_cache("s").unwrap() {
        assert_eq!(
            group_ids.len(),
            physical_base,
            "the cached codes cover physical rows, so they must be compacted"
        );
        assert!(dict_strings.len() <= 4096);
    }
}

#[test]
fn numeric_group_by_skips_deleted_rows() {
    // The native numeric GROUP BY lane used to stand down whenever any row group had
    // deletions, which pushed every later grouping onto the generic path (1M rows, 100
    // deletes: 0.86 -> 9.68 ms, 11.3x). It now skips deleted rows inside its per-row loop
    // using the row group's deletion bitmap, so the per-group stats cover live rows only.
    let dir = tempdir().unwrap();
    let path = dir.path().join("numeric_group_deletes.apex");
    let schema = [
        ("g".to_string(), ColumnType::Int64),
        ("v".to_string(), ColumnType::Int64),
    ];
    let rows = 100_000usize;
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Fast,
            &schema,
        )
        .unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert(
            "g".to_string(),
            (0..rows as i64).map(|i| i % 10).collect::<Vec<i64>>(),
        );
        int_cols.insert("v".to_string(), vec![1i64; rows]);
        storage
            .insert_typed_with_nulls_full(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    let group_of = |id: i64| (id - 1) % 10;
    let baseline = storage
        .execute_numeric_group_agg_mmap("g", &[("v", false)], &[3], 0)
        .unwrap()
        .expect("native numeric GROUP BY");
    let expected_per_group = (rows / 10) as i64;
    for part in &baseline {
        assert_eq!(part.stats[0].0, expected_per_group, "group {}", part.key);
    }

    // Delete three rows spread across the id space (ids 3, 5_000, 99_999).
    for id in [3i64, 5_000, 99_999] {
        assert!(storage.delete(id as u64));
    }
    storage.save_delete_only().unwrap();
    let after = storage
        .execute_numeric_group_agg_mmap("g", &[("v", false)], &[3], 0)
        .unwrap()
        .expect("native numeric GROUP BY after deletes");
    let mut deleted_per_group: HashMap<i64, i64> = HashMap::new();
    for id in [3i64, 5_000, 99_999] {
        *deleted_per_group.entry(group_of(id)).or_default() += 1;
    }
    for part in &after {
        let removed = deleted_per_group.get(&part.key).copied().unwrap_or(0);
        assert_eq!(
            part.stats[0].0,
            expected_per_group - removed,
            "group {} must exclude deleted rows",
            part.key
        );
        assert_eq!(
            part.stats[0].1,
            (expected_per_group - removed) as f64,
            "group {} sum must exclude deleted rows",
            part.key
        );
    }
    assert_eq!(after.len(), baseline.len(), "no group may disappear");
}

#[test]
fn covering_range_count_reads_no_payload() {
    // A range that covers a row group entirely makes COUNT(*) the row count, so the
    // filter column need not be touched. Before this the lane evaluated the predicate
    // row by row (~13 ms per 1M rows), while a compressed table answered it in ~1.3 ms
    // through a batch path.
    let dir = tempdir().unwrap();
    let path = dir.path().join("covering_count.apex");
    let schema = [
        ("k".to_string(), ColumnType::Int64),
        ("v".to_string(), ColumnType::Int64),
    ];
    let rows = 300_000usize;
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Fast,
            &schema,
        )
        .unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("k".to_string(), (0..rows as i64).collect::<Vec<i64>>());
        int_cols.insert("v".to_string(), (0..rows as i64).collect::<Vec<i64>>());
        storage
            .insert_typed_with_nulls_full(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    // Covers every row group: count must be the row count, and no value column read.
    let covered = storage
        .execute_filtered_numeric_agg_mmap("k", 0.0, f64::INFINITY, &["*"])
        .unwrap()
        .expect("fused aggregation");
    assert_eq!(covered[0].0, rows as i64, "covering COUNT(*)");
    // A range that matches nothing is pruned by the zone map as before.
    let empty = storage
        .execute_filtered_numeric_agg_mmap("k", -10.0, -1.0, &["*"])
        .unwrap()
        .expect("fused aggregation");
    assert_eq!(empty[0].0, 0, "non-overlapping range");
    // Partially covering still evaluates the predicate per row and stays correct.
    let partial = storage
        .execute_filtered_numeric_agg_mmap("k", 100_000.0, f64::INFINITY, &["*"])
        .unwrap()
        .expect("fused aggregation");
    assert_eq!(partial[0].0, (rows - 100_000) as i64, "partial COUNT(*)");
    // Asking for a value column keeps count semantics and, because the range covers the
    // group and the column has no NULLs, accumulates with the tight loop.
    let with_value = storage
        .execute_filtered_numeric_agg_mmap("k", 0.0, f64::INFINITY, &["*", "v"])
        .unwrap()
        .expect("fused aggregation");
    assert_eq!(with_value[0].0, rows as i64, "COUNT(*) beside COUNT(v)");
    assert_eq!(with_value[1].0, rows as i64, "COUNT(v)");
    assert_eq!(
        with_value[1].1,
        (0..rows as i64).sum::<i64>() as f64,
        "SUM(v) over a covering range"
    );
    assert_eq!(with_value[1].2, 0.0, "MIN(v) over a covering range");
    assert_eq!(with_value[1].3, (rows - 1) as f64, "MAX(v) over a covering range");
    // A narrow integer column may be bit-packed; the covering path decodes it in
    // bulk (BITPACK payloads: [count][bit_width][min][packed]) instead of per row.
    let narrow = storage
        .execute_filtered_numeric_agg_mmap("k", 0.0, f64::INFINITY, &["*", "k"])
        .unwrap()
        .expect("fused aggregation");
    assert_eq!(narrow[0].0, rows as i64, "COUNT(*) beside COUNT(k)");
    assert_eq!(narrow[1].0, rows as i64, "COUNT(k)");
    assert_eq!(narrow[1].1, (0..rows as i64).sum::<i64>() as f64, "SUM(k)");
    assert_eq!(narrow[1].2, 0.0, "MIN(k)");
    assert_eq!(narrow[1].3, (rows - 1) as f64, "MAX(k)");

    // A partially covering range on a bit-packed filter column counts straight from
    // the packed bits for count-only aggregates (the per-row decoder cost ~7.9 ms per
    // 1M rows against ~0.4 ms here).
    let partial_count = storage
        .execute_filtered_numeric_agg_mmap("k", 100_000.0, f64::INFINITY, &["*"])
        .unwrap()
        .expect("fused aggregation");
    assert_eq!(partial_count[0].0, (rows - 100_000) as i64, "partial COUNT(*)");
    let between_count = storage
        .execute_filtered_numeric_agg_mmap("k", 0.0, 999.0, &["*"])
        .unwrap()
        .expect("fused aggregation");
    assert_eq!(between_count[0].0, 1_000, "BETWEEN COUNT(*)");

    // A covering count stays exact (and metadata-only) after deletions: the shortcut now
    // uses the footer's active-row count instead of requiring `deletion_count == 0`,
    // which used to push every later covering count onto the per-row path (4x).
    assert!(storage.delete(5_000));
    storage.save_delete_only().unwrap();
    let after_delete = storage
        .execute_filtered_numeric_agg_mmap("k", 0.0, f64::INFINITY, &["*"])
        .unwrap()
        .expect("fused aggregation");
    assert_eq!(
        after_delete[0].0,
        rows as i64 - 1,
        "a covering COUNT(*) must exclude the deleted row"
    );
    // A value aggregate must also exclude it (it keeps the per-row path when the row
    // group has deletions, so only its arithmetic matters here).
    let values_after_delete = storage
        .execute_filtered_numeric_agg_mmap("k", 0.0, f64::INFINITY, &["*", "v"])
        .unwrap()
        .expect("fused aggregation");
    assert_eq!(
        values_after_delete[0].0,
        rows as i64 - 1,
        "COUNT(*) beside a value aggregate"
    );

    // A partially covering range with both columns bit-packed streams them together,
    // so the per-row header re-parse and bit walk disappear.
    let partial_values = storage
        .execute_filtered_numeric_agg_mmap("k", 100_000.0, f64::INFINITY, &["*", "v"])
        .unwrap()
        .expect("fused aggregation");
    assert_eq!(partial_values[0].0, (rows - 100_000) as i64, "partial COUNT(*)");
    assert_eq!(partial_values[1].0, (rows - 100_000) as i64, "partial COUNT(v)");
    assert_eq!(
        partial_values[1].1,
        (100_000..rows as i64).sum::<i64>() as f64,
        "partial SUM(v)"
    );
    assert_eq!(partial_values[1].2, 100_000.0, "partial MIN(v)");
    assert_eq!(partial_values[1].3, (rows - 1) as f64, "partial MAX(v)");
    let between_values = storage
        .execute_filtered_numeric_agg_mmap("k", 0.0, 999.0, &["*", "v"])
        .unwrap()
        .expect("fused aggregation");
    assert_eq!(between_values[1].0, 1_000, "BETWEEN COUNT(v)");
    assert_eq!(between_values[1].1, (0..1_000).sum::<i64>() as f64, "BETWEEN SUM(v)");

    // A partially covering range keeps the per-row path for value aggregates.
    let partial_value = storage
        .execute_filtered_numeric_agg_mmap("k", 100_000.0, f64::INFINITY, &["*", "v"])
        .unwrap()
        .expect("fused aggregation");
    assert_eq!(partial_value[0].0, (rows - 100_000) as i64, "partial COUNT(*)");
    assert_eq!(
        partial_value[1].1,
        (100_000..rows as i64).sum::<i64>() as f64,
        "partial SUM(v)"
    );
}

#[test]
fn string_tail_zone_prunes_equal_length_shared_prefix_rows() {
    // Timestamp/UUID-shaped values are all the same length and share their
    // first eight bytes, so neither the byte-length zone nor the first-eight
    // value zone can tell them apart. The tail zone (second eight bytes) is
    // what makes such a column prunable.
    use crate::storage::{ScanComparison, ScanPredicate, ScanPredicateExpr, ScanValue};
    let dir = tempdir().unwrap();
    let path = dir.path().join("string_tail_zone.apex");
    let schema = [
        ("code".to_string(), ColumnType::String),
        ("pad".to_string(), ColumnType::Int64),
    ];
    let rows = 400_000usize;
    let codes: Vec<String> = (0..rows)
        .map(|i| format!("2026-09-26T{i:06}"))
        .collect();
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Fast,
            &schema,
        )
        .unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("pad".to_string(), (0..rows as i64).collect::<Vec<i64>>());
        let mut string_cols = HashMap::new();
        string_cols.insert("code".to_string(), codes.clone());
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                string_cols,
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    let footer = storage.get_or_load_footer().unwrap().expect("footer");
    assert!(footer.row_groups.len() >= 3, "fixture needs multiple groups");
    let code_idx = footer.schema.get_index("code").unwrap();
    for rg_i in 0..footer.row_groups.len() {
        let zones: Vec<_> = footer.zone_maps[rg_i]
            .iter()
            .filter(|z| z.col_idx as usize == code_idx)
            .collect();
        assert_eq!(zones.len(), 3);
        // Every value is 17 bytes ("2026-09-26T" + six digits) and starts with
        // the same eight bytes: only the tail entry varies across the group.
        assert_eq!(zones[0].min_bits, 17);
        assert_eq!(zones[0].max_bits, 17);
        assert_eq!(zones[1].min_bits, zones[1].max_bits);
        assert!(zones[1].is_string_prefix);
        assert!(zones[2].min_bits < zones[2].max_bits, "row group {rg_i}");
        assert!(zones[2].is_string_tail);
    }

    let scan = |value: &str| {
        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::Compare {
            column: "code".to_string(),
            op: ScanComparison::Eq,
            value: ScanValue::String(value.to_string()),
        });
        let before = predicate_pruned_row_groups();
        let streams = storage
            .scan_rg_batches_ranges(Some(&["code"]), false, Some(&predicate), 1)
            .unwrap()
            .expect("persisted table must stream batches");
        let mut seen = 0usize;
        for stream in streams {
            for batch in stream {
                seen += batch.unwrap().num_rows();
            }
        }
        (seen, predicate_pruned_row_groups() - before)
    };

    // The first and last stored values live in the first and last group.
    let first = scan(&codes[0]);
    let last = scan(&codes[rows - 1]);
    assert!(first.0 > 0 && first.1 >= footer.row_groups.len() - 2);
    assert!(last.0 > 0 && last.1 >= footer.row_groups.len() - 2);
    assert!(
        first.1 > footer.row_groups.len() / 2,
        "the tail zone must prune most groups (pruned={})",
        first.1
    );
    // A missing value that shares all sixteen bytes with a stored one differs
    // only in its last byte, outside the stored window: the zones cannot prove
    // it absent, so exactly the twin's group survives (and the predicate still
    // returns no row for it at the SQL level).
    let stored = &codes[rows / 2];
    let missing = format!("{}9", &stored[..16]);
    assert_ne!(&missing, stored);
    let (rows_seen, pruned) = scan(&missing);
    assert_eq!(
        pruned,
        footer.row_groups.len() - 1,
        "only the sixteen-byte twin's group can survive"
    );
    assert!(rows_seen > 0, "the twin group is scanned, not skipped");

    // A value whose tail window lies beyond every stored one prunes every group.
    let beyond = "2026-09-26T999999";
    let (rows_seen, pruned) = scan(beyond);
    assert_eq!(rows_seen, 0, "no group can hold it");
    assert_eq!(pruned, footer.row_groups.len());

    // Ordering comparisons must not use the tail entry: a range whose bounds
    // sit between stored tails still matches every stored value.
    let predicate = ScanPredicateExpr::Predicate(ScanPredicate::Compare {
        column: "code".to_string(),
        op: ScanComparison::Gt,
        value: ScanValue::String("2026-09-26T000000".to_string()),
    });
    let before = predicate_pruned_row_groups();
    let streams = storage
        .scan_rg_batches_ranges(Some(&["code"]), false, Some(&predicate), 1)
        .unwrap()
        .expect("persisted table must stream batches");
    let mut seen = 0usize;
    for stream in streams {
        for batch in stream {
            seen += batch.unwrap().num_rows();
        }
    }
    assert_eq!(seen, rows, "an ordering comparison skips no group");
    assert_eq!(
        predicate_pruned_row_groups() - before,
        0,
        "the tail zone must not prune an ordering comparison"
    );
}

#[test]
fn string_value_zones_prune_rows_whose_length_zone_cannot() {
    // A fixed-width string column gives the byte-length zone nothing to prune:
    // every value is eight bytes, so only the lexicographic value range can
    // skip row groups. This is the case the value zone exists for.
    use crate::storage::{ScanComparison, ScanPredicate, ScanPredicateExpr, ScanValue};
    let dir = tempdir().unwrap();
    let path = dir.path().join("string_value_zone.apex");
    let schema = [
        ("code".to_string(), ColumnType::String),
        ("pad".to_string(), ColumnType::Int64),
    ];
    let rows = 400_000usize;
    let bucket = 131_072usize;
    let codes: Vec<String> = (0..rows)
        .map(|i| format!("k{:07}", i / bucket))
        .collect();
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Fast,
            &schema,
        )
        .unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("pad".to_string(), (0..rows as i64).collect::<Vec<i64>>());
        let mut string_cols = HashMap::new();
        string_cols.insert("code".to_string(), codes.clone());
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                string_cols,
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    let footer = storage.get_or_load_footer().unwrap().expect("footer");
    assert!(footer.row_groups.len() >= 3, "fixture needs multiple groups");
    let code_idx = footer.schema.get_index("code").unwrap();
    for rg_i in 0..footer.row_groups.len() {
        let entries: Vec<_> = footer.zone_maps[rg_i]
            .iter()
            .filter(|z| z.col_idx as usize == code_idx)
            .collect();
        // The first eight bytes already separate the buckets, and every value is
        // exactly eight bytes, so the tail window is a constant all-zero range:
        // the writer omits it (it would repeat what the value zone says while
        // costing footer bytes and a reader check on every scan).
        assert_eq!(entries.len(), 2, "length zone + value zone");
        // Uniform width: the length zone spans exactly one value.
        assert_eq!(entries[0].min_bits, 8);
        assert_eq!(entries[0].max_bits, 8);
        assert!(!entries[0].is_string_prefix && !entries[0].is_string_tail);
        // Uniform bucket: the value zone spans exactly one prefix key.
        assert_eq!(entries[1].min_bits, entries[1].max_bits);
        assert!(entries[1].is_string_prefix);
        assert!(!entries[1].is_string_tail);
    }

    let scan = |value: &str| {
        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::Compare {
            column: "code".to_string(),
            op: ScanComparison::Eq,
            value: ScanValue::String(value.to_string()),
        });
        let before = predicate_pruned_row_groups();
        let streams = storage
            .scan_rg_batches_ranges(Some(&["code"]), false, Some(&predicate), 1)
            .unwrap()
            .expect("persisted table must stream batches");
        let mut seen = 0usize;
        for stream in streams {
            for batch in stream {
                seen += batch.unwrap().num_rows();
            }
        }
        (seen, predicate_pruned_row_groups() - before)
    };

    let probe = crate::storage::on_demand::string_prefix_key(b"k0000001");
    let matching_groups = footer
        .row_groups
        .iter()
        .enumerate()
        .filter(|(rg_i, _)| {
            footer.zone_maps[*rg_i].iter().any(|z| {
                z.is_string_prefix
                    && z.col_idx as usize == code_idx
                    && z.min_bits == probe
                    && z.max_bits == probe
            })
        })
        .count();
    assert!(matching_groups > 0 && matching_groups < footer.row_groups.len());
    let (rows_seen, pruned) = scan("k0000001");
    assert_eq!(rows_seen, bucket);
    assert_eq!(pruned, footer.row_groups.len() - matching_groups);
}

#[test]
fn string_zone_maps_prune_row_groups_by_value() {
    // String zone maps used to store byte lengths, which no predicate could
    // use. They now store the lexicographic value range, so a clustered string
    // column can skip row groups for `=`, `IN` and range predicates.
    use crate::storage::{ScanComparison, ScanPredicate, ScanPredicateExpr, ScanValue};
    let dir = tempdir().unwrap();
    let path = dir.path().join("string_zone.apex");
    let schema = [
        ("name".to_string(), ColumnType::String),
        ("pad".to_string(), ColumnType::Int64),
    ];
    let rows = 400_000usize;
    // 131_072-row buckets make every adaptive row group uniformly in one
    // lexical bucket (see `compute_adaptive_row_group_size`).
    let bucket = 131_072usize;
    // The distinguishing part must fall inside the eight-byte prefix the zone
    // stores; values that share their first eight bytes can never be told
    // apart by it (that is the conservative case, covered by a unit test).
    let names: Vec<String> = (0..rows)
        .map(|i| format!("b{:07}-row{i}", i / bucket))
        .collect();
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Fast,
            &schema,
        )
        .unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("pad".to_string(), (0..rows as i64).collect::<Vec<i64>>());
        let mut string_cols = HashMap::new();
        string_cols.insert("name".to_string(), names.clone());
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                string_cols,
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    let footer = storage.get_or_load_footer().unwrap().expect("footer");
    assert!(footer.row_groups.len() >= 3, "fixture needs multiple groups");
    let name_idx = footer.schema.get_index("name").unwrap();
    for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
        let entries: Vec<_> = footer.zone_maps[rg_i]
            .iter()
            .filter(|z| z.col_idx as usize == name_idx)
            .collect();
        assert_eq!(
            entries.len(),
            3,
            "a string column carries the byte-length zone, the value zone and the tail zone"
        );
        let length_zone = entries[0];
        let value_zone = entries[1];
        let tail_zone = entries[2];
        // The length entry comes first so readers that predate the value-range
        // flags keep resolving the column's zone with the legacy meaning.
        assert!(!length_zone.is_string_prefix, "length zone must come first");
        assert!(!length_zone.is_string_tail);
        assert!(value_zone.is_string_prefix, "value semantics must be declared");
        assert!(!value_zone.is_string_tail);
        assert!(tail_zone.is_string_tail, "tail semantics must be declared");
        assert!(!tail_zone.is_string_prefix);
        for zone in [length_zone, value_zone, tail_zone] {
            assert!(!zone.is_float);
        }
        // Values are "b{bucket:07}-row{index}": the byte length varies with the
        // row index inside the group but never drops below the fixed prefix.
        assert!(
            (13..=18).contains(&length_zone.min_bits),
            "row group {rg_i} rows={} length zone=({}, {})",
            rg_meta.row_count,
            length_zone.min_bits,
            length_zone.max_bits
        );
        assert!(length_zone.max_bits >= length_zone.min_bits);
        // Each group lies inside one 131_072-row bucket, so the eight-byte
        // prefix (and therefore the value zone) is constant.
        assert_eq!(value_zone.min_bits, value_zone.max_bits, "row group {rg_i}");
        assert!(rg_meta.row_count > 0);
    }

    let scan = |value: &str, op: ScanComparison| {
        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::Compare {
            column: "name".to_string(),
            op,
            value: ScanValue::String(value.to_string()),
        });
        let before = predicate_pruned_row_groups();
        let streams = storage
            .scan_rg_batches_ranges(Some(&["name"]), false, Some(&predicate), 1)
            .unwrap()
            .expect("persisted table must stream batches");
        let mut seen = 0usize;
        for stream in streams {
            for batch in stream {
                seen += batch.unwrap().num_rows();
            }
        }
        (seen, predicate_pruned_row_groups() - before)
    };

    // The bucket value exists only in the row groups that carry its bucket
    // prefix; the tail range then narrows that set to the group(s) whose second
    // eight bytes can hold the value.
    let probe = crate::storage::on_demand::string_prefix_key(b"b0000001-row");
    let matching_groups = footer
        .row_groups
        .iter()
        .enumerate()
        .filter(|(rg_i, _)| {
            footer.zone_maps[*rg_i]
                .iter()
                .any(|z| z.is_string_prefix && z.col_idx as usize == name_idx
                    && z.min_bits == probe
                    && z.max_bits == probe)
        })
        .count();
    assert!(matching_groups > 0 && matching_groups < footer.row_groups.len());
    let existing = format!("b0000001-row{}", bucket);
    let probe_tail = crate::storage::on_demand::string_tail_key(existing.as_bytes());
    let exact_groups: Vec<&_> = footer
        .row_groups
        .iter()
        .enumerate()
        .filter(|(rg_i, _)| {
            let zones: Vec<_> = footer.zone_maps[*rg_i]
                .iter()
                .filter(|z| z.col_idx as usize == name_idx)
                .collect();
            zones.iter().all(|z| {
                if z.is_string_tail {
                    probe_tail >= z.min_bits && probe_tail <= z.max_bits
                } else if z.is_string_prefix {
                    probe >= z.min_bits && probe <= z.max_bits
                } else {
                    (existing.len() as i64) >= z.min_bits
                        && (existing.len() as i64) <= z.max_bits
                }
            })
        })
        .map(|(_, meta)| meta)
        .collect();
    assert_eq!(exact_groups.len(), 1, "the tail zone isolates the owning group");
    assert!(
        matching_groups > exact_groups.len(),
        "the tail zone must prune beyond the shared prefix"
    );
    let expected_rows: usize = exact_groups.iter().map(|m| m.row_count as usize).sum();
    let (rows_seen, pruned) = scan(&existing, ScanComparison::Eq);
    assert_eq!(rows_seen, expected_rows, "only the owning group may survive");
    assert_eq!(pruned, footer.row_groups.len() - exact_groups.len());

    // A value outside every bucket prunes every group.
    let (rows_seen, _) = scan("zzz", ScanComparison::Eq);
    assert_eq!(rows_seen, 0);

    // The byte-length zone is decisive on its own: a probe whose length no
    // group can hold proves every group empty even when its eight-byte prefix
    // matches a bucket.
    let too_long = format!("b0000001-row{}x", rows);
    let (rows_seen, pruned) = scan(&too_long, ScanComparison::Eq);
    assert_eq!(rows_seen, 0);
    assert_eq!(pruned, footer.row_groups.len());
}

#[test]
fn legacy_length_string_zones_never_prune_by_order() {
    // Files written before the value-prefix flag store byte lengths in the
    // string zone. Those numbers must never be compared against string values
    // for an ordering comparison, and equality may only prune when the length is
    // provably absent from the range.
    use crate::storage::on_demand::{OnDemandSchema, RgColumnZoneMap, RowGroupMeta, V4Footer};
    use crate::storage::{ScanComparison, ScanPredicate, ScanValue};

    let mut schema = OnDemandSchema::new();
    schema.add_column("name", ColumnType::String);
    let footer = V4Footer {
        schema,
        row_groups: vec![RowGroupMeta {
            offset: 0,
            data_size: 0,
            row_count: 8,
            min_id: 1,
            max_id: 8,
            deletion_count: 0,
        }],
        zone_maps: vec![vec![RgColumnZoneMap {
            col_idx: 0,
            min_bits: 3,
            max_bits: 7,
            has_nulls: false,
            is_float: false,
            is_string_prefix: false,
            is_string_tail: false,
        }]],
        col_offsets: Vec::new(),
    };
    let disjoint = |op: ScanComparison, value: &str| {
        let predicate = ScanPredicate::Compare {
            column: "name".to_string(),
            op,
            value: ScanValue::String(value.to_string()),
        };
        zone_map_rg_disjoint_for_test(
            0,
            &footer,
            &crate::storage::ScanPredicateExpr::Predicate(predicate),
        )
    };
    // Lengths 1 and 12 fall outside the stored [3, 7] byte-length range, so no
    // value of that length can be present.
    for value in ["a", "zzzzzzzzzzzz"] {
        assert!(disjoint(ScanComparison::Eq, value), "length proves {value:?} absent");
    }
    // A length inside the range proves nothing about the value.
    for value in ["abc", "abcd", "abcdefg"] {
        assert!(!disjoint(ScanComparison::Eq, value), "must not prune {value:?}");
    }
    // Lengths say nothing about lexicographic order.
    for op in [
        ScanComparison::Lt,
        ScanComparison::Le,
        ScanComparison::Gt,
        ScanComparison::Ge,
        ScanComparison::NotEq,
    ] {
        for value in ["a", "zzzzzzzzzzzz", "abc"] {
            assert!(
                !disjoint(op, value),
                "length-semantics zones must not prune for {op:?} {value:?}"
            );
        }
    }
}

#[test]
fn bool_zone_maps_prune_row_groups_in_the_batch_stream() {
    // Bool columns used to carry no zone map at all, so `WHERE flag = true`
    // scanned every row group. The writer now records the observed bit range
    // (false < true) and the batch stream prunes a group whose range cannot
    // contain the requested value.
    use crate::storage::{ScanComparison, ScanPredicate, ScanPredicateExpr, ScanValue};
    let dir = tempdir().unwrap();
    let path = dir.path().join("bool_zone.apex");
    let schema = [
        ("flag".to_string(), ColumnType::Bool),
        ("pad".to_string(), ColumnType::Int64),
    ];
    let rows = 400_000usize;
    // Runs of 131_072 rows: that is the largest adaptive row-group size, and
    // the smaller choices (65_536 / 32_768) divide it, so every row group is
    // uniformly true or uniformly false whichever size is picked.
    let run = 131_072usize;
    let flags: Vec<bool> = (0..rows).map(|i| (i / run) % 2 == 0).collect();
    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Fast,
            &schema,
        )
        .unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("pad".to_string(), (0..rows as i64).collect::<Vec<i64>>());
        let mut bool_cols = HashMap::new();
        bool_cols.insert("flag".to_string(), flags.clone());
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                bool_cols,
            )
            .unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    let footer = storage.get_or_load_footer().unwrap().expect("footer");
    assert!(
        footer.row_groups.len() >= 3,
        "fixture needs multiple row groups"
    );
    let flag_idx = footer.schema.get_index("flag").unwrap();

    // Every row group must carry a Bool zone, and the fixture must make those
    // zones uniform so pruning is provable from the file itself.
    let mut all_true_rows = 0usize;
    let mut all_false_rows = 0usize;
    let mut all_true_groups = 0usize;
    let mut all_false_groups = 0usize;
    for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
        let zone = footer.zone_maps[rg_i]
            .iter()
            .find(|z| z.col_idx as usize == flag_idx)
            .map(|z| (z.min_bits, z.max_bits, z.is_float))
            .expect("every row group must carry a zone map for the Bool column");
        assert_eq!(zone.2, false, "a Bool zone is never a float zone");
        match zone {
            (1, 1, _) => {
                all_true_rows += rg_meta.row_count as usize;
                all_true_groups += 1;
            }
            (0, 0, _) => {
                all_false_rows += rg_meta.row_count as usize;
                all_false_groups += 1;
            }
            other => panic!("row group {rg_i} is not uniform: {other:?}"),
        }
    }

    let scan_rows = |value: bool| {
        let predicate = ScanPredicateExpr::Predicate(ScanPredicate::Compare {
            column: "flag".to_string(),
            op: ScanComparison::Eq,
            value: ScanValue::Bool(value),
        });
        let before = predicate_pruned_row_groups();
        let streams = storage
            .scan_rg_batches_ranges(Some(&["flag"]), false, Some(&predicate), 1)
            .unwrap()
            .expect("persisted table must stream batches");
        let mut seen = 0usize;
        for stream in streams {
            for batch in stream {
                seen += batch.unwrap().num_rows();
            }
        }
        (seen, predicate_pruned_row_groups() - before)
    };

    let (true_rows, true_pruned) = scan_rows(true);
    assert_eq!(true_rows, all_true_rows);
    assert_eq!(true_pruned, all_false_groups);

    let (false_rows, false_pruned) = scan_rows(false);
    assert_eq!(false_rows, all_false_rows);
    assert_eq!(false_pruned, all_true_groups);
    assert!(all_false_groups > 0 && all_true_groups > 0);
}

#[test]
fn projection_without_id_still_applies_a_preexisting_delta() {
    // The overlay locates rows by ID, so a scan that starts with a pending
    // delta must collect them even when `_id` is not projected.
    let dir = tempdir().unwrap();
    let path = dir.path().join("skip_ids_delta.apex");
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("value".to_string(), (0..8i64).collect::<Vec<i64>>());
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    storage
        .delta_store
        .write()
        .update_cell(6, "value", crate::data::Value::Int64(900));

    let batch = storage
        .to_arrow_batch_mmap(Some(&["value"]), false, None, false)
        .unwrap()
        .unwrap();
    let values = batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(
        (0..values.len()).map(|i| values.value(i)).collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4, 900, 6, 7],
    );
}

#[test]
fn unsupported_format_version_is_rejected() {
    // Every V4 code path is gated on an exact version match, so an unknown
    // version must fail at parse time instead of falling into legacy reads.
    let dir = tempdir().unwrap();
    let path = dir.path().join("future_version.apex");
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("v".to_string(), (0..16i64).collect::<Vec<i64>>());
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let mut bytes = std::fs::read(&path).unwrap();
    let mut header = [0u8; HEADER_SIZE];
    header.copy_from_slice(&bytes[..HEADER_SIZE]);
    assert!(OnDemandHeader::from_bytes(&header).is_ok(), "current files must parse");

    // Patch the on-disk version to one this build has never seen.
    bytes[8..12].copy_from_slice(&99u32.to_le_bytes());
    std::fs::write(&path, &bytes).unwrap();
    let mut patched = [0u8; HEADER_SIZE];
    patched.copy_from_slice(&bytes[..HEADER_SIZE]);
    let error = OnDemandHeader::from_bytes(&patched).expect_err("version 99 must be rejected");
    let message = error.to_string();
    assert!(message.contains("unsupported"), "{message}");
    assert!(message.contains("99"), "{message}");

    // Opening the file surfaces the same refusal rather than reading legacy.
    assert!(
        OnDemandStorage::open(&path).is_err(),
        "opening a newer-version file must fail"
    );
}

#[test]
fn newer_row_group_encoding_version_is_rejected() {
    // Column-body rules are versioned per Row Group; a newer revision must be
    // refused, not interpreted with this build's rules.
    let dir = tempdir().unwrap();
    let path = dir.path().join("future_rg_encoding.apex");
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("v".to_string(), (0..2000i64).collect::<Vec<i64>>());
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let mut bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes[HEADER_SIZE + 29], RG_ENCODING_VERSION);
    bytes[HEADER_SIZE + 29] = RG_ENCODING_VERSION + 1;
    std::fs::write(&path, &bytes).unwrap();

    let storage = OnDemandStorage::open(&path).unwrap();
    let error = storage
        .to_arrow_batch(None, false)
        .expect_err("a newer Row Group encoding must be refused");
    let message = error.to_string();
    assert!(message.contains("encoding version"), "{message}");
}

#[test]
fn width_fidelity_stores_float32_at_four_bytes_and_round_trips() {
    // A Float32 column is declared at 4 bytes; storing it at 8 bytes doubles
    // its I/O for no precision the declaration asked for. The encoding is
    // opt-in because builds predating it cannot read the narrow payload.
    let dir = tempdir().unwrap();
    let narrow_path = dir.path().join("narrow.apex");
    let wide_path = dir.path().join("wide.apex");
    let schema = [("v".to_string(), ColumnType::Float32)];
    let values: Vec<f64> = (0..4096).map(|i| i as f64 * 0.5).collect();

    let narrow_size = {
        let mut storage =
            OnDemandStorage::create_with_schema_and_durability(
                &narrow_path,
                crate::storage::DurabilityLevel::Fast,
                &schema,
            )
            .unwrap();
        assert!(
            storage.set_width_fidelity(true).unwrap(),
            "an empty table accepts the opt-in"
        );
        let mut float_cols = HashMap::new();
        float_cols.insert("v".to_string(), values.clone());
        storage
            .insert_typed(
                HashMap::new(),
                float_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
        std::fs::metadata(&narrow_path).unwrap().len()
    };

    let wide_size = {
        let storage =
            OnDemandStorage::create_with_schema_and_durability(
                &wide_path,
                crate::storage::DurabilityLevel::Fast,
                &schema,
            )
            .unwrap();
        assert!(
            storage.set_width_fidelity(false).unwrap(),
            "opting out keeps the V4-compatible full-width layout"
        );
        let mut float_cols = HashMap::new();
        float_cols.insert("v".to_string(), values.clone());
        let storage = storage;
        storage
            .insert_typed(
                HashMap::new(),
                float_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
        std::fs::metadata(&wide_path).unwrap().len()
    };

    // The narrow file must shed the whole 4-bytes-per-value difference.
    let saved = wide_size.saturating_sub(narrow_size);
    assert_eq!(
        saved,
        values.len() as u64 * 4,
        "wide {wide_size} vs narrow {narrow_size} must differ by 4 bytes/value"
    );

    let bytes = std::fs::read(&narrow_path).unwrap();
    assert_eq!(
        u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
        FORMAT_VERSION_V5,
        "a narrow Float32 payload is a V5 file"
    );
    assert_eq!(
        u32::from_le_bytes(
            std::fs::read(&wide_path).unwrap()[8..12].try_into().unwrap()
        ),
        FORMAT_VERSION_V4,
        "the opt-out keeps the V4 layout (and V4 file version)"
    );

    // Values come back at f32 precision, widened exactly.
    let storage = OnDemandStorage::open(&narrow_path).unwrap();
    assert!(storage.width_fidelity(), "the flag persists in the header");
    let result = storage.read_columns(Some(&["v"]), 0, None).unwrap();
    match &result["v"] {
        ColumnData::Float64(read) => {
            assert_eq!(read.len(), values.len());
            for (index, value) in read.iter().enumerate() {
                assert_eq!(*value, (values[index] as f32) as f64);
            }
        }
        other => panic!("unexpected column: {other:?}"),
    }

    // The default path stores the same values at full width.
    let wide = OnDemandStorage::open(&wide_path).unwrap();
    assert!(!wide.width_fidelity());
    let result = wide.read_columns(Some(&["v"]), 0, None).unwrap();
    match &result["v"] {
        ColumnData::Float64(read) => assert_eq!(read, &values),
        other => panic!("unexpected column: {other:?}"),
    }
}

#[test]
fn width_fidelity_falls_back_when_a_value_exceeds_float32() {
    // `1e39` does not fit in f32; narrowing it would silently become an
    // infinity, so the writer keeps the full-width form for that row group.
    let dir = tempdir().unwrap();
    let path = dir.path().join("narrow_overflow.apex");
    let schema = [("v".to_string(), ColumnType::Float32)];
    let values = vec![1.0, 2.0, 1e39, 4.0];

    let mut storage = OnDemandStorage::create_with_schema_and_durability(
        &path,
        crate::storage::DurabilityLevel::Fast,
        &schema,
    )
    .unwrap();
    assert!(storage.set_width_fidelity(true).unwrap());
    let mut float_cols = HashMap::new();
    float_cols.insert("v".to_string(), values.clone());
    storage
        .insert_typed(
            HashMap::new(),
            float_cols,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save().unwrap();
    drop(storage);

    let storage = OnDemandStorage::open(&path).unwrap();
    let result = storage.read_columns(Some(&["v"]), 0, None).unwrap();
    match &result["v"] {
        ColumnData::Float64(read) => assert_eq!(read, &values),
        other => panic!("unexpected column: {other:?}"),
    }
}

#[test]
fn width_fidelity_indexed_extraction_reads_every_projected_column() {
    // `extract_rows_by_indices_to_arrow` decodes matching columns at index in
    // its parallel lane. A narrow Float32 payload has no 8-byte stride, so an
    // encoding missing from that lane silently produced an all-NULL column:
    // every filter over it then matched nothing and the query returned empty.
    let dir = tempdir().unwrap();
    let path = dir.path().join("narrow_extract.apex");
    let schema = [
        ("score".to_string(), ColumnType::Float32),
        ("age".to_string(), ColumnType::Int64),
        ("city".to_string(), ColumnType::String),
    ];
    let rows = 4096usize;
    let scores: Vec<f64> = (0..rows).map(|i| (i % 97) as f64 * 0.5).collect();
    let ages: Vec<i64> = (0..rows).map(|i| (i % 13) as i64).collect();
    let cities: Vec<String> = (0..rows).map(|i| ["n", "s", "e"][i % 3].to_string()).collect();

    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Fast,
            &schema,
        )
        .unwrap();
        assert!(storage.set_width_fidelity(true).unwrap());
        let mut int_cols = HashMap::new();
        int_cols.insert("age".to_string(), ages.clone());
        let mut float_cols = HashMap::new();
        float_cols.insert("score".to_string(), scores.clone());
        let mut string_cols = HashMap::new();
        string_cols.insert("city".to_string(), cities.clone());
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

    // More than 500 selected rows and more than one projected column is what
    // makes the extraction take the parallel lane.
    let storage = OnDemandStorage::open(&path).unwrap();
    let indices: Vec<usize> = (0..rows).step_by(4).collect();
    assert!(indices.len() > 500);
    let batch = storage
        .extract_rows_by_indices_to_arrow(&indices, Some(&["score", "age"]))
        .unwrap()
        .expect("indexed extraction must serve two numeric columns");

    assert_eq!(batch.num_rows(), indices.len());
    assert_eq!(batch.num_columns(), 2);
    let score_array = batch
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    let age_array = batch
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    for (out_idx, &row) in indices.iter().enumerate() {
        assert_eq!(
            score_array.value(out_idx),
            (scores[row] as f32) as f64,
            "narrow Float32 value at row {row} must survive indexed extraction"
        );
        assert_eq!(age_array.value(out_idx), ages[row]);
        assert!(
            !arrow::array::Array::is_null(score_array, out_idx),
            "no value may be dropped"
        );
    }
}

#[test]
fn width_fidelity_point_lookup_keeps_the_rcix_fast_lane() {
    // `retrieve_rcix` decodes one cell per column from the footer's column
    // offsets. An encoding it does not know made it report "no such row", so a
    // `_id = X` query on a table with a narrow Float32 column dropped out of
    // the O(1) point-lookup lane into a full scan (measured 0.003 ms -> 0.12 ms
    // on a 200k-row table). The value must come back and the lane must survive.
    let dir = tempdir().unwrap();
    let path = dir.path().join("narrow_point.apex");
    let schema = [
        ("score".to_string(), ColumnType::Float32),
        ("age".to_string(), ColumnType::Int64),
    ];
    let rows = 512usize;
    let scores: Vec<f64> = (0..rows).map(|i| i as f64 * 0.5).collect();
    let ages: Vec<i64> = (0..rows).map(|i| (i % 13) as i64).collect();

    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Fast,
            &schema,
        )
        .unwrap();
        assert!(storage.set_width_fidelity(true).unwrap());
        let mut int_cols = HashMap::new();
        int_cols.insert("age".to_string(), ages.clone());
        let mut float_cols = HashMap::new();
        float_cols.insert("score".to_string(), scores.clone());
        storage
            .insert_typed(
                int_cols,
                float_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    let id = 7u64;
    let expected_score = (scores[(id - 1) as usize] as f32) as f64;

    let row = storage
        .retrieve_rcix(id)
        .unwrap()
        .expect("the RCIX point lane must serve a narrow Float32 column");
    let score = row
        .iter()
        .find(|(name, _)| name == "score")
        .map(|(_, value)| value.clone())
        .expect("score column present");
    assert_eq!(score, crate::data::Value::Float64(expected_score));
    let age = row
        .iter()
        .find(|(name, _)| name == "age")
        .map(|(_, value)| value.clone())
        .expect("age column present");
    assert_eq!(age, crate::data::Value::Int64(ages[(id - 1) as usize]));

    // The projected variant and the fallback cell reader must agree.
    let projected = storage
        .retrieve_rcix_projected(id, &["age", "score", "_id"])
        .unwrap()
        .expect("projected point lane must serve a narrow Float32 column");
    let projected_score = projected
        .iter()
        .find(|(name, _)| name == "score")
        .map(|(_, value)| value.clone())
        .expect("score column present");
    assert_eq!(projected_score, crate::data::Value::Float64(expected_score));

    let values = storage
        .read_row_by_id_values(id)
        .unwrap()
        .expect("cell reader must find the row");
    let read_score = values
        .iter()
        .find(|(name, _)| name == "score")
        .map(|(_, value)| value.clone())
        .expect("score column present");
    assert_eq!(read_score, crate::data::Value::Float64(expected_score));
}

#[test]
fn indexed_extraction_decodes_rle_columns_at_index() {
    // RLE Int64 used to have no at-index decoder in the parallel lane, so the
    // lane declined (correct, but a serial fallback). It now decodes the run
    // list at index; the values must stay exact either way.
    let dir = tempdir().unwrap();
    let path = dir.path().join("rle_extract.apex");
    let schema = [
        ("wide".to_string(), ColumnType::Int64),
        ("tag".to_string(), ColumnType::Int64),
    ];
    let rows = 4096usize;
    // Two long runs of 60-bit-separated values: RLE beats bit-packing here.
    let wide: Vec<i64> = (0..rows)
        .map(|i| if i < rows / 2 { 0 } else { 1i64 << 60 })
        .collect();
    let tag: Vec<i64> = (0..rows).map(|i| (i % 7) as i64).collect();

    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Fast,
            &schema,
        )
        .unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("wide".to_string(), wide.clone());
        int_cols.insert("tag".to_string(), tag.clone());
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let bytes = std::fs::read(&path).unwrap();
    assert!(
        bytes.contains(&COL_ENCODING_RLE),
        "fixture must physically store the RLE encoding"
    );

    let storage = OnDemandStorage::open(&path).unwrap();
    let indices: Vec<usize> = (0..rows).step_by(4).collect();
    let batch = storage
        .extract_rows_by_indices_to_arrow(&indices, Some(&["wide", "tag"]))
        .unwrap()
        .expect("indexed extraction must serve two Int64 columns");
    let wide_array = batch
        .column_by_name("wide")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let tag_array = batch
        .column_by_name("tag")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    for (out_idx, &row) in indices.iter().enumerate() {
        assert_eq!(wide_array.value(out_idx), wide[row]);
        assert_eq!(tag_array.value(out_idx), tag[row]);
        assert!(!arrow::array::Array::is_null(wide_array, out_idx));
    }
}

#[test]
fn indexed_extraction_decodes_rle_bool_columns_at_index() {
    // A clustered Bool column is RLE_BOOL-encoded (long runs). The parallel
    // lane had no branch for it: before the round-14 guard the column came back
    // all-NULL, and with the guard the whole lane declined. It now decodes.
    let dir = tempdir().unwrap();
    let path = dir.path().join("rle_bool_extract.apex");
    let schema = [
        ("flag".to_string(), ColumnType::Bool),
        ("age".to_string(), ColumnType::Int64),
    ];
    let rows = 4096usize;
    let flags: Vec<bool> = (0..rows).map(|i| i < rows / 2).collect();
    let ages: Vec<i64> = (0..rows).map(|i| (i % 13) as i64).collect();

    {
        let storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Fast,
            &schema,
        )
        .unwrap();
        let mut int_cols = HashMap::new();
        int_cols.insert("age".to_string(), ages.clone());
        let mut bool_cols = HashMap::new();
        bool_cols.insert("flag".to_string(), flags.clone());
        storage
            .insert_typed(
                int_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                bool_cols,
            )
            .unwrap();
        storage.save().unwrap();
    }

    let bytes = std::fs::read(&path).unwrap();
    assert!(
        bytes.contains(&COL_ENCODING_RLE_BOOL),
        "fixture must physically store the RLE_BOOL encoding"
    );

    let storage = OnDemandStorage::open(&path).unwrap();
    let indices: Vec<usize> = (0..rows).step_by(4).collect();
    let batch = storage
        .extract_rows_by_indices_to_arrow(&indices, Some(&["flag", "age"]))
        .unwrap()
        .expect("indexed extraction must serve a Bool and an Int64 column");
    let flag_array = batch
        .column_by_name("flag")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    let age_array = batch
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    for (out_idx, &row) in indices.iter().enumerate() {
        assert_eq!(flag_array.value(out_idx), flags[row], "flag at row {row}");
        assert_eq!(age_array.value(out_idx), ages[row]);
    }
}

#[test]
fn width_fidelity_requires_an_empty_table() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("narrow_late.apex");
    let schema = [("v".to_string(), ColumnType::Float32)];
    let mut storage = OnDemandStorage::create_with_schema_and_durability(
        &path,
        crate::storage::DurabilityLevel::Fast,
        &schema,
    )
    .unwrap();
    let mut float_cols = HashMap::new();
    float_cols.insert("v".to_string(), vec![1.0, 2.0]);
    storage
        .insert_typed(
            HashMap::new(),
            float_cols,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    assert!(
        !storage.set_width_fidelity(true).unwrap(),
        "the flag cannot change once rows exist"
    );
}

#[test]
fn width_fidelity_env_switch_applies_to_new_tables() {
    // The deployment switch is read when a table is created and persisted in
    // the header, so reopening keeps the encoding.
    let dir = tempdir().unwrap();
    let path = dir.path().join("env_narrow.apex");
    let schema = [("v".to_string(), ColumnType::Float32)];
    {
        std::env::set_var("APEX_WIDTH_FIDELITY", "1");
        let mut storage = OnDemandStorage::create_with_schema_and_durability(
            &path,
            crate::storage::DurabilityLevel::Fast,
            &schema,
        )
        .unwrap();
        assert!(
            storage.width_fidelity(),
            "APEX_WIDTH_FIDELITY=1 must opt a new table in"
        );
        let mut float_cols = HashMap::new();
        float_cols.insert("v".to_string(), vec![1.0, 2.0, 3.0, 4.0]);
        storage
            .insert_typed(
                HashMap::new(),
                float_cols,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
        std::env::remove_var("APEX_WIDTH_FIDELITY");
    }

    let reopened = OnDemandStorage::open(&path).unwrap();
    assert!(
        reopened.width_fidelity(),
        "the opt-in must survive a reopen through the header flag"
    );
}

/// The V4 compat append in `StorageEngine::write_typed` assigns ids from the
/// backend's `next_id` instead of the allocator, so it has to reserve them
/// against the table's shared row-id mark itself.
///
/// The branch only runs for a legacy V4 file (`footer_offset > 0`), which is why
/// the fixture is built with `save_v4()` rather than by writing a table through
/// the current default layout.
#[test]
fn v4_compat_append_reserves_ids_against_the_shared_mark() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy_v4.apex");
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        storage
            .insert_typed(
                HashMap::from([("a".to_string(), vec![1_i64, 2, 3, 4, 5])]),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save_v4().unwrap();
        assert!(
            storage.header.read().footer_offset > 0,
            "the fixture must be a legacy V4 file so the compat append branch runs"
        );
    }

    // Another process already owns the ids from 100 on.
    let claimed = crate::storage::epoch::reserve_row_ids(&path, 100, 1);
    assert!(claimed >= 100, "{claimed}");

    let ids = crate::Database::write_typed(
        &path,
        HashMap::from([("a".to_string(), vec![6_i64])]),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        crate::storage::DurabilityLevel::Safe,
    )
    .unwrap();
    assert_eq!(ids.len(), 1, "{ids:?}");
    assert!(
        ids[0] > claimed,
        "a V4 compat append must stay above the ids another process reserved: {ids:?} vs {claimed}"
    );
}

/// A BITPACK point read must cover the whole value window.
///
/// `retrieve_rcix` (and its projected twin) used to read a fixed three-byte
/// window, so any value whose bits straddle more than 24 bits silently lost its
/// high bits. With a 50k-row base of `0..49_999` the appended rows push the row
/// group's bit width to 22, and `4_000_001` came back as `67_841` through the
/// point lane while the scan (which decodes the full stream) was correct.
#[test]
fn wide_bitpack_point_reads_keep_their_high_bits() {
    use crate::data::Value;

    let dir = tempdir().unwrap();
    let path = dir.path().join("wide_bitpack.apex");
    let storage = OnDemandStorage::create(&path).unwrap();
    let base: Vec<i64> = (0..50_000).collect();
    storage
        .insert_typed(
            HashMap::from([("a".to_string(), base)]),
            HashMap::new(),
            HashMap::from([("s".to_string(), vec!["v".to_string(); 50_000])]),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save_v4().unwrap();

    let appended: Vec<HashMap<String, ColumnValue>> = (0..3)
        .map(|offset| {
            HashMap::from([
                ("a".to_string(), ColumnValue::Int64(4_000_000 + offset)),
                ("s".to_string(), ColumnValue::String("x".to_string())),
            ])
        })
        .collect();
    let ids = storage.insert_rows(&appended).unwrap();
    storage.save().unwrap();
    assert_eq!(ids.len(), 3, "{ids:?}");

    for (offset, id) in ids.iter().enumerate() {
        let expected = 4_000_000 + offset as i64;
        let point = storage.retrieve_rcix(*id).unwrap().expect("row must exist");
        let value = point
            .iter()
            .find(|(name, _)| name == "a")
            .map(|(_, value)| value.clone())
            .unwrap();
        assert_eq!(value, Value::Int64(expected), "retrieve_rcix id={id}");

        let projected = storage
            .retrieve_rcix_projected(*id, &["a"])
            .unwrap()
            .expect("projected row must exist");
        let projected_value = projected
            .iter()
            .find(|(name, _)| name == "a")
            .map(|(_, value)| value.clone())
            .unwrap();
        assert_eq!(projected_value, Value::Int64(expected), "projected id={id}");

        // The values lane agrees with the point lane and with the expected value.
        let values = storage.read_row_by_id_values(*id).unwrap().expect("row");
        let values_value = values
            .iter()
            .find(|(name, _)| name == "a")
            .map(|(_, value)| value.clone())
            .unwrap();
        assert_eq!(values_value, Value::Int64(expected), "values lane id={id}");
    }
}

/// A compressed row group is decompressed once, not once per point read.
///
/// Compressed groups carry no RCIX, so the point-read fallback decodes the whole
/// group; without a cache every read paid that cost again (a zstd point read
/// measured ~1.43 ms against 0.014 ms on the plain twin).
#[test]
fn compressed_point_reads_reuse_the_decompressed_row_group() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("compressed_point.apex");
    let storage = OnDemandStorage::create(&path).unwrap();
    assert!(storage
        .set_compression(CompressionType::Zstd)
        .unwrap_or(false));
    let rows = 40_000usize;
    storage
        .insert_typed(
            HashMap::from([
                ("a".to_string(), (0..rows as i64).collect::<Vec<i64>>()),
                ("b".to_string(), (0..rows as i64).map(|v| v * 3).collect()),
            ]),
            HashMap::new(),
            HashMap::from([(
                "s".to_string(),
                (0..rows).map(|i| format!("v{}", i % 50)).collect::<Vec<String>>(),
            )]),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save_v4().unwrap();
    drop(storage);

    // A fresh instance has no in-memory mirror, so the reads take the compressed
    // fallback this cache exists for (the writer instance would answer from its
    // own buffer).
    let storage = OnDemandStorage::open(&path).unwrap();
    assert_eq!(storage.row_count() as usize, rows);
    assert_eq!(storage.decompressed_rg_cache.read().len(), 0);

    for id in [1u64, 2, 3, 4] {
        let row = storage
            .read_row_by_id_values(id)
            .unwrap()
            .expect("row must exist");
        let value = row
            .iter()
            .find(|(name, _)| name == "a")
            .map(|(_, value)| value.clone())
            .unwrap();
        assert_eq!(value, crate::data::Value::Int64(id as i64 - 1), "id={id}");
        // The other encodings in the same group take their own targeted arms.
        let b = row
            .iter()
            .find(|(name, _)| name == "b")
            .map(|(_, value)| value.clone())
            .unwrap();
        assert_eq!(b, crate::data::Value::Int64((id as i64 - 1) * 3), "id={id}");
        let s = row
            .iter()
            .find(|(name, _)| name == "s")
            .map(|(_, value)| value.clone())
            .unwrap();
        assert_eq!(
            s,
            crate::data::Value::String(format!("v{}", (id as usize - 1) % 50)),
            "id={id}"
        );
    }
    // All four ids live in the first row group: one decompressed body is cached.
    assert_eq!(
        storage.decompressed_rg_cache.read().len(),
        1,
        "the second and later reads must reuse the cached body"
    );
    // The cache is dropped together with the page cache on any table change.
    storage.invalidate_page_cache();
    assert_eq!(
        storage.decompressed_rg_cache.read().len(),
        0,
        "invalidation must drop decompressed bodies"
    );
}

/// A compressed row group has no RCIX, so every touch decodes the whole group.
/// The write path therefore caps the group size for compressed tables while the
/// plain layout keeps the larger adaptive groups, where RCIX already gives O(1)
/// column access. This pins both halves: the cap and its absence.
#[test]
fn compressed_tables_use_capped_row_groups_and_plain_tables_do_not() {
    let dir = tempdir().unwrap();
    let rows = (OnDemandStorage::COMPRESSED_ROW_GROUP_SIZE as usize) * 2 + 100;

    let build = |name: &str, compression: Option<CompressionType>| {
        let path = dir.path().join(name);
        let storage = OnDemandStorage::create(&path).unwrap();
        if let Some(compression) = compression {
            assert!(storage.set_compression(compression).unwrap_or(false));
        }
        storage
            .insert_typed(
                HashMap::from([("a".to_string(), (0..rows as i64).collect::<Vec<i64>>())]),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save_v4().unwrap();
        let header = storage.header.read();
        let (row_group_size, row_group_count) = (header.row_group_size, header.row_group_count);
        drop(header);
        drop(storage);
        (path, row_group_size, row_group_count)
    };

    let chunk = OnDemandStorage::COMPRESSED_ROW_GROUP_SIZE as usize;
    let (zstd_path, zstd_size, zstd_groups) = build("capped_zstd.apex", Some(CompressionType::Zstd));
    assert_eq!(zstd_size, OnDemandStorage::COMPRESSED_ROW_GROUP_SIZE);
    assert_eq!(zstd_groups as usize, (rows + chunk - 1) / chunk);

    let (plain_path, plain_size, plain_groups) = build("capped_plain.apex", None);
    assert_eq!(
        plain_size, DEFAULT_ROW_GROUP_SIZE,
        "the plain layout keeps the large adaptive group; RCIX already gives it O(1) column access"
    );
    assert_eq!(plain_groups, 1);

    // The cap is a layout choice, not a data one: both files read back whole.
    for (label, path) in [("zstd", zstd_path), ("plain", plain_path)] {
        let storage = OnDemandStorage::open(&path).unwrap();
        assert_eq!(storage.row_count() as usize, rows);
        let columns = storage.read_columns(Some(&["a"]), 0, None).unwrap();
        match &columns["a"] {
            ColumnData::Int64(values) => {
                assert_eq!(values.len(), rows, "{label}");
                assert_eq!(values[0], 0, "{label}");
                assert_eq!(values[rows - 1], rows as i64 - 1, "{label}");
            }
            other => panic!("unexpected {label} column: {other:?}"),
        }
    }
}

#[test]
fn find_row_group_for_id_follows_the_sorted_id_directory() {
    let group = |row_count: u32, min_id: u64, max_id: u64| RowGroupMeta {
        offset: 0,
        data_size: 0,
        row_count,
        min_id,
        max_id,
        deletion_count: 0,
    };
    // Ranges left by appends and by a rewrite that dropped the deleted rows
    // (the second range ends at 149 while the next starts at 200).
    let groups = vec![group(100, 0, 99), group(50, 100, 149), group(10, 200, 209)];
    assert_eq!(find_row_group_for_id(&groups, 0), Some(0));
    assert_eq!(find_row_group_for_id(&groups, 99), Some(0));
    assert_eq!(find_row_group_for_id(&groups, 100), Some(1));
    assert_eq!(find_row_group_for_id(&groups, 149), Some(1));
    // Inside the hole: no group covers the id.
    assert_eq!(find_row_group_for_id(&groups, 150), None);
    assert_eq!(find_row_group_for_id(&groups, 199), None);
    assert_eq!(find_row_group_for_id(&groups, 200), Some(2));
    assert_eq!(find_row_group_for_id(&groups, 209), Some(2));
    assert_eq!(find_row_group_for_id(&groups, 210), None);
    // The empty bootstrap row group of an empty table matches nothing.
    let empty = vec![group(0, 0, 0)];
    assert_eq!(find_row_group_for_id(&empty, 0), None);
    assert_eq!(find_row_group_for_id(&groups[..0], 0), None);
}

#[test]
fn apply_pending_deletes_ignores_a_short_entry() {
    // The deferred-delete entry starts with "APXP" + four u64 + rg_count (a
    // 40-byte fixed header). The old guard accepted anything >= 24 bytes and
    // then read `buf[28..36]`, so a 29-byte entry panicked a reader thread
    // ("range end index 36 out of range for slice of length 29") instead of
    // being ignored.
    let dir = tempdir().unwrap();
    let path = dir.path().join("short.apex");
    std::fs::write(&path, b"base").unwrap();
    for len in [0usize, 3, 4, 24, 29, 33, 36, 39] {
        let mut buf = vec![0u8; len];
        if len >= 4 {
            buf[0..4].copy_from_slice(b"APXP");
        }
        super::global_pending_deletes()
            .write()
            .unwrap()
            .get_or_insert_with(std::collections::HashMap::new)
            .insert(path.clone(), buf);
        super::apply_pending_deletes(&path).unwrap();
    }
    super::global_pending_deletes()
        .write()
        .unwrap()
        .as_mut()
        .unwrap()
        .remove(&path);
}

#[test]
fn positions_for_ids_requires_dense_undeleted_row_groups() {
    // The FTS row selection resolves candidate positions from the row-group
    // directory instead of reading the whole `_id` column. That shortcut is only
    // exact while the persisted ids are dense and undeleted, so anything else
    // must report "unknown" and let the caller walk the column.
    let dir = tempdir().unwrap();
    let path = dir.path().join("positions.apex");
    let storage = OnDemandStorage::create(&path).unwrap();
    let ints: Vec<i64> = (0..300).collect();
    storage
        .insert_typed(
            HashMap::from([("a".to_string(), ints)]),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save_v4().unwrap();

    // Dense rows: `id - min_id` is the local index (`_id`s start at 1). An id no
    // group covers is skipped, exactly like the id reader skips a deleted row.
    assert_eq!(
        storage.positions_for_ids(&[1, 8, 300, 5_000]).unwrap(),
        Some(vec![0, 7, 299])
    );

    // A persisted deletion marks its row group, so the mapping is no longer
    // provable and the caller has to read the column.
    storage.delete(5);
    storage.save_v4().unwrap();
    let reopened = OnDemandStorage::open(&path).unwrap();
    assert_eq!(reopened.positions_for_ids(&[0]).unwrap(), None);
}

#[test]
fn compressed_point_lane_reports_the_decline_to_its_callers() {
    // `retrieve_rcix` only serves uncompressed row groups. It used to report the
    // same `None` for "row absent" and "lane unavailable", so the Arrow point
    // readers concluded absence and the SQL fast path fell back to a full scan.
    // The lane now exposes the distinction; the decoding reader answers the row.
    let dir = tempdir().unwrap();
    let rows = 4_000usize;
    let ints: Vec<i64> = (0..rows as i64).collect();
    let strings: Vec<String> = (0..rows).map(|i| format!("v{}", i % 50)).collect();

    let build = |name: &str, compression: Option<CompressionType>| {
        let path = dir.path().join(name);
        let storage = OnDemandStorage::create(&path).unwrap();
        if let Some(compression) = compression {
            assert!(storage.set_compression(compression).unwrap_or(false));
        }
        storage
            .insert_typed(
                HashMap::from([("a".to_string(), ints.clone())]),
                HashMap::new(),
                HashMap::from([("s".to_string(), strings.clone())]),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save_v4().unwrap();
        drop(storage);
        OnDemandStorage::open(&path).unwrap()
    };

    let plain = build("plain_lane.apex", None);
    let zstd = build("zstd_lane.apex", Some(CompressionType::Zstd));
    let id = 1_234u64;

    // Uncompressed: the lean lane serves the row and reports itself available.
    assert!(plain.point_read_lane_available(id).unwrap());
    assert!(plain.retrieve_rcix(id).unwrap().is_some());

    // Compressed: the lean lane declines, and the caller is told so.
    assert!(
        !zstd.point_read_lane_available(id).unwrap(),
        "a compressed row group must report the lean lane as unavailable"
    );
    assert!(
        zstd.retrieve_rcix(id).unwrap().is_none(),
        "the lean lane declines compressed row groups"
    );
    let row = zstd
        .read_row_by_id_values(id)
        .unwrap()
        .expect("the decoding lane must still answer the row");
    let value = |name: &str| {
        row.iter()
            .find(|(column, _)| column == name)
            .map(|(_, value)| value.clone())
            .unwrap()
    };
    assert_eq!(value("a"), crate::data::Value::Int64(id as i64 - 1));
    assert_eq!(
        value("s"),
        crate::data::Value::String(format!("v{}", (id as usize - 1) % 50))
    );

    // An id past the last group is an absent row, not an unavailable lane, so
    // both readers may answer `None` without consulting the decoder.
    let missing = rows as u64 + 5;
    assert!(plain.point_read_lane_available(missing).unwrap());
    assert!(zstd.point_read_lane_available(missing).unwrap());
    assert!(plain.read_row_by_id_values(missing).unwrap().is_none());
    assert!(zstd.read_row_by_id_values(missing).unwrap().is_none());
}

/// The writer records the per-column directory (RCIX) *before* compressing a row
/// group, so a compressed group carries one too, and the directory addresses the
/// decompressed body. The decoding lane can therefore jump straight to the
/// requested columns, and a projection can avoid decoding the rest entirely,
/// instead of walking the body in column order.
/// `_id IN (...)` on a compressed table must be answered by the storage batch
/// lane. That lane carries the same column directory a plain group does (the
/// write path records it before compressing) and already decompresses the body,
/// but it declined every compressed group up front, so the whole statement fell
/// through to the SQL executor.
#[test]
fn compressed_batch_point_reads_use_the_decompressed_body() {
    let dir = tempdir().unwrap();
    let rows = (OnDemandStorage::COMPRESSED_ROW_GROUP_SIZE as usize) * 2 + 777;

    let build = |name: &str, compression: Option<CompressionType>| {
        let path = dir.path().join(name);
        let storage = OnDemandStorage::create(&path).unwrap();
        if let Some(compression) = compression {
            assert!(storage.set_compression(compression).unwrap_or(false));
        }
        storage
            .insert_typed(
                HashMap::from([
                    ("a".to_string(), (0..rows as i64).collect::<Vec<i64>>()),
                    (
                        "b".to_string(),
                        (0..rows as i64).map(|value| value * 5).collect::<Vec<i64>>(),
                    ),
                ]),
                HashMap::from([(
                    "f".to_string(),
                    (0..rows).map(|value| value as f64 * 0.5).collect::<Vec<f64>>(),
                )]),
                HashMap::from([(
                    "s".to_string(),
                    (0..rows).map(|value| format!("v{}", value % 32)).collect::<Vec<String>>(),
                )]),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save_v4().unwrap();
        drop(storage);
        OnDemandStorage::open(&path).unwrap()
    };

    let plain = build("batch_plain.apex", None);
    let zstd = build("batch_zstd.apex", Some(CompressionType::Zstd));
    let lz4 = build("batch_lz4.apex", Some(CompressionType::Lz4));

    // Spread across row groups, including a group boundary and the last row.
    let ids: Vec<u64> = vec![1, 2, 4_096, 4_097, 4_098, 5_000, rows as u64 - 1, rows as u64];

    let read = |storage: &OnDemandStorage, ids: &[u64]| {
        storage
            .retrieve_many_mmap_columns(ids)
            .unwrap()
            .expect("the batch lane must answer rather than decline")
    };
    // The projected batch lane is only reachable from the Python bindings, so the
    // method itself is compiled under `python`; this test must build in the
    // core-only configuration too, which is why only these assertions are gated.
    #[cfg(feature = "python")]
    let projected = |storage: &OnDemandStorage, ids: &[u64]| {
        storage
            .retrieve_many_mmap_columns_projected(ids, &["b".to_string(), "s".to_string()])
            .unwrap()
            .expect("the projected batch lane must answer rather than decline")
    };

    let baseline = read(&plain, &ids);
    assert_eq!(baseline.row_count, ids.len());
    for (label, storage) in [("zstd", &zstd), ("lz4", &lz4)] {
        let batch = read(storage, &ids);
        assert_eq!(batch.row_count, ids.len(), "{label}");
        assert_eq!(
            batch.columns.len(),
            baseline.columns.len(),
            "{label}: column count"
        );
        for ((name, got), (_, want)) in batch.columns.iter().zip(baseline.columns.iter()) {
            match (got, want) {
                (MmapBatchColumn::I64(got), MmapBatchColumn::I64(want)) => {
                    assert_eq!(got, want, "{label}: column {name}")
                }
                (MmapBatchColumn::F64(got), MmapBatchColumn::F64(want)) => {
                    assert_eq!(got, want, "{label}: column {name}")
                }
                (MmapBatchColumn::Str(got), MmapBatchColumn::Str(want)) => {
                    assert_eq!(got, want, "{label}: column {name}")
                }
                _ => panic!("unexpected column shapes for {label}: column {name}"),
            }
        }

        // The projected form returns `_id` plus exactly the named columns.
        #[cfg(feature = "python")]
        {
            let narrow = projected(storage, &ids);
            assert_eq!(narrow.row_count, ids.len(), "{label}");
            let names: Vec<&str> =
                narrow.columns.iter().map(|(name, _)| name.as_str()).collect();
            assert_eq!(names, vec!["_id", "b", "s"], "{label}");
        }
    }

    // An id that does not exist is skipped, and the lane still answers: it is a
    // result, not a decline that would push the statement back to the executor.
    let missing = vec![1u64, rows as u64 + 11];
    let partial = read(&zstd, &missing);
    assert_eq!(partial.row_count, 1);
    #[cfg(feature = "python")]
    assert_eq!(projected(&zstd, &missing).row_count, 1);

    // A deleted row must not come back through the batch lane either.
    let removed = 5_000u64;
    assert!(zstd.delete(removed));
    zstd.save_v4().unwrap();
    let after_delete = read(&zstd, &[removed - 1, removed, removed + 1]);
    assert_eq!(after_delete.row_count, 2);
}

#[test]
fn compressed_point_reads_use_the_column_directory_and_honor_a_projection() {
    let dir = tempdir().unwrap();
    // More rows than `COMPRESSED_ROW_GROUP_SIZE` so the table spans several
    // compressed groups and a point read lands in the middle one.
    let rows = (OnDemandStorage::COMPRESSED_ROW_GROUP_SIZE as usize) * 2 + 512;

    let build = |name: &str, compression: Option<CompressionType>| {
        let path = dir.path().join(name);
        let storage = OnDemandStorage::create(&path).unwrap();
        if let Some(compression) = compression {
            assert!(storage.set_compression(compression).unwrap_or(false));
        }
        storage
            .insert_typed(
                HashMap::from([
                    ("a".to_string(), (0..rows as i64).collect::<Vec<i64>>()),
                    (
                        "b".to_string(),
                        (0..rows as i64).map(|value| value * 7).collect::<Vec<i64>>(),
                    ),
                ]),
                HashMap::from([(
                    "f".to_string(),
                    (0..rows).map(|value| value as f64 * 0.25).collect::<Vec<f64>>(),
                )]),
                HashMap::from([(
                    "s".to_string(),
                    (0..rows).map(|value| format!("v{}", value % 50)).collect::<Vec<String>>(),
                )]),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save_v4().unwrap();
        drop(storage);
        OnDemandStorage::open(&path).unwrap()
    };

    let plain = build("directory_plain.apex", None);
    let zstd = build("directory_zstd.apex", Some(CompressionType::Zstd));
    // The fixture must actually be compressed, or the test proves nothing.
    let footer = zstd.get_or_load_footer().unwrap().unwrap();
    assert!(footer.row_groups.len() >= 2, "expected several compressed groups");
    for group in &footer.row_groups {
        let mut header = [0u8; 32];
        zstd.read_cached_bytes(group.offset, &mut header).unwrap();
        assert_ne!(header[28], RG_COMPRESS_NONE, "a group was left uncompressed");
    }
    assert!(
        footer.col_offsets.iter().all(|offsets| offsets.len() >= 4),
        "every compressed group must carry its column directory"
    );
    drop(footer);

    let value = |row: &[(String, crate::data::Value)], name: &str| {
        row.iter()
            .find(|(column, _)| column == name)
            .map(|(_, value)| value.clone())
            .expect("column present")
    };

    for id in [1u64, 4_096, 4_097, rows as u64 / 2, rows as u64] {
        let before = compressed_directory_point_reads();
        let zstd_row = zstd.read_row_by_id_values(id).unwrap().expect("row present");
        assert!(
            compressed_directory_point_reads() > before,
            "a compressed point read must be served from the column directory"
        );
        let plain_row = plain.read_row_by_id_values(id).unwrap().expect("row present");
        for name in ["_id", "a", "b", "f", "s"] {
            assert_eq!(value(&zstd_row, name), value(&plain_row, name), "id {id} column {name}");
        }
        let index = id as i64 - 1;
        assert_eq!(value(&zstd_row, "a"), crate::data::Value::Int64(index));
        assert_eq!(value(&zstd_row, "b"), crate::data::Value::Int64(index * 7));
        assert_eq!(value(&zstd_row, "f"), crate::data::Value::Float64(index as f64 * 0.25));
        assert_eq!(
            value(&zstd_row, "s"),
            crate::data::Value::String(format!("v{}", (id as usize - 1) % 50))
        );

        // A projection returns `_id` plus exactly the named columns, and the
        // values are the ones the unprojected read reported.
        let projected = zstd
            .read_row_by_id_values_projected(id, Some(&["b", "s"]))
            .unwrap()
            .expect("row present");
        let names: Vec<&str> = projected.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, vec!["_id", "b", "s"], "id {id}");
        assert_eq!(value(&projected, "b"), value(&zstd_row, "b"));
        assert_eq!(value(&projected, "s"), value(&zstd_row, "s"));

        let id_only = zstd
            .read_row_by_id_values_projected(id, Some(&["_id"]))
            .unwrap()
            .expect("row present");
        assert_eq!(id_only.len(), 1, "id {id}");
        assert_eq!(id_only[0].0, "_id");
    }

    // An unknown column name simply selects nothing; `_id` still answers.
    let unknown = zstd
        .read_row_by_id_values_projected(7, Some(&["nope"]))
        .unwrap()
        .expect("row present");
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0].0, "_id");

    // Reading past the last group is an absent row for both the full and the
    // projected form, and an unusable lane must not turn it into an error.
    let missing = rows as u64 + 3;
    assert!(zstd.read_row_by_id_values(missing).unwrap().is_none());
    assert!(zstd
        .read_row_by_id_values_projected(missing, Some(&["b"]))
        .unwrap()
        .is_none());
}

/// Deleting and updating rows through the overlay must not let the directory
/// lane answer with a stale body: the directory describes the base file, so the
/// narrow projection has to keep agreeing with the full read across writes.
#[test]
fn compressed_directory_point_reads_stay_correct_across_delete_and_update() {
    let dir = tempdir().unwrap();
    let rows = (OnDemandStorage::COMPRESSED_ROW_GROUP_SIZE as usize) + 256;
    let path = dir.path().join("directory_mutate.apex");
    let storage = OnDemandStorage::create(&path).unwrap();
    assert!(storage.set_compression(CompressionType::Zstd).unwrap_or(false));
    storage
        .insert_typed(
            HashMap::from([
                ("a".to_string(), (0..rows as i64).collect::<Vec<i64>>()),
                (
                    "b".to_string(),
                    (0..rows as i64).map(|value| value * 3).collect::<Vec<i64>>(),
                ),
            ]),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save_v4().unwrap();

    let deleted = 4_321u64;
    assert!(storage.delete(deleted));
    // Persist the deletion vector into the base file: the directory lane reads
    // the base bytes, so the delete has to reach them to be observable.
    storage.save_v4().unwrap();

    let after_delete = storage.read_row_by_id_values(deleted).unwrap();
    assert!(
        after_delete.is_none(),
        "a deleted row must not be resurrected by the directory lane"
    );
    assert!(storage
        .read_row_by_id_values_projected(deleted, Some(&["b"]))
        .unwrap()
        .is_none());

    let value = |row: &[(String, crate::data::Value)], name: &str| {
        row.iter()
            .find(|(column, _)| column == name)
            .map(|(_, value)| value.clone())
            .expect("column present")
    };
    let survivor = storage.read_row_by_id_values(3).unwrap().expect("row present");
    let projected = storage
        .read_row_by_id_values_projected(3, Some(&["b"]))
        .unwrap()
        .expect("row present");
    assert_eq!(value(&projected, "b"), value(&survivor, "b"));

    // An appended row must read back the same through both forms too: the base
    // file's directory does not describe rows the append added.
    storage
        .insert_typed(
            HashMap::from([("a".to_string(), vec![rows as i64 + 1])]),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
        .unwrap();
    storage.save_v4().unwrap();
    let appended = rows as u64 + 1;
    let full = storage.read_row_by_id_values(appended).unwrap().expect("row present");
    let narrow = storage
        .read_row_by_id_values_projected(appended, Some(&["a"]))
        .unwrap()
        .expect("row present");
    assert_eq!(value(&full, "a"), crate::data::Value::Int64(rows as i64 + 1));
    assert_eq!(value(&narrow, "a"), value(&full, "a"));
}

#[test]
fn row_group_id_directory_stays_sorted_across_delete_and_append() {
    // `find_row_group_for_id` binary-searches `min_id`, which is only sound while
    // the published row groups stay ordered by id. Appends extend the id
    // sequence and the rewrite paths walk the active rows in order; this test
    // pins that invariant — and with it that every live id stays reachable
    // through the point lanes — across a deletion plus an incremental append.
    let dir = tempdir().unwrap();
    let path = dir.path().join("rg_directory.apex");
    let rows = 150_000usize; // several 131,072-row groups for one int64 column
    {
        let storage = OnDemandStorage::create(&path).unwrap();
        storage
            .insert_typed(
                HashMap::from([("a".to_string(), (0..rows as i64).collect::<Vec<i64>>())]),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }
    {
        let storage = OnDemandStorage::open(&path).unwrap();
        assert!(storage.delete(5_000));
        assert!(storage.delete(70_000));
        storage.save_delete_only().unwrap();
        storage
            .insert_typed(
                HashMap::from([(
                    "a".to_string(),
                    (rows as i64..rows as i64 + 2_000).collect::<Vec<i64>>(),
                )]),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
            .unwrap();
        storage.save().unwrap();
    }

    let storage = OnDemandStorage::open(&path).unwrap();
    let footer = storage.get_or_load_footer().unwrap().expect("footer");
    assert!(
        footer.row_groups.len() >= 2,
        "the fixture must span several row groups"
    );
    assert!(
        footer
            .row_groups
            .windows(2)
            .all(|pair| pair[0].min_id <= pair[1].min_id),
        "the id directory must stay sorted for the binary search to be sound"
    );
    for id in [1u64, 5_001, 70_001, rows as u64, rows as u64 + 1, rows as u64 + 1_999] {
        let expected_group = footer
            .row_groups
            .iter()
            .position(|rg| rg.min_id <= id && id <= rg.max_id && rg.row_count > 0)
            .unwrap_or_else(|| panic!("some group must cover live id {id}"));
        assert_eq!(
            find_row_group_for_id(&footer.row_groups, id),
            Some(expected_group),
            "id={id}"
        );
        assert!(
            storage.read_row_by_id_values(id).unwrap().is_some(),
            "the point lane must still find live id {id}"
        );
    }
    for id in [5_000u64, 70_000] {
        assert!(
            storage.read_row_by_id_values(id).unwrap().is_none(),
            "deleted id {id} must stay absent"
        );
    }
}





