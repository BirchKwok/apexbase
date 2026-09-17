// Serial batched Filter -> GROUP BY -> HAVING -> TopK over the stable
// row-group stream.
//
// Consumes row-group-sized morsels into an incremental group state, so scan
// memory stays bounded by one row group plus the group map regardless of
// table size. Aggregate semantics mirror the single-batch kernel: COUNT is
// the group row count, SUM/MIN/MAX skip NULLs, AVG divides the sum by the
// group row count, and NULL group keys form a single NULL group.

use arrow::array::LargeStringArray;

/// Per-batch view of one group key column.
enum BatchKeyView<'a> {
    Int(&'a Int64Array),
    Float(&'a Float64Array),
    Bool(&'a BooleanArray),
    Str(&'a StringArray),
    LargeStr(&'a LargeStringArray),
}

impl<'a> BatchKeyView<'a> {
    fn new(column: &'a ArrayRef) -> Option<Self> {
        if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
            Some(Self::Int(arr))
        } else if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
            Some(Self::Float(arr))
        } else if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
            Some(Self::Bool(arr))
        } else if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
            Some(Self::Str(arr))
        } else if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
            Some(Self::LargeStr(arr))
        } else {
            None
        }
    }
}

/// Interned distinct values of one group key column across all batches.
/// Slot 0 is always the NULL group. `bytes` is the running heap footprint of
/// the interning structures, maintained incrementally so the per-query
/// memory budget can be charged without rescanning the maps (S1).
enum BatchKeyLane {
    Int {
        dict: AHashMap<i64, u32>,
        values: Vec<Option<i64>>,
        bytes: usize,
    },
    Float {
        dict: AHashMap<u64, u32>,
        values: Vec<Option<u64>>,
        bytes: usize,
    },
    // Slot 0 is the NULL group; slot 1 is false; slot 2 is true. The slot
    // number is the value, so no per-slot storage is needed.
    Bool,
    String {
        dict: AHashMap<String, u32>,
        values: Vec<Option<String>>,
        bytes: usize,
    },
}

/// Bytes of one interned integer/float group-key value: dictionary entry plus
/// the value slot.
const LANE_SCALAR_VALUE_BYTES: usize =
    std::mem::size_of::<u64>() + std::mem::size_of::<u32>() + std::mem::size_of::<Option<u64>>() + 8;
/// Fixed bytes of one interned string group-key value (the two owned copies
/// are charged separately by length).
const LANE_STRING_VALUE_BYTES: usize =
    std::mem::size_of::<String>() + std::mem::size_of::<u32>() + std::mem::size_of::<Option<String>>() + 8;

impl BatchKeyLane {
    fn new_int() -> Self {
        Self::Int {
            dict: AHashMap::new(),
            values: vec![None],
            bytes: 0,
        }
    }

    fn new_float() -> Self {
        Self::Float {
            dict: AHashMap::new(),
            values: vec![None],
            bytes: 0,
        }
    }

    fn new_bool() -> Self {
        Self::Bool
    }

    fn new_string() -> Self {
        Self::String {
            dict: AHashMap::new(),
            values: vec![None],
            bytes: 0,
        }
    }

    /// Running footprint of this lane's interning state.
    fn bytes(&self) -> usize {
        match self {
            Self::Int { bytes, .. } | Self::Float { bytes, .. } | Self::String { bytes, .. } => {
                *bytes
            }
            Self::Bool => 0,
        }
    }

    fn compatible(&self, view: &BatchKeyView) -> bool {
        matches!(
            (self, view),
            (Self::Int { .. }, BatchKeyView::Int(_))
                | (Self::Float { .. }, BatchKeyView::Float(_))
                | (Self::Bool { .. }, BatchKeyView::Bool(_))
                | (Self::String { .. }, BatchKeyView::Str(_))
                | (Self::String { .. }, BatchKeyView::LargeStr(_))
        )
    }

    /// Intern the row value and return its group ID (0 = NULL group).
    fn id_at(&mut self, view: &BatchKeyView, row: usize) -> u32 {
        match (self, view) {
            (Self::Int { dict, values, bytes }, BatchKeyView::Int(arr)) => {
                if arr.is_null(row) {
                    0
                } else {
                    let value = arr.value(row);
                    *dict.entry(value).or_insert_with(|| {
                        let id = values.len() as u32;
                        values.push(Some(value));
                        *bytes += LANE_SCALAR_VALUE_BYTES;
                        id
                    })
                }
            }
            (Self::Float { dict, values, bytes }, BatchKeyView::Float(arr)) => {
                if arr.is_null(row) {
                    0
                } else {
                    // Intern by bit pattern so NaN keeps one group per bit
                    // pattern, matching the byte-hash behavior of the
                    // single-batch kernel.
                    let bits = arr.value(row).to_bits();
                    *dict.entry(bits).or_insert_with(|| {
                        let id = values.len() as u32;
                        values.push(Some(bits));
                        *bytes += LANE_SCALAR_VALUE_BYTES;
                        id
                    })
                }
            }
            (Self::Bool, BatchKeyView::Bool(arr)) => {
                if arr.is_null(row) {
                    0
                } else if arr.value(row) {
                    2
                } else {
                    1
                }
            }
            (Self::String { dict, values, bytes }, BatchKeyView::Str(arr)) => {
                if arr.is_null(row) {
                    0
                } else {
                    let value = arr.value(row);
                    *dict.entry(value.to_string()).or_insert_with(|| {
                        let id = values.len() as u32;
                        values.push(Some(value.to_string()));
                        *bytes += LANE_STRING_VALUE_BYTES + 2 * value.len();
                        id
                    })
                }
            }
            (Self::String { dict, values, bytes }, BatchKeyView::LargeStr(arr)) => {
                if arr.is_null(row) {
                    0
                } else {
                    let value = arr.value(row);
                    *dict.entry(value.to_string()).or_insert_with(|| {
                        let id = values.len() as u32;
                        values.push(Some(value.to_string()));
                        *bytes += LANE_STRING_VALUE_BYTES + 2 * value.len();
                        id
                    })
                }
            }
            _ => unreachable!("lane compatibility checked at batch start"),
        }
    }

    /// Output values in group-ID order for the packed group keys.
    fn output_int(&self, ids: &[u32]) -> Option<Vec<Option<i64>>> {
        match self {
            Self::Int { values, .. } => Some(
                ids.iter()
                    .map(|&id| values.get(id as usize).and_then(|slot| *slot))
                    .collect(),
            ),
            _ => None,
        }
    }

    fn output_float(&self, ids: &[u32]) -> Option<Vec<Option<f64>>> {
        match self {
            Self::Float { values, .. } => Some(
                ids.iter()
                    .map(|&id| {
                        values
                            .get(id as usize)
                            .and_then(|slot| *slot)
                            .map(f64::from_bits)
                    })
                    .collect(),
            ),
            _ => None,
        }
    }

    fn output_bool(&self, ids: &[u32]) -> Option<Vec<Option<bool>>> {
        match self {
            Self::Bool => Some(
                ids.iter()
                    .map(|&id| match id {
                        1 => Some(false),
                        2 => Some(true),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        }
    }

    fn output_string(&self, ids: &[u32]) -> Option<Vec<Option<String>>> {
        match self {
            Self::String { values, .. } => Some(
                ids.iter()
                    .map(|&id| values.get(id as usize).and_then(|slot| slot.clone()))
                    .collect(),
            ),
            _ => None,
        }
    }

    /// Decode one interned group ID (slot 0 = NULL group) so a partial
    /// fold's lanes can be re-interned into the merged lanes (R5.7).
    fn value_at(&self, id: u32) -> Option<BatchKeyValue> {
        match self {
            Self::Int { values, .. } => values
                .get(id as usize)
                .and_then(|slot| *slot)
                .map(BatchKeyValue::Int),
            Self::Float { values, .. } => values
                .get(id as usize)
                .and_then(|slot| *slot)
                .map(BatchKeyValue::FloatBits),
            Self::Bool => match id {
                1 => Some(BatchKeyValue::Bool(false)),
                2 => Some(BatchKeyValue::Bool(true)),
                _ => None,
            },
            Self::String { values, .. } => values
                .get(id as usize)
                .and_then(|slot| slot.as_ref())
                .map(|value| BatchKeyValue::Str(value.clone())),
        }
    }

    /// Intern a decoded value into this lane; None when the lane variant
    /// does not match (the caller falls back to the serial fold).
    fn intern_value(&mut self, value: Option<BatchKeyValue>) -> Option<u32> {
        match (self, value) {
            (Self::Int { dict, values, bytes }, Some(BatchKeyValue::Int(value))) => Some(
                *dict.entry(value).or_insert_with(|| {
                    let id = values.len() as u32;
                    values.push(Some(value));
                    *bytes += LANE_SCALAR_VALUE_BYTES;
                    id
                }),
            ),
            (Self::Float { dict, values, bytes }, Some(BatchKeyValue::FloatBits(bits))) => Some(
                *dict.entry(bits).or_insert_with(|| {
                    let id = values.len() as u32;
                    values.push(Some(bits));
                    *bytes += LANE_SCALAR_VALUE_BYTES;
                    id
                }),
            ),
            (Self::Bool, Some(BatchKeyValue::Bool(value))) => {
                Some(if value { 2 } else { 1 })
            }
            (Self::String { dict, values, bytes }, Some(BatchKeyValue::Str(value))) => {
                let id = *dict.entry(value.clone()).or_insert_with(|| {
                    let id = values.len() as u32;
                    *bytes += LANE_STRING_VALUE_BYTES + 2 * value.len();
                    values.push(Some(value));
                    id
                });
                Some(id)
            }
            _ => None,
        }
    }
}

/// Decoded group-key value exchanged between partial folds (R5.7).
#[derive(Clone, Debug)]
enum BatchKeyValue {
    Int(i64),
    FloatBits(u64),
    Bool(bool),
    Str(String),
}

/// Incremental aggregate state of one group. Field-for-field the same
/// semantics as the single-batch GroupState (minus first_row).
#[derive(Clone)]
struct BatchGroupState {
    count: i64,
    sum_int: i64,
    sum_float: f64,
    min_int: Option<i64>,
    max_int: Option<i64>,
    min_float: Option<f64>,
    max_float: Option<f64>,
}

impl BatchGroupState {
    fn new() -> Self {
        Self {
            count: 0,
            sum_int: 0,
            sum_float: 0.0,
            min_int: None,
            max_int: None,
            min_float: None,
            max_float: None,
        }
    }

    /// Fold another partial state into this one. Int sums wrap (associative),
    /// float sums add chunk partials in chunk order (deterministic); the
    /// phase-A parity fixtures use exactly representable values, so the
    /// merge is bit-equal to the serial fold.
    fn merge_from(&mut self, other: &BatchGroupState) {
        self.count += other.count;
        self.sum_int = self.sum_int.wrapping_add(other.sum_int);
        self.sum_float += other.sum_float;
        self.min_int = match (self.min_int, other.min_int) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max_int = match (self.max_int, other.max_int) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        self.min_float = match (self.min_float, other.min_float) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max_float = match (self.max_float, other.max_float) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
    }
}

/// Incremental GROUP BY state over row-group-sized batches.
struct BatchGroupAggregator {
    group_cols: Vec<String>,
    key1: Option<BatchKeyLane>,
    key2: Option<BatchKeyLane>,
    source_name: Option<String>,
    source_is_int: Option<bool>,
    groups: AHashMap<u64, BatchGroupState>,
    /// Bytes of `groups` entries, maintained incrementally (S1 budget).
    groups_bytes: usize,
}

/// Bytes charged per distinct group entry: the key plus the aggregate state
/// and hash-map control overhead.
const GROUP_ENTRY_BYTES: usize =
    std::mem::size_of::<u64>() + std::mem::size_of::<BatchGroupState>() + 8;

/// Charge aggregation-state growth against the optional query budget (S1).
#[inline]
fn charge_state(budget: Option<&QueryMemoryBudget>, bytes: usize) -> io::Result<()> {
    match budget {
        Some(budget) => budget.reserve(bytes),
        None => Ok(()),
    }
}

/// Release a charge whose operator state was dropped before the query ended.
#[inline]
fn release_state(budget: Option<&QueryMemoryBudget>, bytes: usize) {
    if let Some(budget) = budget {
        budget.release(bytes);
    }
}

impl BatchGroupAggregator {
    /// Parse the statement shape. Returns None when the query is outside
    /// this kernel's gate (the single-batch path keeps those shapes).
    fn new(stmt: &SelectStatement) -> Option<Self> {
        let group_cols: Vec<String> = stmt
            .group_by
            .iter()
            .map(|s| {
                let trimmed = s.trim_matches('"');
                match trimmed.rfind('.') {
                    Some(dot_pos) => trimmed[dot_pos + 1..].to_string(),
                    None => trimmed.to_string(),
                }
            })
            .collect();
        if group_cols.is_empty() || group_cols.len() > 2 {
            return None;
        }

        let mut source_name = None;
        for column in &stmt.columns {
            match column {
                SelectColumn::Column(name) | SelectColumn::ColumnAlias { column: name, .. } => {
                    let clean = name.trim_matches('"');
                    let clean = clean.rsplit('.').next().unwrap_or(clean);
                    if !group_cols.iter().any(|group| group == clean.trim_matches('"')) {
                        return None;
                    }
                }
                SelectColumn::Aggregate {
                    func,
                    column,
                    distinct: false,
                    ..
                } => {
                    match func {
                        AggregateFunc::Count => {
                            let star = column.as_deref().map_or(true, |c| {
                                let clean = c.trim_matches('"');
                                let clean = clean.rsplit('.').next().unwrap_or(clean);
                                clean == "*" || clean == "1"
                            });
                            if !star {
                                return None;
                            }
                        }
                        AggregateFunc::Sum
                        | AggregateFunc::Avg
                        | AggregateFunc::Min
                        | AggregateFunc::Max => {
                            let Some(raw) = column else {
                                return None;
                            };
                            let clean = raw.trim_matches('"');
                            let clean = clean.rsplit('.').next().unwrap_or(clean);
                            match &source_name {
                                Some(existing) if existing != clean => return None,
                                Some(_) => {}
                                None => source_name = Some(clean.to_string()),
                            }
                        }
                    }
                }
                _ => return None,
            }
        }

        Some(Self {
            group_cols,
            key1: None,
            key2: None,
            source_name,
            source_is_int: None,
            groups: AHashMap::new(),
            groups_bytes: 0,
        })
    }

    /// Footprint of the accumulated group state plus the interned key lanes.
    /// O(1): every component is maintained incrementally as groups are added.
    fn state_bytes(&self) -> usize {
        self.groups_bytes
            + self.key1.as_ref().map_or(0, BatchKeyLane::bytes)
            + self.key2.as_ref().map_or(0, BatchKeyLane::bytes)
    }

    fn resolve_key<'b>(
        &mut self,
        slot: usize,
        batch: &'b RecordBatch,
    ) -> Option<BatchKeyView<'b>> {
        let name = &self.group_cols[slot];
        let column = batch.column_by_name(name)?;
        let view = BatchKeyView::new(column)?;
        let lane = match (slot, &mut self.key1, &mut self.key2) {
            (0, lane @ None, _) => {
                *lane = match view {
                    BatchKeyView::Int(_) => Some(BatchKeyLane::new_int()),
                    BatchKeyView::Float(_) => Some(BatchKeyLane::new_float()),
                    BatchKeyView::Bool(_) => Some(BatchKeyLane::new_bool()),
                    BatchKeyView::Str(_) | BatchKeyView::LargeStr(_) => {
                        Some(BatchKeyLane::new_string())
                    }
                };
                self.key1.as_mut().unwrap()
            }
            (1, _, lane @ None) => {
                *lane = Some(match view {
                    BatchKeyView::Int(_) => BatchKeyLane::new_int(),
                    BatchKeyView::Float(_) => BatchKeyLane::new_float(),
                    BatchKeyView::Bool(_) => BatchKeyLane::new_bool(),
                    BatchKeyView::Str(_) | BatchKeyView::LargeStr(_) => {
                        BatchKeyLane::new_string()
                    }
                });
                self.key2.as_mut().unwrap()
            }
            _ => {
                let lane = if slot == 0 {
                    self.key1.as_mut()?
                } else {
                    self.key2.as_mut()?
                };
                if !lane.compatible(&view) {
                    return None;
                }
                return Some(view);
            }
        };
        if !lane.compatible(&view) {
            return None;
        }
        Some(view)
    }

    /// Consume one selected batch. Returns None when a required column is
    /// missing or unresolvable; the caller falls back to the single-batch
    /// path for the whole query.
    fn consume_batch(&mut self, batch: &RecordBatch) -> Option<()> {
        let view1 = self.resolve_key(0, batch)?;
        let view2 = if self.group_cols.len() == 2 {
            Some(self.resolve_key(1, batch)?)
        } else {
            None
        };

        let source_int: Option<&Int64Array> = match &self.source_name {
            None => None,
            Some(name) => {
                let column = batch.column_by_name(name)?;
                column.as_any().downcast_ref::<Int64Array>()
            }
        };
        let source_float: Option<&Float64Array> = if source_int.is_none() {
            match &self.source_name {
                None => None,
                Some(name) => {
                    let column = batch.column_by_name(name)?;
                    let Some(arr) = column.as_any().downcast_ref::<Float64Array>() else {
                        return None;
                    };
                    Some(arr)
                }
            }
        } else {
            None
        };
        let is_int_source = source_int.is_some();
        match self.source_is_int {
            Some(previous) if previous != is_int_source => return None,
            _ => self.source_is_int = Some(is_int_source),
        }

        let num_rows = batch.num_rows();
        let groups_before = self.groups.len();
        match (source_int, source_float) {
            (Some(int_arr), None) => {
                for row in 0..num_rows {
                    let id1 = self.lane_id(0, &view1, row);
                    let key = match &view2 {
                        Some(view2) => ((id1 as u64) << 32) | self.lane_id(1, view2, row) as u64,
                        None => (id1 as u64) << 32,
                    };
                    let state = self.groups.entry(key).or_insert_with(BatchGroupState::new);
                    state.count += 1;
                    if !int_arr.is_null(row) {
                        let value = int_arr.value(row);
                        state.sum_int = state.sum_int.wrapping_add(value);
                        state.min_int = Some(state.min_int.map_or(value, |m| m.min(value)));
                        state.max_int = Some(state.max_int.map_or(value, |m| m.max(value)));
                    }
                }
            }
            (None, Some(float_arr)) => {
                for row in 0..num_rows {
                    let id1 = self.lane_id(0, &view1, row);
                    let key = match &view2 {
                        Some(view2) => ((id1 as u64) << 32) | self.lane_id(1, view2, row) as u64,
                        None => (id1 as u64) << 32,
                    };
                    let state = self.groups.entry(key).or_insert_with(BatchGroupState::new);
                    state.count += 1;
                    if !float_arr.is_null(row) {
                        let value = float_arr.value(row);
                        state.sum_float += value;
                        state.min_float = Some(state.min_float.map_or(value, |m| m.min(value)));
                        state.max_float = Some(state.max_float.map_or(value, |m| m.max(value)));
                    }
                }
            }
            (None, None) => {
                for row in 0..num_rows {
                    let id1 = self.lane_id(0, &view1, row);
                    let key = match &view2 {
                        Some(view2) => ((id1 as u64) << 32) | self.lane_id(1, view2, row) as u64,
                        None => (id1 as u64) << 32,
                    };
                    let state = self.groups.entry(key).or_insert_with(BatchGroupState::new);
                    state.count += 1;
                }
            }
            _ => return None,
        }
        self.groups_bytes += (self.groups.len() - groups_before) * GROUP_ENTRY_BYTES;
        Some(())
    }

    fn lane_id(&mut self, slot: usize, view: &BatchKeyView, row: usize) -> u32 {
        let lane = if slot == 0 {
            self.key1.as_mut().unwrap()
        } else {
            self.key2.as_mut().unwrap()
        };
        lane.id_at(view, row)
    }

    /// Finalize groups into the result batch (before HAVING/ORDER BY/LIMIT,
    /// which the caller applies with the shared operators).
    fn finish(&mut self, stmt: &SelectStatement) -> io::Result<RecordBatch> {
        let states: Vec<(u64, BatchGroupState)> = self.groups.drain().collect();
        let id1s: Vec<u32> = states.iter().map(|(key, _)| (*key >> 32) as u32).collect();
        let id2s: Vec<u32> = states.iter().map(|(key, _)| *key as u32).collect();

        let mut fields: Vec<Field> = Vec::with_capacity(stmt.columns.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(stmt.columns.len());

        for column in &stmt.columns {
            match column {
                SelectColumn::Column(name) | SelectColumn::ColumnAlias { column: name, .. } => {
                    let clean = name.trim_matches('"');
                    let clean = clean.rsplit('.').next().unwrap_or(clean);
                    let clean = clean.trim_matches('"');
                    let output_name = match column {
                        SelectColumn::ColumnAlias { alias, .. } => alias.as_str(),
                        _ => clean,
                    };
                    let slot = self
                        .group_cols
                        .iter()
                        .position(|group| group == clean)
                        .ok_or_else(|| err_input("SELECT column is not a GROUP BY key"))?;
                    let lane = if slot == 0 {
                        self.key1.as_ref().unwrap()
                    } else {
                        self.key2.as_ref().unwrap()
                    };
                    let ids = if slot == 0 { &id1s } else { &id2s };
                    let (arrow_dt, array): (ArrowDataType, ArrayRef) =
                        if let Some(values) = lane.output_int(ids) {
                            (ArrowDataType::Int64, Arc::new(Int64Array::from(values)))
                        } else if let Some(values) = lane.output_float(ids) {
                            (ArrowDataType::Float64, Arc::new(Float64Array::from(values)))
                        } else if let Some(values) = lane.output_bool(ids) {
                            (ArrowDataType::Boolean, Arc::new(BooleanArray::from(values)))
                        } else {
                            let values = lane.output_string(ids).unwrap();
                            (ArrowDataType::Utf8, Arc::new(StringArray::from(values)))
                        };
                    fields.push(Field::new(output_name, arrow_dt, true));
                    arrays.push(array);
                }
                SelectColumn::Aggregate {
                    func,
                    column,
                    alias,
                    ..
                } => {
                    let fn_name = match func {
                        AggregateFunc::Count => "COUNT",
                        AggregateFunc::Sum => "SUM",
                        AggregateFunc::Avg => "AVG",
                        AggregateFunc::Min => "MIN",
                        AggregateFunc::Max => "MAX",
                    };
                    let output_name = alias.clone().unwrap_or_else(|| {
                        if let Some(c) = column {
                            format!("{}({})", fn_name, c)
                        } else {
                            format!("{}(*)", fn_name)
                        }
                    });
                    match func {
                        AggregateFunc::Count => {
                            fields.push(Field::new(&output_name, ArrowDataType::Int64, false));
                            arrays.push(Arc::new(Int64Array::from(
                                states.iter().map(|(_, s)| s.count).collect::<Vec<_>>(),
                            )));
                        }
                        AggregateFunc::Sum => {
                            if self.source_is_int.unwrap_or(false) {
                                // Same field nullability as the single-batch
                                // kernel family: SUM is declared nullable even
                                // though the value is always present.
                                fields
                                    .push(Field::new(&output_name, ArrowDataType::Int64, true));
                                arrays.push(Arc::new(Int64Array::from(
                                    states.iter().map(|(_, s)| s.sum_int).collect::<Vec<_>>(),
                                )));
                            } else {
                                fields.push(
                                    Field::new(&output_name, ArrowDataType::Float64, true),
                                );
                                arrays.push(Arc::new(Float64Array::from(
                                    states.iter().map(|(_, s)| s.sum_float).collect::<Vec<_>>(),
                                )));
                            }
                        }
                        AggregateFunc::Avg => {
                            let avgs: Vec<Option<f64>> = states
                                .iter()
                                .map(|(_, s)| {
                                    if s.count > 0 {
                                        Some(if self.source_is_int.unwrap_or(false) {
                                            s.sum_int as f64 / s.count as f64
                                        } else {
                                            s.sum_float / s.count as f64
                                        })
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            fields.push(Field::new(&output_name, ArrowDataType::Float64, true));
                            arrays.push(Arc::new(Float64Array::from(avgs)));
                        }
                        AggregateFunc::Min => {
                            if self.source_is_int.unwrap_or(false) {
                                fields.push(Field::new(&output_name, ArrowDataType::Int64, true));
                                arrays.push(Arc::new(Int64Array::from(
                                    states.iter().map(|(_, s)| s.min_int).collect::<Vec<_>>(),
                                )));
                            } else {
                                fields.push(
                                    Field::new(&output_name, ArrowDataType::Float64, true),
                                );
                                arrays.push(Arc::new(Float64Array::from(
                                    states.iter().map(|(_, s)| s.min_float).collect::<Vec<_>>(),
                                )));
                            }
                        }
                        AggregateFunc::Max => {
                            if self.source_is_int.unwrap_or(false) {
                                fields.push(Field::new(&output_name, ArrowDataType::Int64, true));
                                arrays.push(Arc::new(Int64Array::from(
                                    states.iter().map(|(_, s)| s.max_int).collect::<Vec<_>>(),
                                )));
                            } else {
                                fields.push(
                                    Field::new(&output_name, ArrowDataType::Float64, true),
                                );
                                arrays.push(Arc::new(Float64Array::from(
                                    states.iter().map(|(_, s)| s.max_float).collect::<Vec<_>>(),
                                )));
                            }
                        }
                    }
                }
                _ => return Err(err_input("unsupported batch GROUP BY output")),
            }
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| err_data(e.to_string()))
    }

    /// Intern a decoded group-key value into the slot lane, creating the
    /// lane from the value's type on first use (parallel merge, R5.7).
    /// Returns None when the lane variant does not match the value.
    fn intern_lane_value(&mut self, slot: usize, value: Option<BatchKeyValue>) -> Option<u32> {
        let value = value?; // slot 0 is the NULL group in every lane
        let lane = match (slot, &mut self.key1, &mut self.key2) {
            (0, lane @ None, _) => {
                *lane = Some(Self::lane_for_value(&value));
                self.key1.as_mut().unwrap()
            }
            (1, _, lane @ None) => {
                *lane = Some(Self::lane_for_value(&value));
                self.key2.as_mut().unwrap()
            }
            _ => if slot == 0 {
                self.key1.as_mut()?
            } else {
                self.key2.as_mut()?
            },
        };
        lane.intern_value(Some(value))
    }

    fn lane_for_value(value: &BatchKeyValue) -> BatchKeyLane {
        match value {
            BatchKeyValue::Int(_) => BatchKeyLane::new_int(),
            BatchKeyValue::FloatBits(_) => BatchKeyLane::new_float(),
            BatchKeyValue::Bool(_) => BatchKeyLane::new_bool(),
            BatchKeyValue::Str(_) => BatchKeyLane::new_string(),
        }
    }
}

/// Per-thread fold outcome of one morsel chunk (parallel batch pipeline,
/// R5.7 phase A).
enum ParallelFusedOutcome {
    Folded(BatchGroupAggregator, u64),
    Err(io::Error),
    FallBack,
    Cancelled,
}

/// Process-wide in-flight worker budget for the parallel batch pipeline
/// (design §14.8.3; R5.11 B phase): capacity min(hardware_concurrency - 1,
/// 8) - the cap fixed by the R5.11 speedup curves (8 workers: 1M 5.49x
/// vs 3.28x at 4, 200K plateau at 4, no loss) and the cap-8 contention
/// matrix (12/12 cells throughput >= serial, p99 max 1.77x < 2.0x).
/// Lazy, zero state until the first parallel request. Registered in
/// docs/RESOURCE_OWNERSHIP.md.
static PARALLEL_SCAN_TOKENS: std::sync::OnceLock<std::sync::atomic::AtomicUsize> =
    std::sync::OnceLock::new();

/// In-flight worker budget capacity: min(hardware_concurrency - 1, 8).
fn parallel_scan_capacity() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
        .saturating_sub(1)
        .min(8)
}

fn parallel_scan_tokens() -> &'static std::sync::atomic::AtomicUsize {
    PARALLEL_SCAN_TOKENS
        .get_or_init(|| std::sync::atomic::AtomicUsize::new(parallel_scan_capacity()))
}

/// `APEX_PARALLEL_SCAN` diagnostic override (R5.12 semantics): unset
/// lets the cost-based default decide; explicit 0/1 or malformed values
/// force the serial default; explicit N >= 2 forces N requested workers,
/// mirroring the APEX_BATCH_SCAN diagnostics.
enum ParallelScanOverride {
    Auto,
    ForceSerial,
    Request(usize),
}

/// Holds in-flight worker tokens; returns them on drop so every exit path
/// of the parallel path releases the budget (RAII).
struct ParallelTokenGuard {
    count: usize,
}

impl Drop for ParallelTokenGuard {
    fn drop(&mut self) {
        parallel_scan_tokens()
            .fetch_add(self.count, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
pub(crate) fn exhaust_parallel_tokens_for_test() -> ParallelTokenGuard {
    let count = parallel_scan_tokens().swap(0, std::sync::atomic::Ordering::AcqRel);
    ParallelTokenGuard { count }
}

impl ApexExecutor {
    /// Serial batched Filter -> GROUP BY -> HAVING -> TopK over the stable
    /// row-group stream. Bounded-scan-memory counterpart of
    /// `try_scan_group_pipeline`; returns `Ok(None)` to fall back to the
    /// single-batch path when the query shape, the table state, or a batch
    /// column is outside this kernel's gate.
    fn try_batch_group_pipeline(
        backend: &TableStorageBackend,
        stmt: &SelectStatement,
        predicate: &crate::storage::ScanPredicateExpr,
        table_key: &str,
    ) -> io::Result<Option<ApexResult>> {
        if !Self::batch_scan_enabled() {
            return Ok(None);
        }
        if stmt.where_clause.is_none()
            || !stmt.joins.is_empty()
            || stmt.distinct
            || stmt.distinct_on.is_some()
            || stmt.group_by_exprs.iter().any(Option::is_some)
            || stmt
                .columns
                .iter()
                .any(|column| matches!(column, SelectColumn::WindowFunction { .. }))
        {
            return Ok(None);
        }

        // Same HAVING extra-aggregate injection as execute_group_by so the
        // HAVING expression can reference aggregates not in the SELECT list.
        let select_col_count = stmt.columns.len();
        let mut owned_stmt = None;
        let extra_agg_count = if let Some(having_expr) = &stmt.having {
            let extras = Self::collect_having_extra_aggs(having_expr, &stmt.columns);
            if !extras.is_empty() {
                let count = extras.len();
                let mut s = stmt.clone();
                for (func, col) in extras {
                    let fn_name = match func {
                        AggregateFunc::Count => "COUNT",
                        AggregateFunc::Sum => "SUM",
                        AggregateFunc::Avg => "AVG",
                        AggregateFunc::Min => "MIN",
                        AggregateFunc::Max => "MAX",
                    };
                    let alias = format!("{}({})", fn_name, col.as_deref().unwrap_or("*"));
                    s.columns.push(SelectColumn::Aggregate {
                        func,
                        column: col,
                        distinct: false,
                        alias: Some(alias),
                    });
                }
                owned_stmt = Some(s);
                count
            } else {
                0
            }
        } else {
            0
        };
        let effective_stmt = owned_stmt.as_ref().unwrap_or(stmt);

        // Shape gate: unsupported aggregate shapes never reach a stream.
        if BatchGroupAggregator::new(effective_stmt).is_none() {
            return Ok(None);
        }
        // Re-verify with the shared gate so injected HAVING aggregates are
        // covered by the same rules as the single-batch path.
        if !Self::can_use_incremental_aggregation(effective_stmt) {
            return Ok(None);
        }

        let Some(columns) = Self::get_col_refs(stmt) else {
            return Ok(None);
        };
        let column_refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
        let Some(projection) = Self::filter_columns_for_backend(backend, &column_refs) else {
            return Ok(None);
        };
        let request = crate::storage::ScanRequest {
            projection: Some(&projection),
            predicate: Some(predicate),
        };
        // Parallel batch scan (architecture review R5.11/R5.12): fused
        // scan+fold workers over contiguous row-group ranges; the
        // process-wide in-flight budget decides how many are actually
        // granted. APEX_PARALLEL_SCAN=N (N >= 2) forces N, 0/1/malformed
        // forces serial, unset runs the cost-based auto-enable; a query
        // granted no free tokens stays on the serial streaming default,
        // so oversubscription stays bounded.
        let granted = Self::parallel_workers_requested(table_key, stmt)
            .and_then(|requested| Self::try_acquire_parallel_tokens(requested));
        // Per-query aggregation budget (S1); shared by every worker fold.
        let budget = crate::query::executor::query_memory_budget();
        let (agg, batch_count, parallel_threads) = match granted {
            Some(threads) => {
                let _token_guard = ParallelTokenGuard { count: threads };
                let Some(mut ranges) = backend.scan_batches_ranges(&request, threads)? else {
                    return Ok(None);
                };
                if ranges.len() >= 2 {
                    // Pool workers cannot see the caller's thread-local
                    // cancel slot; capture the shared token before
                    // dispatching.
                    let cancel = crate::query::executor::query_cancel_token();
                    let Some((agg, count)) = Self::parallel_fused_scan_fold(
                        ranges,
                        effective_stmt,
                        cancel,
                        budget.as_deref(),
                    )?
                    else {
                        return Ok(None);
                    };
                    (agg, count, Some(threads))
                } else {
                    // A single row group has nothing to parallelize.
                    let Some((agg, count)) =
                        Self::serial_fold_stream(&mut ranges[0], effective_stmt, budget.as_deref())?
                    else {
                        return Ok(None);
                    };
                    (agg, count, None)
                }
            }
            None => {
                let Some(mut stream) = backend.scan_batches(&request)? else {
                    return Ok(None);
                };
                let Some((agg, count)) =
                    Self::serial_fold_stream(&mut stream, effective_stmt, budget.as_deref())?
                else {
                    return Ok(None);
                };
                (agg, count, None)
            }
        };
        let mut agg = agg;

        let mut result = ApexResult::Data(agg.finish(effective_stmt)?);

        // HAVING applies after aggregation, before TopK — the same order as
        // the single-batch path.
        if let Some(having_expr) = &effective_stmt.having {
            if let ApexResult::Data(batch) = &result {
                let mask = Self::evaluate_predicate(batch, having_expr)?;
                let filtered = compute::filter_record_batch(batch, &mask)
                    .map_err(|e| err_data(e.to_string()))?;
                result = ApexResult::Data(filtered);
            }
        }
        if !effective_stmt.order_by.is_empty() {
            if let ApexResult::Data(batch) = result {
                let resolved = Self::resolve_order_by_cols(
                    &effective_stmt.columns,
                    &effective_stmt.order_by,
                );
                let k = effective_stmt
                    .limit
                    .map(|limit| limit + effective_stmt.offset.unwrap_or(0));
                let sorted = Self::apply_order_by_topk(&batch, &resolved, k)?;
                result = ApexResult::Data(sorted);
            }
        }
        if let ApexResult::Data(batch) = result {
            let limited = Self::apply_limit_offset(
                &batch,
                effective_stmt.limit,
                effective_stmt.offset,
            )?;
            result = ApexResult::Data(limited);
        }

        // Strip the HAVING-only aggregate columns injected above.
        if extra_agg_count > 0 {
            if let ApexResult::Data(batch) = result {
                let keep = select_col_count.min(batch.num_columns());
                let new_schema = Arc::new(Schema::new(
                    batch.schema().fields()[..keep]
                        .iter()
                        .map(|f| f.as_ref().clone())
                        .collect::<Vec<_>>(),
                ));
                let new_arrays: Vec<ArrayRef> = (0..keep).map(|i| batch.column(i).clone()).collect();
                result = ApexResult::Data(
                    RecordBatch::try_new(new_schema, new_arrays)
                        .map_err(|e| err_data(e.to_string()))?,
                );
            }
        }

        crate::query::executor::record_path("batched_scan_pipeline");
        match parallel_threads {
            Some(threads) => crate::query::executor::record_path_detail_f(format_args!(
                "(batches={batch_count}, parallel={threads})"
            )),
            None => crate::query::executor::record_path_detail_f(
                format_args!("(batches={batch_count})"),
            ),
        }
        Ok(Some(result))
    }

    /// The batched scan pipeline is enabled by default; `APEX_BATCH_SCAN=0`
    /// disables it for A/B diagnostics (architecture review R3).
    fn batch_scan_enabled() -> bool {
        match std::env::var_os("APEX_BATCH_SCAN") {
            Some(value) => value != "0",
            None => true,
        }
    }

    fn parallel_scan_override() -> ParallelScanOverride {
        let Some(value) = std::env::var_os("APEX_PARALLEL_SCAN") else {
            return ParallelScanOverride::Auto;
        };
        let Some(text) = value.to_str() else {
            return ParallelScanOverride::ForceSerial;
        };
        let Some(requested) = text.parse::<usize>().ok() else {
            return ParallelScanOverride::ForceSerial;
        };
        if requested >= 2 {
            ParallelScanOverride::Request(requested)
        } else {
            ParallelScanOverride::ForceSerial
        }
    }

    /// Requested worker count for the parallel batch pipeline: the
    /// explicit override always wins; with APEX_PARALLEL_SCAN unset the
    /// cost-based default decides (R5.12).
    fn parallel_workers_requested(
        table_key: &str,
        stmt: &crate::query::sql_parser::SelectStatement,
    ) -> Option<usize> {
        match Self::parallel_scan_override() {
            ParallelScanOverride::Request(requested) => Some(requested),
            ParallelScanOverride::ForceSerial => None,
            ParallelScanOverride::Auto => Self::auto_parallel_workers(table_key, stmt),
        }
    }

    /// Cost-based auto-enable (architecture review R5.12): request the
    /// in-flight budget cap when the R5.3-calibrated serial prediction
    /// for (table, shape) reaches the 2 ms threshold and the measured
    /// parallel history (if any) is still strictly faster than that
    /// prediction - otherwise stay serial. The explicit override bypasses
    /// this entirely. Contention degrades through the existing
    /// min(requested, available) token mechanism.
    fn auto_parallel_workers(
        table_key: &str,
        stmt: &crate::query::sql_parser::SelectStatement,
    ) -> Option<usize> {
        let (predicted_serial_us, parallel_samples, parallel_time_avg_us) =
            crate::query::planner::parallel_decision_input(table_key, stmt)?;
        if predicted_serial_us < crate::query::planner::PARALLEL_SCAN_AUTO_ENABLE_US {
            return None;
        }
        if parallel_samples > 0 && parallel_time_avg_us >= predicted_serial_us {
            return None;
        }
        Some(parallel_scan_capacity())
    }

    /// Take in-flight fold tokens for one query: min(requested, available);
    /// below two threads the query stays serial (R5.7 phase A, §14.8.3).
    fn try_acquire_parallel_tokens(requested: usize) -> Option<usize> {
        let tokens = parallel_scan_tokens();
        let mut available = tokens.load(Ordering::Acquire);
        loop {
            let want = requested.min(available);
            if want < 2 {
                return None;
            }
            match tokens.compare_exchange_weak(
                available,
                available - want,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(want),
                Err(current) => available = current,
            }
        }
    }

    /// Serial streaming fold: the default path and the parallel-path
    /// fallbacks (single row group, or no free tokens). Cancellation is
    /// checked at batch boundaries (one atomic load per row group),
    /// never per row (architecture review R4).
    fn serial_fold_stream(
        stream: &mut crate::storage::BatchMorselStream,
        stmt: &SelectStatement,
        budget: Option<&QueryMemoryBudget>,
    ) -> io::Result<Option<(BatchGroupAggregator, u64)>> {
        let Some(mut agg) = BatchGroupAggregator::new(stmt) else {
            return Ok(None);
        };
        let mut charged = agg.state_bytes();
        let mut batch_count: u64 = 0;
        loop {
            if crate::query::executor::query_cancelled() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "query cancelled",
                ));
            }
            match stream.next() {
                Some(Ok(crate::storage::BatchMorselOutcome::Morsel(morsel))) => {
                    batch_count += 1;
                    let batch = morsel.into_record_batch()?;
                    if agg.consume_batch(&batch).is_none() {
                        // The single-batch path continues this query; release
                        // what this kernel reserved so it is not double-counted.
                        release_state(budget, charged);
                        return Ok(None);
                    }
                    let current = agg.state_bytes();
                    if let Err(error) = charge_state(budget, current - charged) {
                        release_state(budget, charged);
                        return Err(error);
                    }
                    charged = current;
                }
                Some(Ok(crate::storage::BatchMorselOutcome::Unsupported)) => {
                    release_state(budget, charged);
                    return Ok(None);
                }
                Some(Err(error)) => return Err(error),
                None => break,
            }
        }
        Ok(Some((agg, batch_count)))
    }

    /// Deterministic merge of one folded partial into the running merged
    /// state: re-intern the partial's key lane values into the merged
    /// lanes and add per-group state (range/chunk order, deterministic).
    fn merge_partial_into(
        merged: &mut BatchGroupAggregator,
        partial: &BatchGroupAggregator,
    ) -> Option<()> {
        merged.source_is_int = merged.source_is_int.or(partial.source_is_int);
        let two_keys = partial.group_cols.len() == 2;
        let groups_before = merged.groups.len();
        for (key, state) in partial.groups.iter() {
            let id1 = (*key >> 32) as u32;
            let value1 = partial.key1.as_ref().and_then(|lane| lane.value_at(id1));
            let Some(global1) = merged.intern_lane_value(0, value1) else {
                return None;
            };
            let global2 = if two_keys {
                let id2 = *key as u32;
                let value2 = partial.key2.as_ref().and_then(|lane| lane.value_at(id2));
                let Some(global2) = merged.intern_lane_value(1, value2) else {
                    return None;
                };
                global2
            } else {
                0
            };
            let merged_key = ((global1 as u64) << 32) | global2 as u64;
            merged
                .groups
                .entry(merged_key)
                .or_insert_with(BatchGroupState::new)
                .merge_from(state);
        }
        merged.groups_bytes += (merged.groups.len() - groups_before) * GROUP_ENTRY_BYTES;
        Some(())
    }

    /// Fused parallel scan+fold (R5.11 B phase): each worker owns one
    /// contiguous row-group range as an independent stream over the same
    /// stable read view and folds its morsels into a partial group state;
    /// partials merge in range order (deterministic, re-interning key
    /// values through the merged lanes). The per-query dedicated pool
    /// keeps the scan fold off the rayon global pool, which the vector
    /// kernels own (§14.8.3); the pool is dropped at the end of the
    /// query, so no background threads survive it.
    fn parallel_fused_scan_fold(
        ranges: Vec<crate::storage::BatchMorselStream>,
        stmt: &SelectStatement,
        cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
        budget: Option<&QueryMemoryBudget>,
    ) -> io::Result<Option<(BatchGroupAggregator, u64)>> {
        use rayon::prelude::*;

        let worker_count = ranges.len();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(worker_count)
            .build()
            .map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::Other, error.to_string())
            })?;
        // Each worker scans+folds its range into a partial state;
        // collect keeps range order for the deterministic merge. Every
        // partial charges the query budget as it grows, so the peak across
        // all live partials (which is where parallel state amplifies) is
        // bounded, not just the merged result.
        let partials: Vec<ParallelFusedOutcome> = pool.install(|| {
            ranges
                .into_par_iter()
                .map(|mut stream| {
                    let mut agg = match BatchGroupAggregator::new(stmt) {
                        Some(agg) => agg,
                        None => return ParallelFusedOutcome::FallBack,
                    };
                    let mut charged = agg.state_bytes();
                    let mut batch_count: u64 = 0;
                    for outcome in &mut stream {
                        // Cancellation is checked at batch boundaries (one
                        // atomic load per row group), never per row (R4).
                        if cancel
                            .as_ref()
                            .is_some_and(|token| token.load(std::sync::atomic::Ordering::Acquire))
                        {
                            release_state(budget, charged);
                            return ParallelFusedOutcome::Cancelled;
                        }
                        match outcome {
                            Ok(crate::storage::BatchMorselOutcome::Morsel(morsel)) => {
                                batch_count += 1;
                                let batch = match morsel.into_record_batch() {
                                    Ok(batch) => batch,
                                    Err(error) => {
                                        release_state(budget, charged);
                                        return ParallelFusedOutcome::Err(error)
                                    }
                                };
                                if agg.consume_batch(&batch).is_none() {
                                    release_state(budget, charged);
                                    return ParallelFusedOutcome::FallBack;
                                }
                                let current = agg.state_bytes();
                                if let Err(error) = charge_state(budget, current - charged) {
                                    release_state(budget, charged);
                                    return ParallelFusedOutcome::Err(error);
                                }
                                charged = current;
                            }
                            Ok(crate::storage::BatchMorselOutcome::Unsupported) => {
                                release_state(budget, charged);
                                return ParallelFusedOutcome::FallBack;
                            }
                            Err(error) => {
                                release_state(budget, charged);
                                return ParallelFusedOutcome::Err(error);
                            }
                        }
                    }
                    ParallelFusedOutcome::Folded(agg, batch_count)
                })
                .collect()
        });
        // Charges held by partial folds that have not been merged yet.
        let mut unmerged: usize = partials
            .iter()
            .map(|outcome| match outcome {
                ParallelFusedOutcome::Folded(partial, _) => partial.state_bytes(),
                _ => 0,
            })
            .sum();
        let mut merged_charge: usize = 0;
        let mut merged: Option<BatchGroupAggregator> = None;
        let mut batch_count: u64 = 0;
        for outcome in partials {
            match outcome {
                ParallelFusedOutcome::Cancelled => {
                    release_state(budget, unmerged + merged_charge);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "query cancelled",
                    ));
                }
                ParallelFusedOutcome::Err(error) => {
                    release_state(budget, unmerged + merged_charge);
                    return Err(error);
                }
                ParallelFusedOutcome::FallBack => {
                    release_state(budget, unmerged + merged_charge);
                    return Ok(None);
                }
                ParallelFusedOutcome::Folded(partial, count) => {
                    batch_count += count;
                    let partial_bytes = partial.state_bytes();
                    unmerged = unmerged.saturating_sub(partial_bytes);
                    let merged = merged.get_or_insert_with(|| {
                        BatchGroupAggregator::new(stmt).expect(
                            "the shape gate verified this statement before the fold"
                        )
                    });
                    let before = merged.state_bytes();
                    if Self::merge_partial_into(merged, &partial).is_none() {
                        release_state(budget, unmerged + merged_charge + partial_bytes);
                        return Ok(None);
                    }
                    let growth = merged.state_bytes() - before;
                    if let Err(error) = charge_state(budget, growth) {
                        release_state(budget, unmerged + merged_charge + partial_bytes);
                        return Err(error);
                    }
                    merged_charge += growth;
                    // The partial is dropped after this iteration; only the
                    // merged state stays resident.
                    release_state(budget, partial_bytes);
                }
            }
        }
        let Some(merged) = merged else {
            return Ok(None);
        };
        Ok(Some((merged, batch_count)))
    }
}
