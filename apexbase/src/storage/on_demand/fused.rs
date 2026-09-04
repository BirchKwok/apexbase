// ============================================================================
// Fused predicate-lane group aggregation
//
// Single streaming pass over row groups that evaluates a small boolean
// predicate over the group column's dictionary ids and up to two numeric
// lanes, accumulating count/sum/min/max per group directly. No candidate
// index, no gather, no intermediate materialization.
// ============================================================================

/// Numeric lane of the fused group aggregation kernel.
#[derive(Debug, Clone)]
pub struct FusedLaneSpec {
    pub col: String,
    /// Integer-family column: values are compared as i64 (exact).
    pub is_int: bool,
}

/// Predicate leaf: a typed comparison against a numeric lane, or a
/// membership test over the group column's dictionary id.
#[derive(Debug, Clone, PartialEq)]
pub enum FusedLeaf {
    /// Lane value inside the inclusive `[lo, hi]` range.
    RangeI64 { lane: usize, lo: i64, hi: i64 },
    RangeF64 { lane: usize, lo: f64, hi: f64 },
    /// Lane value in a sorted, de-duplicated value set (≤16 values).
    InI64 { lane: usize, values: Vec<i64> },
    InF64 { lane: usize, values: Vec<f64> },
    /// Per-slot membership flags over the group column's dictionary
    /// (slot i is 1 when value i is in the IN set). Built once per query;
    /// the per-row test is a single load.
    DictIn { flags: Vec<u8> },
    /// Group column dictionary id equals the key.
    DictEq { key: u16 },
}

/// Boolean tree over fused leaves.
#[derive(Debug, Clone)]
pub enum FusedPredicate {
    Leaf(FusedLeaf),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
}

/// Per-group aggregation produced by the fused kernel.
#[derive(Debug, Clone, Default)]
pub struct FusedGroupAgg {
    pub sum: f64,
    pub count: i64,
    pub min: f64,
    pub max: f64,
}

/// Outcome of decoding one numeric lane for the fused kernel.
enum FusedLaneDecoded<'a> {
    /// PLAIN int column, 8-byte aligned: zero-copy view of the row-group body.
    PlainI64(&'a [i64]),
    /// PLAIN float column, 8-byte aligned: zero-copy view of the row-group body.
    PlainF64(&'a [f64]),
    /// PLAIN int column at a misaligned offset: raw body bytes kept in
    /// place; consumers read 8-byte slots with `read_unaligned` (no copy).
    RawI64(&'a [u8]),
    /// PLAIN float column at a misaligned offset: raw body bytes in place.
    RawF64(&'a [u8]),
    /// Non-PLAIN column decoded into the caller's i64 buffer.
    BufferI64,
    /// Narrow BITPACK integer decoded as one-byte deltas plus a shared base.
    BufferU8(i64),
    /// Non-PLAIN column decoded into the caller's f64 buffer.
    BufferF64,
    /// The lane could not be decoded.
    Missing,
}

/// One row group's decoded lane. Typed views are zero-copy slices; raw
/// views keep misaligned PLAIN data in place so the hot loops read it
/// unaligned instead of paying a per-RG copy and allocation.
enum FusedLaneView<'a> {
    I64(&'a [i64]),
    F64(&'a [f64]),
    U8(&'a [u8], i64),
    RawI64(&'a [u8]),
    RawF64(&'a [u8]),
    Missing,
}

impl<'a> FusedLaneView<'a> {
    /// Number of values held by the view.
    fn len(&self) -> usize {
        match self {
            FusedLaneView::I64(d) => d.len(),
            FusedLaneView::F64(d) => d.len(),
            FusedLaneView::U8(d, _) => d.len(),
            FusedLaneView::RawI64(d) => d.len() / 8,
            FusedLaneView::RawF64(d) => d.len() / 8,
            FusedLaneView::Missing => 0,
        }
    }

    /// Byte address of value 0. Consumers read 8-byte slots with
    /// `read_unaligned`, so aligned and raw views share one code path.
    fn data_ptr(&self) -> *const u8 {
        match self {
            FusedLaneView::I64(d) => d.as_ptr() as *const u8,
            FusedLaneView::F64(d) => d.as_ptr() as *const u8,
            FusedLaneView::U8(d, _) => d.as_ptr(),
            FusedLaneView::RawI64(d) => d.as_ptr(),
            FusedLaneView::RawF64(d) => d.as_ptr(),
            FusedLaneView::Missing => std::ptr::null(),
        }
    }
}

/// Materialize raw 8-byte slots into a typed buffer (little-endian bit
/// casts compile down to a single memcpy).
fn copy_raw_i64(d: &[u8], out: &mut Vec<i64>) {
    out.clear();
    out.reserve(d.len() / 8);
    for chunk in d.chunks_exact(8) {
        out.push(i64::from_ne_bytes(chunk.try_into().unwrap()));
    }
}

fn copy_raw_f64(d: &[u8], out: &mut Vec<f64>) {
    out.clear();
    out.reserve(d.len() / 8);
    for chunk in d.chunks_exact(8) {
        out.push(f64::from_bits(u64::from_ne_bytes(chunk.try_into().unwrap())));
    }
}

/// Decode a narrow BITPACK stream into byte deltas. Eight values occupy
/// exactly `bit_width` bytes, so each group needs one word load and eight
/// byte stores instead of two word loads plus an 8-byte store per row.
fn bitpack_fill_u8(packed: &[u8], count: usize, bit_width: usize, out: &mut [u8]) {
    debug_assert!(bit_width > 0 && bit_width <= 8 && out.len() >= count);
    let mask = (1u64 << bit_width) - 1;
    let groups = count / 8;
    for group in 0..groups {
        let byte_offset = group * bit_width;
        let word = if byte_offset + 8 <= packed.len() {
            unsafe { std::ptr::read_unaligned(packed.as_ptr().add(byte_offset) as *const u64) }
        } else {
            let mut tail = [0u8; 8];
            let available = packed.len().saturating_sub(byte_offset).min(8);
            tail[..available].copy_from_slice(&packed[byte_offset..byte_offset + available]);
            u64::from_le_bytes(tail)
        };
        let row = group * 8;
        out[row] = (word & mask) as u8;
        out[row + 1] = ((word >> bit_width) & mask) as u8;
        out[row + 2] = ((word >> (2 * bit_width)) & mask) as u8;
        out[row + 3] = ((word >> (3 * bit_width)) & mask) as u8;
        out[row + 4] = ((word >> (4 * bit_width)) & mask) as u8;
        out[row + 5] = ((word >> (5 * bit_width)) & mask) as u8;
        out[row + 6] = ((word >> (6 * bit_width)) & mask) as u8;
        out[row + 7] = ((word >> (7 * bit_width)) & mask) as u8;
    }
    for row in groups * 8..count {
        let bit_offset = row * bit_width;
        let byte_offset = bit_offset / 8;
        let shift = bit_offset % 8;
        let mut word = packed[byte_offset] as u16;
        if shift + bit_width > 8 {
            word |= (packed[byte_offset + 1] as u16) << 8;
        }
        out[row] = ((word >> shift) as u64 & mask) as u8;
    }
}

impl<'a> FusedLaneDecoded<'a> {
    /// Record this lane's view (zero-copy body view or the caller's
    /// buffer) in the per-row-group view vector.
    fn push_view(
        self,
        buf_i64: &'a Vec<i64>,
        buf_f64: &'a Vec<f64>,
        buf_u8: &'a Vec<u8>,
        views: &mut Vec<FusedLaneView<'a>>,
    ) {
        match self {
            FusedLaneDecoded::PlainI64(v) => views.push(FusedLaneView::I64(v)),
            FusedLaneDecoded::PlainF64(v) => views.push(FusedLaneView::F64(v)),
            FusedLaneDecoded::RawI64(v) => views.push(FusedLaneView::RawI64(v)),
            FusedLaneDecoded::RawF64(v) => views.push(FusedLaneView::RawF64(v)),
            FusedLaneDecoded::BufferI64 => views.push(FusedLaneView::I64(buf_i64)),
            FusedLaneDecoded::BufferF64 => views.push(FusedLaneView::F64(buf_f64)),
            FusedLaneDecoded::BufferU8(base) => views.push(FusedLaneView::U8(buf_u8, base)),
            FusedLaneDecoded::Missing => views.push(FusedLaneView::Missing),
        }
    }
}

/// Per-slot compiled predicate: the group column's dictionary leaves are
/// constant-folded for this dictionary slot; the remaining numeric lane
/// leaves are referenced by index into the plan's lane-leaf table.
#[derive(Debug, Clone, Copy)]
enum SlotPlan {
    All,
    None,
    One(u8),
    NotOne(u8),
    And(u8, u8),
    Or(u8, u8),
    /// `(a op1 b) op2 c`, all three lane leaves.
    Bin3 { op1: u8, op2: u8, a: u8, b: u8, c: u8 },
    /// Deeper trees are evaluated through the shared node table.
    Tree { root: u16 },
}

/// Fallback tree node: indices reference the plan's shared node table,
/// whose first `leaves.len()` entries are the lane leaves.
#[derive(Debug, Clone, Copy)]
enum SlotNode {
    Leaf(u8),
    And(u16, u16),
    Or(u16, u16),
    Not(u16),
}

/// How the per-slot predicate is assembled from the slot-independent
/// common part and the per-slot extra part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FuseMode {
    /// Every slot keeps its own flat plan (no common part).
    Pure,
    /// plan(slot) = common AND extra(slot).
    AndCommon,
    /// plan(slot) = common OR extra(slot).
    OrCommon,
}

/// Representation of the per-slot extra part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExtraKind {
    /// Every extra is All: no per-row extra work.
    All,
    /// Extras restricted to All (0), None (0xFF) or One(leaf) (leaf + 1).
    OneMask,
    /// Full per-slot SlotPlan table.
    Full,
}

/// Per-query compiled plan: a slot-independent common expression plus a
/// compact per-slot extra part, over the shared lane-leaf table. The hot
/// path is one flat expression plus at most one small per-slot dispatch —
/// no recursion and no tree indirection.
struct FusedCompiledPlan {
    mode: FuseMode,
    common: SlotPlan,
    extra_kind: ExtraKind,
    extra_mask: Vec<u8>,
    slot_plans: Vec<SlotPlan>,
    leaves: Vec<FusedLeaf>,
    nodes: Vec<SlotNode>,
    all_none: bool,
}

impl FusedCompiledPlan {
    /// Full predicate mask for one row: 0 or 1.
    #[inline(always)]
    fn row_mask(&self, gid: usize, row: usize, leaf_views: &[FusedLeafView]) -> i64 {
        match self.mode {
            FuseMode::Pure => self.slot_mask(gid, row, leaf_views),
            FuseMode::AndCommon => {
                let cm = self.common_mask(row, leaf_views);
                if cm == 0 {
                    return 0;
                }
                match self.extra_kind {
                    ExtraKind::All => cm,
                    ExtraKind::OneMask => {
                        let e = unsafe { *self.extra_mask.get_unchecked(gid) };
                        if e == 0 {
                            cm
                        } else if e == 0xFF {
                            0
                        } else {
                            cm & leaf_views[(e - 1) as usize].eval(row) as i64
                        }
                    }
                    ExtraKind::Full => cm & self.slot_mask(gid, row, leaf_views),
                }
            }
            FuseMode::OrCommon => {
                if self.common_mask(row, leaf_views) != 0 {
                    return 1;
                }
                match self.extra_kind {
                    ExtraKind::All => 1,
                    ExtraKind::OneMask => {
                        let e = unsafe { *self.extra_mask.get_unchecked(gid) };
                        if e == 0 {
                            1
                        } else if e == 0xFF {
                            0
                        } else {
                            leaf_views[(e - 1) as usize].eval(row) as i64
                        }
                    }
                    ExtraKind::Full => self.slot_mask(gid, row, leaf_views),
                }
            }
        }
    }

    /// Evaluate the slot-independent common part: 0 or 1.
    #[inline(always)]
    fn common_mask(&self, row: usize, leaf_views: &[FusedLeafView]) -> i64 {
        match self.common {
            SlotPlan::All => 1,
            SlotPlan::None => 0,
            SlotPlan::One(l) => leaf_views[l as usize].eval(row) as i64,
            SlotPlan::NotOne(l) => leaf_views[l as usize].eval(row) as i64 ^ 1,
            SlotPlan::And(a, b) => {
                (leaf_views[a as usize].eval(row) && leaf_views[b as usize].eval(row)) as i64
            }
            SlotPlan::Or(a, b) => {
                (leaf_views[a as usize].eval(row) || leaf_views[b as usize].eval(row)) as i64
            }
            SlotPlan::Bin3 { op1, op2, a, b, c } => {
                let x = leaf_views[a as usize].eval(row);
                let y = leaf_views[b as usize].eval(row);
                let m = if op1 == 0 { x && y } else { x || y };
                (if op2 == 0 { m && leaf_views[c as usize].eval(row) } else { m || leaf_views[c as usize].eval(row) }) as i64
            }
            SlotPlan::Tree { .. } => unreachable!("Tree common parts fall back to Pure mode"),
        }
    }

    /// Evaluate the slot's predicate for one row: 0 or 1. Kept out of the
    /// hot inlines: only Pure-mode and Full-extra rows reach it.
    #[inline]
    fn slot_mask(
        &self,
        gid: usize,
        row: usize,
        leaf_views: &[FusedLeafView],
    ) -> i64 {
        let plan = unsafe { *self.slot_plans.get_unchecked(gid) };
        match plan {
            SlotPlan::All => 1,
            SlotPlan::None => 0,
            SlotPlan::One(l) => leaf_views[l as usize].eval(row) as i64,
            SlotPlan::NotOne(l) => leaf_views[l as usize].eval(row) as i64 ^ 1,
            SlotPlan::And(a, b) => {
                (leaf_views[a as usize].eval(row) && leaf_views[b as usize].eval(row)) as i64
            }
            SlotPlan::Or(a, b) => {
                (leaf_views[a as usize].eval(row) || leaf_views[b as usize].eval(row)) as i64
            }
            SlotPlan::Bin3 { op1, op2, a, b, c } => {
                let x = leaf_views[a as usize].eval(row);
                let y = leaf_views[b as usize].eval(row);
                let m = if op1 == 0 { x && y } else { x || y };
                (if op2 == 0 { m && leaf_views[c as usize].eval(row) } else { m || leaf_views[c as usize].eval(row) }) as i64
            }
            SlotPlan::Tree { root } => self.node_mask(root as usize, row, leaf_views),
        }
    }

    /// Fallback evaluation for trees deeper than the flat shapes.
    #[inline]
    fn node_mask(
        &self,
        r: usize,
        row: usize,
        leaf_views: &[FusedLeafView],
    ) -> i64 {
        match &self.nodes[r] {
            SlotNode::Leaf(l) => leaf_views[*l as usize].eval(row) as i64,
            SlotNode::And(a, b) => {
                if self.node_mask(*a as usize, row, leaf_views) == 0 {
                    0
                } else {
                    self.node_mask(*b as usize, row, leaf_views)
                }
            }
            SlotNode::Or(a, b) => {
                if self.node_mask(*a as usize, row, leaf_views) != 0 {
                    1
                } else {
                    self.node_mask(*b as usize, row, leaf_views)
                }
            }
            SlotNode::Not(a) => self.node_mask(*a as usize, row, leaf_views) ^ 1,
        }
    }
}

/// A lane leaf resolved against one row group: data pointer and comparison
/// data are inlined so the per-row evaluation is a single match with no
/// Option indexing or expect paths.
enum FusedLeafView<'a> {
    RangeI64(&'a [i64], i64, i64),
    RangeF64(&'a [f64], f64, f64),
    InI64(&'a [i64], &'a [i64]),
    InF64(&'a [f64], &'a [f64]),
    Missing,
}

impl<'a> FusedLeafView<'a> {
    #[inline(always)]
    fn eval(&self, row: usize) -> bool {
        match self {
            FusedLeafView::RangeI64(d, lo, hi) => {
                let v = unsafe { *d.get_unchecked(row) };
                v >= *lo && v <= *hi
            }
            FusedLeafView::RangeF64(d, lo, hi) => {
                let v = unsafe { *d.get_unchecked(row) };
                v >= *lo && v <= *hi
            }
            FusedLeafView::InI64(d, values) => {
                let v = unsafe { *d.get_unchecked(row) };
                match values.len() {
                    1 => values[0] == v,
                    2 => values[0] == v || values[1] == v,
                    3 => values[0] == v || values[1] == v || values[2] == v,
                    4 => values[0] == v || values[1] == v || values[2] == v || values[3] == v,
                    _ => values.binary_search(&v).is_ok(),
                }
            }
            FusedLeafView::InF64(d, values) => {
                let v = unsafe { *d.get_unchecked(row) };
                // f64 is not Ord: explicit small-set comparisons,
                // partition_point probe for larger sets.
                match values.len() {
                    1 => values[0] == v,
                    2 => values[0] == v || values[1] == v,
                    3 => values[0] == v || values[1] == v || values[2] == v,
                    4 => values[0] == v || values[1] == v || values[2] == v || values[3] == v,
                    _ => {
                        let pos = values.partition_point(|x| *x < v);
                        values.get(pos).is_some_and(|x| *x == v)
                    }
                }
            }
            FusedLeafView::Missing => false,
        }
    }
}

/// Resolve the plan's lane leaves against one row group's decoded views.
fn resolve_fused_leaf_views<'a>(
    plan: &'a FusedCompiledPlan,
    views_i64: &[Option<&'a [i64]>],
    views_f64: &[Option<&'a [f64]>],
) -> Vec<FusedLeafView<'a>> {
    let mut out: Vec<FusedLeafView> = Vec::with_capacity(plan.leaves.len());
    for leaf in &plan.leaves {
        match leaf {
            FusedLeaf::RangeI64 { lane, lo, hi } => out.push(match views_i64[*lane] {
                Some(d) => FusedLeafView::RangeI64(d, *lo, *hi),
                None => FusedLeafView::Missing,
            }),
            FusedLeaf::RangeF64 { lane, lo, hi } => out.push(match views_f64[*lane] {
                Some(d) => FusedLeafView::RangeF64(d, *lo, *hi),
                None => FusedLeafView::Missing,
            }),
            FusedLeaf::InI64 { lane, values } => out.push(match views_i64[*lane] {
                Some(d) => FusedLeafView::InI64(d, values),
                None => FusedLeafView::Missing,
            }),
            FusedLeaf::InF64 { lane, values } => out.push(match views_f64[*lane] {
                Some(d) => FusedLeafView::InF64(d, values),
                None => FusedLeafView::Missing,
            }),
            FusedLeaf::DictIn { .. } | FusedLeaf::DictEq { .. } => unreachable!(
                "dictionary leaves are folded per slot at compile time"
            ),
        }
    }
    out
}

/// Aggregate value source for the fused row loop.
enum FusedValueView<'a> {
    None,
    F64(&'a [f64]),
    I64(&'a [i64]),
}

/// One row group's row-loop state, gathered so the per-row loop compiles
/// with a small register frame (kept out of the large RG-scan function).
struct FusedRgWork<'a> {
    gids: &'a [u16],
    del_bytes: &'a [u8],
    has_deleted: bool,
    limit: usize,
    leaf_views: &'a [FusedLeafView<'a>],
    counts: &'a mut [i64],
    sums: &'a mut [f64],
    mins: &'a mut [f64],
    maxs: &'a mut [f64],
    agg_mask: u8,
    value: FusedValueView<'a>,
}

/// Run the fused per-row loop for one row group: predicate mask per row,
/// branchless count scatter, and the aggregates the query asks for (bit 0
/// sum, bit 1 min, bit 2 max).
#[inline(never)]
fn run_fused_rg_rows(plan: &FusedCompiledPlan, w: &mut FusedRgWork) {
    match w.value {
        FusedValueView::None => {
            for i in 0..w.limit {
                if w.has_deleted && (w.del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                    continue;
                }
                let gid = unsafe { *w.gids.get_unchecked(i) } as usize;
                let m = plan.row_mask(gid, i, w.leaf_views);
                unsafe { *w.counts.get_unchecked_mut(gid) += m; }
            }
        }
        FusedValueView::F64(av) => {
            for i in 0..w.limit {
                if w.has_deleted && (w.del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                    continue;
                }
                let gid = unsafe { *w.gids.get_unchecked(i) } as usize;
                let m = plan.row_mask(gid, i, w.leaf_views);
                unsafe { *w.counts.get_unchecked_mut(gid) += m; }
                let v = unsafe { *av.get_unchecked(i) };
                if w.agg_mask & 1 != 0 {
                    unsafe { *w.sums.get_unchecked_mut(gid) += m as f64 * v; }
                }
                if m != 0 {
                    if w.agg_mask & 2 != 0 && v < unsafe { *w.mins.get_unchecked(gid) } {
                        unsafe { *w.mins.get_unchecked_mut(gid) = v; }
                    }
                    if w.agg_mask & 4 != 0 && v > unsafe { *w.maxs.get_unchecked(gid) } {
                        unsafe { *w.maxs.get_unchecked_mut(gid) = v; }
                    }
                }
            }
        }
        FusedValueView::I64(av) => {
            for i in 0..w.limit {
                if w.has_deleted && (w.del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
                    continue;
                }
                let gid = unsafe { *w.gids.get_unchecked(i) } as usize;
                let m = plan.row_mask(gid, i, w.leaf_views);
                unsafe { *w.counts.get_unchecked_mut(gid) += m; }
                let v = unsafe { *av.get_unchecked(i) } as f64;
                if w.agg_mask & 1 != 0 {
                    unsafe { *w.sums.get_unchecked_mut(gid) += m as f64 * v; }
                }
                if m != 0 {
                    if w.agg_mask & 2 != 0 && v < unsafe { *w.mins.get_unchecked(gid) } {
                        unsafe { *w.mins.get_unchecked_mut(gid) = v; }
                    }
                    if w.agg_mask & 4 != 0 && v > unsafe { *w.maxs.get_unchecked(gid) } {
                        unsafe { *w.maxs.get_unchecked_mut(gid) = v; }
                    }
                }
            }
        }
    }
}

// ============================================================================
// Truth-table row program
//
// When the predicate holds at most LUT_MAX_LEAVES distinct lane leaves, the
// whole boolean tree is compiled once into a truth table over
// (comparison bits, group id). The per-row loop then evaluates each leaf as
// a straight-line comparison and looks the result up: any AND/OR/NOT shape
// runs at the same flat speed.
// ============================================================================

const LUT_MAX_LEAVES: u8 = 3;
const LUT_MAX_IN: usize = 8;
const LUT_MAX_GROUPS: usize = 4096;

/// Per-leaf comparison shape in the straight-line LUT loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LcMode {
    RangeI64,
    RangeF64,
    InI64,
    InF64,
}

/// Per-query truth-table program: `k` lane-leaf comparisons combined through
/// `lut`, indexed by (comparison bits, group id) with a power-of-two group
/// stride so the index is a shift-or.
struct FusedLutProgram {
    k: u8,
    lane: [u8; LUT_MAX_LEAVES as usize],
    mode: [LcMode; LUT_MAX_LEAVES as usize],
    lo_i: [i64; LUT_MAX_LEAVES as usize],
    hi_i: [i64; LUT_MAX_LEAVES as usize],
    lo_f: [f64; LUT_MAX_LEAVES as usize],
    hi_f: [f64; LUT_MAX_LEAVES as usize],
    nv: [u8; LUT_MAX_LEAVES as usize],
    vals_i: [[i64; 16]; LUT_MAX_LEAVES as usize],
    vals_f: [[f64; 16]; LUT_MAX_LEAVES as usize],
    lut: Vec<u8>,
    stride_shift: u8,
    all_none: bool,
}

/// Build the truth-table program while the lane-leaf budget allows it.
fn try_build_lut_program(
    predicate: &FusedPredicate,
    num_groups: usize,
) -> Option<FusedLutProgram> {
    if num_groups > LUT_MAX_GROUPS {
        return None;
    }
    let mut leaves: Vec<FusedLeaf> = Vec::new();
    collect_fused_lane_leaves(predicate, &mut leaves);
    let k = leaves.len() as u8;
    if k > LUT_MAX_LEAVES {
        return None;
    }
    let mut prog = FusedLutProgram {
        k,
        lane: [0; LUT_MAX_LEAVES as usize],
        mode: [LcMode::RangeI64; LUT_MAX_LEAVES as usize],
        lo_i: [0; LUT_MAX_LEAVES as usize],
        hi_i: [0; LUT_MAX_LEAVES as usize],
        lo_f: [0.0; LUT_MAX_LEAVES as usize],
        hi_f: [0.0; LUT_MAX_LEAVES as usize],
        nv: [0; LUT_MAX_LEAVES as usize],
        vals_i: [[0; 16]; LUT_MAX_LEAVES as usize],
        vals_f: [[0.0; 16]; LUT_MAX_LEAVES as usize],
        lut: Vec::new(),
        stride_shift: 0,
        all_none: false,
    };
    for (j, leaf) in leaves.iter().enumerate() {
        match leaf {
            FusedLeaf::RangeI64 { lane, lo, hi } => {
                prog.lane[j] = *lane as u8;
                prog.mode[j] = LcMode::RangeI64;
                prog.lo_i[j] = *lo;
                prog.hi_i[j] = *hi;
            }
            FusedLeaf::RangeF64 { lane, lo, hi } => {
                prog.lane[j] = *lane as u8;
                prog.mode[j] = LcMode::RangeF64;
                prog.lo_f[j] = *lo;
                prog.hi_f[j] = *hi;
            }
            FusedLeaf::InI64 { lane, values } => {
                if values.len() > LUT_MAX_IN {
                    return None;
                }
                prog.lane[j] = *lane as u8;
                prog.mode[j] = LcMode::InI64;
                prog.nv[j] = values.len() as u8;
                for (t, v) in values.iter().enumerate() {
                    prog.vals_i[j][t] = *v;
                }
            }
            FusedLeaf::InF64 { lane, values } => {
                if values.len() > LUT_MAX_IN {
                    return None;
                }
                prog.lane[j] = *lane as u8;
                prog.mode[j] = LcMode::InF64;
                prog.nv[j] = values.len() as u8;
                for (t, v) in values.iter().enumerate() {
                    prog.vals_f[j][t] = *v;
                }
            }
            FusedLeaf::DictIn { .. } | FusedLeaf::DictEq { .. } => unreachable!(
                "collect_fused_lane_leaves keeps only lane leaves"
            ),
        }
    }

    let stride = num_groups.max(1).next_power_of_two();
    prog.stride_shift = stride.trailing_zeros() as u8;
    prog.lut.resize((1 << k) * stride, 0);
    for gid in 0..num_groups {
        for cb in 0..(1u8 << k) {
            let m = lut_eval_tree(predicate, &|leaf: &FusedLeaf| match leaf {
                FusedLeaf::DictIn { flags } => flags[gid] != 0,
                FusedLeaf::DictEq { key } => gid == *key as usize,
                lane => {
                    let bit = leaves
                        .iter()
                        .position(|l| l == lane)
                        .expect("lane leaf registered during collection");
                    (cb >> bit) & 1 != 0
                }
            }) as u8;
            prog.lut[(cb as usize) << prog.stride_shift | gid] = m;
        }
    }
    prog.all_none = prog.lut.iter().all(|&x| x == 0);
    Some(prog)
}

/// Build-time truth-table fill: evaluate the whole tree for one
/// (comparison bits, group) pair.
fn lut_eval_tree(pred: &FusedPredicate, leaf: &dyn Fn(&FusedLeaf) -> bool) -> bool {
    match pred {
        FusedPredicate::Leaf(l) => leaf(l),
        FusedPredicate::And(a, b) => lut_eval_tree(a, leaf) && lut_eval_tree(b, leaf),
        FusedPredicate::Or(a, b) => lut_eval_tree(a, leaf) || lut_eval_tree(b, leaf),
        FusedPredicate::Not(a) => !lut_eval_tree(a, leaf),
    }
}

/// One row group's resolved LUT state: leaf data pointers into this group's
/// decoded views, the per-query truth table, and the aggregate value lane.
struct FusedLutWork<'a> {
    k: u8,
    mode: [LcMode; LUT_MAX_LEAVES as usize],
    pr: [*const u8; LUT_MAX_LEAVES as usize],
    elem_width: [u8; LUT_MAX_LEAVES as usize],
    truth_u8: [[u8; 256]; LUT_MAX_LEAVES as usize],
    lo_i: [i64; LUT_MAX_LEAVES as usize],
    hi_i: [i64; LUT_MAX_LEAVES as usize],
    lo_f: [f64; LUT_MAX_LEAVES as usize],
    hi_f: [f64; LUT_MAX_LEAVES as usize],
    nv: [u8; LUT_MAX_LEAVES as usize],
    vals_i: &'a [[i64; 16]; LUT_MAX_LEAVES as usize],
    vals_f: &'a [[f64; 16]; LUT_MAX_LEAVES as usize],
    lut: &'a [u8],
    stride_shift: u8,
    val_raw: *const u8,
    val_is_i64: bool,
}

/// Aggregate scatter for the LUT loop: bit 0 sum, bit 1 min, bit 2 max.
/// Mask and buffer pointers are scalars so the loop keeps them in
/// registers; nothing is reloaded through the row-work reference.
#[inline(always)]
fn lut_scatter(
    agg_mask: u8,
    sums: *mut f64,
    mins: *mut f64,
    maxs: *mut f64,
    val_raw: *const u8,
    val_is_i64: bool,
    i: usize,
    gid: usize,
) {
    if agg_mask != 0 {
        let bits = unsafe { std::ptr::read_unaligned(val_raw.add(i * 8) as *const u64) };
        let av = if val_is_i64 {
            bits as i64 as f64
        } else {
            f64::from_bits(bits)
        };
        if agg_mask & 1 != 0 {
            unsafe { *sums.add(gid) += av; }
        }
        if agg_mask & 2 != 0 && av < unsafe { *mins.add(gid) } {
            unsafe { *mins.add(gid) = av; }
        }
        if agg_mask & 4 != 0 && av > unsafe { *maxs.add(gid) } {
            unsafe { *maxs.add(gid) = av; }
        }
    }
}

/// Compile-time-selected leaf comparison: the const mode constant makes the
/// compiler keep only the chosen arm, so each monomorphized LUT loop holds
/// its comparisons as straight-line code (0 RangeI64, 1 RangeF64, 2 InI64,
/// 3 InF64).
#[inline(always)]
fn lc_eval<const mode: u8>(r: &FusedLutWork, j: usize, i: usize) -> bool {
    match mode {
        0 => {
            if r.elem_width[j] == 1 {
                let delta = unsafe { *r.pr[j].add(i) } as usize;
                return unsafe { *r.truth_u8[j].get_unchecked(delta) } != 0;
            }
            let v = unsafe { std::ptr::read_unaligned(r.pr[j].add(i * 8) as *const i64) };
            v >= r.lo_i[j] && v <= r.hi_i[j]
        }
        1 => {
            let bits = unsafe { std::ptr::read_unaligned(r.pr[j].add(i * 8) as *const u64) };
            let v = f64::from_bits(bits);
            v >= r.lo_f[j] && v <= r.hi_f[j]
        }
        2 => {
            // Unrolled small-N comparison: a runtime-length slice loop
            // makes the compiler emit a vectorized inner loop with heavy
            // reduction code; the first four slots are compared directly.
            if r.elem_width[j] == 1 {
                let delta = unsafe { *r.pr[j].add(i) } as usize;
                return unsafe { *r.truth_u8[j].get_unchecked(delta) } != 0;
            }
            let v = unsafe { std::ptr::read_unaligned(r.pr[j].add(i * 8) as *const i64) };
            let vals = &r.vals_i[j];
            let nv = r.nv[j] as usize;
            let mut b = v == vals[0];
            if nv > 1 {
                b |= v == vals[1];
            }
            if nv > 2 {
                b |= v == vals[2];
            }
            if nv > 3 {
                b |= v == vals[3];
            }
            let mut t = 4usize;
            while t < nv {
                b |= v == vals[t];
                t += 1;
            }
            b
        }
        _ => {
            let bits = unsafe { std::ptr::read_unaligned(r.pr[j].add(i * 8) as *const u64) };
            let v = f64::from_bits(bits);
            let vals = &r.vals_f[j];
            let nv = r.nv[j] as usize;
            let mut b = v == vals[0];
            if nv > 1 {
                b |= v == vals[1];
            }
            if nv > 2 {
                b |= v == vals[2];
            }
            if nv > 3 {
                b |= v == vals[3];
            }
            let mut t = 4usize;
            while t < nv {
                b |= v == vals[t];
                t += 1;
            }
            b
        }
    }
}

/// LUT row loop, k = 0: the predicate depends on the group id alone.
fn lut_loop_0(w: &mut FusedRgWork, r: &FusedLutWork) {
    // Row-work fields are copied to locals once: the hot loop never
    // reloads them through the reference.
    let gids = w.gids;
    let del_bytes = w.del_bytes;
    let has_deleted = w.has_deleted;
    let limit = w.limit;
    let counts = w.counts.as_mut_ptr();
    let sums = w.sums.as_mut_ptr();
    let mins = w.mins.as_mut_ptr();
    let maxs = w.maxs.as_mut_ptr();
    let agg_mask = w.agg_mask;
    let val_raw = r.val_raw;
    let val_is_i64 = r.val_is_i64;
    let lut = r.lut;
    for i in 0..limit {
        if has_deleted && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
            continue;
        }
        let gid = unsafe { *gids.get_unchecked(i) } as usize;
        let m = unsafe { *lut.get_unchecked(gid) };
        if m != 0 {
            unsafe { *counts.add(gid) += 1; }
            lut_scatter(agg_mask, sums, mins, maxs, val_raw, val_is_i64, i, gid);
        }
    }
}

/// LUT row loop, k = 1.
fn lut_loop_1<const m0: u8>(w: &mut FusedRgWork, r: &FusedLutWork) {
    // Row-work fields are copied to locals once: the hot loop never
    // reloads them through the reference.
    let gids = w.gids;
    let del_bytes = w.del_bytes;
    let has_deleted = w.has_deleted;
    let limit = w.limit;
    let counts = w.counts.as_mut_ptr();
    let sums = w.sums.as_mut_ptr();
    let mins = w.mins.as_mut_ptr();
    let maxs = w.maxs.as_mut_ptr();
    let agg_mask = w.agg_mask;
    let val_raw = r.val_raw;
    let val_is_i64 = r.val_is_i64;
    let lut = r.lut;
    let shift = r.stride_shift;
    for i in 0..limit {
        if has_deleted && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
            continue;
        }
        let gid = unsafe { *gids.get_unchecked(i) } as usize;
        let b0 = lc_eval::<m0>(r, 0, i) as usize;
        let m = unsafe { *lut.get_unchecked((b0 << shift) | gid) };
        if m != 0 {
            unsafe { *counts.add(gid) += 1; }
            lut_scatter(agg_mask, sums, mins, maxs, val_raw, val_is_i64, i, gid);
        }
    }
}

/// LUT row loop, k = 2.
fn lut_loop_2<const m0: u8, const m1: u8>(w: &mut FusedRgWork, r: &FusedLutWork) {
    // Row-work fields are copied to locals once: the hot loop never
    // reloads them through the reference.
    let gids = w.gids;
    let del_bytes = w.del_bytes;
    let has_deleted = w.has_deleted;
    let limit = w.limit;
    let counts = w.counts.as_mut_ptr();
    let sums = w.sums.as_mut_ptr();
    let mins = w.mins.as_mut_ptr();
    let maxs = w.maxs.as_mut_ptr();
    let agg_mask = w.agg_mask;
    let val_raw = r.val_raw;
    let val_is_i64 = r.val_is_i64;
    let lut = r.lut;
    let shift = r.stride_shift;
    for i in 0..limit {
        if has_deleted && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
            continue;
        }
        let gid = unsafe { *gids.get_unchecked(i) } as usize;
        let b0 = lc_eval::<m0>(r, 0, i) as usize;
        let b1 = lc_eval::<m1>(r, 1, i) as usize;
        let m = unsafe { *lut.get_unchecked(((b0 | (b1 << 1)) << shift) | gid) };
        if m != 0 {
            unsafe { *counts.add(gid) += 1; }
            lut_scatter(agg_mask, sums, mins, maxs, val_raw, val_is_i64, i, gid);
        }
    }
}

/// LUT row loop, k = 3.
fn lut_loop_3<const m0: u8, const m1: u8, const m2: u8>(
    w: &mut FusedRgWork,
    r: &FusedLutWork,
) {
    // Row-work fields are copied to locals once: the hot loop never
    // reloads them through the reference.
    let gids = w.gids;
    let del_bytes = w.del_bytes;
    let has_deleted = w.has_deleted;
    let limit = w.limit;
    let counts = w.counts.as_mut_ptr();
    let sums = w.sums.as_mut_ptr();
    let mins = w.mins.as_mut_ptr();
    let maxs = w.maxs.as_mut_ptr();
    let agg_mask = w.agg_mask;
    let val_raw = r.val_raw;
    let val_is_i64 = r.val_is_i64;
    let lut = r.lut;
    let shift = r.stride_shift;
    for i in 0..limit {
        if has_deleted && (del_bytes[i / 8] >> (i % 8)) & 1 == 1 {
            continue;
        }
        let gid = unsafe { *gids.get_unchecked(i) } as usize;
        let b0 = lc_eval::<m0>(r, 0, i) as usize;
        let b1 = lc_eval::<m1>(r, 1, i) as usize;
        let b2 = lc_eval::<m2>(r, 2, i) as usize;
        let m = unsafe {
            *lut.get_unchecked(((b0 | (b1 << 1) | (b2 << 2)) << shift) | gid)
        };
        if m != 0 {
            unsafe { *counts.add(gid) += 1; }
            lut_scatter(agg_mask, sums, mins, maxs, val_raw, val_is_i64, i, gid);
        }
    }
}

/// Run the LUT row loop for one row group. The per-leaf mode dispatch runs
/// once per row group and monomorphizes the loop: each arm is a
/// straight-line loop with its comparisons inlined. Kept out of the large
/// RG-scan function so the loop compiles with a small register frame.
#[inline(never)]
fn run_lut_rg_rows(r: &FusedLutWork, w: &mut FusedRgWork) {
    // Const args use the LcMode order: 0 RangeI64, 1 RangeF64, 2 InI64, 3 InF64.
    match r.k {
        0 => lut_loop_0(w, r),
        1 => match r.mode[0] {
            LcMode::RangeI64 => lut_loop_1::<0>(w, r),
            LcMode::RangeF64 => lut_loop_1::<1>(w, r),
            LcMode::InI64 => lut_loop_1::<2>(w, r),
            LcMode::InF64 => lut_loop_1::<3>(w, r),
        },
        2 => match (r.mode[0], r.mode[1]) {
            (LcMode::RangeI64, LcMode::RangeI64) => lut_loop_2::<0, 0>(w, r),
            (LcMode::RangeI64, LcMode::RangeF64) => lut_loop_2::<0, 1>(w, r),
            (LcMode::RangeI64, LcMode::InI64) => lut_loop_2::<0, 2>(w, r),
            (LcMode::RangeI64, LcMode::InF64) => lut_loop_2::<0, 3>(w, r),
            (LcMode::RangeF64, LcMode::RangeI64) => lut_loop_2::<1, 0>(w, r),
            (LcMode::RangeF64, LcMode::RangeF64) => lut_loop_2::<1, 1>(w, r),
            (LcMode::RangeF64, LcMode::InI64) => lut_loop_2::<1, 2>(w, r),
            (LcMode::RangeF64, LcMode::InF64) => lut_loop_2::<1, 3>(w, r),
            (LcMode::InI64, LcMode::RangeI64) => lut_loop_2::<2, 0>(w, r),
            (LcMode::InI64, LcMode::RangeF64) => lut_loop_2::<2, 1>(w, r),
            (LcMode::InI64, LcMode::InI64) => lut_loop_2::<2, 2>(w, r),
            (LcMode::InI64, LcMode::InF64) => lut_loop_2::<2, 3>(w, r),
            (LcMode::InF64, LcMode::RangeI64) => lut_loop_2::<3, 0>(w, r),
            (LcMode::InF64, LcMode::RangeF64) => lut_loop_2::<3, 1>(w, r),
            (LcMode::InF64, LcMode::InI64) => lut_loop_2::<3, 2>(w, r),
            (LcMode::InF64, LcMode::InF64) => lut_loop_2::<3, 3>(w, r),
        },
        3 => match (r.mode[0], r.mode[1], r.mode[2]) {
            (LcMode::RangeI64, LcMode::RangeI64, LcMode::RangeI64) => lut_loop_3::<0, 0, 0>(w, r),
            (LcMode::RangeI64, LcMode::RangeI64, LcMode::RangeF64) => lut_loop_3::<0, 0, 1>(w, r),
            (LcMode::RangeI64, LcMode::RangeI64, LcMode::InI64) => lut_loop_3::<0, 0, 2>(w, r),
            (LcMode::RangeI64, LcMode::RangeI64, LcMode::InF64) => lut_loop_3::<0, 0, 3>(w, r),
            (LcMode::RangeI64, LcMode::RangeF64, LcMode::RangeI64) => lut_loop_3::<0, 1, 0>(w, r),
            (LcMode::RangeI64, LcMode::RangeF64, LcMode::RangeF64) => lut_loop_3::<0, 1, 1>(w, r),
            (LcMode::RangeI64, LcMode::RangeF64, LcMode::InI64) => lut_loop_3::<0, 1, 2>(w, r),
            (LcMode::RangeI64, LcMode::RangeF64, LcMode::InF64) => lut_loop_3::<0, 1, 3>(w, r),
            (LcMode::RangeI64, LcMode::InI64, LcMode::RangeI64) => lut_loop_3::<0, 2, 0>(w, r),
            (LcMode::RangeI64, LcMode::InI64, LcMode::RangeF64) => lut_loop_3::<0, 2, 1>(w, r),
            (LcMode::RangeI64, LcMode::InI64, LcMode::InI64) => lut_loop_3::<0, 2, 2>(w, r),
            (LcMode::RangeI64, LcMode::InI64, LcMode::InF64) => lut_loop_3::<0, 2, 3>(w, r),
            (LcMode::RangeI64, LcMode::InF64, LcMode::RangeI64) => lut_loop_3::<0, 3, 0>(w, r),
            (LcMode::RangeI64, LcMode::InF64, LcMode::RangeF64) => lut_loop_3::<0, 3, 1>(w, r),
            (LcMode::RangeI64, LcMode::InF64, LcMode::InI64) => lut_loop_3::<0, 3, 2>(w, r),
            (LcMode::RangeI64, LcMode::InF64, LcMode::InF64) => lut_loop_3::<0, 3, 3>(w, r),
            (LcMode::RangeF64, LcMode::RangeI64, LcMode::RangeI64) => lut_loop_3::<1, 0, 0>(w, r),
            (LcMode::RangeF64, LcMode::RangeI64, LcMode::RangeF64) => lut_loop_3::<1, 0, 1>(w, r),
            (LcMode::RangeF64, LcMode::RangeI64, LcMode::InI64) => lut_loop_3::<1, 0, 2>(w, r),
            (LcMode::RangeF64, LcMode::RangeI64, LcMode::InF64) => lut_loop_3::<1, 0, 3>(w, r),
            (LcMode::RangeF64, LcMode::RangeF64, LcMode::RangeI64) => lut_loop_3::<1, 1, 0>(w, r),
            (LcMode::RangeF64, LcMode::RangeF64, LcMode::RangeF64) => lut_loop_3::<1, 1, 1>(w, r),
            (LcMode::RangeF64, LcMode::RangeF64, LcMode::InI64) => lut_loop_3::<1, 1, 2>(w, r),
            (LcMode::RangeF64, LcMode::RangeF64, LcMode::InF64) => lut_loop_3::<1, 1, 3>(w, r),
            (LcMode::RangeF64, LcMode::InI64, LcMode::RangeI64) => lut_loop_3::<1, 2, 0>(w, r),
            (LcMode::RangeF64, LcMode::InI64, LcMode::RangeF64) => lut_loop_3::<1, 2, 1>(w, r),
            (LcMode::RangeF64, LcMode::InI64, LcMode::InI64) => lut_loop_3::<1, 2, 2>(w, r),
            (LcMode::RangeF64, LcMode::InI64, LcMode::InF64) => lut_loop_3::<1, 2, 3>(w, r),
            (LcMode::RangeF64, LcMode::InF64, LcMode::RangeI64) => lut_loop_3::<1, 3, 0>(w, r),
            (LcMode::RangeF64, LcMode::InF64, LcMode::RangeF64) => lut_loop_3::<1, 3, 1>(w, r),
            (LcMode::RangeF64, LcMode::InF64, LcMode::InI64) => lut_loop_3::<1, 3, 2>(w, r),
            (LcMode::RangeF64, LcMode::InF64, LcMode::InF64) => lut_loop_3::<1, 3, 3>(w, r),
            (LcMode::InI64, LcMode::RangeI64, LcMode::RangeI64) => lut_loop_3::<2, 0, 0>(w, r),
            (LcMode::InI64, LcMode::RangeI64, LcMode::RangeF64) => lut_loop_3::<2, 0, 1>(w, r),
            (LcMode::InI64, LcMode::RangeI64, LcMode::InI64) => lut_loop_3::<2, 0, 2>(w, r),
            (LcMode::InI64, LcMode::RangeI64, LcMode::InF64) => lut_loop_3::<2, 0, 3>(w, r),
            (LcMode::InI64, LcMode::RangeF64, LcMode::RangeI64) => lut_loop_3::<2, 1, 0>(w, r),
            (LcMode::InI64, LcMode::RangeF64, LcMode::RangeF64) => lut_loop_3::<2, 1, 1>(w, r),
            (LcMode::InI64, LcMode::RangeF64, LcMode::InI64) => lut_loop_3::<2, 1, 2>(w, r),
            (LcMode::InI64, LcMode::RangeF64, LcMode::InF64) => lut_loop_3::<2, 1, 3>(w, r),
            (LcMode::InI64, LcMode::InI64, LcMode::RangeI64) => lut_loop_3::<2, 2, 0>(w, r),
            (LcMode::InI64, LcMode::InI64, LcMode::RangeF64) => lut_loop_3::<2, 2, 1>(w, r),
            (LcMode::InI64, LcMode::InI64, LcMode::InI64) => lut_loop_3::<2, 2, 2>(w, r),
            (LcMode::InI64, LcMode::InI64, LcMode::InF64) => lut_loop_3::<2, 2, 3>(w, r),
            (LcMode::InI64, LcMode::InF64, LcMode::RangeI64) => lut_loop_3::<2, 3, 0>(w, r),
            (LcMode::InI64, LcMode::InF64, LcMode::RangeF64) => lut_loop_3::<2, 3, 1>(w, r),
            (LcMode::InI64, LcMode::InF64, LcMode::InI64) => lut_loop_3::<2, 3, 2>(w, r),
            (LcMode::InI64, LcMode::InF64, LcMode::InF64) => lut_loop_3::<2, 3, 3>(w, r),
            (LcMode::InF64, LcMode::RangeI64, LcMode::RangeI64) => lut_loop_3::<3, 0, 0>(w, r),
            (LcMode::InF64, LcMode::RangeI64, LcMode::RangeF64) => lut_loop_3::<3, 0, 1>(w, r),
            (LcMode::InF64, LcMode::RangeI64, LcMode::InI64) => lut_loop_3::<3, 0, 2>(w, r),
            (LcMode::InF64, LcMode::RangeI64, LcMode::InF64) => lut_loop_3::<3, 0, 3>(w, r),
            (LcMode::InF64, LcMode::RangeF64, LcMode::RangeI64) => lut_loop_3::<3, 1, 0>(w, r),
            (LcMode::InF64, LcMode::RangeF64, LcMode::RangeF64) => lut_loop_3::<3, 1, 1>(w, r),
            (LcMode::InF64, LcMode::RangeF64, LcMode::InI64) => lut_loop_3::<3, 1, 2>(w, r),
            (LcMode::InF64, LcMode::RangeF64, LcMode::InF64) => lut_loop_3::<3, 1, 3>(w, r),
            (LcMode::InF64, LcMode::InI64, LcMode::RangeI64) => lut_loop_3::<3, 2, 0>(w, r),
            (LcMode::InF64, LcMode::InI64, LcMode::RangeF64) => lut_loop_3::<3, 2, 1>(w, r),
            (LcMode::InF64, LcMode::InI64, LcMode::InI64) => lut_loop_3::<3, 2, 2>(w, r),
            (LcMode::InF64, LcMode::InI64, LcMode::InF64) => lut_loop_3::<3, 2, 3>(w, r),
            (LcMode::InF64, LcMode::InF64, LcMode::RangeI64) => lut_loop_3::<3, 3, 0>(w, r),
            (LcMode::InF64, LcMode::InF64, LcMode::RangeF64) => lut_loop_3::<3, 3, 1>(w, r),
            (LcMode::InF64, LcMode::InF64, LcMode::InI64) => lut_loop_3::<3, 3, 2>(w, r),
            (LcMode::InF64, LcMode::InF64, LcMode::InF64) => lut_loop_3::<3, 3, 3>(w, r),
        },
        _ => unreachable!("lut program has at most 3 leaves"),
    }
}

/// Intermediate constant-folding result: a per-slot constant, or a node in
/// the shared table.
enum Fold {
    Const(bool),
    Node(u16),
}

/// Cost estimate used to order AND/OR children (cheaper first).
fn fused_leaf_cost(leaf: &FusedLeaf) -> u32 {
    match leaf {
        FusedLeaf::RangeI64 { .. } | FusedLeaf::RangeF64 { .. } => 1,
        FusedLeaf::InI64 { .. } | FusedLeaf::InF64 { .. } => 2,
        FusedLeaf::DictIn { .. } | FusedLeaf::DictEq { .. } => 0,
    }
}

fn collect_fused_lane_leaves(pred: &FusedPredicate, out: &mut Vec<FusedLeaf>) {
    match pred {
        FusedPredicate::Leaf(leaf) => {
            if !matches!(leaf, FusedLeaf::DictIn { .. } | FusedLeaf::DictEq { .. })
                && !out.iter().any(|l| l == leaf)
            {
                out.push(leaf.clone());
            }
        }
        FusedPredicate::And(left, right) | FusedPredicate::Or(left, right) => {
            collect_fused_lane_leaves(left, out);
            collect_fused_lane_leaves(right, out);
        }
        FusedPredicate::Not(inner) => collect_fused_lane_leaves(inner, out),
    }
}

/// Constant-fold one dictionary slot: dictionary leaves become constants,
/// lane leaves are kept by table index, AND/OR children are ordered cheaper
/// first. Wrapper nodes are appended to the shared node table.
fn fold_fused_slot(
    pred: &FusedPredicate,
    slot: usize,
    leaves: &[FusedLeaf],
    nodes: &mut Vec<SlotNode>,
) -> (Fold, u32) {
    match pred {
        FusedPredicate::Leaf(leaf) => match leaf {
            FusedLeaf::DictIn { flags } => (Fold::Const(flags.get(slot).copied() == Some(1)), 0),
            FusedLeaf::DictEq { key } => (Fold::Const(slot as u16 == *key), 0),
            lane => {
                let id = leaves
                    .iter()
                    .position(|l| l == lane)
                    .expect("lane leaf registered during collection");
                (Fold::Node(id as u16), fused_leaf_cost(lane))
            }
        },
        FusedPredicate::And(left, right) => fold_fused_binary(
            true,
            fold_fused_slot(left, slot, leaves, nodes),
            fold_fused_slot(right, slot, leaves, nodes),
            nodes,
        ),
        FusedPredicate::Or(left, right) => fold_fused_binary(
            false,
            fold_fused_slot(left, slot, leaves, nodes),
            fold_fused_slot(right, slot, leaves, nodes),
            nodes,
        ),
        FusedPredicate::Not(inner) => {
            let (fold, cost) = fold_fused_slot(inner, slot, leaves, nodes);
            match fold {
                Fold::Const(value) => (Fold::Const(!value), cost),
                Fold::Node(node) => {
                    let idx = nodes.len() as u16;
                    nodes.push(SlotNode::Not(node));
                    (Fold::Node(idx), cost)
                }
            }
        }
    }
}

fn fold_fused_binary(
    and_op: bool,
    (left, left_cost): (Fold, u32),
    (right, right_cost): (Fold, u32),
    nodes: &mut Vec<SlotNode>,
) -> (Fold, u32) {
    let apply = |a: bool, b: bool| if and_op { a && b } else { a || b };
    match (left, right) {
        (Fold::Const(a), Fold::Const(b)) => (Fold::Const(apply(a, b)), 0),
        (Fold::Const(true), rest) if and_op => (rest, right_cost),
        (rest, Fold::Const(true)) if and_op => (rest, left_cost),
        (Fold::Const(false), rest) if and_op => (Fold::Const(false), 0),
        (Fold::Const(false), rest) => (rest, right_cost),
        (rest, Fold::Const(false)) if and_op => (Fold::Const(false), 0),
        (rest, Fold::Const(false)) => (rest, left_cost),
        (Fold::Const(true), _) => (Fold::Const(true), 0),
        (_, Fold::Const(true)) => (Fold::Const(true), 0),
        (left, right) => {
            let (a, b, a_cost, b_cost) = if left_cost <= right_cost {
                (left, right, left_cost, right_cost)
            } else {
                (right, left, right_cost, left_cost)
            };
            let idx = nodes.len() as u16;
            nodes.push(if and_op {
                SlotNode::And(fold_fused_node(a), fold_fused_node(b))
            } else {
                SlotNode::Or(fold_fused_node(a), fold_fused_node(b))
            });
            (Fold::Node(idx), a_cost + b_cost + 1)
        }
    }
}

fn fold_fused_node(fold: Fold) -> u16 {
    match fold {
        Fold::Node(node) => node,
        Fold::Const(_) => unreachable!("constants are folded away"),
    }
}

/// When one child of a binary root is a leaf and the other is a binary node
/// over two leaves, return the flat 3-leaf shape `(x op1 y) op2 c`.
fn fused_bin3_parts(
    nodes: &[SlotNode],
    n_leaves: usize,
    a: u16,
    b: u16,
) -> Option<(u8, u8, u8, u8)> {
    let is_leaf = |n: u16| (n as usize) < n_leaves;
    let inner = |n: u16| -> Option<(u8, u8, u8)> {
        match nodes[n as usize] {
            SlotNode::And(x, y) if is_leaf(x) && is_leaf(y) => Some((0, x as u8, y as u8)),
            SlotNode::Or(x, y) if is_leaf(x) && is_leaf(y) => Some((1, x as u8, y as u8)),
            _ => None,
        }
    };
    if is_leaf(a) {
        inner(b).map(|(op1, x, y)| (op1, x, y, a as u8))
    } else if is_leaf(b) {
        inner(a).map(|(op1, x, y)| (op1, x, y, b as u8))
    } else {
        None
    }
}

fn fold_to_slot_plan(fold: Fold, nodes: &[SlotNode], n_leaves: usize) -> SlotPlan {
    let is_leaf = |n: u16| (n as usize) < n_leaves;
    match fold {
        Fold::Const(true) => SlotPlan::All,
        Fold::Const(false) => SlotPlan::None,
        Fold::Node(r) => {
            let r = r as usize;
            match nodes[r] {
                SlotNode::Leaf(l) => SlotPlan::One(l),
                SlotNode::And(a, b) if is_leaf(a) && is_leaf(b) => SlotPlan::And(a as u8, b as u8),
                SlotNode::Or(a, b) if is_leaf(a) && is_leaf(b) => SlotPlan::Or(a as u8, b as u8),
                SlotNode::Not(a) if is_leaf(a) => SlotPlan::NotOne(a as u8),
                SlotNode::And(a, b) | SlotNode::Or(a, b) => {
                    let and_root = matches!(nodes[r], SlotNode::And(_, _));
                    match fused_bin3_parts(nodes, n_leaves, a, b) {
                        Some((op1, x, y, c)) => SlotPlan::Bin3 {
                            op1,
                            op2: and_root as u8,
                            a: x,
                            b: y,
                            c,
                        },
                        None => SlotPlan::Tree { root: r as u16 },
                    }
                }
                SlotNode::Not(_) => SlotPlan::Tree { root: r as u16 },
            }
        }
    }
}

/// Whether the subtree contains group-column dictionary leaves.
fn has_fused_dict_leaf(pred: &FusedPredicate) -> bool {
    match pred {
        FusedPredicate::Leaf(leaf) => {
            matches!(leaf, FusedLeaf::DictIn { .. } | FusedLeaf::DictEq { .. })
        }
        FusedPredicate::And(a, b) | FusedPredicate::Or(a, b) => {
            has_fused_dict_leaf(a) || has_fused_dict_leaf(b)
        }
        FusedPredicate::Not(inner) => has_fused_dict_leaf(inner),
    }
}

/// When a root And/Or combines a dict-free child with a dict-containing
/// child, return (pure child, mixed child, and_root).
fn split_fused_common(pred: &FusedPredicate) -> Option<(&FusedPredicate, &FusedPredicate, bool)> {
    match pred {
        FusedPredicate::And(a, b) => {
            let (da, db) = (has_fused_dict_leaf(a), has_fused_dict_leaf(b));
            if da && !db {
                Some((b, a, true))
            } else if db && !da {
                Some((a, b, true))
            } else {
                None
            }
        }
        FusedPredicate::Or(a, b) => {
            let (da, db) = (has_fused_dict_leaf(a), has_fused_dict_leaf(b));
            if da && !db {
                Some((b, a, false))
            } else if db && !da {
                Some((a, b, false))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Encode extras restricted to All / None / One(leaf) as 0 / 0xFF / leaf+1.
fn classify_fused_extras(slot_plans: &[SlotPlan]) -> Option<Vec<u8>> {
    let mut mask = Vec::with_capacity(slot_plans.len());
    for plan in slot_plans {
        let value = match plan {
            SlotPlan::All => 0u8,
            SlotPlan::None => 0xFFu8,
            SlotPlan::One(l) => l.checked_add(1)?,
            _ => return None,
        };
        mask.push(value);
    }
    Some(mask)
}

/// Fold the per-slot plans of one predicate source (the whole predicate in
/// Pure mode, the mixed child otherwise).
fn fold_fused_slot_plans(
    source: &FusedPredicate,
    num_groups: usize,
    leaves: &[FusedLeaf],
    nodes: &mut Vec<SlotNode>,
) -> Option<Vec<SlotPlan>> {
    let mut slot_plans = Vec::with_capacity(num_groups);
    for slot in 0..num_groups {
        let (fold, _cost) = fold_fused_slot(source, slot, leaves, nodes);
        slot_plans.push(fold_to_slot_plan(fold, nodes, leaves.len()));
        if nodes.len() > 65534 {
            return None;
        }
    }
    Some(slot_plans)
}

/// Compile the predicate: a slot-independent common part plus a compact
/// per-slot extra part. Returns `None` when the leaf or node tables would
/// overflow (caller falls back to the generic pipeline).
fn compile_fused_plan(predicate: &FusedPredicate, num_groups: usize) -> Option<FusedCompiledPlan> {
    let mut leaves: Vec<FusedLeaf> = Vec::new();
    collect_fused_lane_leaves(predicate, &mut leaves);
    if leaves.len() > 254 {
        return None;
    }
    let mut nodes: Vec<SlotNode> = (0..leaves.len() as u8).map(SlotNode::Leaf).collect();

    let mut mode = FuseMode::Pure;
    let mut common = SlotPlan::All;
    let mut extra_source: Option<&FusedPredicate> = None;

    if !has_fused_dict_leaf(predicate) {
        // Fully numeric: the same flat plan serves every slot.
        let (fold, _cost) = fold_fused_slot(predicate, 0, &leaves, &mut nodes);
        let flat = fold_to_slot_plan(fold, &nodes, leaves.len());
        if !matches!(flat, SlotPlan::Tree { .. }) {
            mode = FuseMode::AndCommon;
            common = flat;
        }
    } else if let Some((pure, mixed, and_root)) = split_fused_common(predicate) {
        let (fold, _cost) = fold_fused_slot(pure, 0, &leaves, &mut nodes);
        let flat = fold_to_slot_plan(fold, &nodes, leaves.len());
        if !matches!(flat, SlotPlan::Tree { .. }) {
            mode = if and_root { FuseMode::AndCommon } else { FuseMode::OrCommon };
            common = flat;
            extra_source = Some(mixed);
        }
    }

    let mut slot_plans: Vec<SlotPlan> = if mode == FuseMode::Pure {
        fold_fused_slot_plans(predicate, num_groups, &leaves, &mut nodes)?
    } else if extra_source.is_none() {
        Vec::new()
    } else {
        fold_fused_slot_plans(extra_source.unwrap(), num_groups, &leaves, &mut nodes)?
    };

    let mut extra_kind = ExtraKind::All;
    let mut extra_mask: Vec<u8> = Vec::new();
    if mode != FuseMode::Pure && !slot_plans.is_empty() {
        if slot_plans.iter().all(|p| matches!(p, SlotPlan::All)) {
            if mode == FuseMode::OrCommon {
                // common OR true: the predicate is a tautology.
                mode = FuseMode::Pure;
                common = SlotPlan::All;
                slot_plans = vec![SlotPlan::All; num_groups];
                extra_kind = ExtraKind::Full;
            } else {
                extra_kind = ExtraKind::All;
            }
        } else if let Some(mask) = classify_fused_extras(&slot_plans) {
            extra_mask = mask;
            extra_kind = ExtraKind::OneMask;
        } else {
            extra_kind = ExtraKind::Full;
        }
    }

    let all_none = match (mode, extra_kind) {
        (FuseMode::Pure, _) => slot_plans.iter().all(|p| matches!(p, SlotPlan::None)),
        (FuseMode::AndCommon, ExtraKind::All) => false,
        (FuseMode::AndCommon, ExtraKind::OneMask) => {
            extra_mask.iter().all(|&e| e == 0xFF)
        }
        (FuseMode::AndCommon, ExtraKind::Full) => {
            slot_plans.iter().all(|p| matches!(p, SlotPlan::None))
        }
        (FuseMode::OrCommon, _) => false,
    };

    Some(FusedCompiledPlan {
        mode,
        common,
        extra_kind,
        extra_mask,
        slot_plans,
        leaves,
        nodes,
        all_none,
    })
}

impl OnDemandStorage {
    /// Decode one numeric lane of a row group for the fused kernel.
    /// PLAIN encodings are read straight out of the (mmap-backed) body with
    /// zero copies; other encodings are decoded into the caller-owned
    /// buffers. Exactly one of the four outcomes is returned.
    fn decode_fused_lane<'a>(
        &self,
        body: &'a [u8],
        col_off: usize,
        null_bitmap_len: usize,
        rg_rows: usize,
        col_type: ColumnType,
        encoding_version: u8,
        buf_i64: &mut Vec<i64>,
        buf_f64: &mut Vec<f64>,
        buf_u8: &mut Vec<u8>,
        compact_bitpack: bool,
    ) -> io::Result<FusedLaneDecoded<'a>> {
        let is_int = matches!(
            col_type,
            ColumnType::Int64
                | ColumnType::Int8
                | ColumnType::Int16
                | ColumnType::Int32
                | ColumnType::UInt8
                | ColumnType::UInt16
                | ColumnType::UInt32
                | ColumnType::UInt64
        );
        if col_off + null_bitmap_len >= body.len() {
            return Ok(FusedLaneDecoded::Missing);
        }
        let data = &body[col_off + null_bitmap_len..];
        if data.is_empty() {
            return Ok(FusedLaneDecoded::Missing);
        }
        let enc = if encoding_version >= 1 { data[0] } else { COL_ENCODING_PLAIN };
        let payload = if encoding_version >= 1 { &data[1..] } else { data };
        if enc == COL_ENCODING_PLAIN && payload.len() >= 8 {
            let count =
                u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
            let n = count.min(rg_rows).min((payload.len() - 8) / 8);
            let raw = &payload[8..8 + n * 8];
            if is_int {
                // Column offsets are not alignment-guaranteed: view the
                // values in place when 8-byte aligned, otherwise keep the
                // raw bytes in place for unaligned reads (zero copies).
                if (raw.as_ptr() as usize) % align_of::<i64>() == 0 {
                    let values: &[i64] =
                        unsafe { std::slice::from_raw_parts(raw.as_ptr().cast(), n) };
                    return Ok(FusedLaneDecoded::PlainI64(values));
                }
                return Ok(FusedLaneDecoded::RawI64(raw));
            }
            if (raw.as_ptr() as usize) % align_of::<f64>() == 0 {
                let values: &[f64] =
                    unsafe { std::slice::from_raw_parts(raw.as_ptr().cast(), n) };
                return Ok(FusedLaneDecoded::PlainF64(values));
            }
            return Ok(FusedLaneDecoded::RawF64(raw));
        }
        // BITPACK int columns decode straight into the caller buffer:
        // the generic path would allocate a second per-RG Vec.
        if is_int
            && encoding_version >= 1
            && enc == COL_ENCODING_BITPACK
            && payload.len() >= 17
        {
            let count = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
            let bit_width = payload[8] as usize;
            let min_val = i64::from_le_bytes(payload[9..17].try_into().unwrap());
            let n = count.min(rg_rows);
            let packed_bytes = (n as u64 * bit_width as u64 + 7) / 8;
            if bit_width < 64 && payload.len() >= 17 + packed_bytes as usize {
                let packed = &payload[17..17 + packed_bytes as usize];
                if compact_bitpack && bit_width > 0 && bit_width <= 8 {
                    buf_u8.clear();
                    buf_u8.resize(n, 0);
                    bitpack_fill_u8(packed, n, bit_width, buf_u8);
                    return Ok(FusedLaneDecoded::BufferU8(min_val));
                }
                buf_i64.clear();
                buf_i64.resize(n, 0);
                bitpack_fill(packed, n, bit_width, min_val, buf_i64);
                return Ok(FusedLaneDecoded::BufferI64);
            }
        }
        let (col_data, _) = if encoding_version >= 1 {
            read_column_encoded(data, col_type)?
        } else {
            ColumnData::from_bytes_typed(data, col_type)?
        };
        match col_data {
            ColumnData::Int64(values) => {
                buf_i64.clear();
                buf_i64.extend_from_slice(&values);
                Ok(FusedLaneDecoded::BufferI64)
            }
            ColumnData::Float64(values) => {
                buf_f64.clear();
                buf_f64.extend_from_slice(&values);
                Ok(FusedLaneDecoded::BufferF64)
            }
            _ => Ok(FusedLaneDecoded::Missing),
        }
    }

    /// Fused predicate-lane group aggregation over the persisted V4 file.
    ///
    /// `group_ids` must be aligned with physical row positions (the global
    /// string dictionary cache layout). Physically deleted rows are skipped
    /// via each row group's deletion vector. Returns per-group
    /// count/sum/min/max indexed by dictionary slot, or `None` when the file
    /// layout is outside this kernel's capability (caller falls back).
    pub fn execute_fused_group_agg_mmap(
        &self,
        dict_strings: &[String],
        group_ids: &[u16],
        lanes: &[FusedLaneSpec],
        predicate: &FusedPredicate,
        agg_lane: Option<usize>,
        agg_mask: u8,
    ) -> io::Result<Option<Vec<FusedGroupAgg>>> {
        let num_groups = dict_strings.len();
        if num_groups == 0 || lanes.is_empty() || lanes.len() > 2 {
            return Ok(None);
        }
        if let Some(al) = agg_lane {
            if al >= lanes.len() {
                return Ok(None);
            }
        }
        let footer = match self.get_or_load_footer()? {
            Some(f) => f,
            None => return Ok(None),
        };
        let schema = &footer.schema;
        let lane_idx: Vec<usize> = match lanes
            .iter()
            .map(|l| schema.get_index(l.col.as_str()))
            .collect::<Option<Vec<_>>>()
        {
            Some(v) => v,
            None => return Ok(None),
        };
        // Lane type sanity: is_int must match the physical column type.
        for (spec, &idx) in lanes.iter().zip(lane_idx.iter()) {
            let col_type = schema.columns[idx].1;
            let physical_int = matches!(
                col_type,
                ColumnType::Int64
                    | ColumnType::Int8
                    | ColumnType::Int16
                    | ColumnType::Int32
                    | ColumnType::UInt8
                    | ColumnType::UInt16
                    | ColumnType::UInt32
                    | ColumnType::UInt64
            );
            if spec.is_int != physical_int {
                return Ok(None);
            }
        }

        let max_col_idx = *lane_idx.iter().max().unwrap();
        let all_rcix = footer.row_groups.iter().enumerate().all(|(rg_i, rg_meta)| {
            if rg_meta.row_count == 0 {
                return true;
            }
            footer
                .col_offsets
                .get(rg_i)
                .map_or(false, |v| v.len() > max_col_idx)
        });
        if !all_rcix {
            return Ok(None);
        }

        // Fast path: the whole predicate compiled into a truth table over
        // (lane-leaf comparison bits, group id); fallback: the generic
        // per-slot plan.
        let lut_prog = try_build_lut_program(predicate, num_groups);
        let plan = if lut_prog.is_none() {
            let Some(plan) = compile_fused_plan(predicate, num_groups) else {
                return Ok(None);
            };
            Some(plan)
        } else {
            None
        };
        if plan
            .as_ref()
            .map_or(false, |p| p.all_none)
            || lut_prog.as_ref().map_or(false, |p| p.all_none)
        {
            // The predicate cannot match any row: skip the scan entirely.
            return Ok(Some(
                (0..num_groups)
                    .map(|_| FusedGroupAgg {
                        sum: 0.0,
                        count: 0,
                        min: f64::INFINITY,
                        max: f64::NEG_INFINITY,
                    })
                    .collect(),
            ));
        }
        let null_bitmap_len_of = |rg_rows: usize| (rg_rows + 7) / 8;

        let mut group_counts = vec![0i64; num_groups];
        let mut group_sums = vec![0.0f64; num_groups];
        let mut group_mins = vec![f64::INFINITY; num_groups];
        let mut group_maxs = vec![f64::NEG_INFINITY; num_groups];

        let file_guard = self.file.read();
        let file = file_guard
            .as_ref()
            .ok_or_else(|| err_not_conn("File not open"))?;
        let mut mmap_guard = self.mmap_cache.write();
        let mmap_ref = mmap_guard.get_or_create(file)?;
        let mut rg_row_offset = 0usize;
        let mut buf_i64_0 = Vec::new();
        let mut buf_f64_0 = Vec::new();
        let mut buf_u8_0 = Vec::new();
        let mut buf_i64_1 = Vec::new();
        let mut buf_f64_1 = Vec::new();
        let mut buf_u8_1 = Vec::new();

        for (rg_i, rg_meta) in footer.row_groups.iter().enumerate() {
            let rg_rows = rg_meta.row_count as usize;
            if rg_rows == 0 {
                rg_row_offset += rg_rows;
                continue;
            }
            let rg_end = (rg_meta.offset + rg_meta.data_size) as usize;
            if rg_end > mmap_ref.len() {
                return Err(err_data("RG past EOF"));
            }
            let rg_bytes = &mmap_ref[rg_meta.offset as usize..rg_end];
            if rg_bytes.len() < 32 {
                rg_row_offset += rg_rows;
                continue;
            }
            let compress_flag = rg_bytes[28];
            let encoding_version = rg_bytes[29];
            let null_bitmap_len = null_bitmap_len_of(rg_rows);
            let rcix = &footer.col_offsets[rg_i];
            let has_deleted = rg_meta.deletion_count > 0;
            let del_start = rg_id_section_len(
                rg_rows,
                rg_bytes.get(30).copied().unwrap_or(RG_IDS_PLAIN),
            );

            let decompressed_buf = decompress_rg_body(compress_flag, &rg_bytes[32..])?;
            let body: &[u8] = decompressed_buf.as_deref().unwrap_or(&rg_bytes[32..]);
            let del_bytes: &[u8] = if has_deleted && del_start + null_bitmap_len <= body.len() {
                &body[del_start..del_start + null_bitmap_len]
            } else {
                &[]
            };

            let gids_slice =
                &group_ids[rg_row_offset..(rg_row_offset + rg_rows).min(group_ids.len())];
            let rg_n = gids_slice.len();

            // Lanes (<=2) are decoded unrolled: each lane owns a fresh
            // buffer pair so its views never conflict with the other lane.
            // PLAIN columns view the body directly and use no buffer.
            let mut lane_views: Vec<FusedLaneView> = Vec::with_capacity(lanes.len());
            {
                let decoded = self.decode_fused_lane(
                    body,
                    rcix[lane_idx[0]] as usize,
                    null_bitmap_len,
                    rg_rows,
                    schema.columns[lane_idx[0]].1,
                    encoding_version,
                    &mut buf_i64_0,
                    &mut buf_f64_0,
                    &mut buf_u8_0,
                    lut_prog.is_some() && (agg_mask == 0 || agg_lane != Some(0)),
                )?;
                decoded.push_view(&buf_i64_0, &buf_f64_0, &buf_u8_0, &mut lane_views);
            }
            if lanes.len() > 1 {
                let decoded = self.decode_fused_lane(
                    body,
                    rcix[lane_idx[1]] as usize,
                    null_bitmap_len,
                    rg_rows,
                    schema.columns[lane_idx[1]].1,
                    encoding_version,
                    &mut buf_i64_1,
                    &mut buf_f64_1,
                    &mut buf_u8_1,
                    lut_prog.is_some() && (agg_mask == 0 || agg_lane != Some(1)),
                )?;
                decoded.push_view(&buf_i64_1, &buf_f64_1, &buf_u8_1, &mut lane_views);
            }
            if lane_views
                .iter()
                .any(|v| matches!(v, FusedLaneView::Missing))
            {
                return Err(err_data("fused lane decode failed"));
            }
            let mut limit = rg_n;
            for v in &lane_views {
                limit = limit.min(v.len());
            }

            // `limit` is bounded by every lane's length, so the views are
            // valid for the whole loop range.
            if let Some(prog) = &lut_prog {
                let mut work = FusedLutWork {
                    k: prog.k,
                    mode: prog.mode,
                    pr: [std::ptr::null(); LUT_MAX_LEAVES as usize],
                    elem_width: [8; LUT_MAX_LEAVES as usize],
                    truth_u8: [[0; 256]; LUT_MAX_LEAVES as usize],
                    lo_i: prog.lo_i,
                    hi_i: prog.hi_i,
                    lo_f: prog.lo_f,
                    hi_f: prog.hi_f,
                    nv: prog.nv,
                    vals_i: &prog.vals_i,
                    vals_f: &prog.vals_f,
                    lut: &prog.lut,
                    stride_shift: prog.stride_shift,
                    val_raw: std::ptr::null(),
                    val_is_i64: false,
                };
                for (j, &lane) in prog.lane.iter().take(prog.k as usize).enumerate() {
                    work.pr[j] = lane_views[lane as usize].data_ptr();
                    if let FusedLaneView::U8(_, base) = &lane_views[lane as usize] {
                        work.elem_width[j] = 1;
                        let nv = work.nv[j] as usize;
                        for delta in 0..256usize {
                            let value = base.wrapping_add(delta as i64);
                            work.truth_u8[j][delta] = match work.mode[j] {
                                LcMode::RangeI64 => {
                                    (value >= work.lo_i[j] && value <= work.hi_i[j]) as u8
                                }
                                LcMode::InI64 => work.vals_i[j][..nv]
                                    .iter()
                                    .any(|candidate| *candidate == value)
                                    as u8,
                                _ => unreachable!("compact lanes are integer predicates"),
                            };
                        }
                    }
                }
                if agg_mask != 0 {
                    if let Some(al) = agg_lane {
                        let v = &lane_views[al];
                        work.val_raw = v.data_ptr();
                        work.val_is_i64 = matches!(
                            v,
                            FusedLaneView::I64(_) | FusedLaneView::RawI64(_)
                        );
                    }
                }
                run_lut_rg_rows(
                    &work,
                    &mut FusedRgWork {
                        gids: gids_slice,
                        del_bytes,
                        has_deleted,
                        limit,
                        leaf_views: &[],
                        counts: &mut group_counts,
                        sums: &mut group_sums,
                        mins: &mut group_mins,
                        maxs: &mut group_maxs,
                        agg_mask,
                        value: FusedValueView::None,
                    },
                );
            } else {
                let plan = plan.as_ref().expect("generic plan built");
                // The generic path consumes typed slices; misaligned PLAIN
                // lanes are materialized into the (otherwise idle) decode
                // buffers, one scratch pair per lane.
                let mut views_i64: Vec<Option<&[i64]>> = Vec::with_capacity(lanes.len());
                let mut views_f64: Vec<Option<&[f64]>> = Vec::with_capacity(lanes.len());
                let mut scratch_i64_0 = Vec::new();
                let mut scratch_f64_0 = Vec::new();
                let mut scratch_i64_1 = Vec::new();
                let mut scratch_f64_1 = Vec::new();
                match &lane_views[0] {
                    FusedLaneView::I64(d) => {
                        views_i64.push(Some(d));
                        views_f64.push(None);
                    }
                    FusedLaneView::F64(d) => {
                        views_i64.push(None);
                        views_f64.push(Some(d));
                    }
                    FusedLaneView::RawI64(d) => {
                        copy_raw_i64(d, &mut scratch_i64_0);
                        views_i64.push(Some(&scratch_i64_0));
                        views_f64.push(None);
                    }
                    FusedLaneView::RawF64(d) => {
                        copy_raw_f64(d, &mut scratch_f64_0);
                        views_f64.push(Some(&scratch_f64_0));
                        views_i64.push(None);
                    }
                    FusedLaneView::U8(_, _) => {
                        unreachable!("compact lanes require LUT program")
                    }
                    FusedLaneView::Missing => unreachable!("checked above"),
                }
                if lanes.len() > 1 {
                    match &lane_views[1] {
                        FusedLaneView::I64(d) => {
                            views_i64.push(Some(d));
                            views_f64.push(None);
                        }
                        FusedLaneView::F64(d) => {
                            views_i64.push(None);
                            views_f64.push(Some(d));
                        }
                        FusedLaneView::RawI64(d) => {
                            copy_raw_i64(d, &mut scratch_i64_1);
                            views_i64.push(Some(&scratch_i64_1));
                            views_f64.push(None);
                        }
                        FusedLaneView::RawF64(d) => {
                            copy_raw_f64(d, &mut scratch_f64_1);
                            views_f64.push(Some(&scratch_f64_1));
                            views_i64.push(None);
                        }
                        FusedLaneView::U8(_, _) => {
                            unreachable!("compact lanes require LUT program")
                        }
                        FusedLaneView::Missing => unreachable!("checked above"),
                    }
                }
                let leaf_views = resolve_fused_leaf_views(plan, &views_i64, &views_f64);
                // Aggregate only what the query asks for (bit 0 sum, bit 1
                // min, bit 2 max).
                let value_view = if agg_mask != 0 {
                    match agg_lane {
                        Some(al) if views_f64[al].is_some() => {
                            FusedValueView::F64(views_f64[al].unwrap())
                        }
                        Some(al) if views_i64[al].is_some() => {
                            FusedValueView::I64(views_i64[al].unwrap())
                        }
                        _ => FusedValueView::None,
                    }
                } else {
                    FusedValueView::None
                };

                run_fused_rg_rows(
                    plan,
                    &mut FusedRgWork {
                        gids: gids_slice,
                        del_bytes,
                        has_deleted,
                        limit,
                        leaf_views: &leaf_views,
                        counts: &mut group_counts,
                        sums: &mut group_sums,
                        mins: &mut group_mins,
                        maxs: &mut group_maxs,
                        agg_mask,
                        value: value_view,
                    },
                );
            }
            rg_row_offset += rg_rows;
        }
        Ok(Some(
            (0..num_groups)
                .map(|g| FusedGroupAgg {
                    sum: group_sums[g],
                    count: group_counts[g],
                    min: group_mins[g],
                    max: group_maxs[g],
                })
                .collect(),
        ))
    }
}
