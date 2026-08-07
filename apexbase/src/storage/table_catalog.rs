//! Per-database table registry backed by a memory-mapped binary catalog
//! (`.apex_tables`).
//!
//! The registry is the authoritative source of table names for a database
//! directory. All mutations run under an exclusive advisory lock
//! (`.apex_tables.lock`) and update a fixed-slot memory-mapped region in
//! place, so CREATE/DROP no longer pay a full file rewrite per DDL. Legacy
//! databases without a catalog, or with the earlier one-shot binary/JSON
//! formats, are backfilled/migrated on first access.
//!
//! Each slot carries its own CRC32, so accidental corruption or manual edits
//! are detected and rejected. Readers take an optimistic generation snapshot:
//! they scan slots, verify CRCs, and retry if the generation changed.

use fs2::FileExt;
use memmap2::{MmapMut, MmapOptions};
use once_cell::sync::Lazy;
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

pub const TABLE_CATALOG_FILE: &str = ".apex_tables";
pub const TABLE_CATALOG_LOCK: &str = ".apex_tables.lock";
pub const TABLE_SCHEMA_FILE: &str = ".apex_schemas";

const CATALOG_V2_MAGIC: &[u8; 8] = b"APXTBL02";
const FORMAT_VERSION: u32 = 2;
const CATALOG_HEADER: usize = 32;
const SLOT_NAME_CAP: usize = 128;
const SLOT_SIZE: usize = 4 + SLOT_NAME_CAP + 4; // name_len + name + crc
const DEFAULT_CAPACITY: u32 = 1024;
const MAX_SNAPSHOT_ATTEMPTS: u32 = 4;

const SCHEMA_MAGIC: &[u8; 8] = b"APXSCM01";
const SCHEMA_VERSION: u32 = 1;
const SCHEMA_HEADER: usize = 24;
const SCHEMA_REGION_CAPACITY: usize = 256 * 1024;

/// Process-global mapped catalogs keyed by database directory.
static CATALOGS: Lazy<dashmap::DashMap<PathBuf, Arc<MappedCatalog>>> =
    Lazy::new(dashmap::DashMap::new);

/// Process-global mapped lazy-schema registries keyed by database directory.
static SCHEMAS: Lazy<dashmap::DashMap<PathBuf, Arc<MappedSchemas>>> =
    Lazy::new(dashmap::DashMap::new);

/// Generation + file mtime-keyed registry snapshot cache. The generation comes
/// from the mapped header, so repeated reads avoid re-scanning slots. The mtime
/// (captured when the catalog file was mapped) guards against external rewrites
/// of the catalog file: the generation alone is insufficient because a tampered
/// file can keep the same generation, and a remapped catalog can otherwise
/// return a stale cached snapshot. Mapping freshness itself is re-validated by
/// `MappedCatalog::is_stale` on every map/cache entry, so the cache check costs
/// no additional `stat`.
static REGISTRY_CACHE: Lazy<
    dashmap::DashMap<PathBuf, (u64, Option<SystemTime>, BTreeMap<String, PathBuf>)>,
> =
    Lazy::new(dashmap::DashMap::new);

/// A memory-mapped catalog file plus its persistent exclusive lock handle.
struct MappedCatalog {
    base_dir: PathBuf,
    file: fs::File,
    mapping: MmapMut,
    lock_file: fs::File,
    capacity: u32,
    file_len: u64,
    modified: SystemTime,
}

unsafe impl Send for MappedCatalog {}
unsafe impl Sync for MappedCatalog {}

impl MappedCatalog {
    fn header_u64(&self, offset: usize) -> u64 {
        // Header fields are 8-byte aligned at offsets 0/8/16/24.
        unsafe {
            (self.mapping.as_ptr().add(offset) as *const AtomicU64)
                .as_ref()
                .unwrap_unchecked()
                .load(Ordering::Acquire)
        }
    }

    fn header_u32(&self, offset: usize) -> u32 {
        unsafe {
            (self.mapping.as_ptr().add(offset) as *const AtomicU32)
                .as_ref()
                .unwrap_unchecked()
                .load(Ordering::Acquire)
        }
    }

    fn set_header_u64(&self, offset: usize, value: u64) {
        unsafe {
            (self.mapping.as_ptr().add(offset) as *const AtomicU64)
                .as_ref()
                .unwrap_unchecked()
                .store(value, Ordering::Release);
        }
    }

    fn set_header_u32(&self, offset: usize, value: u32) {
        unsafe {
            (self.mapping.as_ptr().add(offset) as *const AtomicU32)
                .as_ref()
                .unwrap_unchecked()
                .store(value, Ordering::Release);
        }
    }

    fn generation(&self) -> u64 {
        self.header_u64(16)
    }

    /// True when the on-disk catalog file was replaced or rewritten outside
    /// the current mapping (e.g. another process or manual tampering).
    fn is_stale(&self) -> bool {
        let meta_path = self.base_dir.join(TABLE_CATALOG_FILE);
        match fs::metadata(&meta_path) {
            Ok(meta) => {
                meta.len() != self.file_len
                    || meta.modified().ok() != Some(self.modified)
            }
            Err(_) => true,
        }
    }

    fn bump_generation(&self) -> u64 {
        let next = self.generation() + 1;
        self.set_header_u64(16, next);
        next
    }

    fn slot_offset(index: usize) -> usize {
        CATALOG_HEADER + index * SLOT_SIZE
    }

    fn slot_name_len(&self, index: usize) -> u32 {
        self.header_u32(Self::slot_offset(index))
    }

    fn set_slot(&self, index: usize, name: &str) {
        let offset = Self::slot_offset(index);
        let ptr = self.mapping.as_ptr() as *mut u8;
        let name_bytes = name.as_bytes();
        let len = name_bytes.len().min(SLOT_NAME_CAP) as u32;
        unsafe {
            // name_len
            std::ptr::copy_nonoverlapping(
                &len.to_le_bytes() as *const u8,
                ptr.add(offset),
                4,
            );
            // name bytes
            std::ptr::copy_nonoverlapping(
                name_bytes.as_ptr(),
                ptr.add(offset + 4),
                name_bytes.len().min(SLOT_NAME_CAP),
            );
            // zero padding
            if name_bytes.len() < SLOT_NAME_CAP {
                std::ptr::write_bytes(
                    ptr.add(offset + 4 + name_bytes.len()),
                    0,
                    SLOT_NAME_CAP - name_bytes.len(),
                );
            }
        }
        // CRC over the first SLOT_SIZE-4 bytes of this slot.
        let slot_bytes = unsafe { std::slice::from_raw_parts(ptr.add(offset), SLOT_SIZE - 4) };
        let crc = crc32fast::hash(slot_bytes);
        unsafe {
            std::ptr::copy_nonoverlapping(
                &crc.to_le_bytes() as *const u8,
                ptr.add(offset + SLOT_SIZE - 4),
                4,
            );
        }
    }

    fn clear_slot(&self, index: usize) {
        let offset = Self::slot_offset(index);
        unsafe {
            std::ptr::write_bytes(
                (self.mapping.as_ptr() as *mut u8).add(offset),
                0,
                SLOT_SIZE,
            );
        }
    }

    /// Scan used slots into a map, verifying per-slot CRCs.
    fn scan(&self) -> io::Result<BTreeMap<String, PathBuf>> {
        let last_slot = self.header_u32(28) as usize;
        let mut tables = BTreeMap::new();
        let bytes = self.mapping.as_ref();
        for index in 0..=last_slot.min(self.capacity as usize - 1) {
            let offset = Self::slot_offset(index);
            if offset + SLOT_SIZE > bytes.len() {
                break;
            }
            let name_len =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            if name_len == 0 {
                continue;
            }
            let expected_crc =
                u32::from_le_bytes(bytes[offset + SLOT_SIZE - 4..offset + SLOT_SIZE].try_into().unwrap());
            let actual_crc = crc32fast::hash(&bytes[offset..offset + SLOT_SIZE - 4]);
            if actual_crc != expected_crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "table catalog {} failed its checksum (corrupt or tampered)",
                        self.base_dir.join(TABLE_CATALOG_FILE).display()
                    ),
                ));
            }
            let name_bytes = &bytes[offset + 4..offset + 4 + name_len.min(SLOT_NAME_CAP)];
            let name = std::str::from_utf8(name_bytes)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "table catalog name is not UTF-8"))?;
            tables.insert(name.to_string(), table_path(&self.base_dir, name));
        }
        Ok(tables)
    }

    fn find_free_slot(&self, last_slot: u32) -> Option<u32> {
        for index in 0..=last_slot.min(self.capacity - 1) {
            if self.slot_name_len(index as usize) == 0 {
                return Some(index);
            }
        }
        None
    }

    /// Persist a set of table names into the mapped slot region.
    ///
    /// Used to materialize a directory backfill: legacy databases without a
    /// catalog are adopted in memory on first mutation, and those entries must
    /// be written to the mapped slots so the registry is authoritative on disk
    /// (and survives a remap), not only in the process-local cache.
    fn persist_tables(&self, tables: &BTreeMap<String, PathBuf>) -> io::Result<()> {
        if tables.is_empty() {
            return Ok(());
        }
        let mut last_slot = self.header_u32(28);
        let mut count = self.header_u32(24);
        for name in tables.keys() {
            let slot = self.find_free_slot(last_slot).or_else(|| {
                if last_slot + 1 < self.capacity {
                    last_slot += 1;
                    Some(last_slot)
                } else {
                    None
                }
            }).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "table catalog {} is full ({} tables)",
                        self.base_dir.join(TABLE_CATALOG_FILE).display(),
                        self.capacity
                    ),
                )
            })?;
            self.set_slot(slot as usize, name);
            last_slot = last_slot.max(slot);
            count += 1;
        }
        self.set_header_u32(28, last_slot);
        self.set_header_u32(24, count);
        self.bump_generation();
        Ok(())
    }
}

/// Per-database lazy schema registry (`.apex_schemas`).
///
/// Holds the column schema of tables whose `.apex` file is materialized lazily
/// (CREATE records the schema here instead of paying a per-table file write).
/// The file is created once per database, writers are serialized by the same
/// exclusive `.apex_tables.lock`, records carry their own CRC, and
/// `release()`/`clear()` drop the mapping so Windows can rewrite the file.
struct MappedSchemas {
    base_dir: PathBuf,
    file: fs::File,
    mapping: MmapMut,
    file_len: u64,
    modified: SystemTime,
}

impl MappedSchemas {
    fn header_u32(&self, offset: usize) -> u32 {
        unsafe {
            (self.mapping.as_ptr().add(offset) as *const AtomicU32)
                .as_ref()
                .unwrap_unchecked()
                .load(Ordering::Acquire)
        }
    }

    fn set_header_u32(&self, offset: usize, value: u32) {
        unsafe {
            (self.mapping.as_ptr().add(offset) as *const AtomicU32)
                .as_ref()
                .unwrap_unchecked()
                .store(value, Ordering::Release);
        }
    }

    fn is_stale(&self) -> bool {
        let meta_path = self.base_dir.join(TABLE_SCHEMA_FILE);
        match fs::metadata(&meta_path) {
            Ok(meta) => {
                meta.len() != self.file_len
                    || meta.modified().ok() != Some(self.modified)
            }
            Err(_) => true,
        }
    }

    fn next_offset(&self) -> usize {
        self.header_u32(16) as usize
    }

    /// Read all live schema records with generation-based retry.
    ///
    /// Record layout: [name_len:u32][schema_len:u32][name][schema][crc:u32].
    /// A rewrite zeroes the old region and republishes records plus the
    /// generation, so a scan that observes torn data simply retries.
    fn scan_records(&self) -> io::Result<Vec<(String, Vec<u8>)>> {
        for _ in 0..MAX_SNAPSHOT_ATTEMPTS {
            let generation = self.header_u32(20);
            let end = self.next_offset().min(self.mapping.len());
            let bytes = self.mapping.as_ref();
            let mut records = Vec::new();
            let mut pos = SCHEMA_HEADER;
            let mut stable = true;
            while pos < end {
                if pos + 8 > end {
                    break;
                }
                let name_len =
                    u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
                let schema_len =
                    u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap()) as usize;
                let total = 8 + name_len + schema_len + 4;
                if name_len == 0 || pos + total > end {
                    // Concurrent rewrite in progress; retry after generation
                    // check below.
                    stable = false;
                    break;
                }
                let crc_offset = pos + total - 4;
                let expected =
                    u32::from_le_bytes(bytes[crc_offset..crc_offset + 4].try_into().unwrap());
                let actual = crc32fast::hash(&bytes[pos..crc_offset]);
                if actual != expected {
                    if self.header_u32(20) == generation {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "table schema {} failed its checksum (corrupt or tampered)",
                                self.base_dir.join(TABLE_SCHEMA_FILE).display()
                            ),
                        ));
                    }
                    stable = false;
                    break;
                }
                let name = std::str::from_utf8(&bytes[pos + 8..pos + 8 + name_len])
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "table schema name is not UTF-8",
                        )
                    })?;
                records.push((
                    name.to_string(),
                    bytes[pos + 8 + name_len..crc_offset].to_vec(),
                ));
                pos += total;
            }
            if stable && self.header_u32(20) == generation {
                return Ok(records);
            }
        }
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "table schema registry is being modified concurrently",
        ))
    }

    /// Replace the whole record set in place.
    fn rewrite(&self, records: &[(String, Vec<u8>)]) -> io::Result<()> {
        let mut total = SCHEMA_HEADER;
        for (name, schema) in records {
            total += 8 + name.len() + schema.len() + 4;
        }
        let capacity = SCHEMA_HEADER + SCHEMA_REGION_CAPACITY;
        if total > capacity || total > self.mapping.len() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "table schema registry {} is full",
                    self.base_dir.join(TABLE_SCHEMA_FILE).display()
                ),
            ));
        }
        let ptr = self.mapping.as_ptr() as *mut u8;
        let old_next = self.next_offset();
        if old_next > SCHEMA_HEADER {
            unsafe {
                std::ptr::write_bytes(ptr.add(SCHEMA_HEADER), 0, old_next - SCHEMA_HEADER);
            }
        }
        let mut offset = SCHEMA_HEADER;
        for (name, schema) in records {
            unsafe {
                let name_len = name.len() as u32;
                std::ptr::copy_nonoverlapping(
                    &name_len.to_le_bytes() as *const u8,
                    ptr.add(offset),
                    4,
                );
                let schema_len = schema.len() as u32;
                std::ptr::copy_nonoverlapping(
                    &schema_len.to_le_bytes() as *const u8,
                    ptr.add(offset + 4),
                    4,
                );
                std::ptr::copy_nonoverlapping(name.as_ptr(), ptr.add(offset + 8), name.len());
                std::ptr::copy_nonoverlapping(
                    schema.as_ptr(),
                    ptr.add(offset + 8 + name.len()),
                    schema.len(),
                );
                let crc_offset = offset + 8 + name.len() + schema.len();
                let record =
                    std::slice::from_raw_parts(ptr.add(offset), crc_offset - offset);
                let crc = crc32fast::hash(record);
                std::ptr::copy_nonoverlapping(
                    &crc.to_le_bytes() as *const u8,
                    ptr.add(crc_offset),
                    4,
                );
            }
            offset += 8 + name.len() + schema.len() + 4;
        }
        self.set_header_u32(16, offset as u32);
        self.set_header_u32(12, records.len() as u32);
        self.set_header_u32(20, self.header_u32(20).wrapping_add(1));
        Ok(())
    }
}

/// Load (creating if needed) the mapped schema registry for a database.
fn ensure_schemas(base_dir: &Path) -> io::Result<Arc<MappedSchemas>> {
    if let Some(entry) = SCHEMAS.get(base_dir) {
        if !entry.value().is_stale() {
            return Ok(Arc::clone(entry.value()));
        }
        drop(entry);
        SCHEMAS.remove(base_dir);
    }

    let path = base_dir.join(TABLE_SCHEMA_FILE);
    if !path.exists() {
        create_schema_file(&path)?;
    }

    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    let meta = file.metadata()?;
    let file_len = meta.len();
    if file_len < SCHEMA_HEADER as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("table schema {} is truncated", path.display()),
        ));
    }
    let mapping = unsafe {
        MmapOptions::new()
            .len(file_len as usize)
            .map_mut(&file)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
    };
    let bytes = mapping.as_ref();
    if bytes.len() < 8 || &bytes[..8] != SCHEMA_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "table schema {} has an invalid magic header",
                path.display()
            ),
        ));
    }
    if bytes.len() < 12 || u32::from_le_bytes(bytes[8..12].try_into().unwrap()) != SCHEMA_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "table schema {} has an unsupported version",
                path.display()
            ),
        ));
    }
    let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    let schemas = Arc::new(MappedSchemas {
        base_dir: base_dir.to_path_buf(),
        file,
        mapping,
        file_len,
        modified,
    });
    SCHEMAS.insert(base_dir.to_path_buf(), Arc::clone(&schemas));
    Ok(schemas)
}

fn create_schema_file(path: &Path) -> io::Result<()> {
    let file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
    file.set_len((SCHEMA_HEADER + SCHEMA_REGION_CAPACITY) as u64)?;
    let mut header = vec![0u8; SCHEMA_HEADER];
    header[..8].copy_from_slice(SCHEMA_MAGIC);
    header[8..12].copy_from_slice(&SCHEMA_VERSION.to_le_bytes());
    header[16..20].copy_from_slice(&(SCHEMA_HEADER as u32).to_le_bytes());
    (&file).write_all(&header)?;
    Ok(())
}

/// Exclusive registry lock held across a check/create/register sequence.
pub struct CatalogLock {
    catalog: Arc<MappedCatalog>,
}

impl CatalogLock {
    fn new(catalog: Arc<MappedCatalog>) -> io::Result<Self> {
        catalog.lock_file.lock_exclusive()?;
        Ok(Self { catalog })
    }

    /// Current registry contents (generation-cached).
    pub fn snapshot(&self) -> io::Result<BTreeMap<String, PathBuf>> {
        snapshot_from_catalog(&self.catalog)
    }

    /// Insert a table entry and persist it through the mapped region.
    pub fn insert(&self, name: &str) -> io::Result<()> {
        let catalog = &self.catalog;
        let mut tables = snapshot_from_catalog(catalog)?;
        if tables.is_empty() {
            // First mutation after opening a legacy database without a
            // catalog: adopt existing `*.apex` files before registering.
            for (table_name, path) in scan_apex_files(&catalog.base_dir)? {
                tables.entry(table_name).or_insert(path);
            }
            // The caller created the `name` file under the same lock, so it is
            // not a pre-existing table.
            tables.remove(name);
            // Persist the backfilled entries so the registry is durable, not
            // just cached in this process.
            catalog.persist_tables(&tables)?;
        }
        if tables.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Table '{}' already exists", name),
            ));
        }

        let mut last_slot = catalog.header_u32(28);
        let slot = catalog
            .find_free_slot(last_slot)
            .or_else(|| {
                if last_slot + 1 < catalog.capacity {
                    last_slot += 1;
                    Some(last_slot)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "table catalog {} is full ({} tables)",
                        catalog.base_dir.join(TABLE_CATALOG_FILE).display(),
                        catalog.capacity
                    ),
                )
            })?;
        catalog.set_slot(slot as usize, name);
        catalog.set_header_u32(28, last_slot.max(slot));
        catalog.set_header_u32(24, catalog.header_u32(24) + 1);
        let generation = catalog.bump_generation();

        tables.insert(name.to_string(), table_path(&catalog.base_dir, name));
        REGISTRY_CACHE.insert(
            catalog.base_dir.clone(),
            (generation, Some(catalog.modified), tables),
        );
        Ok(())
    }

    /// Persist (or replace) the lazy schema record for a table.
    ///
    /// Only called under the exclusive registry lock, so the append/tombstone
    /// sequence is serialized against other writers. Readers validate CRCs and
    /// observe the published `next_offset` only after the record is complete.
    pub fn set_schema(&self, name: &str, schema_bytes: &[u8]) -> io::Result<()> {
        let schemas = ensure_schemas(&self.catalog.base_dir)?;
        let mut records = schemas.scan_records()?;
        records.retain(|(existing, _)| existing != name);
        records.push((name.to_string(), schema_bytes.to_vec()));
        schemas.rewrite(&records)
    }

    /// Remove the lazy schema record for a table (no-op when absent).
    pub fn remove_schema(&self, name: &str) -> io::Result<()> {
        let path = self.catalog.base_dir.join(TABLE_SCHEMA_FILE);
        if !path.exists() {
            return Ok(());
        }
        let schemas = ensure_schemas(&self.catalog.base_dir)?;
        let mut records = schemas.scan_records()?;
        records.retain(|(existing, _)| existing != name);
        schemas.rewrite(&records)
    }

    /// Remove a table entry and persist it through the mapped region.
    pub fn remove(&self, name: &str) -> io::Result<Option<PathBuf>> {
        let catalog = &self.catalog;
        let mut tables = snapshot_from_catalog(catalog)?;
        let adopted_backfill = tables.is_empty();
        if adopted_backfill {
            for (table_name, path) in scan_apex_files(&catalog.base_dir)? {
                tables.entry(table_name).or_insert(path);
            }
        }
        let Some(removed) = tables.remove(name) else {
            return Ok(None);
        };

        if adopted_backfill {
            // The dropped table had no slot of its own; persist the remaining
            // backfilled entries so they survive a remap.
            catalog.persist_tables(&tables)?;
            let generation = catalog.generation();
            REGISTRY_CACHE.insert(
                catalog.base_dir.clone(),
                (generation, Some(catalog.modified), tables),
            );
            return Ok(Some(removed));
        }

        let bytes = catalog.mapping.as_ref();
        let last_slot = catalog.header_u32(28) as usize;
        for index in 0..=last_slot.min(catalog.capacity as usize - 1) {
            let offset = MappedCatalog::slot_offset(index);
            if offset + SLOT_SIZE > bytes.len() {
                break;
            }
            let name_len =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            if name_len == 0 {
                continue;
            }
            let slot_name = &bytes[offset + 4..offset + 4 + name_len.min(SLOT_NAME_CAP)];
            if slot_name == name.as_bytes() {
                catalog.clear_slot(index);
                catalog.set_header_u32(24, catalog.header_u32(24).saturating_sub(1));
                // Shrink last_slot when trailing slots are empty.
                let mut new_last = last_slot as u32;
                while new_last > 0 && catalog.slot_name_len(new_last as usize) == 0 {
                    new_last -= 1;
                }
                if catalog.slot_name_len(new_last as usize) == 0 {
                    new_last = 0;
                }
                catalog.set_header_u32(28, new_last);
                break;
            }
        }
        let generation = catalog.bump_generation();
        REGISTRY_CACHE.insert(
            catalog.base_dir.clone(),
            (generation, Some(catalog.modified), tables),
        );
        Ok(Some(removed))
    }
}

impl Drop for CatalogLock {
    fn drop(&mut self) {
        let _ = self.catalog.lock_file.unlock();
        // Deliberately keep the process-global mapping alive across DDL
        // statements: releasing it here would force an open+mmap on every
        // CREATE/DROP/LIST, which measurably regresses those metadata paths.
        // The mapping is released by `release()` when the owning client
        // closes, so Windows can then rewrite/delete `.apex_tables`.
    }
}

/// Release the process-global catalog mapping for a database directory.
///
/// Called when the last client for the database closes. Windows cannot
/// rewrite or truncate a file while a user-mapped section is open (OS error
/// 1224 / ERROR_USER_MAPPED_FILE), so the mapping must be dropped before a
/// test or another process tampers with / deletes `.apex_tables`. The next
/// access remaps the file; `REGISTRY_CACHE` (validated by generation + mtime)
/// keeps reads O(1).
pub fn release(base_dir: &Path) {
    CATALOGS.remove(base_dir);
    SCHEMAS.remove(base_dir);
}

/// Acquire the exclusive registry lock for a database directory.
pub fn lock(base_dir: &Path) -> io::Result<CatalogLock> {
    let catalog = ensure_mapped(base_dir)?;
    CatalogLock::new(catalog)
}

/// Current registry contents (metadata, or a directory backfill when the
/// metadata file does not exist). Read-only: never persists a backfill.
pub fn snapshot(base_dir: &Path) -> io::Result<BTreeMap<String, PathBuf>> {
    let meta_path = base_dir.join(TABLE_CATALOG_FILE);
    if !meta_path.exists() {
        return scan_apex_files(base_dir);
    }

    let catalog = ensure_mapped(base_dir)?;
    snapshot_from_catalog(&catalog)
}

/// Resolve a registry snapshot from an already-mapped catalog.
///
/// The mtime stamp comes from the mapping (captured when the file was
/// mapped/remapped), so a cache hit costs no extra `stat`. External rewrites
/// are still detected because the mapping freshness is validated by
/// `MappedCatalog::is_stale` before this helper is reached; a stale mapping is
/// remapped and both caches are dropped before any lookup happens here.
fn snapshot_from_catalog(
    catalog: &Arc<MappedCatalog>,
) -> io::Result<BTreeMap<String, PathBuf>> {
    let mtime = Some(catalog.modified);
    let base_dir = &catalog.base_dir;
    for _ in 0..MAX_SNAPSHOT_ATTEMPTS {
        let generation = catalog.generation();
        if let Some(entry) = REGISTRY_CACHE.get(base_dir) {
            if entry.value().0 == generation && entry.value().1 == mtime {
                return Ok(entry.value().2.clone());
            }
        }
        let tables = catalog.scan()?;
        if catalog.generation() == generation {
            REGISTRY_CACHE.insert(
                base_dir.clone(),
                (generation, mtime, tables.clone()),
            );
            return Ok(tables);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::WouldBlock,
        "table catalog is being modified concurrently",
    ))
}

/// Resolve the registered path for a table name.
pub fn resolve(base_dir: &Path, name: &str) -> io::Result<Option<PathBuf>> {
    Ok(snapshot(base_dir)?.remove(name))
}

/// List all registered table names.
pub fn list(base_dir: &Path) -> io::Result<Vec<String>> {
    Ok(snapshot(base_dir)?.into_keys().collect())
}

/// Serialized schema for a lazily-materialized table, if one is registered.
pub fn schema_bytes(base_dir: &Path, name: &str) -> io::Result<Option<Vec<u8>>> {
    let path = base_dir.join(TABLE_SCHEMA_FILE);
    if !path.exists() {
        return Ok(None);
    }
    Ok(ensure_schemas(base_dir)?
        .scan_records()?
        .into_iter()
        .find(|(existing, _)| existing == name)
        .map(|(_, bytes)| bytes))
}

/// True when the table file exists, or when the table is registered in the
/// catalog even though its file has not been materialized yet.
pub fn file_exists_or_registered(table_path: &Path) -> io::Result<bool> {
    if table_path.exists() {
        return Ok(true);
    }
    let base_dir = table_path.parent().unwrap_or(Path::new("."));
    let name = table_path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    Ok(resolve(base_dir, name)?.is_some())
}

/// Create (materialize) a table file using the registered lazy schema when
/// available, otherwise with the default empty schema.
pub fn materialize_table_backend(
    path: &Path,
    durability: crate::storage::DurabilityLevel,
) -> io::Result<crate::storage::TableStorageBackend> {
    let base_dir = path.parent().unwrap_or(Path::new("."));
    let name = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    let schema = match schema_bytes(base_dir, name)? {
        Some(bytes) => Some(crate::storage::OnDemandSchema::from_bytes(&bytes)?),
        None => None,
    };
    let has_constraints = schema.as_ref().is_some_and(|s| {
        s.constraints
            .iter()
            .any(|c| *c != crate::storage::on_demand::ColumnConstraints::default())
    });
    if let Some(schema) = &schema {
        crate::storage::TableStorageBackend::create_with_schema_and_durability(
            path,
            durability,
            &schema.columns,
        )?;
    } else {
        crate::storage::TableStorageBackend::create_with_durability(path, durability)?;
    }
    // The create path leaves the in-memory column vectors empty after writing
    // the initial file; reopen so inserts/queries see the footer schema.
    let backend = crate::storage::TableStorageBackend::open_for_insert_with_durability(
        path,
        durability,
    )?;
    if has_constraints {
        if let Some(schema) = &schema {
            for (idx, (column, _)) in schema.columns.iter().enumerate() {
                if let Some(cons) = schema.constraints.get(idx) {
                    backend.storage.set_column_constraints(column, cons.clone());
                }
            }
            // Persist the constraints into the footer schema (save_v4).
            backend.save()?;
        }
    }
    Ok(backend)
}

/// Ensure a table file exists before a DML/DDL path opens it directly.
///
/// Registered lazy tables are materialized with their catalog schema;
/// unregistered paths keep failing with NotFound.
pub fn ensure_table_file(
    path: &Path,
    durability: crate::storage::DurabilityLevel,
) -> io::Result<()> {
    if path.exists() {
        return Ok(());
    }
    if !file_exists_or_registered(path)? {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("table '{}' does not exist", path.display()),
        ));
    }
    materialize_table_backend(path, durability)?;
    Ok(())
}

/// Remove registry and lock files (used by `drop_if_exists`).
pub fn clear(base_dir: &Path) -> io::Result<()> {
    REGISTRY_CACHE.remove(base_dir);
    CATALOGS.remove(base_dir);
    SCHEMAS.remove(base_dir);
    let _ = fs::remove_file(base_dir.join(TABLE_CATALOG_FILE));
    let _ = fs::remove_file(base_dir.join(format!("{}.tmp", TABLE_CATALOG_FILE)));
    let _ = fs::remove_file(base_dir.join(TABLE_CATALOG_LOCK));
    let _ = fs::remove_file(base_dir.join(TABLE_SCHEMA_FILE));
    Ok(())
}

/// Canonical on-disk path for a table file.
pub fn table_path(base_dir: &Path, name: &str) -> PathBuf {
    base_dir.join(format!("{}.apex", name))
}

/// Strip quoting and any `db.` prefix from a table reference.
pub fn bare_name(name: &str) -> &str {
    let trimmed = name.trim_matches('"').trim_matches('`');
    trimmed.rsplit('.').next().unwrap_or(trimmed)
}

fn ensure_mapped(base_dir: &Path) -> io::Result<Arc<MappedCatalog>> {
    if let Some(entry) = CATALOGS.get(base_dir) {
        if !entry.value().is_stale() {
            return Ok(Arc::clone(entry.value()));
        }
        drop(entry);
        CATALOGS.remove(base_dir);
        REGISTRY_CACHE.remove(base_dir);
    }

    let meta_path = base_dir.join(TABLE_CATALOG_FILE);
    if !meta_path.exists() {
        write_v2_file(&meta_path)?;
    }

    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&meta_path)?;
    let meta = file.metadata()?;
    let file_len = meta.len() as usize;
    if file_len < CATALOG_HEADER {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("table catalog {} is truncated", meta_path.display()),
        ));
    }
    let mapping = unsafe {
        MmapOptions::new()
            .len(file_len)
            .map_mut(&file)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
    };
    let capacity = {
        // Read capacity from the v2 header.
        let bytes = mapping.as_ref();
        if bytes.len() < 8 || &bytes[..8] != CATALOG_V2_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "table catalog {} has an invalid magic header",
                    meta_path.display()
                ),
            ));
        }
        u32::from_le_bytes(bytes[12..16].try_into().unwrap())
    };
    let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    let lock_path = base_dir.join(TABLE_CATALOG_LOCK);
    let lock_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&lock_path)?;
    let catalog = Arc::new(MappedCatalog {
        base_dir: base_dir.to_path_buf(),
        file,
        mapping,
        lock_file,
        capacity,
        file_len: file_len as u64,
        modified,
    });
    CATALOGS.insert(base_dir.to_path_buf(), Arc::clone(&catalog));
    Ok(catalog)
}

fn write_v2_file(path: &Path) -> io::Result<()> {
    let capacity = DEFAULT_CAPACITY;
    let file_size = CATALOG_HEADER + capacity as usize * SLOT_SIZE;
    let mut buf = vec![0u8; file_size];
    buf[..8].copy_from_slice(CATALOG_V2_MAGIC);
    buf[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    buf[12..16].copy_from_slice(&capacity.to_le_bytes());
    // generation at 16..24 stays 0; entry_count at 24..28; last_slot at 28..32.
    fs::write(path, &buf)
}

fn scan_apex_files(base_dir: &Path) -> io::Result<BTreeMap<String, PathBuf>> {
    let mut tables = BTreeMap::new();
    if let Ok(entries) = fs::read_dir(base_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("apex") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    tables.insert(stem.to_string(), path);
                }
            }
        }
    }
    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "apex_catalog_test_{}_{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn mapped_registry_roundtrip() {
        let dir = temp_dir("roundtrip");
        {
            let registry = lock(&dir).unwrap();
            registry.insert("videos").unwrap();
            registry.insert("frames").unwrap();
        }
        let tables = snapshot(&dir).unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains_key("videos"));
        assert!(tables.contains_key("frames"));

        let data = fs::read(dir.join(TABLE_CATALOG_FILE)).unwrap();
        assert_eq!(&data[..8], CATALOG_V2_MAGIC);
        assert_ne!(data.first(), Some(&b'{'));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn tampered_registry_is_rejected() {
        let dir = temp_dir("tamper");
        {
            let registry = lock(&dir).unwrap();
            registry.insert("t").unwrap();
        }
        // Release the process-global mapping before rewriting the file. This
        // mirrors what ApexStorageImpl::close() does for Python clients; on
        // Windows a live user-mapped section blocks the rewrite (OS error
        // 1224).
        release(&dir);
        let path = dir.join(TABLE_CATALOG_FILE);
        let mut data = fs::read(&path).unwrap();
        // Flip a byte inside the first slot name region.
        let mid = CATALOG_HEADER + 8;
        data[mid] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let err = snapshot(&dir).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("checksum"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn lazy_schema_roundtrip_and_replace() {
        let dir = temp_dir("lazy_schema");
        {
            let registry = lock(&dir).unwrap();
            registry.insert("t").unwrap();
            registry.set_schema("t", b"schema-v1").unwrap();
        }
        assert_eq!(schema_bytes(&dir, "t").unwrap().as_deref(), Some(&b"schema-v1"[..]));
        assert_eq!(schema_bytes(&dir, "missing").unwrap(), None);

        {
            let registry = lock(&dir).unwrap();
            registry.set_schema("t", b"schema-v2").unwrap();
        }
        assert_eq!(schema_bytes(&dir, "t").unwrap().as_deref(), Some(&b"schema-v2"[..]));

        {
            let registry = lock(&dir).unwrap();
            registry.remove_schema("t").unwrap();
        }
        assert_eq!(schema_bytes(&dir, "t").unwrap(), None);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn lazy_schema_survives_remap() {
        let dir = temp_dir("lazy_remap");
        {
            let registry = lock(&dir).unwrap();
            registry.insert("t").unwrap();
            registry.set_schema("t", b"persisted").unwrap();
        }
        // Release the process-global mapping (as close() does on Windows) and
        // verify the schema is still readable after a fresh map.
        release(&dir);
        assert_eq!(
            schema_bytes(&dir, "t").unwrap().as_deref(),
            Some(&b"persisted"[..])
        );
        release(&dir);

        let _ = fs::remove_dir_all(&dir);
    }
}
