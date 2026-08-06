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
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

pub const TABLE_CATALOG_FILE: &str = ".apex_tables";
pub const TABLE_CATALOG_LOCK: &str = ".apex_tables.lock";

const CATALOG_V2_MAGIC: &[u8; 8] = b"APXTBL02";
const FORMAT_VERSION: u32 = 2;
const CATALOG_HEADER: usize = 32;
const SLOT_NAME_CAP: usize = 128;
const SLOT_SIZE: usize = 4 + SLOT_NAME_CAP + 4; // name_len + name + crc
const DEFAULT_CAPACITY: u32 = 1024;
const MAX_SNAPSHOT_ATTEMPTS: u32 = 4;

/// Process-global mapped catalogs keyed by database directory.
static CATALOGS: Lazy<dashmap::DashMap<PathBuf, Arc<MappedCatalog>>> =
    Lazy::new(dashmap::DashMap::new);

/// Generation-keyed registry snapshot cache. The generation comes from the
/// mapped header, so repeated reads avoid re-scanning slots.
static REGISTRY_CACHE: Lazy<dashmap::DashMap<PathBuf, (u64, BTreeMap<String, PathBuf>)>> =
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
        snapshot(&self.catalog.base_dir)
    }

    /// Insert a table entry and persist it through the mapped region.
    pub fn insert(&self, name: &str) -> io::Result<()> {
        let catalog = &self.catalog;
        let mut tables = snapshot(&catalog.base_dir)?;
        if tables.is_empty() {
            // First mutation after opening a legacy database without a
            // catalog: adopt existing `*.apex` files before registering.
            for (table_name, path) in scan_apex_files(&catalog.base_dir)? {
                tables.entry(table_name).or_insert(path);
            }
            // The caller created the `name` file under the same lock, so it is
            // not a pre-existing table.
            tables.remove(name);
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
        REGISTRY_CACHE.insert(catalog.base_dir.clone(), (generation, tables));
        Ok(())
    }

    /// Remove a table entry and persist it through the mapped region.
    pub fn remove(&self, name: &str) -> io::Result<Option<PathBuf>> {
        let catalog = &self.catalog;
        let mut tables = snapshot(&catalog.base_dir)?;
        if tables.is_empty() {
            for (table_name, path) in scan_apex_files(&catalog.base_dir)? {
                tables.entry(table_name).or_insert(path);
            }
        }
        let Some(removed) = tables.remove(name) else {
            return Ok(None);
        };

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
        REGISTRY_CACHE.insert(catalog.base_dir.clone(), (generation, tables));
        Ok(Some(removed))
    }
}

impl Drop for CatalogLock {
    fn drop(&mut self) {
        let _ = self.catalog.lock_file.unlock();
    }
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
    for _ in 0..MAX_SNAPSHOT_ATTEMPTS {
        let generation = catalog.generation();
        if let Some(entry) = REGISTRY_CACHE.get(base_dir) {
            if entry.value().0 == generation {
                return Ok(entry.value().1.clone());
            }
        }
        let tables = catalog.scan()?;
        if catalog.generation() == generation {
            REGISTRY_CACHE.insert(base_dir.to_path_buf(), (generation, tables.clone()));
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

/// Remove registry and lock files (used by `drop_if_exists`).
pub fn clear(base_dir: &Path) -> io::Result<()> {
    REGISTRY_CACHE.remove(base_dir);
    CATALOGS.remove(base_dir);
    let _ = fs::remove_file(base_dir.join(TABLE_CATALOG_FILE));
    let _ = fs::remove_file(base_dir.join(format!("{}.tmp", TABLE_CATALOG_FILE)));
    let _ = fs::remove_file(base_dir.join(TABLE_CATALOG_LOCK));
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
    let file_len = file.metadata()?.len() as usize;
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
    let modified = file.metadata()?.modified().unwrap_or(SystemTime::UNIX_EPOCH);
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
}
