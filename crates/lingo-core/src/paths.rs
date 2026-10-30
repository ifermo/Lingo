//! Manifest construction and path safety.
//!
//! Everything that crosses the wire is treated as untrusted: paths are
//! normalized, traversal is rejected, and writes/deletes are constrained to the
//! selected destination root.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use tracing::warn;
use uuid::Uuid;
use walkdir::WalkDir;

use crate::model::{Manifest, ManifestEntry, PathMap, Staging};

pub const MAX_MANIFEST_ENTRIES: usize = 100_000;
pub const MAX_PATH_BYTES: usize = 4096;
pub const TEMP_DIR_NAME: &str = ".lingo-part";

#[derive(Debug, thiserror::Error)]
pub enum PathError {
    #[error("path is empty")]
    Empty,
    #[error("path is not relative")]
    NotRelative,
    #[error("path contains an unsafe component: {0}")]
    UnsafeComponent(String),
    #[error("path is too long")]
    TooLong,
    #[error("path is not valid UTF-8")]
    NotUtf8,
    #[error("duplicate top-level name: {0}")]
    DuplicateTop(String),
    #[error("manifest contains too many entries")]
    TooManyEntries,
    #[error("nothing to stage")]
    EmptyManifest,
    #[error("path escapes the destination root")]
    Escape,
    #[error("missing source mapping for {0}")]
    MissingSourceMapping(String),
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Normalize an untrusted relative path into forward-slash form.
pub fn normalize_relative(path: &Path) -> Result<String, PathError> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or(PathError::NotUtf8)?;
                if part.is_empty() || part.contains('\0') || part.chars().any(char::is_control) {
                    return Err(PathError::UnsafeComponent(part.to_string()));
                }
                parts.push(part);
            }
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(PathError::UnsafeComponent("..".to_string()));
            }
            Component::RootDir | Component::Prefix(_) => return Err(PathError::NotRelative),
        }
    }
    if parts.is_empty() {
        return Err(PathError::Empty);
    }
    let normalized = parts.join("/");
    if normalized.len() > MAX_PATH_BYTES {
        return Err(PathError::TooLong);
    }
    Ok(normalized)
}

/// Validate a manifest path received from a peer.
pub fn validate_manifest_path(path: &str) -> Result<String, PathError> {
    normalize_relative(Path::new(path))
}

/// Scan local paths (files or directories) into a staging manifest.
pub fn scan_paths(paths: &[PathBuf]) -> Result<Staging, PathError> {
    if paths.is_empty() {
        return Err(PathError::Empty);
    }

    let mut entries: BTreeMap<String, u64> = BTreeMap::new();
    let mut path_map: PathMap = BTreeMap::new();

    for input in paths {
        let meta = std::fs::symlink_metadata(input).map_err(|source| PathError::Io {
            path: input.clone(),
            source,
        })?;

        if meta.file_type().is_symlink() {
            warn!(path = %input.display(), "skipping symlink while staging");
            continue;
        }

        let top = input
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(PathError::NotUtf8)?;
        let top = normalize_relative(Path::new(top))?;

        if let Some(existing) = path_map.get(&top) {
            if Path::new(existing) != input {
                return Err(PathError::DuplicateTop(top));
            }
            continue;
        }
        path_map.insert(top.clone(), input.to_string_lossy().to_string());

        if meta.is_file() {
            entries.insert(top, meta.len());
            continue;
        }

        if !meta.is_dir() {
            warn!(path = %input.display(), "skipping unsupported file type");
            continue;
        }

        for entry in WalkDir::new(input).follow_links(false) {
            let entry = entry.map_err(|source| PathError::Io {
                path: input.clone(),
                source: source.into(),
            })?;
            if entry.depth() == 0 || entry.file_type().is_dir() {
                continue;
            }
            if entry.file_type().is_symlink() {
                warn!(path = %entry.path().display(), "skipping symlink while staging");
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(input)
                .map_err(|_| PathError::Escape)?;
            let relative = normalize_relative(relative)?;
            let manifest_path = format!("{top}/{relative}");
            let size = entry
                .metadata()
                .map_err(|source| PathError::Io {
                    path: entry.path().to_path_buf(),
                    source: source.into(),
                })?
                .len();
            entries.insert(manifest_path, size);
            if entries.len() > MAX_MANIFEST_ENTRIES {
                return Err(PathError::TooManyEntries);
            }
        }
    }

    if entries.is_empty() {
        return Err(PathError::EmptyManifest);
    }

    let manifest: Manifest = entries
        .into_iter()
        .map(|(path, size)| ManifestEntry { path, size })
        .collect();

    Ok(Staging {
        manifest,
        path_map,
        note: String::new(),
    })
}

/// Join a validated manifest path to a destination root.
pub fn destination_path(root: &Path, manifest_path: &str) -> Result<PathBuf, PathError> {
    let relative = validate_manifest_path(manifest_path)?;
    Ok(root.join(relative))
}

/// Resolve a manifest path back to the local source selected during staging.
pub fn resolve_source(path_map: &PathMap, manifest_path: &str) -> Result<PathBuf, PathError> {
    let normalized = validate_manifest_path(manifest_path)?;
    let mut parts = normalized.split('/');
    let top = parts.next().ok_or(PathError::Empty)?;
    let base = path_map
        .get(top)
        .ok_or_else(|| PathError::MissingSourceMapping(top.to_string()))?;
    let mut path = PathBuf::from(base);
    for part in parts {
        path.push(part);
    }
    Ok(path)
}

pub fn temp_dir(root: &Path, tid: Uuid) -> PathBuf {
    root.join(TEMP_DIR_NAME).join(tid.to_string())
}

pub fn temp_file(root: &Path, tid: Uuid, index: usize) -> PathBuf {
    temp_dir(root, tid).join(format!("{index}.part"))
}

pub fn remove_temp_dir(root: &Path, tid: Uuid) -> std::io::Result<()> {
    match std::fs::remove_dir_all(temp_dir(root, tid)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Ensure `candidate` does not escape `root` through an existing symlink.
pub fn ensure_within_root(root: &Path, candidate: &Path) -> Result<(), PathError> {
    let canonical_root = root.canonicalize().map_err(|source| PathError::Io {
        path: root.to_path_buf(),
        source,
    })?;

    let mut probe = candidate.to_path_buf();
    while !probe.exists() {
        if !probe.pop() {
            break;
        }
    }

    if probe.exists() {
        let canonical = probe.canonicalize().map_err(|source| PathError::Io {
            path: probe.clone(),
            source,
        })?;
        if !canonical.starts_with(&canonical_root) {
            return Err(PathError::Escape);
        }
    }
    Ok(())
}

/// Find a non-colliding file name by appending ` (n)` before the extension.
pub fn unique_path(path: &Path) -> PathBuf {
    unique_path_avoiding(path, &BTreeSet::new())
}

/// Like [`unique_path`], but also avoids names already reserved by this transfer.
pub fn unique_path_avoiding(path: &Path, reserved: &BTreeSet<PathBuf>) -> PathBuf {
    if !path.exists() && !reserved.contains(path) {
        return path.to_path_buf();
    }
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("file");
    let extension = path.extension().and_then(|s| s.to_str());
    for index in 1..10_000 {
        let name = match extension {
            Some(ext) => format!("{stem} ({index}).{ext}"),
            None => format!("{stem} ({index})"),
        };
        let candidate = parent.join(name);
        if !candidate.exists() && !reserved.contains(&candidate) {
            return candidate;
        }
    }
    path.to_path_buf()
}

/// Delete files listed in a manifest below `root`, never following symlinks.
pub fn delete_transfer_files(root: &Path, manifest: &Manifest) -> Result<usize, PathError> {
    ensure_within_root(root, root)?;
    let mut removed = 0;
    for entry in manifest {
        let candidate = destination_path(root, &entry.path)?;
        if !candidate.starts_with(root) {
            return Err(PathError::Escape);
        }
        match std::fs::symlink_metadata(&candidate) {
            Ok(meta) if meta.is_file() => {
                std::fs::remove_file(&candidate).map_err(|source| PathError::Io {
                    path: candidate,
                    source,
                })?;
                removed += 1;
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(PathError::Io {
                    path: candidate,
                    source,
                });
            }
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tempdir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn rejects_absolute_and_parent_paths() {
        assert!(matches!(
            normalize_relative(Path::new("/etc/passwd")),
            Err(PathError::NotRelative)
        ));
        assert!(matches!(
            normalize_relative(Path::new("../secret")),
            Err(PathError::UnsafeComponent(_))
        ));
        assert!(matches!(
            normalize_relative(Path::new("a/../../b")),
            Err(PathError::UnsafeComponent(_))
        ));
        assert_eq!(normalize_relative(Path::new("a/./b")).unwrap(), "a/b");
    }

    #[test]
    fn scans_files_and_directories() {
        let dir = tempdir();
        let root = dir.path();
        fs::write(root.join("one.txt"), b"hello").unwrap();
        fs::create_dir(root.join("folder")).unwrap();
        fs::write(root.join("folder/two.bin"), vec![0u8; 16]).unwrap();

        let staging = scan_paths(&[root.join("one.txt"), root.join("folder")]).unwrap();

        assert_eq!(staging.manifest.len(), 2);
        assert_eq!(
            staging
                .manifest
                .iter()
                .find(|entry| entry.path == "one.txt")
                .unwrap()
                .size,
            5
        );
        assert_eq!(
            staging
                .manifest
                .iter()
                .find(|entry| entry.path == "folder/two.bin")
                .unwrap()
                .size,
            16
        );
        assert_eq!(staging.path_map.len(), 2);
        assert_eq!(staging.total_size(), 21);
    }

    #[test]
    fn rejects_duplicate_top_names_from_different_sources() {
        let dir = tempdir();
        let a = dir.path().join("a").join("same");
        let b = dir.path().join("b").join("same");
        fs::create_dir_all(&a).unwrap();
        fs::create_dir_all(&b).unwrap();
        fs::write(a.join("same.txt"), b"a").unwrap();
        fs::write(b.join("same.txt"), b"b").unwrap();

        assert!(matches!(
            scan_paths(&[a, b]),
            Err(PathError::DuplicateTop(_))
        ));
    }

    #[test]
    fn destination_paths_stay_below_root() {
        let root = Path::new("/tmp/lingo-root");
        assert_eq!(
            destination_path(root, "folder/file.txt").unwrap(),
            root.join("folder/file.txt")
        );
        assert!(destination_path(root, "../file.txt").is_err());
        assert!(destination_path(root, "/etc/passwd").is_err());
    }

    #[test]
    fn unique_path_appends_a_counter() {
        let dir = tempdir();
        let file = dir.path().join("report.txt");
        fs::write(&file, b"x").unwrap();
        assert_eq!(
            unique_path(&file).file_name().unwrap().to_str().unwrap(),
            "report (1).txt"
        );
    }

    #[test]
    fn unique_path_avoids_reserved_names() {
        let dir = tempdir();
        let root = dir.path();
        let file = root.join("report.txt");
        fs::write(&file, b"x").unwrap();
        let mut reserved = BTreeSet::new();
        let first = unique_path_avoiding(&file, &reserved);
        assert_eq!(
            first.file_name().unwrap().to_str().unwrap(),
            "report (1).txt"
        );
        reserved.insert(first);
        let second = unique_path_avoiding(&file, &reserved);
        assert_eq!(
            second.file_name().unwrap().to_str().unwrap(),
            "report (2).txt"
        );
    }

    #[test]
    fn deletes_only_files_in_manifest() {
        let dir = tempdir();
        let root = dir.path();
        fs::create_dir(root.join("payload")).unwrap();
        let file = root.join("payload/a.txt");
        fs::write(&file, b"x").unwrap();
        let keep = root.join("keep.txt");
        fs::write(&keep, b"y").unwrap();

        let manifest = vec![ManifestEntry {
            path: "payload/a.txt".into(),
            size: 1,
        }];
        assert_eq!(delete_transfer_files(root, &manifest).unwrap(), 1);
        assert!(!file.exists());
        assert!(keep.exists());
    }

    #[test]
    fn resolves_nested_sources_from_the_path_map() {
        let map = BTreeMap::from([
            ("folder".to_string(), "/tmp/source/folder".to_string()),
            (
                "single.txt".to_string(),
                "/tmp/source/single.txt".to_string(),
            ),
        ]);
        assert_eq!(
            resolve_source(&map, "folder/nested/file.txt").unwrap(),
            Path::new("/tmp/source/folder/nested/file.txt")
        );
        assert_eq!(
            resolve_source(&map, "single.txt").unwrap(),
            Path::new("/tmp/source/single.txt")
        );
        assert!(matches!(
            resolve_source(&map, "../escape.txt"),
            Err(PathError::UnsafeComponent(_))
        ));
        assert!(matches!(
            resolve_source(&map, "missing.txt"),
            Err(PathError::MissingSourceMapping(_))
        ));
    }

    #[test]
    fn temp_paths_are_scoped_to_the_transfer() {
        let root = Path::new("/tmp/lingo-root");
        let tid = Uuid::nil();
        assert_eq!(
            temp_file(root, tid, 3),
            root.join(TEMP_DIR_NAME)
                .join(tid.to_string())
                .join("3.part")
        );
    }
}
