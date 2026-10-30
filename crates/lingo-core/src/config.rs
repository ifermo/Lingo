//! Application paths.

use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot determine the user data directory")]
    NoDataDir,
}

#[derive(Clone, Debug)]
pub struct Paths {
    pub data_dir: PathBuf,
    pub db: PathBuf,
    pub key: PathBuf,
}

impl Paths {
    pub fn discover() -> Result<Self, ConfigError> {
        let base = dirs::data_dir().ok_or(ConfigError::NoDataDir)?;
        Ok(Self::with_root(base.join("Lingo")))
    }

    pub fn with_root(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref().to_path_buf();
        Self {
            db: root.join("db.sqlite"),
            key: root.join("id.key"),
            data_dir: root,
        }
    }

    pub fn ensure(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.data_dir)
    }
}
