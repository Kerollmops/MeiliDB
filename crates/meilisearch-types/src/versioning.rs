use std::fs;
use std::io::{self, ErrorKind};
use std::path::Path;

/// The name of the file that contains the version of the database.
pub const VERSION_FILE_NAME: &str = "VERSION";

pub static VERSION_MAJOR: &str = env!("CARGO_PKG_VERSION_MAJOR");
pub static VERSION_MINOR: &str = env!("CARGO_PKG_VERSION_MINOR");
pub static VERSION_PATCH: &str = env!("CARGO_PKG_VERSION_PATCH");

/// Persists the version of the current Meilisearch binary to a VERSION file
pub fn create_current_version_file(db_path: &Path) -> io::Result<()> {
    create_version_file(db_path, VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH)
}

pub fn create_version_file(
    db_path: &Path,
    major: &str,
    minor: &str,
    patch: &str,
) -> io::Result<()> {
    let version_path = db_path.join(VERSION_FILE_NAME);
    fs::write(version_path, format!("{}.{}.{}", major, minor, patch))
}

pub fn get_version(db_path: &Path) -> Result<(String, String, String), VersionFileError> {
    let version_path = db_path.join(VERSION_FILE_NAME);

    match fs::read_to_string(version_path) {
        Ok(version) => parse_version(&version),
        Err(error) => match error.kind() {
            ErrorKind::NotFound => Err(VersionFileError::MissingVersionFile),
            _ => Err(error.into()),
        },
    }
}

pub fn parse_version(version: &str) -> Result<(String, String, String), VersionFileError> {
    let version_components = version.trim().split('.').collect::<Vec<_>>();
    let (major, minor, patch) = match &version_components[..] {
        [major, minor, patch] => (major.to_string(), minor.to_string(), patch.to_string()),
        _ => return Err(VersionFileError::MalformedVersionFile),
    };
    Ok((major, minor, patch))
}

#[derive(thiserror::Error, Debug)]
pub enum VersionFileError {
    #[error(
        "Meilisearch (v{}) failed to infer the version of the database.
        To update Meilisearch please follow our guide on https://www.meilisearch.com/docs/learn/update_and_migration/updating.",
        env!("CARGO_PKG_VERSION").to_string()
    )]
    MissingVersionFile,
    #[error("Version file is corrupted and thus Meilisearch is unable to determine the version of the database.")]
    MalformedVersionFile,
    #[error(
        "Your database version ({major}.{minor}.{patch}) is incompatible with your current engine version ({}).\n\
        To migrate data between Meilisearch versions, please follow our guide on https://www.meilisearch.com/docs/learn/update_and_migration/updating.",
        env!("CARGO_PKG_VERSION").to_string()
    )]
    VersionMismatch { major: String, minor: String, patch: String },

    #[error(transparent)]
    IoError(#[from] std::io::Error),
}
