use crate::constants::{VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH};
use crate::progress::{Progress, VariableNameStep};
use crate::{Index, Result, UserError};

pub fn upgrade(index: &Index, base_version: (u32, u32, u32), progress: Progress) -> Result<()> {
    let wtxn = index.env.write_txn()?;
    let from = index.get_version(&wtxn)?;
    let upgrade_functions =
        [(v1_12_to_v1_13 as fn(&Index, Progress) -> Result<()>, "Upgrading from v1.12 to v1.13")];

    let current_major: u32 = VERSION_MAJOR.parse().unwrap();
    let current_minor: u32 = VERSION_MINOR.parse().unwrap();
    let current_patch: u32 = VERSION_PATCH.parse().unwrap();

    let start = match from {
        // If there was no version it means we're coming from the base version specified by the index-scheduler
        None if base_version.0 == 1 && base_version.1 == 12 => 0,
        Some((1, 12, _)) => 0,

        // --- Error handling
        None => {
            return Err(UserError::TooOldForUpgrade(
                base_version.0,
                base_version.1,
                base_version.2,
            )
            .into());
        }
        Some((major, minor, patch)) if major == 0 || (major == 1 && minor < 12) => {
            return Err(UserError::TooOldForUpgrade(major, minor, patch).into());
        }
        Some((major, minor, patch)) if major > current_major => {
            return Err(UserError::CannotDowngrade(major, minor, patch).into());
        }
        Some((major, minor, patch)) if major == current_major && minor > current_minor => {
            return Err(UserError::CannotDowngrade(major, minor, patch).into());
        }
        Some((major, minor, patch))
            if major == current_major && minor == current_minor && patch > current_patch =>
        {
            return Err(UserError::CannotDowngrade(major, minor, patch).into());
        }
        Some((major, minor, patch)) => {
            return Err(UserError::CannotUpgradeToUnknownVersion(major, minor, patch).into())
        }
    };

    enum UpgradeVersion {}
    let upgrade_path = &upgrade_functions[start..];

    for (i, (upgrade_function, upgrade_msg)) in upgrade_path.iter().enumerate() {
        progress.update_progress(VariableNameStep::<UpgradeVersion>::new(
            upgrade_msg.to_string(),
            i as u32,
            upgrade_path.len() as u32,
        ));
        (upgrade_function)(index, progress.clone())?;
    }

    Ok(())
}

fn v1_12_to_v1_13(_index: &Index, _progress: Progress) -> Result<()> {
    Ok(())
}
