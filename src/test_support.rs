//! Helpers shared by the unit and integration tests.

use std::path::Path;
use std::process::Command;

/// Makes a freshly written test script executable without a race.
///
/// Tests spawn processes concurrently. A child forked while this process holds
/// a write handle to the script inherits that handle until it execs, and
/// executing the script meanwhile fails with ETXTBSY. Recreating the file
/// through `install` keeps every write handle to the final file in that child,
/// which has exited before this returns.
pub fn seal_executable(path: &Path) {
    let staged = path.with_extension("staged");
    std::fs::rename(path, &staged).unwrap();
    let status = Command::new("install")
        .arg("-m")
        .arg("700")
        .arg(&staged)
        .arg(path)
        .status()
        .unwrap();
    assert!(status.success(), "install failed for {}", path.display());
    std::fs::remove_file(&staged).unwrap();
}
