use std::fs;
use std::path::{Path, PathBuf};

use nativelink_config::cas_server::CasConfig;
use nativelink_error::{Code, Error};

/// Locate `nativelink-config/tests/duplicate_servers.json5`, which moves
/// depending on how the test is run.
///
/// Under Cargo the cwd is the crate root, so `tests` sits directly below it.
/// Under Bazel the cwd is the runfiles root, and where the data lands there
/// depends on whether nativelink is the main repo or an external one: as an
/// external repo the runfiles root is `_main`, and the data is under a
/// *sibling* `<repo>/nativelink-config/tests` rather than anywhere below cwd.
/// Probing beats guessing at the layout.
fn find_duplicate_servers_config() -> PathBuf {
    let cwd = Path::new(".")
        .canonicalize()
        .expect("Can canonicalize current dir");

    for candidate in [
        cwd.join("nativelink-config").join("tests"),
        cwd.join("tests"),
    ] {
        let candidate = candidate.join("duplicate_servers.json5");
        if candidate.is_file() {
            return candidate;
        }
    }

    // External-repo layout: look at the runfiles root's other repositories.
    if let Some(runfiles_root) = cwd.parent()
        && let Ok(entries) = fs::read_dir(runfiles_root)
    {
        for entry in entries.flatten() {
            let candidate = entry
                .path()
                .join("nativelink-config")
                .join("tests")
                .join("duplicate_servers.json5");
            if candidate.is_file() {
                return candidate;
            }
        }
    }

    panic!("Could not locate nativelink-config/tests/duplicate_servers.json5 from {cwd:?}");
}

#[test]
fn test_duplicate_servers() {
    let config_path = find_duplicate_servers_config();

    let err =
        CasConfig::try_from_json5_file(config_path.as_os_str().to_str().unwrap()).unwrap_err();
    assert_eq!(
        err,
        Error::new(
            Code::InvalidArgument,
            "CAS and AC use the same store 'MAIN_STORE' in the config".into()
        )
    );
}
