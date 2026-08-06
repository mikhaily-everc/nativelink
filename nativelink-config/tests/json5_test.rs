use std::fs;
use std::path::{Path, PathBuf};

use nativelink_config::cas_server::CasConfig;

/// Locate `nativelink-config/examples`, which moves depending on how the test
/// is run.
///
/// Under Cargo the cwd is the crate root, so `examples` sits directly below it.
/// Under Bazel the cwd is the runfiles root, and where the data lands there
/// depends on whether nativelink is the main repo or an external one: as an
/// external repo the runfiles root is `_main`, and the data is under a
/// *sibling* `<repo>/nativelink-config/examples` rather than anywhere below
/// cwd. Probing beats guessing at the layout.
fn find_examples_dir() -> PathBuf {
    let cwd = Path::new(".")
        .canonicalize()
        .expect("Can canonicalize current dir");

    for candidate in [
        cwd.join("nativelink-config").join("examples"),
        cwd.join("examples"),
    ] {
        if candidate.is_dir() {
            return candidate;
        }
    }

    // External-repo layout: look at the runfiles root's other repositories.
    if let Some(runfiles_root) = cwd.parent()
        && let Ok(entries) = fs::read_dir(runfiles_root)
    {
        for entry in entries.flatten() {
            let candidate = entry.path().join("nativelink-config").join("examples");
            if candidate.is_dir() {
                return candidate;
            }
        }
    }

    panic!("Could not locate nativelink-config/examples starting from {cwd:?}");
}

#[test]
fn test_example_parsing() {
    let examples_path = find_examples_dir();

    let mut found_at_least_one_entry = false;

    for entry in fs::read_dir(&examples_path)
        .unwrap_or_else(|e| panic!("Failed to read from {:?}: {}", &examples_path, e))
    {
        let config_file = entry.unwrap().path().display().to_string();
        if !config_file.contains(".json5") {
            continue;
        }
        CasConfig::try_from_json5_file(&config_file)
            .unwrap_or_else(|e| panic!("Error while reading {config_file}: {e}"));
        found_at_least_one_entry = true;
    }

    assert!(found_at_least_one_entry);
}
