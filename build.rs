#[cfg(not(test))]
fn main() {
    use std::env;
    use std::path::PathBuf;

    let host = env::var("HOST").expect("Cargo must supply HOST");
    let target = env::var("TARGET").expect("Cargo must supply TARGET");
    if let Some(name) = sqlite_build_override(&host, &target, |name| {
        println!("cargo:rerun-if-env-changed={name}");
        env::var_os(name).is_some()
    }) {
        panic!("SQLite build overrides are unsupported: {name}");
    }
    println!("cargo:rerun-if-env-changed=PQ_LIB_DIR");
    let Some(directory) = env::var_os("PQ_LIB_DIR") else {
        return;
    };
    let directory = PathBuf::from(directory)
        .canonicalize()
        .expect("PQ_LIB_DIR must select an existing native library directory");
    assert!(directory.is_dir(), "PQ_LIB_DIR must be a directory");
    let directory = directory
        .to_str()
        .expect("PQ_LIB_DIR must be valid UTF-8 for linker configuration");
    assert!(
        !directory.contains(['\r', '\n']),
        "PQ_LIB_DIR cannot contain line breaks"
    );
    // pq-sys 0.7.8 can retain a stale search path after its environment changes.
    println!("cargo:rustc-link-search=native={directory}");
    println!("cargo:rerun-if-changed={directory}");
    if matches!(
        env::var("CARGO_CFG_TARGET_OS").as_deref(),
        Ok("linux" | "macos")
    ) {
        // The runtime loader must search the library directory selected for this build.
        for argument in ["-Xlinker", "-rpath", "-Xlinker", directory] {
            println!("cargo:rustc-link-arg={argument}");
        }
    }
}

fn sqlite_build_override(
    host: &str,
    target: &str,
    mut is_set: impl FnMut(&str) -> bool,
) -> Option<String> {
    let common = [
        "LIBSQLITE3_SYS_USE_PKG_CONFIG",
        "LIBSQLITE3_FLAGS",
        "SQLITE_MAX_VARIABLE_NUMBER",
        "SQLITE_MAX_EXPR_DEPTH",
        "SQLITE_MAX_COLUMN",
        "SQLITE3_LIB_DIR",
        "SQLITE3_INCLUDE_DIR",
        "SQLITE3_STATIC",
        "CFLAGS",
        "HOST_CFLAGS",
        "TARGET_CFLAGS",
    ];
    let scoped = [host, target].into_iter().flat_map(|triple| {
        [
            format!("CFLAGS_{triple}"),
            format!("CFLAGS_{}", triple.replace('-', "_")),
        ]
    });
    common
        .into_iter()
        .map(str::to_owned)
        .chain(scoped)
        .find(|name| is_set(name))
}

#[cfg(test)]
mod tests {
    use super::sqlite_build_override;

    #[test]
    fn clean_build_accepts_the_default_bundled_configuration() {
        assert_eq!(
            sqlite_build_override("aarch64-apple-darwin", "x86_64-unknown-linux-gnu", |_| {
                false
            }),
            None
        );
    }

    #[test]
    fn sqlite_selection_limits_and_generic_flags_are_refused_when_present() {
        for name in [
            "LIBSQLITE3_SYS_USE_PKG_CONFIG",
            "LIBSQLITE3_FLAGS",
            "SQLITE_MAX_VARIABLE_NUMBER",
            "SQLITE_MAX_EXPR_DEPTH",
            "SQLITE_MAX_COLUMN",
            "SQLITE3_LIB_DIR",
            "SQLITE3_INCLUDE_DIR",
            "SQLITE3_STATIC",
            "CFLAGS",
            "HOST_CFLAGS",
            "TARGET_CFLAGS",
        ] {
            assert_eq!(
                sqlite_build_override("aarch64-apple-darwin", "x86_64-unknown-linux-gnu", |key| {
                    key == name
                }),
                Some(name.to_owned()),
                "{name} must not change the reviewed SQLite build"
            );
        }
    }

    #[test]
    fn both_host_and_target_flag_spellings_are_refused() {
        for name in [
            "CFLAGS_aarch64-apple-darwin",
            "CFLAGS_aarch64_apple_darwin",
            "CFLAGS_x86_64-unknown-linux-gnu",
            "CFLAGS_x86_64_unknown_linux_gnu",
        ] {
            assert_eq!(
                sqlite_build_override("aarch64-apple-darwin", "x86_64-unknown-linux-gnu", |key| {
                    key == name
                }),
                Some(name.to_owned())
            );
        }
    }
}
