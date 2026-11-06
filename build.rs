use std::env;
use std::path::PathBuf;

fn main() {
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
