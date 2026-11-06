use std::process::Command;

#[test]
fn executable_e1_contract() {
    let python = std::env::var_os("RPGMIGRATE_TEST_PYTHON").expect(
        "run this suite through python3 tools/test-db.py -- cargo test --test copy_integration",
    );
    let output = Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/e1_cases.py"
        ))
        .arg("--binary")
        .arg(env!("CARGO_BIN_EXE_rpgmigrate"))
        .output()
        .expect("execute the independent E1 fixture runner");
    assert!(
        output.status.success(),
        "E1 executable checks failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    print!("{}", String::from_utf8_lossy(&output.stdout));
}
