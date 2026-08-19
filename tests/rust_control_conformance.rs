use std::process::{Command, Output};

fn run(arguments: &[&str]) -> Output {
    Command::new("python3")
        .args(arguments)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap()
}

#[test]
fn python_and_rust_backends_have_no_unexplained_control_trace_differences() {
    let current = run(&[
        "tests/run_control_conformance.py",
        "--profile",
        "python-current",
    ]);
    assert!(
        current.status.success(),
        "Python-current conformance failed:\n{}{}",
        String::from_utf8_lossy(&current.stdout),
        String::from_utf8_lossy(&current.stderr)
    );

    let backend = format!("{} conformance-backend", env!("CARGO_BIN_EXE_panetone"));
    let target = run(&[
        "tests/run_control_conformance.py",
        "--profile",
        "target",
        "--backend",
        &backend,
    ]);
    assert!(
        target.status.success(),
        "target conformance failed:\n{}{}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
}
