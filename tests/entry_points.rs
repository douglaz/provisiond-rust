//! The stub entry points must refuse: exit non-zero and print the refusal for their own name.

use std::process::Command;

#[test]
fn stubs_refuse_and_exit_nonzero() {
    for (name, path) in [
        ("api", env!("CARGO_BIN_EXE_api")),
        ("engine", env!("CARGO_BIN_EXE_engine")),
        ("migrate", env!("CARGO_BIN_EXE_migrate")),
    ] {
        let output = Command::new(path).output().expect("spawn stub");
        assert!(!output.status.success(), "{name} exited with success");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            stderr.trim_end(),
            provisiond::not_implemented(name),
            "{name} did not print its refusal"
        );
    }
}
