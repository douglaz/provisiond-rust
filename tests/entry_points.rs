//! Unconfigured prototype entries fail closed without starting PostgreSQL.

use std::process::Command;

#[test]
fn unconfigured_entries_refuse_and_exit_nonzero() {
    for (name, path) in [
        ("api", env!("CARGO_BIN_EXE_api")),
        ("engine", env!("CARGO_BIN_EXE_engine")),
        ("migrate", env!("CARGO_BIN_EXE_migrate")),
        ("recovery", env!("CARGO_BIN_EXE_recovery")),
    ] {
        let output = Command::new(path)
            .env_remove("PROVISIOND_PROFILE")
            .output()
            .expect("spawn stub");
        assert!(!output.status.success(), "{name} exited with success");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(&format!("provisiond {name}:")),
            "entry diagnostic: {stderr}"
        );
    }
}

/// Guards against exposing non-UTF-8 configuration values in startup diagnostics.
#[test]
fn invalid_utf8_settings_are_named_without_exposing_values() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    use std::process::Stdio;

    let binaries: [(&str, &str, &[&str]); 4] = [
        ("api", env!("CARGO_BIN_EXE_api"), &[]),
        ("engine", env!("CARGO_BIN_EXE_engine"), &[]),
        ("migrate", env!("CARGO_BIN_EXE_migrate"), &[]),
        ("recovery", env!("CARGO_BIN_EXE_recovery"), &["report"]),
    ];
    for setting in [
        "PROVISIOND_DATABASE_URL",
        "PROVISIOND_STANDBY_NAME",
        "PROVISIOND_STANDBY_ROLE",
        "PROVISIOND_STANDBY_ADDRESS",
    ] {
        let invalid_value: &[u8] = if setting == "PROVISIOND_DATABASE_URL" {
            b"host=127.0.0.1 port=9 user=app password=SAMPLEVALUE\xff dbname=postgres"
        } else {
            b"standby-SAMPLEVALUE\xff"
        };
        let invalid_value = OsString::from_vec(invalid_value.to_vec());
        for (name, path, args) in binaries {
            let output = Command::new(path)
                .args(args)
                .env_clear()
                .env("PROVISIOND_PROFILE", "local-simulator")
                .env(
                    "PROVISIOND_DATABASE_URL",
                    "host=127.0.0.1 port=9 user=app dbname=postgres",
                )
                .env("PROVISIOND_STANDBY_NAME", "provisiond_sync")
                .env("PROVISIOND_STANDBY_ROLE", "replicator")
                .env("PROVISIOND_STANDBY_ADDRESS", "127.0.0.1")
                .env(setting, &invalid_value)
                .stdin(Stdio::null())
                .output()
                .unwrap_or_else(|err| {
                    panic!("{name} with invalid {setting} failed to spawn: {err}")
                });

            assert_eq!(
                output.status.code(),
                Some(1),
                "{name} with invalid {setting}"
            );
            for (stream, bytes) in [("stdout", &output.stdout), ("stderr", &output.stderr)] {
                assert!(
                    !bytes
                        .windows(b"SAMPLEVALUE".len())
                        .any(|bytes| bytes == b"SAMPLEVALUE"),
                    "{name} with invalid {setting} exposed the setting value on {stream}"
                );
            }
            let stderr = String::from_utf8_lossy(&output.stderr);
            let prefix = format!("provisiond {name}: ");
            assert!(
                stderr
                    .lines()
                    .any(|line| line.starts_with(&prefix) && line.contains(setting)),
                "{name} with invalid {setting} did not name the setting in its diagnostic: {stderr}"
            );
        }
    }
}
