//! `api` entry point. Not implemented: exits non-zero without touching any database or provider.

use std::process::ExitCode;

fn main() -> ExitCode {
    eprintln!("{}", provisiond::not_implemented("api"));
    ExitCode::FAILURE
}
