//! provisiond: a pre-conformance prototype of the provisiond specification.
//!
//! The specification pinned in `spec-lock.json` owns every behavioural rule. Nothing in this crate
//! implements a requirement yet; see `docs/scope.md` and `docs/conformance-map.md`.

/// The specification commit this crate is written against.
pub const SPEC_COMMIT: &str = "ac37c7cae5d3ff746515137774a4cdd5a3531375";

/// The refusal every entry point prints until it implements something.
pub fn not_implemented(entry_point: &str) -> String {
    format!(
        "provisiond {entry_point}: not implemented; this build is a pre-conformance prototype \
         (specification {SPEC_COMMIT}) and must not run against real providers or funds"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_commit_matches_lock_file() {
        let lock = include_str!("../spec-lock.json");
        assert!(
            lock.contains(&format!("\"commit\": \"{SPEC_COMMIT}\"")),
            "SPEC_COMMIT and spec-lock.json disagree"
        );
    }

    #[test]
    fn refusal_names_the_entry_point() {
        assert!(not_implemented("engine").starts_with("provisiond engine: not implemented"));
    }
}
