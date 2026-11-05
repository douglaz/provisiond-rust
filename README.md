# provisiond-rust

An implementation of [provisiond](https://github.com/douglaz/provisiond), in Rust.

The specification lives in its own repository and owns every behavioural rule. This repository
holds what that one excludes: the runtime, the database client, executable migrations, deployment
packaging, the provider and rail simulators, and the integration tests that run against real
PostgreSQL.

**Status: not a conforming service.** Nothing here is implemented yet. Do not point real provider
accounts or real funds at any build from this repository.
