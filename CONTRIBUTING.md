# Contributing

Thank you for helping improve `polymarket-watch-rust`. This project handles
trading state and can submit live orders, so changes should preserve its
fail-closed behavior and make risk explicit.

## Before you start

- Search existing issues and pull requests before opening a new one.
- Open an issue first for large changes, protocol changes, or behavior that
  affects live execution.
- Never commit credentials, private keys, wallet secrets, real account data,
  journal files, or proprietary market data.
- Do not propose geoblock evasion, market manipulation, confidential-
  information trading, or other prohibited conduct.

## Local setup

Install a stable Rust toolchain, clone the repository, and copy the example
configuration to a local ignored file:

```sh
cp config.example.toml config.local.toml
cargo test --offline --all-targets --all-features
```

Paper mode is the development default. Do not use a funded account to validate
a contribution.

## Making a change

1. Create a focused branch from `main`.
2. Add tests for behavior changes and failure paths.
3. Keep safety checks fail-closed: malformed, stale, partial, or ambiguous
   external state must not unlock live execution.
4. Update `README.md` or `architecture.md` when commands, configuration, or
   protocol assumptions change.
5. Run the complete quality gate:

```sh
cargo fmt --all -- --check
cargo test --offline --all-targets --all-features
cargo clippy --offline --all-targets --all-features -- -D warnings
cargo build --offline --release
```

If offline mode cannot resolve a newly introduced dependency, explain that in
the pull request and include the corresponding `Cargo.lock` update.

## Pull requests

Keep pull requests small enough to review. Describe:

- the problem and intended behavior;
- safety, compliance, accounting, and compatibility impact;
- tests added or changed;
- commands used to validate the change;
- any remaining risk or follow-up work.

By contributing, you agree that your contribution is licensed under the
repository's MIT License.
