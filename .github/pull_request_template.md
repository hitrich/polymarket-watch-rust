## Summary

Describe the problem and the focused change that solves it.

## Safety and compatibility

Explain any impact on live execution, risk limits, compliance gates,
accounting, persistence, protocol assumptions, or configuration compatibility.

## Validation

List the tests and commands you ran.

## Checklist

- [ ] I added or updated tests for changed behavior and failure paths.
- [ ] I did not commit credentials, private keys, real account data, or journals.
- [ ] I kept live behavior fail-closed for stale, malformed, partial, or ambiguous state.
- [ ] I updated documentation for user-visible or protocol-level changes.
- [ ] `cargo fmt --all -- --check` passes.
- [ ] `cargo test --offline --all-targets --all-features` passes.
- [ ] `cargo clippy --offline --all-targets --all-features -- -D warnings` passes.
