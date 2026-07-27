# polymarket-watch-rust

A single-user Rust runtime for conservative Polymarket paper trading and
fail-closed live execution. It uses official CLOB/Data clients and protocol
types, a single ordered and engine-acknowledged market WebSocket consumer,
exact fixed-point accounting, durable write-ahead journaling, recurring
geographic checks, authenticated order tracking, and a loopback-only operator
console.

This is execution software, not a promise of profit. It does not implement
market manipulation, geoblock evasion, confidential-information trading,
negative-risk markets, or the unverified “140,000 tweets/minute” claim from the
source brief.

## Run paper mode

Replace the sample asset and condition IDs in `config.example.toml` with a real
binary market pair, then run:

```sh
cargo run --release -- --start-paper config.example.toml
```

Add `--gui` for the local console at `http://127.0.0.1:8787`:

```sh
cargo run --release -- --gui --start-paper config.example.toml
```

Without `--start-paper`, the strategy starts paused. The GUI can resume paper
mode, pause, cancel stale/all tracked orders, flatten paper positions, and shut
down the process. Commands return only after the engine executes or rejects
them. Paper submissions, fills, cancellations, and flattening commit as one
fsynced state transition and the last valid state is replayed on restart.
Terminal orders are pruned from active snapshots, and the paper-only journal is
atomically compacted with a commitment to the prior BLAKE3 root before it can
grow without bound. Use a dedicated `journal_path` for each paper/live runtime.
Paper flattening requires hash-backed books that are fresh by both local age
and exchange-event lag, consumes only configured participation within the
slippage band, aggregates depth into one venue-minimum-compliant FAK per asset,
records each level at its actual execution price, and reports rather than
inventing fills for any sub-minimum or illiquid residual position.

## Live mode

Live mode requires all of the following at runtime:

- `mode = "live"`, `live_order_submission_enabled = true`, and the exact
  confirmation `ENABLE_LIVE_POLYMARKET_ORDERS`;
- official production endpoints, V2/pUSD, Polygon chain ID 137, and a matching
  wallet/signature path;
- `PRIVATE_KEY`, `POLYMARKET_API_KEY`, `POLYMARKET_API_SECRET`, and
  `POLYMARKET_API_PASSPHRASE` in `.env` or the process environment;
- `DEPOSIT_WALLET_ADDRESS` when using `deposit_wallet` / `poly1271`;
- `controlled_wallets` declaring the funder and verified signer, with no overlap
  with copied wallets, plus a dedicated `compliance_latch_path`;
- current geographic eligibility, account balance and allowances, clock sync,
  market metadata, BLAKE3 journal verification, heartbeat, paginated remote
  reconciliation, and authenticated user-stream resynchronization.

Even after those checks pass, the live strategy remains paused until an
explicit `resume_live` command. Any ambiguous submit result, feed loss,
heartbeat degradation, stale order, compliance failure, unknown remote order,
reconciliation gap, account-revision race, or undurable post-submit journal
result disables new live submissions. Invalid book continuity immediately
cancels affected orders. Each wire frame is applied as one ordered,
acknowledged batch: a later valid book in that frame clears the repair, while
an unresolved invalidation forces an authoritative WebSocket resubscription.
Account-changing user events must also be visible in stable Data API/CLOB reads
before another order can pass. Reconnect, maintenance, and startup normalize
paginated CLOB trade history to this API key's exact maker/taker clip and diff
it against every durable runtime-owned order ID. Even when both an order update
and its trade event are lost, the trade is reconstructed before account risk
can unlock; a periodic authenticated audit also closes silent user-feed gaps
while the socket remains connected. Every pending fill requires exact
authoritative trade IDs, exact
equality of the complete normalized position map after applying expected
deltas, and a collateral delta inside the fee envelope derived from the trade's
V2 fee terms and five-decimal venue precision. This also prevents a zero-net,
fee-free batch, unrelated position change, or unrelated account debit from
being mistaken for reconciliation. Unfilled cancellations update order
lifecycle state without creating a holdings lock; partially filled
cancellations remain locked until their authoritative trade arrives. Use a
dedicated account and journal; startup intentionally rejects open remote orders
that cannot be mapped to this runtime. A cold restart that discovers a wholly
missed trade without a durable pre-trade account baseline reconstructs the
trade but remains locked for manual reconciliation instead of absorbing an
unprovable balance change.
Startup also looks up every nonterminal exchange order by ID and journals an
authoritative cancelled/matched terminal state, closing the crash window after
a venue cancellation succeeds but before its local record is flushed.

Loss control uses a persistent, non-decreasing observed equity high-water.
Every stable authoritative account snapshot advances it before the snapshot is
published or used for reconciliation. UTC date changes never reset that value
downward from a post-loss snapshot, so a rollover cannot erase a drawdown
immediately before an order.

Compliance latches are persistent and fail closed. A copied signal marked as
confidential, or any controlled-wallet conflict, pauses trading and records its
provenance in the latch file. Clear or replace that file only while the process
is stopped and after an explicit compliance review.

No funded live smoke test is part of CI. Validate with the smallest venue-valid
size and independent account monitoring before trusting production funds.

## Quality gate

```sh
cargo fmt --all -- --check
cargo test --offline --all-targets --all-features
cargo clippy --offline --all-targets --all-features -- -D warnings
cargo build --offline --release
```

See `architecture.md` for the complete safety model, protocol assumptions, and
deliberate limitations.

## Contributing

Contributions are welcome, from focused documentation fixes through tested
protocol and risk-control changes. Start with
[CONTRIBUTING.md](CONTRIBUTING.md) for the full policy and use this section to
find the right part of the codebase.

### Contributor quick start

Install a stable Rust toolchain, then prepare a reproducible local checkout:

```sh
git clone https://github.com/hitrich/polymarket-watch-rust.git
cd polymarket-watch-rust
cargo fetch --locked
cargo test --offline --locked --all-targets --all-features
```

Copy the example configuration only if you need to exercise the runtime:

```sh
cp config.example.toml config.local.toml
```

`config.local.toml` is ignored by Git. Its market identifiers are placeholders
until you replace them with a real binary market pair. Develop and reproduce
changes in paper mode; a funded account is never required for a contribution.

### Where to make changes

| Area | Primary files | Context |
| --- | --- | --- |
| Runtime orchestration | `src/engine.rs`, `src/runtime.rs`, `src/state.rs` | Ordered event handling, commands, snapshots, and lifecycle transitions |
| Market data | `src/market_ws.rs`, `src/book.rs`, `src/discovery.rs` | WebSocket continuity, authoritative books, and market metadata |
| Orders and account state | `src/execution.rs`, `src/user_ws.rs`, `src/reconcile.rs`, `src/matching_engine.rs` | Submission, authenticated updates, remote recovery, and order identity |
| Safety and compliance | `src/risk.rs`, `src/readiness.rs`, `src/compliance.rs`, `src/heartbeat.rs`, `src/secrets.rs` | Pre-trade limits, live gates, geographic checks, health, and secret presence |
| Accounting and persistence | `src/fixed.rs`, `src/journal.rs`, `src/paper.rs` | Exact arithmetic, tamper-evident state, and paper fills |
| Signals | `src/signal.rs`, `src/behavior.rs`, `src/external_feeds.rs`, `src/wallet_watch.rs` | Strategy inputs, behavioral limits, external quotes, and wallet policy |
| Operator console | `src/gui.rs`, `assets/` | Loopback HTTP controls, status rendering, and dashboard assets |
| Protocol fixtures and docs | `tests/`, `fixtures/`, `architecture.md` | Wire-format examples, integration boundaries, and safety rationale |

Most changes are easier to review when they begin in the narrowest responsible
module. Changes to `engine.rs`, live submission, reconciliation, or journal
replay should explain why a smaller boundary is insufficient.

### Safety expectations

Contributors should preserve these project-wide invariants:

- malformed, stale, partial, or ambiguous external state fails closed;
- prices, sizes, balances, and fees use checked fixed-point arithmetic;
- live orders cannot bypass readiness, compliance, reconciliation, or risk
  gates;
- account-changing transitions are durable before dependent behavior unlocks;
- tests use synthetic identifiers and redacted credentials;
- private keys, API credentials, real account snapshots, and runtime journals
  never enter commits, issues, logs, or pull requests.

If a change affects protocol assumptions, configuration, live execution,
accounting, or recovery behavior, update `architecture.md` with the reasoning
and add tests for both the success and rejection paths.

### Good first contributions

Approachable starting points include:

- documentation corrections and clearer configuration examples;
- additional fixtures for malformed, duplicated, stale, or out-of-order
  messages;
- focused unit tests for existing rejection and recovery paths;
- dashboard accessibility and observability improvements that do not expose
  secrets;
- small refactors that clarify a module boundary without changing live
  behavior.

Discuss larger protocol integrations or live-execution changes in an issue
before implementation so reviewers can agree on the safety boundary.

### Pull request workflow

1. Search existing issues and pull requests, then create a focused branch from
   `main`.
2. Add or update tests and document user-visible or protocol-level changes.
3. Run the complete [quality gate](#quality-gate).
4. Open a pull request using the repository template and describe safety,
   compatibility, and validation impact.
5. Keep the branch current with `main`, address review threads, and wait for
   the required `Format, test, lint, and build` check to pass.

The protected `main` branch rejects direct updates, force-pushes, and deletion.
Security-sensitive findings should be reported privately according to
[SECURITY.md](SECURITY.md), not through a public issue.

## License

Licensed under the [MIT License](LICENSE).
