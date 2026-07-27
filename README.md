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

Contributions are welcome. Start with [CONTRIBUTING.md](CONTRIBUTING.md), keep
live credentials out of the repository, and run the quality gate before opening
a pull request. Security-sensitive findings should follow
[SECURITY.md](SECURITY.md).

## License

Licensed under the [MIT License](LICENSE).
