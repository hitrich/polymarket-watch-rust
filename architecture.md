# Single-User Rust Architecture for a Polymarket Bot

Last verified: 2026-07-17

This document turns the pasted architecture into an implementation plan for a
single-user, low-latency Rust bot. It keeps the parts that are technically sound
and removes or reframes the parts that are not verifiable, not realistic, or too
close to prohibited market manipulation.

This is software architecture, not financial or legal advice. Before live
trading, confirm the exact venue, jurisdiction, account type, KYC/KYB status,
API permissions, and market rules that apply to you.

## Verification Verdict

The single-binary Rust architecture is the right approach for a personal bot.
For one operator, do not build SaaS infrastructure: no user accounts, no
dashboard dependency, no API gateway, no Kafka, and no database in the hot path.

The core architecture works as a low-latency design:

```text
WebSocket event
-> in-memory book update
-> signal evaluation
-> risk and compliance gates
-> local order signing
-> CLOB submit
-> reconciliation and append-only logging
```

The following claims from the pasted narrative need correction:

| Claim | Verdict | Architecture response |
| --- | --- | --- |
| Polymarket has public market WebSocket feeds by token IDs | Verified | Use WSS market channel as the primary market-data source. |
| CLOB trading requires authenticated requests and locally signed orders | Verified | Use official SDK or direct REST with EIP-712 order signing and L2 auth headers. |
| One Rust binary can make decisions in a few milliseconds | Realistic | Keep market/signal state memory-only, precompute metadata, and use only the required write-ahead append before live submit. |
| End-to-end order acknowledgement is always 80 ms | Not guaranteed | Measure p50/p95/p99 from the deployment region. Treat 30-100+ ms as a target range, not a contract. |
| 140,000 tweets per minute is ordinary | Not realistic for standard X API | Requires enterprise/firehose/vendor access and measured delivery SLA. |
| Behavioral modeling should push prices to trigger retail sellers | Do not implement | Replace with market-impact guards, anti-manipulation controls, and do-not-trade rules. |
| Wallet copying is a millisecond edge | Usually false | Public wallet activity is delayed and incomplete; use only as a slow, gated signal. |

## Current Polymarket Facts To Build Around

Current public docs say:

- The CLOB is hybrid: off-chain matching with on-chain settlement on Polygon.
- Orders are EIP-712 signed messages.
- Market data, order books, prices, and spreads are public.
- L2 authenticated methods place orders, cancel orders, and query trades.
- Order creation still requires local signing of the order payload.
- The official clients include TypeScript, Python, and Rust.
- CLOB V2 has been live in production at `https://clob.polymarket.com` since
  April 28, 2026. The pre-cutover `https://clob-v2.polymarket.com` host is no
  longer the production integration target, and V1-signed orders are no longer
  accepted in production.
- The public market WebSocket endpoint is
  `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
- The authenticated user WebSocket endpoint is
  `wss://ws-subscriptions-clob.polymarket.com/ws/user`.
- The market WebSocket emits `book`, `price_change`, `tick_size_change`,
  `last_trade_price`, `best_bid_ask`, `new_market`, and `market_resolved`
  events.
- Market and user WebSocket clients must send `PING` every 10 seconds.
- Geographic eligibility must be checked before placing orders. The geoblock
  endpoint is `GET https://polymarket.com/api/geoblock`.
- Polymarket docs list the United States as blocked for order placement on the
  international API. If you are in the United States, confirm whether you are
  using a permitted Polymarket US venue/API before live trading.
- Deployment region and any co-location access are operational assumptions, not
  hard-coded architecture facts. Verify the current CLOB region, permitted
  deployment region, and any KYC/KYB/co-location requirements directly with
  Polymarket before choosing infrastructure.

## Non-Negotiable Boundaries

Do not implement:

- geoblock bypassing;
- VPN/proxy logic to evade restrictions;
- spoofing or false depth;
- wash trading or self-trading across controlled wallets;
- order flooding;
- settlement-price pushing;
- coordinated price manipulation;
- code whose purpose is to move prices to trigger retail behavior;
- strategies based on confidential, stolen, embargoed, or controlled-outcome
  information.

The architecture should enforce these as hard stops, not comments.

## Target System

```text
polymarket-rs
|
+-- startup
|   +-- load config.toml
|   +-- load .env secrets
|   +-- verify venue and geoblock status
|   +-- derive or load API credentials
|   +-- prefetch market metadata
|
+-- hot path
|   +-- market WebSocket reader
|   +-- per-market book state
|   +-- signal engine
|   +-- risk/compliance engine
|   +-- execution router
|   +-- CLOB heartbeat dead-man task
|
+-- cold path
    +-- Gamma/Data discovery refresh
    +-- watched-wallet polling
    +-- external exchange feeds
    +-- fill reconciliation
    +-- append-only journal
    +-- analytics export
```

The hot path must not call a database, Redis, Python, an LLM, a dashboard, or a
separate HTTP service.

## Process Model

Use one Rust binary with Tokio. Split the work into bounded async tasks:

| Task | Responsibility | Hot path |
| --- | --- | --- |
| `market_ws` | Maintain WSS connection and parse market events | Yes |
| `book_actor` | Own order book state for a market or shard | Yes |
| `signal_engine` | Convert book/external state into order intents | Yes |
| `risk_engine` | Apply hard pre-trade checks | Yes |
| `execution_router` | Sign and submit orders, cancel stale orders | Yes |
| `clob_heartbeat` | Send authenticated CLOB heartbeats so open orders auto-cancel if the bot dies | Near-hot |
| `user_ws` | Track own orders, fills, cancels, auth events | Near-hot |
| `reconciler` | Compare local state with CLOB/user endpoints | Cold |
| `discovery` | Refresh Gamma/Data metadata | Cold |
| `wallet_watch` | Poll/stream public wallet activity | Cold |
| `recorder` | Write append-only logs and histograms | Cold |

For a first implementation, one actor can own all books. For lower tail latency,
shard by `asset_id` so one busy market cannot block others.

## Recommended Repository Layout

```text
src/
  main.rs
  config.rs
  compliance.rs
  readiness.rs
  discovery.rs
  market_ws.rs
  user_ws.rs
  book.rs
  signal.rs
  risk.rs
  execution.rs
  wallet_watch.rs
  external_feeds.rs
  reconcile.rs
  journal.rs
  latency.rs
  types.rs
```

## Implementation Status

The repository now contains a persistent Tokio runtime rather than a dashboard
scaffold. Paper mode is operational. Live mode has a real official-SDK adapter,
but remains fail-closed until the running process proves every account,
compliance, feed, heartbeat, journal, and reconciliation gate.

Implemented and covered by local tests as of 2026-07-17:

- a single ordered Polymarket market WebSocket consumer using official SDK
  protocol types, plus the authenticated SDK user stream, with engine-acknowledged
  wire batches, reconnect, heartbeat, full-book handling, multi-asset deltas,
  strict decimal parsing, stale/out-of-order rejection, and post-reconnect
  snapshot gating;
- CLOB V2 authentication, EIP-712 order build/sign/post, typed order responses,
  tracked cancellation, dead-man heartbeat, balance/allowance proof, server
  clock check, Data API positions, and fresh pre-submit account risk snapshots;
- real paper matching with cash and position reservations, partial fills,
  participation caps shared across orders, fees, P&L, TTL cancellation,
  cancel-all, and operator flattening; every paper mutation commits before
  becoming visible, terminal orders are pruned from active state, and bounded
  paper-only journals compact atomically with the prior BLAKE3 root retained;
  flattening consumes only hash-backed visible bid liquidity that passes local
  age and exchange-event-lag limits, stays inside configured participation and
  slippage limits, aggregates participating levels into a venue-minimum-valid
  FAK while retaining actual per-level fill prices, and leaves truthful
  sub-minimum residuals;
- exact six-decimal fixed-point arithmetic with checked operations and
  conservative upward rounding at every exposure boundary;
- BLAKE3-chained, append-before-submit JSONL journaling with `fsync`, atomic
  checkpoints, mode `0600`, single-writer locking, torn/tampered-record
  detection, lifecycle records, UUID client IDs, and mode-separated journals;
- crash recovery that reconciles each nonterminal local attempt against
  paginated remote orders/trades, blocks unknown write windows, rejects unmapped
  remote open orders, restores tracked exchange/client mappings, and performs
  authenticated order-by-ID repair for cancellation-before-journal crash windows;
- authenticated trade-history recovery that attributes only the configured API
  key's maker/taker clip, durably reconstructs a user-stream trade lost after a
  matched order update, rejects conflicting identities, and requires exact CLOB
  trade proof for unchanged-balance zero-net batches;
- revision-stamped authenticated account events, per-trade/per-asset settlement
  reconciliation, double-read stable balance, position, and open-order
  snapshots, post-submit journal-failure latching, and account-wide emergency
  cancellation for every ambiguous live submission boundary;
- persistent compliance latches with declared controlled wallets, funder and
  signer ownership checks, confidential-signal provenance, restart persistence,
  and hard stops in both paper and live submission paths;
- recurring official geoblock checks, production endpoint pinning in live mode,
  strict market discovery, bounded channels and REST deadlines, rate limits,
  automatic live GTC TTL cancellation, affected-order cancellation plus forced
  resubscription on unresolved book invalidation, same-frame authoritative-book
  repair without stale reconnect requests, and a persistent non-decreasing
  observed equity high-water advanced by every stable authoritative account
  snapshot and never reset downward at UTC rollover;
- a real Coinbase Advanced Trade ticker cache and a public Polymarket wallet
  observer with no-replay warm-up, bounded paginated continuity checks,
  maker/taker coverage, deduplication, confidence, slippage, size, age, and
  confidential-information gates;
- a loopback-only control plane whose status, books, balances, orders, fills,
  latency, journal, and command results all come from the running engine, with
  exact Host validation, token-protected status/commands, and graceful shutdown
  acknowledged before the process stops.

Deliberate boundaries and deployment validation still required:

- the social-media claim in the source narrative is not implemented. A lawful
  licensed firehose/vendor contract and measured delivery SLA are prerequisites;
  the software never fabricates a 140,000-post/minute capability;
- negative-risk markets remain rejected until event-level conversion,
  redemption, fee, and cross-outcome exposure accounting exists;
- market hashes are required and top-of-book continuity is checked, but a local
  recomputation is not claimed where the venue does not publish a stable hash
  canonicalization contract;
- no automated test can prove a particular funded wallet, signer authorization,
  geographic eligibility, venue account, or production latency. Those proofs are
  obtained at live startup, and a tiny real-money canary requires an operator's
  explicit configuration and remains outside CI;
- remote reconciliation and account snapshots have whole-operation deadlines;
  accounts whose history cannot be paginated within that budget remain locked
  instead of blocking the runtime actor indefinitely;
- this is execution infrastructure and a conservative reference strategy, not
  evidence of profitability.

## Config

Use explicit configuration. Do not discover tradeable markets in the order path.

```toml
mode = "paper" # paper | live
venue = "polymarket_international" # or a verified permitted venue

# Production uses V2/pUSD. New API users use deposit wallets/POLY_1271;
# existing Safe/proxy/EOA accounts use their matching V2 signature type.
clob_protocol = "v2"
clob_host = "https://clob.polymarket.com"
chain_id = 137
collateral_asset = "pUSD"
wallet_mode = "deposit_wallet" # deposit_wallet | gnosis_safe | proxy | eoa
signature_type = "poly1271" # poly1271 | gnosis_safe | proxy | eoa
funder_address = "0x1111111111111111111111111111111111111111"
geoblock_url = "https://polymarket.com/api/geoblock"
max_geoblock_age_ms = 60000 # reject stale/future responses; recheck at least this often

max_book_age_ms = 250
max_event_lag_ms = 500
max_order_usdc = "10"
max_market_exposure_usdc = "100"
max_daily_loss_usdc = "50"
max_slippage_ticks = 2
max_market_impact_bps = 25
min_top_book_size = "5"
order_ttl_ms = 750

max_new_orders_per_second = 3
max_cancels_per_second = 5
max_position_notional_usdc = "250"

asset_ids = [
  "1234567890123456789012345678901234567890"
]

condition_ids = [
  "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
]

watched_wallets = []
controlled_wallets = ["0x1111111111111111111111111111111111111111"]
external_symbols = ["BTC-USD"]
journal_path = "logs/paper-decisions.jsonl" # use a separate path for live
compliance_latch_path = "logs/compliance-latch.json"
```

Secrets belong in `.env` or the OS keychain, not in `config.toml`.

Runtime secrets for an already deployed, funded, approved trading wallet:

```text
PRIVATE_KEY=...
POLYMARKET_API_KEY=...
POLYMARKET_API_SECRET=...
POLYMARKET_API_PASSPHRASE=...
```

`DEPOSIT_WALLET_ADDRESS` is additionally required only when
`wallet_mode = "deposit_wallet"`. Safe, proxy, and EOA modes use the verified
`funder_address` from configuration and must not receive a false deposit-wallet
secret lock. `clob_host` and `chain_id` are non-secret configuration values.

Provisioning-only secrets for deposit-wallet deployment, relayer/builder flows,
or batch operations should be kept out of the hot runtime environment:

```text
RELAYER_URL=...
BUILDER_API_KEY=...
BUILDER_SECRET=...
BUILDER_PASS_PHRASE=...
```

The exact names depend on the SDK version you pin. A live trading binary should
not need relayer/builder credentials unless it is intentionally provisioning or
modifying wallet infrastructure outside the order path.

## Protocol And Wallet Compatibility

Do not let the live bot infer protocol details from code defaults. The startup
sequence must prove that the configured host, order protocol, collateral asset,
wallet mode, and signature type match each other.

Current public sources expose one production CLOB protocol:

| Protocol | Typical host | Collateral | EIP-712 order domain | Wallet/signature path |
| --- | --- | --- | --- | --- |
| V2 | `https://clob.polymarket.com` | pUSD | version `"2"` | deposit wallet with `signatureType = 3` / `POLY_1271`; existing EOA/proxy/Safe users retain matching V2 signature types |

V1 and the `https://clob-v2.polymarket.com` pre-cutover host are historical
only. A production startup must reject both rather than attempt fallback.

Startup compatibility checks:

```text
read config
-> query CLOB server/version metadata where available
-> fetch target market metadata from the configured host
-> verify tick_size/min_order_size/neg_risk are present
-> verify collateral asset matches protocol
-> verify wallet_mode matches signature_type
-> verify funder address is the address that actually holds funds
-> verify signer is authorized for that funder
-> verify balance and allowance are current
-> reject live mode if any field is unknown or inconsistent
```

For V2 deposit-wallet trading:

```text
signer = owner EOA or approved session signer
funder = deployed deposit wallet address
order maker = deposit wallet address
order signer field = deposit wallet address
signatureType = 3 / POLY_1271 / SignatureType::Poly1271
collateral = pUSD held by the deposit wallet
```

For existing Safe/proxy/EOA trading, keep each V2 wallet/signature path separate
in the adapter. Do not mix wallet-specific signing assumptions inside strategy
or risk code.

## Rust Stack

The checked-in implementation pins the official SDK and uses this core stack:

```toml
[dependencies]
thiserror = "2"
dotenvy = "0.15"

tokio = { version = "1", features = ["full"] }
tokio-tungstenite = { version = "0.29", features = ["rustls-tls-webpki-roots"] }
futures-util = "0.3"
reqwest = { version = "0.13", default-features = false, features = ["json", "rustls"] }

serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.9"
blake3 = "1.8"
fs2 = "0.4"

tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt", "json"] }
polymarket_client_sdk_v2 = { version = "=0.6.0", default-features = false,
  features = ["clob", "ws", "data", "tracing"] }
```

Prefer the official Rust SDK for authentication and order signing if it compiles
cleanly for the pinned version. If not, keep the SDK behind a small adapter and
use direct REST/WebSocket code only where the SDK surface is unstable.

If the pinned SDK does not expose heartbeat support, call `POST /heartbeats`
directly through the same warmed `reqwest` client used for authenticated CLOB
REST. If negative-risk support is ever enabled, add the SDK `ctf` feature or an
equivalent direct contract/API adapter and test it separately from the standard
binary-market path.

## Data Model

Use exact decimal arithmetic. Never use `f64` for price, size, PnL, exposure,
fees, or tick rounding.

Core in-memory types:

```text
MarketMeta
  condition_id
  asset_id_yes
  asset_id_no
  tick_size
  min_order_size
  neg_risk
  active
  accepting_orders
  market_type
  taker_delay_enabled
  fees_enabled

BookState
  asset_id
  bids
  asks
  best_bid
  best_ask
  last_trade_price
  tick_size
  min_order_size
  book_hash
  exchange_timestamp_ms
  local_received_at

OrderIntent
  asset_id
  side
  limit_price
  size
  time_in_force
  post_only
  local_expires_at_ms
  wire_expiration_s
  reason
  strategy_id
  feature_snapshot_id

OrderState
  client_order_id
  exchange_order_id
  intent
  status
  submitted_at
  acknowledged_at
  matched_at
  settled_at
  cancel_requested_at
  terminal_reason
```

## Market Data Flow

Use WebSocket as primary:

```text
connect to WSS market channel
-> send PING every 10 seconds
-> subscribe by explicit asset_ids
-> receive initial book messages
-> apply every entry in price_changes[]
-> update best_bid/best_ask
-> process tick_size_change immediately
-> emit market_resolved as a hard stop
-> update latency histograms
```

Use authoritative WebSocket books for book repair; REST stays outside the
ordered book-transition path:

```text
startup -> subscribe and wait for authoritative WebSocket book per asset
hash/continuity failure -> mark the asset untradeable and cancel affected orders
later valid book in the same acknowledged wire frame -> clear the pending repair
unresolved failure -> disconnect and resubscribe the single market stream
reconnect -> keep every book untradeable until a fresh authoritative book arrives
```

Protocol details to encode in tests:

```text
market channel subscribe payload
  type: "market"
  assets_ids: [asset_id, ...]
  custom_feature_enabled: true # required for custom lifecycle/top-book events

book event
  asset_id
  market
  timestamp
  hash
  bids[]
  asks[]

price_change event
  market
  timestamp
  price_changes[]
    asset_id
    price
    size
    side
    best_bid
    best_ask
    hash

tick_size_change event
  asset_id
  old_tick_size
  new_tick_size

terminal/lifecycle events
  best_bid_ask
  new_market
  market_resolved
```

Book reconstruction algorithm:

```text
on connect:
  subscribe WebSocket
  start 10 second PING loop
  parse each wire frame into one ordered event batch
  wait for engine acknowledgement before consuming the next frame
  reject price_changes until the first authoritative book event per asset
  apply book event as full replacement and mark asset fresh
  apply every entry in each price_changes[] array
  remove a level when its size == 0
  sort bids descending and asks ascending
  round/validate prices against active tick size
  require the venue-provided hash and validate reported top-of-book continuity
  if hash mismatch or sequence gap is suspected:
    mark asset not_tradeable
    cancel affected orders
    retain a repair marker for the remainder of the wire batch

batch repair:
  if a later authoritative book in the same frame validates, clear the marker
  acknowledge the batch with no reconnect request
  otherwise acknowledge the unresolved asset and terminate the session
  reconnect the single ordered stream and resubscribe all configured assets
  do not assume timestamps are gap-free sequence numbers
  require a fresh authoritative WebSocket book/hash before trading

on reconnect:
  cancel or let expire all strategy-owned stale intents
  mark all affected books not_tradeable
  restart PING loop after reconnect
  resubscribe
  trade only after fresh book + metadata are consistent
```

Deterministic replay fixtures:

```text
fixtures/ws/book_initial.json
fixtures/ws/price_changes_multi_asset.json
fixtures/ws/user_subscription.json
in-memory duplex WebSocket session/repair harness in src/market_ws.rs
```

Edge cases:

- WebSocket disconnects and reconnects.
- Duplicate messages.
- Out-of-order messages.
- Missing deltas.
- `size = 0` removes a price level.
- Tick size changes near extreme prices.
- Market is paused, closed, resolved, or not accepting orders.
- YES/NO complement price can diverge because of fees, spread, and inventory.
- Negative-risk markets have different portfolio semantics.
- Local clock drift distorts event-lag metrics.

## User WebSocket Flow

Use the authenticated user channel to track your own order and trade state. It
is not a strategy input by itself; it feeds reconciliation and exposure.

Endpoint:

```text
wss://ws-subscriptions-clob.polymarket.com/ws/user
```

Subscription payload:

```text
type: "user"
markets: [condition_id, ...] # condition IDs, not asset IDs
auth:
  apiKey
  secret
  passphrase
```

Runtime behavior:

```text
connect to user WebSocket
-> send PING every 10 seconds
-> subscribe by condition_ids
-> receive order events
-> receive trade events
-> update own order/fill cache
-> wake reconciler for affected market/account
```

Important distinctions:

- Market channel subscriptions use `asset_id` / token IDs.
- User channel subscriptions use `condition_id` market IDs.
- User channel auth uses CLOB API credentials, not the private key directly.
- Private keys are used for signing orders locally, not for every WebSocket
  frame.
- Missing user channel events must not release risk; the reconciler should query
  CLOB order/trade endpoints before assuming final state.
- Current user-channel examples may omit `trader_side`; maker attribution then
  comes from the authenticated API key on `maker_orders`, while an attributable
  taker uses its own taker order ID. Ambiguous or conflicting ownership stops
  reconciliation rather than mutating inventory heuristically.
- User order examples may encode placement/update/cancellation in `type` while
  omitting `status`; the typed converter derives the lifecycle state from that
  field, verifies API-key ownership, and normalizes documented Unix seconds to
  the runtime's millisecond clock.

Reconnect policy:

```text
on disconnect:
  mark own-order cache uncertain
  stop new live orders for affected markets
  reconnect and restart PING loop
  resubscribe by condition_ids
  refetch open orders/trades
  resume live orders only after cache and remote state agree
```

## Live Readiness Gate

Live mode should be impossible until readiness has passed. This is a cold-path
startup module, but it is required before the execution router accepts live
orders.

```text
readiness.rs
  verify_geography()
  verify_protocol_compatibility()
  verify_wallet_path() # deployment/ownership for deposit, Safe, proxy, or EOA
  verify_signer_authorized()
  verify_funder_address()
  verify_balance_and_allowance()
  verify_api_credentials()
  verify_market_order_parameters()
  verify_clock_sync()
  run_tiny_dry_run_or_read_only_probe()
```

Required V2 deposit-wallet checks:

- deposit wallet exists and is deployed;
- deposit wallet holds the configured pUSD collateral;
- signer is the configured owner or approved delegated signer;
- funder address equals the deposit wallet, not the owner EOA;
- API key, secret, and passphrase authenticate against the configured CLOB host;
- order builder uses `signatureType = 3` / `POLY_1271`;
- approvals and allowance state are visible to the CLOB;
- market metadata is fetched from the same host used for order submission;
- order domain version and chain ID match the configured protocol.

Live unlock rule:

```text
if readiness_status != Ready:
  mode = paper_or_read_only
  execution_router.reject_all_live_orders("readiness_not_ready")
```

Readiness should run on startup and again after reconnects, account changes,
allowance changes, or CLOB host/protocol changes.

## Signal Engine

Signals should be pure functions over memory:

```text
BookState + MarketMeta + OwnPosition + ExternalFeatures
-> optional OrderIntent
```

Rules:

- Signals do not place orders.
- Signals do not cancel orders directly.
- Signals do not perform network calls.
- Signals must include a reason and feature snapshot ID.
- Every signal must be reproducible from journaled input state.

Good signal inputs:

- current best bid/ask and spread;
- local book imbalance;
- external exchange price movement from a licensed feed;
- market-specific metadata;
- your own inventory and resting orders;
- public wallet fills with attribution confidence and slippage caps.

Bad signal inputs:

- confidential tips;
- private order information;
- data obtained by bypassing access controls;
- intent to trigger panic selling or forced behavior.

## External Exchange Bridge

The phrase "exchange bridge" should mean a market-data connector, not a funds
bridge. For latency, do not bridge assets or wait for on-chain transfers in the
trading path.

Use external exchange feeds like this:

```text
Coinbase/Binance/Kraken WebSocket
-> normalize symbol event
-> update in-memory feature cache
-> signal engine reads cache
```

Do not call external REST APIs in the hot path. Do not assume a BTC price move
automatically maps to a Polymarket edge; model the target market, expiry,
resolution rules, spread, fees, and current liquidity.

## Social/Sentiment Layer

Treat the "140,000 tweets per minute" claim as unverified unless you have an
enterprise/firehose/vendor contract. Standard X filtered stream documentation
lists 250 posts/sec for filtered stream, which is about 15,000 posts/minute per
documented connection context.

If you add sentiment:

```text
licensed social/news feed
-> parser/classifier outside the order path
-> aggregate event features
-> publish compact feature snapshots to memory
-> signal engine reads latest snapshot
```

Do not put an LLM in the hot path. If text classification is needed, use a small
local model or precomputed keyword/event classifiers with bounded runtime.

## Wallet-Copy Module

Wallet copying is a slow signal, not a command to mirror blindly.

```text
observed public wallet fill
-> verify source is public and lawful
-> score wallet and attribution confidence
-> compare current book to observed entry
-> reject if price moved too far
-> cap order size and market exposure
-> send OrderIntent through normal risk engine
```

Hard rejects:

- wallet is suspected to trade on confidential or controlled-outcome data;
- attribution confidence is low;
- current price is outside copy slippage;
- liquidity is insufficient;
- market is near resolution or has unclear resolution criteria;
- copying would breach exposure or daily loss limits.

## Risk And Compliance Engine

This is the most important module. It must be strict and local.

Pre-trade checks:

```text
mode is live
-> venue and geoblock allowed
-> market is active and accepting orders
-> market is not resolved or paused
-> book age <= max_book_age_ms
-> event lag <= max_event_lag_ms
-> price is aligned to tick_size
-> size >= min_order_size
-> notional <= max_order_usdc
-> market exposure <= max_market_exposure_usdc
-> total exposure <= max_position_notional_usdc
-> daily realized/unrealized loss <= max_daily_loss_usdc
-> projected slippage <= max_slippage_ticks
-> projected market impact <= max_market_impact_bps
-> negative-risk market policy passes
-> no self-trade against own resting orders
-> no wash-trade pattern across controlled wallets
-> no order-rate limit breach
-> no manipulation or prohibited-conduct flag
```

Fail closed. Missing data rejects the order.

### Negative-Risk Markets

Negative-risk markets are a separate accounting branch, not a normal binary
market edge case. Default policy should be:

```text
if market.negRisk == true and neg_risk_enabled != true:
  reject_order("negative_risk_disabled")
```

Only enable them after implementing all of the following:

- event-level portfolio exposure, not just per-token exposure;
- conversion and redemption semantics for the market group;
- fee, collateral, and payout accounting across linked outcomes;
- order builder support for the required `negRisk: true` option where the API
  requires it;
- reconciliation that can explain portfolio value before and after conversion;
- tests for augmented negative-risk states and cross-outcome exposure limits.

Until those are implemented, trade only ordinary non-negative-risk markets.

## Execution Router

Execution owns signing, submission, and cancellation.

```text
OrderIntent
-> risk accepted
-> assign client_order_id from the durable journal sequence
-> durably append decision/order-attempt write-ahead record
-> build order with current tick/min-size metadata
-> sign order locally
-> read the trusted local clock again and rerun the complete risk/compliance gate
-> submit to CLOB
-> record ack/reject
-> track live/matched/settled/cancelled state
```

Important order-lifecycle details:

- Polymarket orders are limit orders; market orders are limit orders priced to
  execute immediately.
- V2 order types are `GTC`, `GTD`, `FOK`, and `FAK`. Do not invent an `IOC`
  wire type; use `FAK` when immediate partial execution is intended.
- Post-only is valid only for `GTC` and `GTD` orders.
- Local decision freshness and wire expiration are different clocks. Every
  intent has a millisecond `local_expires_at_ms` bounded by `order_ttl_ms`.
  `GTC`, `FOK`, and `FAK` have no wire expiration. `GTD` additionally carries
  a UTC-seconds `wire_expiration_s` that is at least three minutes in the
  future, as required by the V2 create-order API. The venue applies a one-minute
  security threshold before that timestamp, so the minimum effective lifetime
  is about two minutes. This expiration is a wire/API field, not a field in the
  signed EIP-712 order struct.
- Post-only orders should be used when the strategy must never take.
- Some marketable orders can have a taker delay. The docs describe a 250 ms
  delay on selected crypto/finance up/down markets and configured delays on
  sports markets.
- During delay, the order can be pending and not cancelable.
- `matched` is not the same as fully settled finality.
- Reconcile off-chain order state and on-chain settlement state separately.

Write-ahead durability rule:

```text
before live submit:
  read trusted local clock and run the complete risk/compliance gate
  append decision/order-attempt record to durable journal
  fsync according to configured live durability policy
  include client_order_id, the complete intent, full checked book state, all
  gate inputs, source event references, risk result, and binary/config hashes
  read trusted local clock again after fsync and rerun the complete gate
  submit only after the write succeeds

after submit:
  append ack/reject/fill/cancel updates
  async analytics export may lag, but write-ahead records may not
```

If the write-ahead append fails, reject the live order and degrade to
paper/read-only mode.

## CLOB Heartbeat Dead-Man

Run an authenticated heartbeat task whenever live orders can rest on the book.
The heartbeat is separate from WebSocket `PING`: WebSocket `PING` keeps the feed
connection alive, while `POST /heartbeats` is the CLOB open-order auto-cancel
safety mechanism when the bot stops sending heartbeats.

Heartbeat policy:

```text
live mode enabled
-> authenticate heartbeat client
-> send POST /heartbeats on the configured interval
-> record heartbeat ack latency and failures
-> if heartbeat fails repeatedly:
     stop new live orders
     cancel stale/risky open orders where possible
     degrade to paper/read-only until recovered
-> on graceful shutdown:
     cancel strategy-owned open orders unless config explicitly keeps them
```

The bot should never rely on heartbeat auto-cancel as normal order management.
It is a dead-man backstop for crashes, network loss, or process death.

## Matching-Engine Restart Handling

The execution router must treat matching-engine restarts as a distinct exchange
state, not as ordinary transient HTTP failure.

Restart policy:

```text
HTTP 425 restart response:
  mark matching_engine_state = restarting
  stop non-post-only order submission
  use exponential backoff with jitter
  do not blindly retry marketable/non-post-only orders
  keep reconciliation running
  respect any Retry-After header when present

post-restart recovery:
  enter post_only_recovery for at least 2 minutes
  allow only post-only orders that pass normal risk checks
  refresh open orders, trades, balances, allowances, and market metadata
  compare remote state with local journal before normal mode resumes

HTTP 503 restricted/cancel-only/post-only response:
  parse response mode when available
  if cancel-only: cancel stale/risky orders, reject new orders
  if post-only: allow only post-only orders with strict TTL
  parse every POST /orders batch response entry
  treat success=true with empty orderID or non-empty errorMsg as rejected/unknown
  if Retry-After is present: schedule retry after that delay plus jitter
```

Retry rules:

- Never retry a non-post-only order automatically after an unknown exchange
  state transition.
- A retry must create a new decision record linked to the original failed
  attempt.
- If the original intent has expired, reject it instead of retrying.
- During restart or restricted mode, live strategy should prefer read-only,
  cancel-only, or post-only behavior depending on the exchange response.
- Batch order responses must be evaluated per order. A top-level or per-entry
  `success: true` is not enough when `orderID` is empty or `errorMsg` is set.

## Reconciliation

Do not trust one feed. Reconcile:

```text
user WebSocket/order events
-> CLOB get order/trades endpoints
-> local journal
-> position/accounting snapshot
-> on-chain settlement state where relevant
```

Reconciliation should catch:

- order acknowledged but never appears in local state;
- local cancel request not reflected remotely;
- partial fills;
- delayed match;
- rejected order after taker delay;
- stale open order after strategy moved on;
- fill that changes exposure but not local inventory;
- settlement failure or late finality;
- a matched order event whose corresponding user trade event was lost;
- a trade for a durable runtime-owned order whose order and trade WebSocket
  events were both lost; reconnect and periodic authenticated REST audits diff
  all owned order IDs against the durable trade ledger before live unlock. A
  cold start without a provable pre-trade baseline reconstructs the trade and
  stays locked rather than blessing the current balance as reconciled;
- a fee-free buy/sell batch whose net position and cash happen to be unchanged;
- an unrelated debit hidden inside a one-sided cash comparison; each pending
  trade must have exact authoritative identity and its cash delta must remain
  inside the fee envelope implied by the V2 trade terms and venue precision.
- any extra, missing, or changed position outside the pending fills; compare
  the complete normalized asset-to-size map, not only touched assets. A
  zero-fill cancellation changes order lifecycle state but not holdings.

## Local Crash Recovery

On every startup after a local process crash, restart, or unknown shutdown, the
bot must reconstruct state before live trading unlocks.

Recovery sequence:

```text
start in read-only mode
-> replay durable journal from the last checkpoint
-> reconstruct in-flight order attempts by client_order_id
-> reconstruct local positions, exposure, PnL, and daily loss counters
-> query CLOB open orders, recent trades, balances, and allowances
-> query user/order/fill state through REST and then user WebSocket
-> compare remote state against journal state
-> cancel or mark uncertain any orphaned strategy-owned orders
-> rebuild book state from fresh WebSocket books and metadata
-> run readiness, risk, heartbeat, and latency gates
-> unlock live mode only after reconciliation has no unresolved gaps
```

Crash windows to test:

- crash after write-ahead append but before HTTP submit;
- crash after HTTP submit but before HTTP response;
- crash after ack but before ack is journaled;
- crash after fill/trade but before fill is journaled;
- crash during cancel request;
- crash while heartbeat is failing;
- crash during matching-engine restart or post-only recovery.

## Latency Budget

Separate in-process latency from network and exchange latency.

Target for the Rust process:

```text
WebSocket frame parse             <= 0.5 ms p95
Book update                       <= 0.5 ms p95
Signal evaluation                 <= 1.0 ms p95
Risk checks                       <= 1.0 ms p95
Order build/sign                  <= 2-10 ms p95
Decision latency total            <= 5 ms p95 excluding signing
Decision to HTTP write            <= 20 ms p95 including signing
```

External latency:

```text
WebSocket delivery lag            measure only, not controllable
HTTP submit to CLOB               region/network dependent
Order ack                         target 30-100+ ms, measure p50/p95/p99
Fill/match                        depends on book and taker delay
Settlement/finality               not a millisecond hot-path assumption
```

To reduce latency:

- deploy as close as legally and operationally allowed to the CLOB servers;
- use WebSocket feeds instead of REST polling;
- keep market metadata preloaded;
- avoid locks in the hot path where actor ownership is practical;
- use bounded channels and drop stale signals;
- use a warmed HTTP client and connection reuse;
- sign locally with prevalidated inputs;
- write live order-attempt records durably before submit;
- export non-critical analytics asynchronously through a bounded channel;
- measure with monotonic clocks and histograms.

## Benchmark And Canary Gates

Latency targets are not architecture until they are measured. Add benchmark
gates before live mode is enabled.

Local microbenchmarks:

```text
parse market WebSocket frame       p95 <= 0.5 ms, p99 <= 1.0 ms
apply book delta                   p95 <= 0.5 ms, p99 <= 1.0 ms
signal evaluation                  p95 <= 1.0 ms, p99 <= 2.0 ms
risk/compliance checks             p95 <= 1.0 ms, p99 <= 2.0 ms
order build/sign                   p95 <= 10 ms,  p99 <= 20 ms
journal append fsync policy        p95 measured and bounded
```

Live canary gates:

```text
deployment region                  recorded in logs
instance type / CPU model           recorded in logs
CLOB host/protocol                  recorded in logs
HTTP/TLS connection                 warmed before first live order
WebSocket event lag                 p95 alert threshold configured
order submit latency                p95/p99 alert thresholds configured
order ack latency                   p50/p95/p99 recorded per host
cancel ack latency                  p50/p95/p99 recorded per host
```

Live trading should degrade to paper/read-only when:

- p95 event lag exceeds `max_event_lag_ms` for the configured window;
- submit or ack latency exceeds the strategy's stale-decision window;
- TLS/HTTP connection warming fails;
- local queue depth indicates sustained backpressure;
- clock sync is outside the configured tolerance;
- benchmark results are absent for the current binary hash.

## Backpressure And Staleness

Backpressure policy:

```text
market data backlog -> stop trading affected asset and resync
signal queue full -> drop newest low-priority signal or reject all stale signals
execution queue full -> reject new intents
journal queue full -> degrade live mode to paper/read-only
durable journal append failure -> reject all live orders
REST throttling -> stop metadata/account-dependent trading
```

Every strategy-generated `OrderIntent` gets a local millisecond deadline from
checked `now_ms + order_ttl_ms` arithmetic. The risk boundary never accepts a
deadline beyond its own current `now_ms + order_ttl_ms` window. The execution
router reads its trusted clock again after the durable write-ahead append and
reruns the complete gate immediately before calling the adapter. If the intent
or any input has become stale during the append, reject it without attempting
submission. A `GTD` wire expiration is an additional exchange-side UTC-seconds
deadline; it does not replace this local freshness guard.

## Storage

Market data, signal state, and risk caches are memory-only in the hot path.
Live execution is the exception: it must block on a durable write-ahead
decision/order-attempt append before submitting an order to the CLOB.

Persist through append-only local logs:

```text
logs/
  decisions.jsonl
  orders.jsonl
  fills.jsonl
  risk_rejects.jsonl
  latency.jsonl
  errors.jsonl
```

Every durable journal record must include:

```text
schema_version
monotonic_sequence
process_startup_id
wall_clock_timestamp
monotonic_timestamp
config_hash
binary_hash
previous_record_hash
record_hash
source_event_refs
decision_id
client_order_id
strategy_id
risk_result
payload
```

Journal integrity rules:

- Hash-chain every record with `previous_record_hash` and `record_hash`.
- Write checkpoints that include the latest sequence and hash.
- On rotation, write a terminal rotation record in the old file and an opening
  continuation record in the new file.
- On startup, verify the hash chain and checkpoints before live mode unlocks.
- If verification fails, start read-only and require manual repair.

Optional analytics can import those logs into SQLite, DuckDB, ClickHouse, or
Parquet after the fact. Analytics storage must not be required to trade.

## Observability

Record these histograms:

- WebSocket event lag;
- book update time;
- signal evaluation time;
- risk check time;
- signing time;
- submit request time;
- order ack latency;
- fill latency;
- cancel latency;
- reconnect count;
- resync count;
- risk rejects by reason;
- stale signal rejects.

Every accepted or rejected order should have a traceable decision record:

```text
timestamp
strategy_id
asset_id
book_hash
features_snapshot_id
order_intent
risk_result
submitted_order_id
final_status
latency_breakdown
```

## Build Order

Implement in this order:

1. Config loader and typed settings.
2. Geoblock/venue eligibility check.
3. Protocol and wallet compatibility checks.
4. Deposit-wallet readiness gate.
5. Market metadata discovery outside the hot path.
6. Market WebSocket client and parser.
7. In-memory book state with acknowledged batch repair and resubscription.
8. Paper-trading signal engine.
9. Risk/compliance engine.
10. Durable append-only decision and risk logs.
11. Authenticated CLOB client adapter.
12. Authenticated CLOB heartbeat dead-man task.
13. User/order/fill reconciliation.
14. Local crash recovery from journal replay and remote reconciliation.
15. Order cancellation and TTL handling.
16. Latency histograms, microbenchmarks, and replay tests.
17. Manual opt-in tiny live-order smoke test with strict caps.
18. External exchange feed cache.
19. Wallet-watch slow signal.

Do not start with live strategy complexity. Start with paper mode, prove book
correctness, prove risk rejects, then submit tiny live orders only after the
readiness gate, journal, reconciler, and latency gates are working. The live
smoke test must be manual and opt-in, never part of default CI.

## Test Plan

Unit tests:

- protocol/host/collateral/signature compatibility;
- deposit-wallet signer/funder validation;
- negative-risk markets reject by default;
- negative-risk enabled path requires event-level portfolio accounting;
- decimal tick rounding;
- minimum order size;
- stale book rejection;
- market-impact rejection;
- self-trade rejection;
- exposure and daily-loss limits;
- order TTL expiration;
- tick-size-change handling;
- `price_changes[]` array handling;
- `size = 0` level deletion;
- 10 second WebSocket PING scheduler;
- duplicate and out-of-order WebSocket messages.

Integration tests:

- readiness gate blocks live mode until wallet, balance, allowance, and API auth pass;
- durable write-ahead decision/order-attempt append happens before live submit;
- local crash recovery replays journal and reconciles open orders, trades,
  balances, allowances, positions, and risk counters before live unlock;
- CLOB heartbeat task sends authenticated heartbeats and degrades live mode on
  repeated failures;
- snapshot plus delta book reconstruction;
- ordered wire-batch acknowledgement, same-frame repair cancellation, and
  unresolved-repair reconnect without a stale request;
- market channel custom events with `custom_feature_enabled: true`;
- user channel subscribes with condition IDs and auth payload;
- matching-engine restart handling for HTTP 425, restricted 503, Retry-After,
  exponential backoff, and 2-minute post-only recovery;
- post-only `POST /orders` batch responses parse each entry and reject empty
  `orderID` or non-empty `errorMsg` even when `success` is true;
- crash-window tests around pre-submit, submit, ack, fill, cancel, heartbeat
  failure, and matching-engine restart;
- reconnect and resync;
- paper order lifecycle;
- live client adapter against sandbox/read-only mode by default;
- manual opt-in tiny capped live order only when geoblock, venue, account,
  wallet readiness, journal durability, latency gates, and strict caps all pass;
- cancel stale order;
- partial fill reconciliation;
- lost user-trade reconstruction from authenticated paginated CLOB history;
- zero-net reconciliation with and without exact authoritative trade proof;
- heartbeat/control-frame progress while an ordered market batch awaits engine
  acknowledgement;
- market-resolution cancellation and venue-minimum-valid paper flattening;
- geoblock blocked response;
- rate-limit and throttling behavior.

Replay tests:

- record raw WSS messages;
- replay through the parser and book actor;
- compare final book hash and best bid/ask;
- verify deterministic signals;
- verify no expired signal is submitted.

Benchmark tests:

- parse/book/signal/risk/sign microbenchmarks meet configured p95/p99 budgets;
- warmed HTTP/TLS connection path is measured before live order submission;
- live canary metrics are written with binary hash, region, host, and protocol.

## Final Recommended Architecture

Build one local Rust binary:

```text
config.toml + .env
-> startup compliance and market metadata
-> market WebSocket
-> actor-owned in-memory books
-> memory-only signal engine
-> strict risk/compliance engine
-> durable write-ahead order-attempt journal
-> local signer and CLOB execution
-> CLOB heartbeat dead-man task
-> user/fill reconciliation
-> local crash recovery before live unlock
-> append-only logs, source matrix, and latency histograms
```

This design gives the best chance of millisecond decision latency because the
market-data and signal path is memory-only, while live order submission keeps
only the required durable write-ahead step before touching the CLOB. The
architecture can work for a lawful personal bot. The manipulative parts of the
story should not be implemented.

## Source Matrix

Retrieved on 2026-07-17.

| Claim | Source | Status |
| --- | --- | --- |
| Polymarket exposes CLOB trading, order books, prices, spreads, and signed order placement | https://docs.polymarket.com/trading/overview | Documented |
| Market and user WebSocket clients need periodic PING heartbeats | https://docs.polymarket.com/market-data/websocket/overview | Documented |
| Public market WebSocket endpoint and market event types exist | https://docs.polymarket.com/market-data/websocket/market-channel | Documented |
| Market WebSocket subscriptions are by asset/token IDs | https://docs.polymarket.com/market-data/websocket/market-channel | Documented |
| `price_change` events contain a `price_changes` array that must be applied entry by entry | https://docs.polymarket.com/market-data/websocket/market-channel | Documented |
| Custom market events require `custom_feature_enabled: true` | https://docs.polymarket.com/market-data/websocket/overview | Documented |
| Authenticated user WebSocket subscribes by condition IDs and emits user order/trade events | https://docs.polymarket.com/market-data/websocket/user-channel | Documented |
| Authenticated REST trade history identifies whether this API key was maker or taker and exposes maker-order ownership/terms | https://docs.polymarket.com/trading/orders/overview | Documented |
| Orderbook repair should use snapshots and hash validation | https://docs.polymarket.com/trading/orderbook | Documented plus implementation inference |
| Matching-engine restart handling includes HTTP 425, post-only recovery, restricted 503 behavior, backoff, and Retry-After | https://docs.polymarket.com/trading/matching-engine | Documented |
| Authenticated CLOB heartbeat endpoint supports open-order auto-cancel dead-man behavior | https://docs.polymarket.com/api-reference/trade/send-heartbeat | Documented |
| Orders are limit-order based; market orders are immediately executable limit orders | https://docs.polymarket.com/concepts/order-lifecycle | Documented |
| V2 order types are GTC, GTD, FOK, and FAK; GTD expiration must be at least three minutes in the future and is subject to a one-minute security threshold | https://docs.polymarket.com/trading/orders/create and https://docs.polymarket.com/api-reference/trade/post-a-new-order | Documented |
| Selected marketable orders may have taker delay windows | https://docs.polymarket.com/concepts/order-lifecycle | Documented |
| Negative-risk markets require separate exposure/accounting treatment | https://docs.polymarket.com/advanced/neg-risk | Documented |
| CLOB request throttling/rate limits are enforced by Cloudflare | https://docs.polymarket.com/api-reference/rate-limits | Documented |
| Geographic eligibility should be checked with the geoblock endpoint | https://docs.polymarket.com/api-reference/geoblock | Documented |
| Official clients include Rust | https://docs.polymarket.com/api-reference/clients-sdks | Documented |
| CLOB V2 is the only production protocol and runs at `https://clob.polymarket.com` | https://docs.polymarket.com/changelog and https://docs.polymarket.com/v2-migration | Documented |
| Deposit-wallet trading requires wallet/funder/signer/signature-type readiness | https://docs.polymarket.com/trading/deposit-wallets and https://github.com/Polymarket/rs-clob-client-v2 | Documented plus implementation inference |
| Standard X filtered stream capacity is far below 140,000 posts/minute | https://docs.x.com/x-api/fundamentals/rate-limits | Documented |
| One-binary Rust hot path can make local decisions in milliseconds | Rust/Tokio architecture inference; must be benchmarked locally | Inferred and must be measured |
| End-to-end order acknowledgement latency is deployment dependent | CLOB/network architecture inference; must be measured from target region | Inferred and must be measured |
