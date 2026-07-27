# Runtime console QA

Last verified: 2026-07-17

## Test state

The console was exercised against the running Rust process at
`http://127.0.0.1:8787`; it was not loaded with fixture or hard-coded market
state. `config.example.toml` deliberately contains placeholder market IDs, so
the discovery request returned `404`, the market feed remained unready, and the
runtime displayed `DEGRADED`. The official geoblock response detected `US · NY`
and displayed `BLOCKED`. This is the expected fail-closed state for that sample
configuration and test location.

## Functional browser verification

- Status, readiness, feed health, paper balances, events, journal sequence, and
  command availability rendered from the token-protected `/api/status`.
- After replacing independently merged market subscriptions with one ordered
  wire consumer, the release binary reconnected successfully and reported
  `single ordered Polymarket market stream connected` from the running engine.
- A forged DNS-rebinding `Host` request returned `421`, and a valid loopback
  Host without the random status token returned `403`.
- `Resume paper` was accepted by the engine. The page changed to the active
  paper summary, disabled Resume, enabled Pause, and remained degraded because
  market discovery was not authoritative.
- `Cancel all` returned the real engine result: `open orders cancelled (0
  affected)`.
- `Pause` was accepted by the engine. The page changed to `PAUSED`, enabled
  Resume, and disabled Pause.
- `Shut down runtime` returned `graceful shutdown requested (0 affected)` before
  process cancellation. The page changed to `SHUTTING DOWN`, stopped status
  polling, disabled every command, and the Rust process exited with status 0.
- The final browser warning/error log was empty.

## Responsive and visual verification

- Default desktop viewport: `1265px` document width and `1265px` scroll width;
  no page-level horizontal overflow.
- Mobile override: `390 × 844`; the web content viewport was `375px`, document
  width and scroll width were both `375px`, and the controls section was
  reachable through the sticky section navigation.
- Dense tables scroll inside their own containers on narrow screens without
  widening the document.
- Feed details wrap instead of truncating connection errors or disabled-state
  explanations.
- The mobile execution controls and launch-readiness gates remained visible and
  usable. A visual screenshot was inspected at the mobile breakpoint.

## Automated verification

The repository quality gate passed after the runtime and console work:

```text
cargo test --offline --all-targets --all-features
165 library tests + 3 binary tests + 3 protocol-fixture tests = 171 passed
```

Formatting, linting with warnings denied, the optimized release build, and a
second process-level startup/control/shutdown smoke test all passed as the final
delivery gate. A funded live order is intentionally not part of browser
QA or CI; live startup requires real credentials, wallet/account proofs,
geographic eligibility, fresh authenticated streams, and explicit operator
configuration.

Final result: passed for the verified paper/control-plane scope; live execution
remains correctly unproven without a permitted funded account.
