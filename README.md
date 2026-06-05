# WSaaS — WebSocket data service on Graph Horizon

A Horizon **WebSocket data service**: stream pre-parsed transfers, swaps and
exchange events over a single WebSocket connection, gated by TAP v2 (GraphTally)
micropayments and settled on-chain via `WebSocketDataService.collect()`.

It sits in front of an upstream [Pinax WebSocket](https://pinax.network/products/websockets)
feed: a consumer opens `wss://<gateway>/ws/{chain}/{topic}?receipt=<TAP-Receipt>`,
the gateway validates + persists the EIP-712 receipt, opens the upstream Pinax
stream, and relays every pre-parsed message back. Background tasks aggregate
receipts into RAVs and collect on Arbitrum One — the same payment loop as the
Subgraph/Dispatch/Camp/Seahorn/Substreams/Mainline data services, just over a
WebSocket transport with per-message pricing.

## Layout
- `contracts/` — `WebSocketDataService.sol` (inherits the Horizon `DataService` base; reuses HorizonStaking / GraphTallyCollector / PaymentsEscrow unchanged).
- `crates/ws-gateway/` — Rust/Axum gateway: TAP validation (`tap.rs`), WebSocket relay (`ws.rs`), RAV aggregation (`aggregator.rs`), on-chain collection (`collector.rs`).

## Run
```
cp config.example.toml config.toml   # fill provider/operator + Pinax JWT
GATEWAY_CONFIG=./config.toml cargo run --release -p ws-gateway
```

## Consume
```
wss://<gateway>/ws/{chain}/{topic}?receipt=<url-encoded EIP-712 TAP receipt>
```
e.g. `/ws/eth/transfers`, `/ws/solana/swaps`. No receipt → 400/402.

> Community implementation. Not audited.
