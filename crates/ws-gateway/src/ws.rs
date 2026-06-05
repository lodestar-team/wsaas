//! WebSocket data-service handler.
//!
//! A consumer connects to `/ws/{chain}/{topic}?receipt=<TAP-Receipt JSON>`.
//! The TAP v2 receipt is validated (EIP-712 signature, staleness, authorised
//! sender) and persisted *before* the upgrade — no receipt, no stream. We then
//! open the upstream Pinax WebSocket (`wss://ws.pinax.network/ws/{chain}@{topic}
//! ?token=…`) and relay every pre-parsed transfer/swap/event message to the
//! client. Each connection settles as a QueryFee RAV via the shared collector,
//! exactly like the REST/gRPC siblings — only the transport differs.

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as TMsg;
use tracing::{info, warn};

use crate::{db, tap, AppState};

#[derive(Deserialize)]
pub struct WsQuery {
    /// TAP v2 receipt (same JSON shape as the `TAP-Receipt` header), url-encoded.
    pub receipt: String,
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn is_duplicate_nonce(e: &anyhow::Error) -> bool {
    e.downcast_ref::<sqlx::Error>()
        .map(|db| matches!(db, sqlx::Error::Database(d) if d.is_unique_violation()))
        .unwrap_or(false)
}

pub async fn handler(
    State(state): State<AppState>,
    Path((chain, topic)): Path<(String, String)>,
    Query(q): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> Result<Response, (StatusCode, String)> {
    // ── 1. Validate the TAP receipt before upgrading ──────────────────────────
    let validated = tap::validate_receipt(
        &q.receipt,
        state.domain_sep,
        &state.config.tap.authorized_senders,
        state.config.tap.data_service_address,
        state.config.indexer.service_provider_address,
        state.config.tap.max_receipt_age_ns,
        now_ns(),
    )
    .map_err(|e| (StatusCode::PAYMENT_REQUIRED, e.to_string()))?;

    // ── 2. Persist (reject replayed nonces) ───────────────────────────────────
    match db::insert_receipt(&state.pool, &validated).await {
        Ok(()) => {}
        Err(e) if is_duplicate_nonce(&e) => {
            return Err((StatusCode::PAYMENT_REQUIRED, "receipt nonce already used".into()));
        }
        Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }

    // ── 3. Build the upstream Pinax URL and upgrade ───────────────────────────
    let upstream = format!(
        "{}/ws/{}@{}?token={}",
        state.config.backend.pinax_ws_base.trim_end_matches('/'),
        chain,
        topic,
        state.config.backend.pinax_token,
    );
    info!(%chain, %topic, "ws session authorised; opening upstream");
    Ok(ws.on_upgrade(move |socket| relay(socket, upstream)))
}

/// Pipe the upstream Pinax stream to the consumer until either side closes.
async fn relay(mut client: WebSocket, upstream_url: String) {
    let (upstream, _) = match connect_async(&upstream_url).await {
        Ok(x) => x,
        Err(e) => {
            warn!(error = %e, "upstream connect failed");
            let _ = client
                .send(Message::Text(
                    format!("{{\"error\":\"upstream connect failed: {e}\"}}").into(),
                ))
                .await;
            return;
        }
    };
    let (mut up_tx, mut up_rx) = upstream.split();
    let mut delivered: u64 = 0;

    loop {
        tokio::select! {
            up = up_rx.next() => match up {
                Some(Ok(TMsg::Text(t))) => {
                    if client.send(Message::Text(t.to_string().into())).await.is_err() { break; }
                    delivered += 1;
                }
                Some(Ok(TMsg::Binary(b))) => {
                    if client.send(Message::Binary(b.to_vec().into())).await.is_err() { break; }
                    delivered += 1;
                }
                Some(Ok(TMsg::Close(_))) | None => break,
                Some(Ok(_)) => {} // ping/pong/frame — not billable
                Some(Err(e)) => { warn!(error = %e, "upstream stream error"); break; }
            },
            cl = client.recv() => match cl {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(_)) => break,
                _ => {} // subscription is encoded in the path; ignore client frames
            }
        }
    }
    let _ = up_tx.close().await;
    info!(delivered, "ws session closed");
}
