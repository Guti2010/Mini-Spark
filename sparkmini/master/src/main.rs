// master/src/main.rs

mod handlers;
mod state;
mod failover; // 👈 importante

use crate::state::AppState;
use tokio::net::TcpListener;
use tracing::info;

// ---------- constantes que usa failover.rs ----------
pub const MAX_TASK_ATTEMPTS: u32 = 3;
pub const WORKER_HEARTBEAT_TIMEOUT_SECS: u64 = 3;
pub const FAILOVER_SWEEP_INTERVAL_SECS: u64 = 3;
// ----------------------------------------------------

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("master=debug,axum=info")
        .init();

    // Estado compartido del master
    let state = AppState::new();

    // Router HTTP
    let app = handlers::build_router(state.clone());

    // Loop de failover / heartbeats en segundo plano
    let failover_state = state.clone();
    tokio::spawn(async move {
        failover::run_failover_loop(failover_state).await;
    });

    // Servidor HTTP
    let listener = TcpListener::bind("0.0.0.0:8080")
        .await
        .expect("no se pudo bindear el puerto 8080");

    info!("master escuchando en {}", listener.local_addr().unwrap());

    axum::serve(listener, app)
        .await
        .expect("error sirviendo axum");
}
