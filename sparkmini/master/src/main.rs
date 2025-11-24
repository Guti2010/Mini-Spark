mod state;
mod handlers;
mod monitor;

use crate::state::AppState;
use std::collections::HashMap;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber;

use common::JobInfo;

pub const WORKER_DEAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
pub const MAX_TASK_ATTEMPTS: u32 = 3;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("master=debug,axum=info")
        .init();

    let state = AppState {
        jobs: std::sync::Arc::new(std::sync::Mutex::new(HashMap::<String, JobInfo>::new())),
        workers: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        tasks_queue: std::sync::Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new())),
        in_flight: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
    };

    // router HTTP
    let app = handlers::build_router(state.clone());

    // monitor de heartbeats en segundo plano
    let monitor_state = state.clone();
    tokio::spawn(async move {
        monitor::monitor_workers(monitor_state).await;
    });

    let listener = TcpListener::bind("0.0.0.0:8080").await.unwrap();
    info!("master escuchando en {}", listener.local_addr().unwrap());

    axum::serve(listener, app).await.unwrap();
}
