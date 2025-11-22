use anyhow::Result;
use common::{
    TaskAssignmentRequest, TaskAssignmentResponse, TaskCompleteRequest,
    WorkerRegisterRequest, WorkerRegisterResponse,
};
use hostname;
use reqwest::Client;
use std::{env, time::Duration};
use tracing::{info, warn};
use tracing_subscriber;

/// Obtiene la URL base del master.
/// - En Docker usaremos: MASTER_URL=http://master:8080
/// - Si no está definida, usa http://localhost:8080 (para pruebas locales)
fn master_base_url() -> String {
    env::var("MASTER_URL").unwrap_or_else(|_| "http://localhost:8080".to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("worker=debug,reqwest=info")
        .init();

    let client = Client::new();
    let base_url = master_base_url();

    // Nombre de host (solo para info)
    let hostname_str = hostname::get()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    // 1) Registrarse en el master
    let register_url = format!("{}/api/v1/workers/register", base_url);
    let res = client
        .post(&register_url)
        .json(&WorkerRegisterRequest {
            hostname: hostname_str,
        })
        .send()
        .await?;

    let register_resp: WorkerRegisterResponse = res.json().await?;
    let worker_id = register_resp.worker_id;
    info!("worker registrado con id = {}", worker_id);

    // 2) Loop infinito pidiendo tareas
    loop {
        let assign_url = format!("{}/api/v1/tasks/next", base_url);
        let resp = client
            .post(&assign_url)
            .json(&TaskAssignmentRequest {
                worker_id: worker_id.clone(),
            })
            .send()
            .await?;

        let assignment: TaskAssignmentResponse = resp.json().await?;

        if let Some(task) = assignment.task {
            info!(
                "tengo tarea {} del job {} (node_id={})",
                task.id, task.job_id, task.node_id
            );
            info!("simulando trabajo por 2s...");
            tokio::time::sleep(Duration::from_secs(2)).await;

            info!(
                "terminé tarea {} (simulada), reportando al master...",
                task.id
            );

            let complete_url = format!("{}/api/v1/tasks/complete", base_url);
            let _ = client
                .post(&complete_url)
                .json(&TaskCompleteRequest {
                    task_id: task.id.clone(),
                    success: true,
                })
                .send()
                .await?;
        } else {
            warn!("no hay tareas, esperando 1s...");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
