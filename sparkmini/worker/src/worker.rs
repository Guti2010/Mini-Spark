use anyhow::Result;
use common::{
    wordcount, TaskAssignmentRequest, TaskAssignmentResponse, TaskCompleteRequest,
    WorkerHeartbeatRequest, WorkerRegisterRequest, WorkerRegisterResponse,
};
use hostname;
use reqwest::Client;
use std::{env, time::Duration};
use tracing::{info, warn};
use tracing_subscriber;

/// Obtiene la URL base del master.
/// - En Docker: MASTER_URL=http://master:8080
/// - Local: default http://localhost:8080
fn master_base_url() -> String {
    env::var("MASTER_URL").unwrap_or_else(|_| "http://localhost:8080".to_string())
}

pub async fn run() -> Result<()> {
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

    // 1.5) Lanzar heartbeat periódico en background
    {
        let hb_client = client.clone();
        let hb_base = base_url.clone();
        let hb_worker_id = worker_id.clone();

        tokio::spawn(async move {
            let url = format!("{}/api/v1/workers/heartbeat", hb_base);
            loop {
                let _ = hb_client
                    .post(&url)
                    .json(&WorkerHeartbeatRequest {
                        worker_id: hb_worker_id.clone(),
                    })
                    .send()
                    .await;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

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
                "tengo tarea {} del job {} (input={} output={})",
                task.id, task.job_id, task.input_path, task.output_path
            );

            // Ejecutar WordCount
            let result = wordcount::wordcount_file(&task.input_path, &task.output_path);
            let success = result.is_ok();

            if let Err(err) = result {
                warn!("error procesando tarea {}: {:?}", task.id, err);
            } else {
                info!("terminé tarea {} correctamente", task.id);
            }

            // Reportar al master
            let complete_url = format!("{}/api/v1/tasks/complete", base_url);
            let _ = client
                .post(&complete_url)
                .json(&TaskCompleteRequest {
                    task_id: task.id.clone(),
                    success,
                })
                .send()
                .await?;
        } else {
            warn!("no hay tareas, esperando 1s...");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
