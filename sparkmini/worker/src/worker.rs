use anyhow::Result;
use common::{
    Dag,
    JobInfo,
    TaskAssignmentRequest,
    TaskAssignmentResponse,
    TaskCompleteRequest,
    WorkerHeartbeatRequest,
    WorkerRegisterRequest,
    WorkerRegisterResponse,
};
use common::engine; 
use hostname;
use reqwest::Client;
use std::{env, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{info, warn};
use tracing_subscriber;
use std::path::Path;
use sysinfo::{System, SystemExt, CpuExt};


const DEFAULT_WORKER_CONCURRENCY: u32 = 2;

/// Loop principal del worker.
/// - Se registra en el master.
/// - Hace heartbeats periódicos.
/// - Pide tareas mientras tenga "slots" libres.
/// - Ejecuta cada tarea en paralelo (hasta WORKER_CONCURRENCY).
pub async fn run() -> Result<()> {
    tracing_subscriber::fmt::init();

    let base_url =
        env::var("MASTER_BASE_URL").unwrap_or_else(|_| "http://master:8080".to_string());
    let client = Client::new();

    let hostname = hostname::get()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let max_concurrency: u32 = env::var("WORKER_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(DEFAULT_WORKER_CONCURRENCY);

    let concurrency: usize = max_concurrency as usize;

    // Registro de worker (enviando max_concurrency)
    let register_url = format!("{}/api/v1/workers/register", base_url);
    let res = client
        .post(&register_url)
        .json(&WorkerRegisterRequest {
            hostname,
            max_concurrency,
        })
        .send()
        .await?;
    let WorkerRegisterResponse { worker_id } = res.json().await?;

    info!(
        "worker {} registrado con concurrency={} contra {}",
        worker_id, concurrency, base_url
    );

    let sem = Arc::new(Semaphore::new(concurrency));

    // System para leer CPU y memoria
    let mut sys = System::new_all();

    loop {
        // --------- Heartbeat al master con CPU/MEM ---------
        sys.refresh_cpu();
        sys.refresh_memory();

        let cpu_percent = sys.global_cpu_info().cpu_usage();
        // used_memory devuelve KB -> lo pasamos a bytes
        let mem_bytes = sys.used_memory() * 1024;

        let hb_url = format!("{}/api/v1/workers/heartbeat", base_url);
        let _ = client
            .post(&hb_url)
            .json(&WorkerHeartbeatRequest {
                worker_id: worker_id.clone(),
                cpu_percent,
                mem_bytes,
            })
            .send()
            .await;

        // --------- Control de concurrencia local ---------
        let permit = match sem.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                // No hay capacidad para nuevas tareas; esperamos un poco
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        // Pedimos tarea al master
        let assign_url = format!("{}/api/v1/tasks/next", base_url);
        let res = client
            .post(&assign_url)
            .json(&TaskAssignmentRequest {
                worker_id: worker_id.clone(),
            })
            .send()
            .await?;

        let assignment: TaskAssignmentResponse = res.json().await?;

        if let Some(task) = assignment.task {
            info!(
                "tengo tarea {} del job {} (input={} output={})",
                task.id, task.job_id, task.input_path, task.output_path
            );

            // --- 1) Obtener el Job (y el DAG) desde el master ---
            let job_url = format!("{}/api/v1/jobs/{}", base_url, task.job_id);
            let job_resp = client.get(&job_url).send().await?;

            if !job_resp.status().is_success() {
                warn!(
                    "no pude obtener job {} para tarea {} (status {})",
                    task.job_id,
                    task.id,
                    job_resp.status()
                );
                // No tenemos DAG -> reportamos fallo y liberamos el slot
                let complete_url = format!("{}/api/v1/tasks/complete", base_url);
                let _ = client
                    .post(&complete_url)
                    .json(&TaskCompleteRequest {
                        task_id: task.id.clone(),
                        success: false,
                    })
                    .send()
                    .await;
                drop(permit);
                continue;
            }

            let job_info: JobInfo = job_resp.json().await?;
            let dag = job_info.dag.clone();

            let client_cloned = client.clone();
            let base_url_cloned = base_url.clone();

            tokio::spawn(async move {
                let tmp_dir = "/data/tmp".to_string();
                let input_path = task.input_path.clone();
                let output_path = task.output_path.clone();
                let num_partitions = task.parallelism.max(1);

                // --- 2) Ejecutar usando el DAG ---
                let handle = tokio::task::spawn_blocking(move || {
                    engine::execute_wordcount_dag_for_file(
                        &dag,
                        &input_path,
                        &tmp_dir,
                        num_partitions,
                        &output_path,
                    )
                });

                let success = match handle.await {
                    Ok(Ok(())) => {
                        info!("terminé tarea {} correctamente", task.id);
                        true
                    }
                    Ok(Err(e)) => {
                        warn!("error procesando tarea {}: {:?}", task.id, e);
                        false
                    }
                    Err(e) => {
                        warn!("panic o join error en tarea {}: {:?}", task.id, e);
                        false
                    }
                };

                let complete_url = format!("{}/api/v1/tasks/complete", base_url_cloned);
                let _ = client_cloned
                    .post(&complete_url)
                    .json(&TaskCompleteRequest {
                        task_id: task.id.clone(),
                        success,
                    })
                    .send()
                    .await;

                drop(permit);
            });
        } else {
            drop(permit);
            info!("worker {} pidió tarea pero no hay", worker_id);
            sleep(Duration::from_secs(2)).await;
        }

    }
}

