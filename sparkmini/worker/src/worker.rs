use anyhow::Result;
use common::{
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

    // Registro de worker
    let register_url = format!("{}/api/v1/workers/register", base_url);
    let res = client
        .post(&register_url)
        .json(&WorkerRegisterRequest { hostname })
        .send()
        .await?;
    let WorkerRegisterResponse { worker_id } = res.json().await?;

    // Concurrencia del worker (cuántas tareas procesa en paralelo)
    let concurrency: usize = env::var("WORKER_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    info!(
        "worker {} registrado con concurrency={} contra {}",
        worker_id, concurrency, base_url
    );

    let sem = Arc::new(Semaphore::new(concurrency));

    loop {
        // Heartbeat al master
        let hb_url = format!("{}/api/v1/workers/heartbeat", base_url);
        let _ = client
            .post(&hb_url)
            .json(&WorkerHeartbeatRequest {
                worker_id: worker_id.clone(),
            })
            .send()
            .await;

        // Intentar reservar un "slot" de concurrencia
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

            // Clonar lo que usamos en la tarea asíncrona
            let client_cloned = client.clone();
            let base_url_cloned = base_url.clone();

            // Lanzamos la ejecución de la tarea en paralelo
            tokio::spawn(async move {
                let tmp_dir = "/data/tmp".to_string();
                let input_path = task.input_path.clone();
                let output_path = task.output_path.clone();
                let num_partitions = task.parallelism.max(1);

                // Ejecutar el WordCount "Spark-like" en un hilo de bloqueo
                let handle = tokio::task::spawn_blocking(move || {
                    let path = Path::new(&input_path);
                    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();

                    // Por ahora asumimos que el campo de texto en CSV/JSON se llama "text"
                    let text_field = "text";

                    match ext.as_str() {
                        // Archivos de texto plano (.txt): lo que ya tenías
                        "txt" => engine::wordcount_file_shuffled_local(
                            &input_path,
                            &tmp_dir,
                            num_partitions,
                            &output_path,
                        ),

                        // CSV con una columna llamada "text"
                        "csv" => engine::wordcount_csv_file_shuffled_local(
                            &input_path,
                            text_field,
                            &tmp_dir,
                            num_partitions,
                            &output_path,
                        ),

                        // JSONL/JSON: una línea por objeto, con un campo "text"
                        "jsonl" | "json" => engine::wordcount_jsonl_file_shuffled_local(
                            &input_path,
                            text_field,
                            &tmp_dir,
                            num_partitions,
                            &output_path,
                        ),

                        // Cualquier otra cosa la tratamos como texto plano por defecto
                        _ => engine::wordcount_file_shuffled_local(
                            &input_path,
                            &tmp_dir,
                            num_partitions,
                            &output_path,
                        ),
                    }
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

                // Reportar al master que terminamos
                let complete_url = format!("{}/api/v1/tasks/complete", base_url_cloned);
                let _ = client_cloned
                    .post(&complete_url)
                    .json(&TaskCompleteRequest {
                        task_id: task.id.clone(),
                        success,
                    })
                    .send()
                    .await;

                // Liberar el "slot" de concurrencia al terminar
                drop(permit);
            });
        } else {
            // No hay tarea: devolvemos el permiso y dormimos
            drop(permit);
            info!("worker {} pidió tarea pero no hay", worker_id);
            sleep(Duration::from_secs(2)).await;
        }
    }
}
