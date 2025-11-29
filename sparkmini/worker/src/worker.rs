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
use common::engine::WordcountTaskState;
use hostname;
use reqwest::Client;
use std::{env, io, time::Duration};
use tokio::time::sleep;
use tracing::{info, warn};
use tracing_subscriber;
use sysinfo::{CpuExt, System, SystemExt};
use std::collections::VecDeque;

const DEFAULT_WORKER_CONCURRENCY: u32 = 2;

/// Representa una tarea activa dentro del worker para ejecución en round-robin.
pub struct ActiveTask {
    pub task: common::Task,
    pub dag: Dag,
    pub state: WordcountTaskState,
}

impl ActiveTask {
    /// Crea una tarea activa a partir del Task y el DAG.
    /// Nota: por ahora el DAG no se usa dentro de WordcountTaskState,
    /// pero lo dejamos por si luego quieres soportar más tipos de jobs.
    pub fn new(task: &common::Task, dag: Dag) -> io::Result<Self> {
        let input = task.input_path.clone();
        let output = task.output_path.clone();
        let state = WordcountTaskState::new(&input, &output)?;

        Ok(Self {
            task: task.clone(),
            dag,
            state,
        })
    }

    /// Ejecuta un “quantum” sobre la tarea.
    ///
    /// Devuelve:
    /// - Ok(true)  => la tarea terminó (se debe reportar complete al master).
    /// - Ok(false) => la tarea aún no termina (se reencola en la run queue).
    pub fn step(&mut self, quantum: Duration) -> io::Result<bool> {
        self.state.step(quantum)
    }
}

/// Loop principal del worker.
/// - Se registra en el master.
/// - Hace heartbeats periódicos con CPU/MEM (para failover / health).
/// - Pide tareas hasta tener `concurrency` activas.
/// - Ejecuta las tareas en round-robin, con quantums de 100 ms.
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

    // System para leer CPU y memoria
    let mut sys = System::new_all();

    // Cola de tareas activas para round-robin
    let mut run_queue: VecDeque<ActiveTask> = VecDeque::new();

    // Quantum por tarea
    let quantum = Duration::from_millis(100);

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

        // --------- Pedir tareas nuevas si hay espacio en la cola ---------
        while run_queue.len() < concurrency {
            let assign_url = format!("{}/api/v1/tasks/next", base_url);
            let res = client
                .post(&assign_url)
                .json(&TaskAssignmentRequest {
                    worker_id: worker_id.clone(),
                })
                .send()
                .await?;

            let assignment: TaskAssignmentResponse = res.json().await?;

            let Some(task) = assignment.task else {
                info!(
                    "worker {} pidió tarea pero no hay tareas en cola (run_queue={})",
                    worker_id,
                    run_queue.len()
                );
                break;
            };

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
                // No tenemos DAG -> reportamos fallo
                let complete_url = format!("{}/api/v1/tasks/complete", base_url);
                let _ = client
                    .post(&complete_url)
                    .json(&TaskCompleteRequest {
                        task_id: task.id.clone(),
                        success: false,
                    })
                    .send()
                    .await;
                continue;
            }

            let job_info: JobInfo = job_resp.json().await?;
            let dag = job_info.dag.clone();

            match ActiveTask::new(&task, dag) {
                Ok(active) => {
                    run_queue.push_back(active);
                }
                Err(e) => {
                    warn!(
                        "error inicializando ActiveTask para tarea {}: {:?}",
                        task.id, e
                    );
                    // Reportamos fallo al master
                    let complete_url = format!("{}/api/v1/tasks/complete", base_url);
                    let _ = client
                        .post(&complete_url)
                        .json(&TaskCompleteRequest {
                            task_id: task.id.clone(),
                            success: false,
                        })
                        .send()
                        .await;
                }
            }
        }

        // --------- Si no hay nada para ejecutar, esperamos un poco ---------
        if run_queue.is_empty() {
            sleep(Duration::from_secs(2)).await;
            continue;
        }

        // --------- Round-robin local sobre run_queue ---------
        let mut active = run_queue
            .pop_front()
            .expect("run_queue no debería estar vacía aquí");
        let task_id = active.task.id.clone();

        match active.step(quantum) {
            Ok(true) => {
                // La tarea terminó en este quantum
                info!("terminé tarea {} correctamente (RR)", task_id);

                let complete_url = format!("{}/api/v1/tasks/complete", base_url);
                let _ = client
                    .post(&complete_url)
                    .json(&TaskCompleteRequest {
                        task_id: task_id.clone(),
                        success: true,
                    })
                    .send()
                    .await;
            }
            Ok(false) => {
                // La tarea aún no termina → la reencolamos al final
                run_queue.push_back(active);
            }
            Err(e) => {
                // Error durante la ejecución de la tarea
                warn!("error procesando tarea {} en RR: {:?}", task_id, e);

                let complete_url = format!("{}/api/v1/tasks/complete", base_url);
                let _ = client
                    .post(&complete_url)
                    .json(&TaskCompleteRequest {
                        task_id: task_id.clone(),
                        success: false,
                    })
                    .send()
                    .await;
            }
        }

    }
}
