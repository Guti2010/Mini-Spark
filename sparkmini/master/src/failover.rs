use std::collections::HashSet;
use std::time::{Duration, SystemTime};

use tokio::time::sleep;
use tracing::{info, warn};

use common::JobStatus;

use crate::state::{AppState, InFlight};
use crate::{FAILOVER_SWEEP_INTERVAL_SECS, MAX_TASK_ATTEMPTS, WORKER_HEARTBEAT_TIMEOUT_SECS};

/// Loop principal de tolerancia a fallos:
/// - detecta workers muertos (sin heartbeat)
/// - reencola tareas
pub async fn run_failover_loop(state: AppState) {
    loop {
        sleep(Duration::from_secs(FAILOVER_SWEEP_INTERVAL_SECS)).await;

        if let Err(e) = sweep_once(&state) {
            warn!("error en failover sweep: {:?}", e);
        }
    }
}

/// Una pasada de chequeo:
/// 1. marca workers muertos
/// 2. saca tareas de in_flight de esos workers
/// 3. reencola las tareas (si no superan MAX_TASK_ATTEMPTS)
fn sweep_once(state: &AppState) -> Result<(), String> {
    let now = SystemTime::now();

    // 1) Detectar qué workers están muertos
    let mut newly_dead_workers: Vec<String> = Vec::new();

    {
        let mut workers = state.workers.lock().map_err(|_| "lock workers")?;

        for (worker_id, meta) in workers.iter_mut() {
            if meta.dead {
                continue;
            }

            match now.duration_since(meta.last_heartbeat) {
                Ok(elapsed) => {
                    if elapsed > Duration::from_secs(WORKER_HEARTBEAT_TIMEOUT_SECS) {
                        meta.dead = true;
                        newly_dead_workers.push(worker_id.clone());
                        warn!(
                            "marcando worker {} como DEAD (sin heartbeat hace {:?})",
                            worker_id, elapsed
                        );
                    }
                }
                Err(_) => {
                    // last_heartbeat en el futuro? raro, lo ignoramos.
                    continue;
                }
            }
        }
    }

    if newly_dead_workers.is_empty() {
        // nada que hacer esta pasada
        return Ok(());
    }

    let dead_set: HashSet<String> = newly_dead_workers.iter().cloned().collect();

    // 2) Sacar tareas de in_flight que pertenecían a esos workers
    use common::Task;
    let mut to_requeue: Vec<Task> = Vec::new();
    let mut jobs_to_fail: Vec<String> = Vec::new();

    {
        let mut in_flight = state.in_flight.lock().map_err(|_| "lock in_flight")?;

        // reconstruimos el mapa filtrando las tareas "sanas"
        let mut new_in_flight = std::collections::HashMap::new();

        for (_task_id, inflight) in in_flight.drain() {
            if dead_set.contains(&inflight.worker_id) {
                let mut task = inflight.task.clone();

                if task.attempt + 1 <= MAX_TASK_ATTEMPTS {
                    task.attempt += 1;
                    info!(
                        "reencolando tarea {} del job {} por caída del worker {} (attempt={})",
                        task.id, task.job_id, inflight.worker_id, task.attempt
                    );
                    to_requeue.push(task);
                } else {
                    warn!(
                        "tarea {} del job {} superó el máximo de intentos ({}) tras caída de worker {}, marcando job como FAILED",
                        task.id, task.job_id, MAX_TASK_ATTEMPTS, inflight.worker_id
                    );
                    jobs_to_fail.push(task.job_id.clone());
                }
            } else {
                // worker sigue vivo, dejamos la tarea en vuelo
                new_in_flight.insert(inflight.task.id.clone(), inflight);
            }
        }

        *in_flight = new_in_flight;
    }

    // 3) Reencolar las tareas que sí se pueden reintentar
    if !to_requeue.is_empty() {
        let mut queue = state.tasks_queue.lock().map_err(|_| "lock tasks_queue")?;
        for t in to_requeue {
            queue.push_back(t);
        }
    }

    // 4) Marcar jobs como FAILED si alguna tarea superó el límite de intentos
    if !jobs_to_fail.is_empty() {
        let mut jobs = state.jobs.lock().map_err(|_| "lock jobs")?;
        for job_id in jobs_to_fail {
            if let Some(job) = jobs.get_mut(&job_id) {
                job.status = JobStatus::Failed;
            }
        }
    }

    Ok(())
}
