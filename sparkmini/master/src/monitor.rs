use crate::state::AppState;
use crate::{MAX_TASK_ATTEMPTS, WORKER_DEAD_TIMEOUT};
use common::Task;
use std::{
    collections::HashSet,
    time::{Duration, SystemTime},
};
use tracing::info;

pub async fn monitor_workers(state: AppState) {
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let now = SystemTime::now();

        // Detectar workers muertos
        let mut dead_workers: HashSet<String> = HashSet::new();
        {
            let mut workers = state.workers.lock().unwrap();
            for (id, meta) in workers.iter_mut() {
                if !meta.dead {
                    if let Ok(elapsed) = now.duration_since(meta.last_heartbeat) {
                        if elapsed > WORKER_DEAD_TIMEOUT {
                            meta.dead = true;
                            info!("marcando worker {} como muerto", id);
                            dead_workers.insert(id.clone());
                        }
                    }
                }
            }
        }

        if dead_workers.is_empty() {
            continue;
        }

        // Reencolar tareas de esos workers
        let mut tasks_to_requeue: Vec<Task> = Vec::new();
        let mut jobs_to_fail: Vec<String> = Vec::new();

        {
            let mut in_flight = state.in_flight.lock().unwrap();
            in_flight.retain(|_task_id, inflight| {
                if dead_workers.contains(&inflight.worker_id) {
                    let mut task = inflight.task.clone();
                    let job_id = task.job_id.clone();

                    if task.attempt + 1 <= MAX_TASK_ATTEMPTS {
                        task.attempt += 1;
                        tasks_to_requeue.push(task);
                    } else {
                        jobs_to_fail.push(job_id);
                    }

                    false // sacar de in_flight
                } else {
                    true // mantener
                }
            });
        }

        if !tasks_to_requeue.is_empty() {
            let mut queue = state.tasks_queue.lock().unwrap();
            for task in tasks_to_requeue {
                info!(
                    "re-encolando tarea {} del job {} por worker muerto",
                    task.id, task.job_id
                );
                queue.push_back(task);
            }
        }

        if !jobs_to_fail.is_empty() {
            let mut jobs = state.jobs.lock().unwrap();
            for job_id in jobs_to_fail {
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.status = common::JobStatus::Failed;
                }
            }
        }
    }
}
