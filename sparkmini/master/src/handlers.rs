use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use common::{
    JobInfo, JobRequest, JobResults, JobStatus, TaskAssignmentRequest,
    TaskAssignmentResponse, TaskCompleteRequest, TaskCompleteResponse,
    WorkerHeartbeatRequest, WorkerHeartbeatResponse, WorkerRegisterRequest,
    WorkerRegisterResponse, WorkerMetrics, WorkerId,
};
use glob::glob;
use std::fs;
use tracing::info;
use chrono::Utc;
use std::time::SystemTime;
use std::collections::HashMap;

use crate::state::{AppState, InFlight, WorkerMeta};
use crate::MAX_TASK_ATTEMPTS;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/jobs", post(create_job))
        .route("/api/v1/jobs/:id", get(get_job))
        .route("/api/v1/jobs/:id/results", get(get_job_results))
        .route("/api/v1/workers", get(list_workers))
        .route("/api/v1/workers/register", post(register_worker))
        .route("/api/v1/workers/heartbeat", post(worker_heartbeat))
        .route("/api/v1/tasks/next", post(assign_task))
        .route("/api/v1/tasks/complete", post(complete_task))
        .with_state(state)
}

/* ---------------- handlers HTTP ---------------- */

async fn health() -> &'static str {
    "ok"
}

// Crea un job nuevo y genera una tarea por archivo que haga match con input_glob
async fn create_job(
    State(state): State<AppState>,
    Json(req): Json<JobRequest>,
) -> Json<JobInfo> {
    use common::Task;

    let job_id = uuid::Uuid::new_v4().to_string();
    let job_output_dir = format!("/data/output/{}", job_id);
    let _ = fs::create_dir_all(&job_output_dir);

    let mut tasks_for_job: Vec<Task> = Vec::new();

    let par = req.parallelism.max(1);
    let mut next_partition: u32 = 0;

    for entry in glob(&req.input_glob).expect("patrón input_glob inválido") {
        if let Ok(path) = entry {
            if path.is_file() {
                let input_path = path.to_string_lossy().to_string();

                let file_name = path.file_name().unwrap().to_string_lossy();
                let output_path = format!("{}/{}", job_output_dir, file_name);

                let partition = next_partition % par;
                next_partition += 1;

                let t = Task {
                    id: uuid::Uuid::new_v4().to_string(),
                    job_id: job_id.clone(),
                    node_id: "wordcount".to_string(),
                    attempt: 0,
                    stage: 0,
                    partition,
                    parallelism: par,
                    input_path,
                    output_path,
                };
                tasks_for_job.push(t);
            }
        }
    }

    let initial_status = if tasks_for_job.is_empty() {
        JobStatus::Succeeded
    } else {
        JobStatus::Accepted
    };

    let submitted_at = Utc::now();
    let total_tasks = tasks_for_job.len() as u32;

    let job_info = JobInfo {
        id: job_id.clone(),
        name: req.name,
        status: initial_status,
        dag: req.dag,
        parallelism: req.parallelism,
        input_glob: req.input_glob,
        output_dir: job_output_dir.clone(),

        submitted_at,
        started_at: None,
        finished_at: None,
        total_tasks,
        completed_tasks: 0,
        failed_tasks: 0,
    };

    {
        let mut jobs = state.jobs.lock().unwrap();
        jobs.insert(job_id.clone(), job_info.clone());
    }

    if !tasks_for_job.is_empty() {
        let mut queue = state.tasks_queue.lock().unwrap();
        for t in tasks_for_job {
            queue.push_back(t);
        }
    }

    Json(job_info)
}




// Devuelve info básica de un job
async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<JobInfo>, StatusCode> {
    let jobs = state.jobs.lock().unwrap();

    if let Some(job) = jobs.get(&id) {
        Ok(Json(job.clone()))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// Lista archivos de salida de un job
async fn get_job_results(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<JobResults>, StatusCode> {
    let jobs = state.jobs.lock().unwrap();
    let job = if let Some(job) = jobs.get(&id) {
        job.clone()
    } else {
        return Err(StatusCode::NOT_FOUND);
    };
    drop(jobs);

    let mut files = Vec::new();

    if let Ok(entries) = fs::read_dir(&job.output_dir) {
        for entry in entries.flatten() {
            if let Ok(ft) = entry.file_type() {
                if ft.is_file() {
                    if let Some(name) = entry.file_name().to_str() {
                        files.push(name.to_string());
                    }
                }
            }
        }
    }

    let results = JobResults {
        job_id: job.id,
        output_dir: job.output_dir,
        files,
    };

    Ok(Json(results))
}

// Registra un worker nuevo
async fn register_worker(
    State(state): State<AppState>,
    Json(req): Json<WorkerRegisterRequest>,
) -> Json<WorkerRegisterResponse> {
    let worker_id = uuid::Uuid::new_v4().to_string();

    {
        let mut workers = state.workers.lock().unwrap();
        workers.insert(
            worker_id.clone(),
            WorkerMeta {
                hostname: req.hostname,
                last_heartbeat: SystemTime::now(),
                dead: false,
                max_concurrency: req.max_concurrency,

                tasks_started: 0,
                tasks_succeeded: 0,
                tasks_failed: 0,
                total_task_time_ms: 0,
            },
        );
    }

    info!(
        "worker registrado: {} (max_concurrency={})",
        worker_id, req.max_concurrency
    );
    Json(WorkerRegisterResponse { worker_id })
}



// Heartbeat de worker
async fn worker_heartbeat(
    State(state): State<AppState>,
    Json(req): Json<WorkerHeartbeatRequest>,
) -> Result<Json<WorkerHeartbeatResponse>, StatusCode> {
    let mut workers = state.workers.lock().unwrap();
    if let Some(meta) = workers.get_mut(&req.worker_id) {
        meta.last_heartbeat = std::time::SystemTime::now();
        Ok(Json(WorkerHeartbeatResponse { ok: true }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// Asigna la siguiente tarea en cola (si hay)
async fn assign_task(
    State(state): State<AppState>,
    Json(req): Json<TaskAssignmentRequest>,
) -> Json<TaskAssignmentResponse> {
    // 1) Cuántas tareas tiene ya este worker en vuelo
    let active_for_worker: usize = {
        let in_flight = state.in_flight.lock().unwrap();
        in_flight
            .values()
            .filter(|entry| entry.worker_id == req.worker_id)
            .count()
    };

    // 2) Capacidad máxima de este worker (max_concurrency)
    let max_for_worker: u32 = {
        let workers = state.workers.lock().unwrap();
        workers
            .get(&req.worker_id)
            .map(|m| m.max_concurrency)
            .unwrap_or(1) // por si el worker no existe por alguna razón
    };

    // Si ya está al tope, no le damos más tareas
    if active_for_worker as u32 >= max_for_worker {
        info!(
            "worker {} pidió tarea pero ya tiene {}/{} en vuelo",
            req.worker_id, active_for_worker, max_for_worker
        );
        return Json(TaskAssignmentResponse { task: None });
    }

    // 3) Sacar la siguiente tarea de la cola global
    let task_opt = {
        let mut queue = state.tasks_queue.lock().unwrap();
        queue.pop_front()
    };

    if let Some(ref t) = task_opt {
        info!(
            "asignando tarea {} (job={}, input={} output={}) al worker {} ({}/{} en vuelo -> +1)",
            t.id,
            t.job_id,
            t.input_path,
            t.output_path,
            req.worker_id,
            active_for_worker,
            max_for_worker,
        );

        // 4) Registrar la tarea en in_flight con timestamp de inicio
        {
            let mut in_flight = state.in_flight.lock().unwrap();
            in_flight.insert(
                t.id.clone(),
                InFlight {
                    task: t.clone(),
                    worker_id: req.worker_id.clone(),
                    started_at: SystemTime::now(),
                },
            );
        }

        // 5) Actualizar el job: marcar como Running y setear started_at si es la primera vez
        {
            let mut jobs = state.jobs.lock().unwrap();
            if let Some(job) = jobs.get_mut(&t.job_id) {
                if matches!(job.status, JobStatus::Accepted) {
                    job.status = JobStatus::Running;
                    if job.started_at.is_none() {
                        job.started_at = Some(Utc::now());
                    }
                }
            }
        }

        // 6) Métricas del worker: incrementar tareas iniciadas
        {
            let mut workers = state.workers.lock().unwrap();
            if let Some(meta) = workers.get_mut(&req.worker_id) {
                meta.tasks_started += 1;
            }
        }
    } else {
        info!(
            "worker {} pidió tarea pero no hay tareas en cola",
            req.worker_id
        );
    }

    Json(TaskAssignmentResponse { task: task_opt })
}

// Worker reporta que terminó una tarea
async fn complete_task(
    State(state): State<AppState>,
    Json(req): Json<TaskCompleteRequest>,
) -> Result<Json<TaskCompleteResponse>, StatusCode> {
    // Sacar la tarea de in_flight
    let maybe_inflight: Option<InFlight> = {
        let mut in_flight = state.in_flight.lock().unwrap();
        in_flight.remove(&req.task_id)
    };

    if let Some(inflight) = maybe_inflight {
        let mut task = inflight.task;
        let job_id = task.job_id.clone();
        let worker_id = inflight.worker_id.clone();

        // ---- Métricas de worker: duración de la tarea ----
        let duration_ms: u64 = inflight
            .started_at
            .elapsed()
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        {
            let mut workers = state.workers.lock().unwrap();
            if let Some(meta) = workers.get_mut(&worker_id) {
                meta.total_task_time_ms += duration_ms;
                if req.success {
                    meta.tasks_succeeded += 1;
                } else {
                    meta.tasks_failed += 1;
                }
            }
        }

        // ---- Caso fallo: reintentos o marcar job como Failed ----
        if !req.success {
            if task.attempt + 1 <= MAX_TASK_ATTEMPTS {
                task.attempt += 1;
                let mut queue = state.tasks_queue.lock().unwrap();
                queue.push_back(task);
            } else {
                let mut jobs = state.jobs.lock().unwrap();
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.failed_tasks += 1;
                    job.status = JobStatus::Failed;
                    job.finished_at = Some(Utc::now());
                }
            }
            return Ok(Json(TaskCompleteResponse { ok: true }));
        }

        // ---- Caso éxito: contar tarea completada ----
        {
            let mut jobs = state.jobs.lock().unwrap();
            if let Some(job) = jobs.get_mut(&job_id) {
                job.completed_tasks += 1;
            }
        }

        // Ver si todavía quedan tareas de este job
        let queue_has_for_job = {
            let queue = state.tasks_queue.lock().unwrap();
            queue.iter().any(|t| t.job_id == job_id)
        };

        let inflight_has_for_job = {
            let inflight_map = state.in_flight.lock().unwrap();
            inflight_map.values().any(|it| it.task.job_id == job_id)
        };

        if !queue_has_for_job && !inflight_has_for_job {
            let mut jobs = state.jobs.lock().unwrap();
            if let Some(job) = jobs.get_mut(&job_id) {
                if !matches!(job.status, JobStatus::Failed) {
                    job.status = JobStatus::Succeeded;
                }
                job.finished_at = Some(Utc::now());
            }
        }

        Ok(Json(TaskCompleteResponse { ok: true }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}


async fn list_workers(State(state): State<AppState>) -> Json<Vec<WorkerMetrics>> {
    let now = SystemTime::now();

    // Contar tareas activas por worker (desde in_flight)
    let in_flight = state.in_flight.lock().unwrap();
    let mut active_by_worker: HashMap<WorkerId, u32> = HashMap::new();
    for inf in in_flight.values() {
        *active_by_worker.entry(inf.worker_id.clone()).or_insert(0) += 1;
    }
    drop(in_flight);

    let workers = state.workers.lock().unwrap();
    let mut out = Vec::new();

    for (wid, meta) in workers.iter() {
        let age_secs = now
            .duration_since(meta.last_heartbeat)
            .unwrap_or_default()
            .as_secs();

        let active = active_by_worker.get(wid).copied().unwrap_or(0);

        let avg_ms = if meta.tasks_succeeded > 0 {
            Some(meta.total_task_time_ms as f64 / meta.tasks_succeeded as f64)
        } else {
            None
        };

        out.push(WorkerMetrics {
            worker_id: wid.clone(),
            hostname: meta.hostname.clone(),
            dead: meta.dead,
            max_concurrency: meta.max_concurrency,
            last_heartbeat_secs_ago: age_secs,
            active_tasks: active,
            tasks_started: meta.tasks_started,
            tasks_succeeded: meta.tasks_succeeded,
            tasks_failed: meta.tasks_failed,
            avg_task_ms: avg_ms,
        });
    }

    Json(out)
}



