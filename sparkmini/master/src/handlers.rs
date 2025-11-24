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
    WorkerRegisterResponse,
};
use glob::glob;
use std::fs;
use tracing::info;

use crate::state::{AppState, InFlight, WorkerMeta};
use crate::MAX_TASK_ATTEMPTS;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/jobs", post(create_job))
        .route("/api/v1/jobs/:id", get(get_job))
        .route("/api/v1/jobs/:id/results", get(get_job_results))
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

    // Directorio de salida específico de este job
    let job_output_dir = format!("{}/{}", req.output_dir, job_id);

    let mut tasks_for_job: Vec<Task> = Vec::new();

    // Para cada archivo que haga match con el glob, creamos una tarea
    for entry in glob(&req.input_glob).expect("patrón input_glob inválido") {
        if let Ok(path) = entry {
            if path.is_file() {
                let input_path = path.to_string_lossy().to_string();

                // Salida: <output_dir>/<job_id>/<nombre_archivo>
                let file_name = path.file_name().unwrap().to_string_lossy();
                let output_path = format!("{}/{}", job_output_dir, file_name);

                let t = Task {
                    id: uuid::Uuid::new_v4().to_string(),
                    job_id: job_id.clone(),
                    node_id: "wordcount".to_string(),
                    attempt: 0,
                    input_path,
                    output_path,
                };
                tasks_for_job.push(t);
            }
        }
    }

    let initial_status = if tasks_for_job.is_empty() {
        JobStatus::Succeeded // nada que hacer
    } else {
        JobStatus::Accepted
    };

    let job_info = JobInfo {
        id: job_id.clone(),
        name: req.name,
        status: initial_status,
        input_glob: req.input_glob,
        output_dir: job_output_dir.clone(),
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
                last_heartbeat: std::time::SystemTime::now(),
                dead: false,
            },
        );
    }

    info!("worker registrado: {}", worker_id);
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
    let mut queue = state.tasks_queue.lock().unwrap();
    let task_opt = queue.pop_front();
    drop(queue);

    if let Some(ref t) = task_opt {
        info!(
            "asignando tarea {} (input={} output={}) al worker {}",
            t.id, t.input_path, t.output_path, req.worker_id
        );

        {
            let mut in_flight = state.in_flight.lock().unwrap();
            in_flight.insert(
                t.id.clone(),
                InFlight {
                    task: t.clone(),
                    worker_id: req.worker_id.clone(),
                    started_at: std::time::SystemTime::now(),
                },
            );
        }

        {
            let mut jobs = state.jobs.lock().unwrap();
            if let Some(job) = jobs.get_mut(&t.job_id) {
                job.status = JobStatus::Running;
            }
        }
    } else {
        info!("worker {} pidió tarea pero no hay", req.worker_id);
    }

    Json(TaskAssignmentResponse { task: task_opt })
}

// Worker reporta que terminó una tarea
async fn complete_task(
    State(state): State<AppState>,
    Json(req): Json<TaskCompleteRequest>,
) -> Result<Json<TaskCompleteResponse>, StatusCode> {
    // Quitamos la tarea de in_flight
    let maybe_inflight: Option<InFlight> = {
        let mut in_flight = state.in_flight.lock().unwrap();
        in_flight.remove(&req.task_id)
    };

    if let Some(inflight) = maybe_inflight {
        let mut task = inflight.task;
        let job_id = task.job_id.clone();

        // Si la tarea falló, intentamos re-encolar hasta MAX_TASK_ATTEMPTS
        if !req.success {
            if task.attempt + 1 <= MAX_TASK_ATTEMPTS {
                task.attempt += 1;
                let mut queue = state.tasks_queue.lock().unwrap();
                queue.push_back(task);
            } else {
                let mut jobs = state.jobs.lock().unwrap();
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.status = JobStatus::Failed;
                }
            }
            return Ok(Json(TaskCompleteResponse { ok: true }));
        }

        // Caso éxito: verificamos si ya no quedan tareas de ese job
        let queue_has_for_job = {
            let queue = state.tasks_queue.lock().unwrap();
            queue.iter().any(|t| t.job_id == job_id)
        };

        let inflight_has_for_job = {
            let inflight = state.in_flight.lock().unwrap();
            inflight.values().any(|it| it.task.job_id == job_id)
        };

        if !queue_has_for_job && !inflight_has_for_job {
            let mut jobs = state.jobs.lock().unwrap();
            if let Some(job) = jobs.get_mut(&job_id) {
                job.status = JobStatus::Succeeded;
            }
        }

        Ok(Json(TaskCompleteResponse { ok: true }))
    } else {
        // no conocíamos esa tarea
        Err(StatusCode::NOT_FOUND)
    }
}
