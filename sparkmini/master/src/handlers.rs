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
use chrono::Utc;

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

    // 1) ID de job y directorio de salida
    let job_id = uuid::Uuid::new_v4().to_string();
    let job_output_dir = format!("{}/{}", req.output_dir, job_id);
    let _ = fs::create_dir_all(&job_output_dir);

    // 2) Construir tareas
    let mut tasks_for_job: Vec<Task> = Vec::new();

    let par = req.parallelism.max(1);
    let mut next_partition: u32 = 0;

    for entry in glob(&req.input_glob).expect("patrón input_glob inválido") {
        if let Ok(path) = entry {
            if path.is_file() {
                let input_path = path.to_string_lossy().to_string();

                // Salida: <output_dir>/<job_id>/<nombre_archivo>
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

    // 3) Métricas iniciales
    let total_tasks = tasks_for_job.len() as u32;

    let initial_status = if total_tasks == 0 {
        JobStatus::Succeeded
    } else {
        JobStatus::Accepted
    };

    let progress_pct = if total_tasks == 0 { 100.0 } else { 0.0 };
    let started_at = None;
    let finished_at = if total_tasks == 0 {
        Some(chrono::Utc::now().to_rfc3339())
    } else {
        None
    };

    // 4) JobInfo
    let job_info = JobInfo {
        id: job_id.clone(),
        name: req.name.clone(),
        status: initial_status,
        dag: req.dag.clone(),
        parallelism: req.parallelism,
        input_glob: req.input_glob.clone(),
        output_dir: job_output_dir.clone(),

        total_tasks,
        completed_tasks: 0,
        failed_tasks: 0,
        retries: 0,
        progress_pct,
        started_at,
        finished_at,
    };

    // 5) Guardar job y encolar tareas
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
                if matches!(job.status, JobStatus::Accepted) {
                    job.status = JobStatus::Running;
                    job.started_at = Some(Utc::now().to_rfc3339());
                }
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
            // Contabilizamos reintento
            {
                let mut jobs = state.jobs.lock().unwrap();
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.retries += 1;
                }
            }

            if task.attempt + 1 <= MAX_TASK_ATTEMPTS {
                task.attempt += 1;
                let mut queue = state.tasks_queue.lock().unwrap();
                queue.push_back(task);
            } else {
                // Sin más reintentos: marcamos fallo definitivo
                let mut jobs = state.jobs.lock().unwrap();
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.status = JobStatus::Failed;
                    job.failed_tasks += 1;
                    job.progress_pct = if job.total_tasks == 0 {
                        100.0
                    } else {
                        (job.completed_tasks as f32 / job.total_tasks as f32) * 100.0
                    };
                    job.finished_at = Some(Utc::now().to_rfc3339());
                }
            }
            return Ok(Json(TaskCompleteResponse { ok: true }));
        }

        // Caso éxito: aumentamos contador de tareas completadas
        {
            let mut jobs = state.jobs.lock().unwrap();
            if let Some(job) = jobs.get_mut(&job_id) {
                job.completed_tasks += 1;
                if job.total_tasks == 0 {
                    job.progress_pct = 100.0;
                } else {
                    job.progress_pct =
                        (job.completed_tasks as f32 / job.total_tasks as f32) * 100.0;
                }
            }
        }

        // Verificamos si ya no quedan tareas de ese job
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
                job.finished_at = Some(Utc::now().to_rfc3339());
                job.progress_pct = 100.0;
            }
        }

        Ok(Json(TaskCompleteResponse { ok: true }))
    } else {
        // no conocíamos esa tarea
        Err(StatusCode::NOT_FOUND)
    }
}

