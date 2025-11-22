use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use common::{
    JobId, JobInfo, JobRequest, JobStatus, Task, TaskAssignmentRequest,
    TaskAssignmentResponse, TaskCompleteRequest, TaskCompleteResponse, TaskId, WorkerId,
    WorkerRegisterRequest, WorkerRegisterResponse,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber;

#[derive(Clone)]
struct AppState {
    jobs: Arc<Mutex<HashMap<JobId, JobInfo>>>,
    workers: Arc<Mutex<HashMap<WorkerId, WorkerMeta>>>,
    // tareas en cola (pendientes de asignar)
    tasks_queue: Arc<Mutex<VecDeque<Task>>>,
    // tareas que ya fueron asignadas a algún worker pero no han reportado "complete"
    in_flight: Arc<Mutex<HashMap<TaskId, Task>>>,
}

#[derive(Debug, Clone)]
struct WorkerMeta {
    hostname: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("master=debug,axum=info")
        .init();

    let state = AppState {
        jobs: Arc::new(Mutex::new(HashMap::new())),
        workers: Arc::new(Mutex::new(HashMap::new())),
        tasks_queue: Arc::new(Mutex::new(VecDeque::new())),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/jobs", post(create_job))
        .route("/api/v1/jobs/:id", get(get_job))
        .route("/api/v1/workers/register", post(register_worker))
        .route("/api/v1/tasks/next", post(assign_task))
        .route("/api/v1/tasks/complete", post(complete_task))
        .with_state(state);

    let listener = TcpListener::bind("0.0.0.0:8080").await.unwrap();
    info!("master escuchando en {}", listener.local_addr().unwrap());

    axum::serve(listener, app).await.unwrap();
}

/* ---------------- handlers HTTP ---------------- */

async fn health() -> &'static str {
    "ok"
}

// Crea un job nuevo y encola 3 tareas "dummy"
async fn create_job(
    State(state): State<AppState>,
    Json(req): Json<JobRequest>,
) -> Json<JobInfo> {
    let job_id = uuid::Uuid::new_v4().to_string();

    let job_info = JobInfo {
        id: job_id.clone(),
        name: req.name,
        status: JobStatus::Accepted,
    };

    // Por ahora: 3 tareas de mentira asociadas a este job
    let mut nuevas_tareas = Vec::new();
    for _ in 0..3 {
        let t = Task {
            id: uuid::Uuid::new_v4().to_string(),
            job_id: job_id.clone(),
            node_id: "dummy-node".to_string(),
            attempt: 0,
        };
        nuevas_tareas.push(t);
    }

    {
        let mut jobs = state.jobs.lock().unwrap();
        jobs.insert(job_id.clone(), job_info.clone());
    }

    {
        let mut cola = state.tasks_queue.lock().unwrap();
        for t in nuevas_tareas {
            cola.push_back(t);
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
            },
        );
    }

    info!("worker registrado: {}", worker_id);
    Json(WorkerRegisterResponse { worker_id })
}

// Asigna la siguiente tarea en cola (si hay)
async fn assign_task(
    State(state): State<AppState>,
    Json(req): Json<TaskAssignmentRequest>,
) -> Json<TaskAssignmentResponse> {
    let mut cola = state.tasks_queue.lock().unwrap();
    let task_opt = cola.pop_front();
    drop(cola); // soltamos el lock

    if let Some(ref t) = task_opt {
        info!("asignando tarea {} al worker {}", t.id, req.worker_id);

        // guardamos la tarea en in_flight
        {
            let mut in_flight = state.in_flight.lock().unwrap();
            in_flight.insert(t.id.clone(), t.clone());
        }

        // marcamos el job como RUNNING
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
    let maybe_task: Option<Task> = {
        let mut in_flight = state.in_flight.lock().unwrap();
        in_flight.remove(&req.task_id)
    };

    if let Some(task) = maybe_task {
        let job_id = task.job_id.clone();

        if !req.success {
            // si falló, marcamos el job como FAILED
            let mut jobs = state.jobs.lock().unwrap();
            if let Some(job) = jobs.get_mut(&job_id) {
                job.status = JobStatus::Failed;
            }
            return Ok(Json(TaskCompleteResponse { ok: true }));
        }

        // Si fue éxito, vemos si ya no quedan tareas (ni pendientes ni en vuelo) de ese job
        let queue_has_for_job = {
            let cola = state.tasks_queue.lock().unwrap();
            cola.iter().any(|t| t.job_id == job_id)
        };

        let inflight_has_for_job = {
            let inflight = state.in_flight.lock().unwrap();
            inflight.values().any(|t| t.job_id == job_id)
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
