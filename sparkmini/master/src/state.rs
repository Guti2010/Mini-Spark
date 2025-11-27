// master/src/state.rs

use common::{JobId, JobInfo, Task, TaskId, WorkerId};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::SystemTime,
};

#[derive(Clone)]
pub struct AppState {
    pub jobs: Arc<Mutex<HashMap<JobId, JobInfo>>>,
    pub workers: Arc<Mutex<HashMap<WorkerId, WorkerMeta>>>,
    // tareas pendientes de asignar
    pub tasks_queue: Arc<Mutex<VecDeque<Task>>>,
    // tareas ya asignadas pero no completadas
    pub in_flight: Arc<Mutex<HashMap<TaskId, InFlight>>>,
    pub worker_order: Arc<Mutex<Vec<WorkerId>>>,
    pub rr_cursor: Arc<Mutex<usize>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            tasks_queue: Arc::new(Mutex::new(VecDeque::new())),
            in_flight: Arc::new(Mutex::new(HashMap::new())),

            worker_order: Arc::new(Mutex::new(Vec::new())),
            rr_cursor: Arc::new(Mutex::new(0)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerMeta {
    pub hostname: String,
    pub last_heartbeat: SystemTime,
    pub dead: bool,
    pub max_concurrency: u32,

    // Métricas
    pub tasks_started: u64,
    pub tasks_succeeded: u64,
    pub tasks_failed: u64,
    pub total_task_time_ms: u64,

    pub last_cpu_percent: Option<f32>,
    pub last_mem_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct InFlight {
    pub task: Task,
    pub worker_id: WorkerId,
    pub started_at: SystemTime,
}
