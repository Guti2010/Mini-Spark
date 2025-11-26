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
}

impl AppState {
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            tasks_queue: Arc::new(Mutex::new(VecDeque::new())),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerMeta {
    pub hostname: String,
    pub last_heartbeat: SystemTime,
    pub dead: bool,
}

#[derive(Debug, Clone)]
pub struct InFlight {
    pub task: Task,
    pub worker_id: WorkerId,
    pub started_at: SystemTime,
}
