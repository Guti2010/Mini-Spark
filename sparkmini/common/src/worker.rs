use serde::{Deserialize, Serialize};

use crate::task::{Task, TaskId};

pub type WorkerId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegisterRequest {
    pub hostname: String,
    pub max_concurrency: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegisterResponse {
    pub worker_id: WorkerId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerHeartbeatRequest {
    pub worker_id: WorkerId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerHeartbeatResponse {
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAssignmentRequest {
    pub worker_id: WorkerId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAssignmentResponse {
    pub task: Option<Task>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompleteRequest {
    pub task_id: TaskId,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompleteResponse {
    pub ok: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerMetrics {
    pub worker_id: WorkerId,
    pub hostname: String,
    pub dead: bool,
    pub max_concurrency: u32,
    pub last_heartbeat_secs_ago: u64,
    pub active_tasks: u32,
    pub tasks_started: u64,
    pub tasks_succeeded: u64,
    pub tasks_failed: u64,
    pub avg_task_ms: Option<f64>,
}

