use serde::{Deserialize, Serialize};

use crate::task::{Task, TaskId};

pub type WorkerId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegisterRequest {
    pub hostname: String,
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
