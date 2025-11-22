use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type JobId = String;
pub type TaskId = String;
pub type WorkerId = String;

/* --------- Definición inicial de jobs y DAG --------- */

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRequest {
    pub name: String,
    // luego agregamos más campos (dag, input_path, etc.)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobStatus {
    Accepted,
    Running,
    Failed,
    Succeeded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInfo {
    pub id: JobId,
    pub name: String,
    pub status: JobStatus,
}

/* --------- Estructuras para DAG (ruta A) --------- */

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dag {
    pub nodes: Vec<DagNode>,
    pub edges: Vec<(String, String)>, // (from, to)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagNode {
    pub id: String,
    pub op: String,                      // "read_csv", "map", etc.
    pub params: HashMap<String, String>, // ejemplo: {"path": "data/*.csv"}
}

/* --------- Estructuras para tareas y workers --------- */

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub job_id: JobId,
    pub node_id: String,
    pub attempt: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegisterRequest {
    pub hostname: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegisterResponse {
    pub worker_id: WorkerId,
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
