use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

use crate::dag::Dag;

pub type JobId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRequest {
    pub name: String,

    /// DAG de operadores (map, filter, flat_map, reduce_by_key, join, etc.)
    pub dag: Dag,

    /// Paralelismo deseado (particiones / tasks en paralelo)
    pub parallelism: u32,

    /// Patrón de archivos de entrada, ej: "/data/input/*.txt"
    pub input_glob: String,

    /// Directorio base de salida, ej: "/data/output"
    pub output_dir: String,
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

    /// DAG original que se envió para este job
    pub dag: Dag,

    /// Paralelismo configurado para el job
    pub parallelism: u32,

    /// Glob de entrada y directorio de salida concreto del job
    pub input_glob: String,
    pub output_dir: String,

    /// -------- Métricas del job --------
    pub submitted_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub total_tasks: u32,
    pub completed_tasks: u32,
    pub failed_tasks: u32,
    pub retries: u32,
}
