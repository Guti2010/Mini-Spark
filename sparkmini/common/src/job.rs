use serde::{Deserialize, Serialize};

pub type JobId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRequest {
    pub name: String,
    /// Patrón de archivos de entrada, ej: "/data/input/*.txt"
    pub input_glob: String,
    /// Directorio base de salida (ej: "/data/output")
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
    /// El glob de entrada que se usó para generar las tareas
    pub input_glob: String,
    /// Directorio de salida específico de este job, ej: "/data/output/<job_id>"
    pub output_dir: String,
}
