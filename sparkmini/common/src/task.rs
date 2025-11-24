use serde::{Deserialize, Serialize};

use crate::job::JobId;

pub type TaskId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub job_id: JobId,
    pub node_id: String,
    pub attempt: u32,

    /// Ruta del archivo de entrada (dentro del contenedor)
    pub input_path: String,
    /// Ruta del archivo de salida para esta partición
    pub output_path: String,
}
