use serde::{Deserialize, Serialize};

use crate::job::JobId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResults {
    pub job_id: JobId,
    /// Directorio donde quedaron los outputs de este job
    pub output_dir: String,
    /// Nombres de archivos de salida dentro de output_dir
    pub files: Vec<String>,
}
