use serde::{Deserialize, Serialize};

use crate::job::JobId;

pub type TaskId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub job_id: JobId,

    /// Nodo lógico del DAG asociado a esta tarea (por ahora "wordcount" fijo)
    pub node_id: String,

    /// Número de intentos (para reintentos)
    pub attempt: u32,

    /// Etapa lógica del job (0, 1, 2, ...)
    /// Por ahora lo dejamos siempre en 0, pero queda listo para multi-stage.
    pub stage: u32,

    /// Partición lógica dentro de la etapa (0..parallelism-1)
    pub partition: u32,

    /// Paralelismo total del job (cuántas particiones máximo)
    pub parallelism: u32,

    /// Ruta del archivo de entrada (dentro del contenedor)
    pub input_path: String,

    /// Ruta del archivo de salida para esta tarea
    pub output_path: String,
}
