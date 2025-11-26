use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dag {
    pub nodes: Vec<DagNode>,
    pub edges: Vec<(String, String)>, // (from, to)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagNode {
    pub id: String,
    pub op: String, // "read_csv", "map", "flat_map", "reduce_by_key", etc.

    /// Ruta o patrón de archivos, ej: "data/*.csv".
    /// Sólo tiene sentido en nodos de lectura (read_xxx).
    pub path: Option<String>,

    /// Paralelismo / particiones para este nodo.
    /// Ej: 4
    pub partitions: Option<u32>,

    /// Nombre de la función asociada, se serializa como "fn" en el JSON.
    /// Ej: "tokenize", "to_lower", "sum".
    #[serde(rename = "fn")]
    pub fn_name: Option<String>,

    /// Campo clave para reduce_by_key o join, ej: "token".
    pub key: Option<String>,
}
