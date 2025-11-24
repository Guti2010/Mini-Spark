use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
