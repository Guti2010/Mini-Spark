pub mod job;
pub mod dag;
pub mod task;
pub mod worker;
pub mod results;
pub mod wordcount;

// Re-export para que el resto del código siga usando common::JobRequest, etc.
pub use job::{JobId, JobInfo, JobRequest, JobStatus};
pub use dag::{Dag, DagNode};
pub use task::{Task, TaskId};
pub use worker::{
    WorkerId,
    WorkerRegisterRequest,
    WorkerRegisterResponse,
    WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
    TaskAssignmentRequest,
    TaskAssignmentResponse,
    TaskCompleteRequest,
    TaskCompleteResponse,
};
pub use results::JobResults;
