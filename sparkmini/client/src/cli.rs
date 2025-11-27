use anyhow::Result;
use clap::{Parser, Subcommand};
use common::{Dag, DagNode, JobInfo, JobRequest, JobResults, WorkerMetrics};
use reqwest::Client;
use std::env;
use common::engine;
/// Igual que en el worker:
/// - En Docker: MASTER_URL=http://master:8080
/// - Local: default http://localhost:8080
fn master_base_url() -> String {
    env::var("MASTER_URL").unwrap_or_else(|_| "http://localhost:8080".to_string())
}

#[derive(Parser)]
#[command(name = "client")]
#[command(about = "CLI simple para hablar con el master")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Envía un job nuevo con un nombre (WordCount fijo)
    Submit {
        #[arg(value_name = "NOMBRE")]
        name: String,
    },
    /// Consulta el estado de un job
    Status {
        #[arg(value_name = "JOB_ID")]
        id: String,
    },
    /// Lista los archivos de salida de un job
    Results {
        #[arg(value_name = "JOB_ID")]
        id: String,
    },

    Workers,

    /// Demo: join entre dos CSV por clave usando el engine local
    Join {
        /// Ruta al CSV de ventas (dentro del contenedor)
        #[arg(value_name = "VENTAS_CSV")]
        left: String,

        /// Ruta al CSV de catalogo (dentro del contenedor)
        #[arg(value_name = "CATALOGO_CSV")]
        right: String,

        /// Nombre de la columna clave
        #[arg(long, default_value = "product_id")]
        key: String,

        /// Ruta de salida JSONL (dentro del contenedor)
        #[arg(long, default_value = "/data/output/join_ventas_catalogo.jsonl")]
        output: String,
    },
}

/// Construye un DAG fijo de WordCount estilo enunciado:
/// read -> flat -> map1 -> agg
/// Devuelve: (dag, input_glob)
fn build_wordcount_dag() -> (Dag, String) {
    // Patrón de entrada que usará el master para crear tareas
    let input_glob = "/data/input/*".to_string();

    // Nodo "read": lee archivos de texto (podría ser read_csv si usas CSV)
    let read = DagNode {
        id: "read".to_string(),
        op: "read_text".to_string(),      // o "read_csv" si quieres CSV
        path: Some(input_glob.clone()),   // igual al input_glob del job
        partitions: Some(4),              // mismo valor que parallelism
        fn_name: None,
        key: None,
    };

    // Nodo "flat": flat_map(tokenize)
    let flat = DagNode {
        id: "flat".to_string(),
        op: "flat_map".to_string(),
        path: None,
        partitions: None,
        fn_name: Some("tokenize".to_string()),
        key: None,
    };

    // Nodo "map1": map(to_lower)
    let map1 = DagNode {
        id: "map1".to_string(),
        op: "map".to_string(),
        path: None,
        partitions: None,
        fn_name: Some("to_lower".to_string()),
        key: None,
    };

    // Nodo "agg": reduce_by_key(sum) usando key="token"
    let agg = DagNode {
        id: "agg".to_string(),
        op: "reduce_by_key".to_string(),
        path: None,
        partitions: None,
        fn_name: Some("sum".to_string()),
        key: Some("token".to_string()),
    };

    let dag = Dag {
        nodes: vec![read, flat, map1, agg],
        edges: vec![
            ("read".to_string(), "flat".to_string()),
            ("flat".to_string(), "map1".to_string()),
            ("map1".to_string(), "agg".to_string()),
        ],
    };

    (dag, input_glob)
}


pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let client = Client::new();
    let base_url = master_base_url();

    match cli.command {
        Commands::Submit { name } => {
            let url = format!("{}/api/v1/jobs", base_url);

            // Construimos el DAG fijo de WordCount y el patrón de entrada
            let (dag, input_glob) = build_wordcount_dag();

            // OJO: JobRequest en common solo tiene estos campos ahora
            let req = JobRequest {
                name,
                dag,
                parallelism: 4, 
                input_glob,
                output_dir: "/data/output".to_string(),
            };

            let resp = client.post(&url).json(&req).send().await?;
            let job_info: JobInfo = resp.json().await?;

            println!("Job creado:");
            println!("  id: {}", job_info.id);
            println!("  nombre: {}", job_info.name);
            println!("  estado: {:?}", job_info.status);
            println!("  parallelism: {}", job_info.parallelism);
            println!("  input_glob: {}", job_info.input_glob);
            println!("  output_dir: {}", job_info.output_dir);
            println!("  submitted_at: {}", job_info.submitted_at);
        }

        Commands::Status { id } => {
            let url = format!("{}/api/v1/jobs/{}", base_url, id);
            let resp = client.get(&url).send().await?;
            if resp.status().is_success() {
                let job: JobInfo = resp.json().await?;
                println!("Job:");
                println!("  id: {}", job.id);
                println!("  nombre: {}", job.name);
                println!("  estado: {:?}", job.status);

                // métricas de tareas
                println!(
                    "  tareas: total={}, completadas={}, fallidas={}, reintentos={}",
                    job.total_tasks, job.completed_tasks, job.failed_tasks, job.retries
                );

                // progreso calculado localmente
                let done = job.completed_tasks + job.failed_tasks;
                if job.total_tasks > 0 {
                    let pct = (done as f64 / job.total_tasks as f64) * 100.0;
                    println!("  progreso: {:.1}%", pct);
                } else {
                    println!("  progreso: (sin tareas)");
                }

                println!("  input_glob: {}", job.input_glob);
                println!("  output_dir: {}", job.output_dir);
                println!("  submitted_at: {}", job.submitted_at);
                if let Some(ref started) = job.started_at {
                    println!("  iniciado: {}", started);
                }
                if let Some(ref done) = job.finished_at {
                    println!("  finalizado: {}", done);
                }
            } else {
                println!("Error: job no encontrado (status {})", resp.status());
            }
        }

        Commands::Results { id } => {
            let url = format!("{}/api/v1/jobs/{id}/results", base_url);
            let resp = client.get(&url).send().await?;

            if resp.status().is_success() {
                let results: JobResults = resp.json().await?;
                println!("Resultados para job {}:", results.job_id);
                println!("  directorio de salida: {}", results.output_dir);
                if results.files.is_empty() {
                    println!("  (sin archivos de salida)");
                } else {
                    println!("  archivos:");
                    for f in results.files {
                        println!("    - {}", f);
                    }
                }
            } else {
                println!("No se encontraron resultados para job {id}");
            }
        }

        Commands::Join { left, right, key, output } => {
            println!("Ejecutando join local entre CSVs:");
            println!("  left : {}", left);
            println!("  right: {}", right);
            println!("  key  : {}", key);
            println!("  out  : {}", output);

            if let Err(e) = engine::join_csv_in_memory(&left, &right, &key, &output) {
                eprintln!("Error ejecutando join: {e:?}");
                std::process::exit(1);
            }

            println!("Join completado. Archivo de salida: {}", output);
        }


        Commands::Workers => {
            let url = format!("{}/api/v1/workers", base_url);
            let resp = client.get(&url).send().await?;
            if resp.status().is_success() {
                let workers: Vec<WorkerMetrics> = resp.json().await?;
                if workers.is_empty() {
                    println!("No hay workers registrados.");
                } else {
                    for w in workers {
                        println!("Worker {}", w.worker_id);
                        println!("  host           : {}", w.hostname);
                        println!("  dead           : {}", w.dead);
                        println!(
                            "  last_heartbeat : {} s ago",
                            w.last_heartbeat_secs_ago
                        );
                        println!(
                            "  concurrency    : max={}",
                            w.max_concurrency
                        );
                        println!(
                            "  tareas         : started={}, ok={}, failed={}",
                            w.tasks_started, w.tasks_succeeded, w.tasks_failed
                        );
                        if let Some(avg) = w.avg_task_ms {
                            println!("  avg_task_ms    : {:.1}", avg);
                        } else {
                            println!("  avg_task_ms    : (sin datos)");
                        }
                        if let Some(cpu) = w.cpu_percent {
                            println!("  cpu_percent    : {:.1}%", cpu);
                        } else {
                            println!("  cpu_percent    : (sin datos)");
                        }
                        if let Some(mem) = w.mem_bytes {
                            println!("  mem_bytes      : {}", mem);
                        } else {
                            println!("  mem_bytes      : (sin datos)");
                        }
                        println!();
                    }
                }
            } else {
                println!(
                    "Error consultando /api/v1/workers (status {})",
                    resp.status()
                );
            }
        }

    }

    Ok(())
}
