use anyhow::Result;
use clap::{Parser, Subcommand};
use common::{JobInfo, JobRequest, JobResults};
use reqwest::Client;
use std::env;

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
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let client = Client::new();
    let base_url = master_base_url();

    match cli.command {
        Commands::Submit { name } => {
            let url = format!("{}/api/v1/jobs", base_url);

            // Defaults:
            // - lee /data/input/*.txt
            // - escribe en /data/output/<job_id>/
            let req = JobRequest {
                name,
                input_glob: "/data/input/*.txt".to_string(),
                output_dir: "/data/output".to_string(),
            };

            let resp = client.post(&url).json(&req).send().await?;
            let job_info: JobInfo = resp.json().await?;

            println!("Job creado:");
            println!("  id: {}", job_info.id);
            println!("  nombre: {}", job_info.name);
            println!("  estado: {:?}", job_info.status);
            println!("  input_glob: {}", job_info.input_glob);
            println!("  output_dir: {}", job_info.output_dir);
        }
        Commands::Status { id } => {
            let url = format!("{}/api/v1/jobs/{id}", base_url);
            let resp = client.get(&url).send().await?;

            if resp.status().is_success() {
                let job_info: JobInfo = resp.json().await?;
                println!("Job:");
                println!("  id: {}", job_info.id);
                println!("  nombre: {}", job_info.name);
                println!("  estado: {:?}", job_info.status);
                println!("  input_glob: {}", job_info.input_glob);
                println!("  output_dir: {}", job_info.output_dir);
            } else {
                println!("No se encontró el job con id {id}");
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
    }

    Ok(())
}
