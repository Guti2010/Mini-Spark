use anyhow::Result;
use clap::{Parser, Subcommand};
use common::{JobInfo, JobRequest};
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
    /// Envía un job nuevo con un nombre
    Submit {
        #[arg(value_name = "NOMBRE")]
        name: String,
    },
    /// Consulta el estado de un job
    Status {
        #[arg(value_name = "JOB_ID")]
        id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = Client::new();
    let base_url = master_base_url();

    match cli.command {
        Commands::Submit { name } => {
            let url = format!("{}/api/v1/jobs", base_url);
            let resp = client.post(&url).json(&JobRequest { name }).send().await?;
            let job_info: JobInfo = resp.json().await?;

            println!("Job creado:");
            println!("  id: {}", job_info.id);
            println!("  nombre: {}", job_info.name);
            println!("  estado: {:?}", job_info.status);
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
            } else {
                println!("No se encontró el job con id {id}");
            }
        }
    }

    Ok(())
}
