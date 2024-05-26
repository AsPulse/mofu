use clap::Parser;
use dotenvy::dotenv;

use crate::config::AppArgs;

pub mod config;

#[tokio::main]
async fn main() {
    let _ = dotenv();

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    tracing::info!("Mofu v{}", env!("CARGO_PKG_VERSION"));

    match mongo_nfs().await {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("{}", e);
            std::process::exit(1);
        }
    }
}

async fn mongo_nfs() -> Result<(), Box<dyn std::error::Error>> {
    let args: AppArgs = AppArgs::parse();

    let config = config::Config::from_file(&args.config).await?;

    tracing::info!("Mofu NFS server starting on port {}", args.port);

    Ok(())
}
