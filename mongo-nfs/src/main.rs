use clap::Parser;
use dotenvy::dotenv;
use futures::{StreamExt, TryStreamExt};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::config::AppArgs;
use crate::vfs::VFSMofuFS;

use self::db::{MongoDB, MongoDBError};

pub mod config;
pub mod db;
mod vfs;

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

    let db = Arc::new(
        tokio_stream::iter(&config.sources)
            .map(|(k, v)| async move {
                let db = MongoDB::new(k.clone(), &v.uri, &v.db).await;
                (k, db)
            })
            .buffer_unordered(5)
            .map(|(k, v)| Ok::<(String, MongoDB), MongoDBError>((k.to_string(), v?)))
            .try_collect::<BTreeMap<_, _>>()
            .await?,
    );

    let fs = VFSMofuFS::new(config, db).await?;

    let listener = NFSTcpListener::bind(&format!("{}:{}", args.host, args.port), fs).await?;

    listener.handle_forever().await?;

    Ok(())
}
