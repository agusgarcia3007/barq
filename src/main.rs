mod catalog;
mod error;
mod executor;
mod protocol;
mod storage;
mod types;

use std::sync::Arc;

use pgwire::api::auth::md5pass::MakeMd5PasswordAuthStartupHandler;
use pgwire::api::auth::DefaultServerParameterProvider;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::catalog::Catalog;
use crate::protocol::{BarqAuthSource, MakeBarqHandler};
use crate::storage::StorageEngine;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "barq=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Get configuration from environment
    let host = std::env::var("BARQ_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("BARQ_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(5432);
    let username = std::env::var("BARQ_USER").unwrap_or_else(|_| "barq".to_string());
    let password = std::env::var("BARQ_PASSWORD").unwrap_or_else(|_| "barq".to_string());

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║                    Barq v{}                           ║", VERSION);
    println!("║     Lightning-fast PostgreSQL-compatible database        ║");
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  Powered by Apache Arrow columnar storage                ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();

    // Initialize catalog and storage
    let catalog = Arc::new(Catalog::new());
    let storage = Arc::new(StorageEngine::new());

    // Create handler factory
    let handler_factory = Arc::new(MakeBarqHandler::new(catalog.clone(), storage.clone()));

    // Create auth source
    let mut auth_source = BarqAuthSource::new();
    auth_source.add_user(&username, &password);

    // Create authenticator
    let authenticator = Arc::new(MakeMd5PasswordAuthStartupHandler::new(
        Arc::new(auth_source),
        Arc::new(DefaultServerParameterProvider::new()),
    ));

    // Bind to address
    let addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&addr).await?;

    tracing::info!("Barq listening on {}", addr);
    println!("Listening on postgresql://{}:{}@{}:{}/barq", username, password, host, port);
    println!();
    println!("Ready to accept connections.");

    // Accept connections
    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                tracing::info!("New connection from {}", peer_addr);

                let handler = handler_factory.make();
                let authenticator = authenticator.make();

                tokio::spawn(async move {
                    if let Err(e) = process_socket(socket, None, authenticator, handler.clone(), handler).await
                    {
                        tracing::error!("Connection error: {}", e);
                    }
                    tracing::info!("Connection closed: {}", peer_addr);
                });
            }
            Err(e) => {
                tracing::error!("Accept error: {}", e);
            }
        }
    }
}
