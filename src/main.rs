mod error;
mod query;
mod storage;

use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::error::{DbError, DbResult};
use crate::query::QueryEngine;

const PORT: u16 = 8080;

#[tokio::main]
async fn main() -> DbResult<()> {
    println!("Barq v0.1.0 - SQL Engine");
    println!("Starting TCP server on port {}...", PORT);

    let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT))
        .await
        .map_err(DbError::IoError)?;

    println!("Barq listening on 0.0.0.0:{}", PORT);
    println!("Ready to accept connections.\n");

    let engine = Arc::new(Mutex::new(QueryEngine::new()));

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let engine = engine.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, engine, addr.to_string()).await {
                        eprintln!("Connection error: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Accept error: {}", e);
            }
        }
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    engine: Arc<Mutex<QueryEngine>>,
    peer: String,
) -> DbResult<()> {
    println!("New connection from {}", peer);

    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // Send welcome message
    writer
        .write_all(b"Barq v0.1.0 - SQL Engine\nReady.\n")
        .await
        .map_err(DbError::IoError)?;

    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await.map_err(DbError::IoError)?;

        if bytes_read == 0 {
            println!("Connection closed: {}", peer);
            break;
        }

        let sql = line.trim();
        if sql.is_empty() {
            continue;
        }

        if sql.eq_ignore_ascii_case("exit") || sql.eq_ignore_ascii_case("quit") {
            writer
                .write_all(b"Goodbye!\n")
                .await
                .map_err(DbError::IoError)?;
            break;
        }

        let start = Instant::now();
        let response = {
            let mut eng = engine.lock().await;
            match eng.execute(sql) {
                Ok(result) => {
                    let elapsed = start.elapsed();
                    format!("{}\n({} us)\n", result.to_response(), elapsed.as_micros())
                }
                Err(e) => format!("Error: {}\n", e),
            }
        };

        writer
            .write_all(response.as_bytes())
            .await
            .map_err(DbError::IoError)?;
    }

    Ok(())
}
