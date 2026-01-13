mod error;
mod query;
mod storage;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use futures_lite::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use futures_lite::AsyncReadExt;
use glommio::net::TcpListener;
use glommio::{LocalExecutorBuilder, Placement};

use crate::error::{DbError, DbResult};
use crate::query::QueryEngine;

const PORT: u16 = 8080;

fn main() -> DbResult<()> {
    println!("Barq v0.1.0 - Thread-Per-Core SQL Engine");
    println!("Starting TCP server on port {}...", PORT);

    LocalExecutorBuilder::new(Placement::Fixed(0))
        .name("barq-core-0")
        .spawn(|| async move { run_server().await })
        .map_err(|e| {
            DbError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{}", e),
            ))
        })?
        .join()
        .map_err(|e| {
            DbError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{:?}", e),
            ))
        })??;

    Ok(())
}

async fn run_server() -> DbResult<()> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT)).map_err(|e| {
        DbError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("{}", e),
        ))
    })?;

    println!("Barq listening on 0.0.0.0:{}", PORT);
    println!("Ready to accept connections.\n");

    // Shared engine for all connections (single-threaded, no locks needed)
    let engine = Rc::new(RefCell::new(QueryEngine::new()));

    loop {
        match listener.accept().await {
            Ok(stream) => {
                let engine = engine.clone();
                glommio::spawn_local(async move {
                    if let Err(e) = handle_connection(stream, engine).await {
                        eprintln!("Connection error: {}", e);
                    }
                })
                .detach();
            }
            Err(e) => {
                eprintln!("Accept error: {}", e);
            }
        }
    }
}

async fn handle_connection(
    stream: glommio::net::TcpStream,
    engine: Rc<RefCell<QueryEngine>>,
) -> DbResult<()> {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    println!("New connection from {}", peer);

    let mut buffered = BufReader::new(stream);

    // Send welcome message
    let welcome = b"Barq v0.1.0 - Thread-Per-Core SQL Engine\nReady.\n";
    buffered.get_mut().write_all(welcome).await.map_err(|e| {
        DbError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("{}", e),
        ))
    })?;

    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = buffered.read_line(&mut line).await.map_err(|e| {
            DbError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{}", e),
            ))
        })?;

        if bytes_read == 0 {
            println!("Connection closed: {}", peer);
            break;
        }

        let sql = line.trim();
        if sql.is_empty() {
            continue;
        }

        if sql.eq_ignore_ascii_case("exit") || sql.eq_ignore_ascii_case("quit") {
            buffered
                .get_mut()
                .write_all(b"Goodbye!\n")
                .await
                .map_err(|e| {
                    DbError::IoError(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("{}", e),
                    ))
                })?;
            break;
        }

        let start = Instant::now();
        let response = {
            let mut eng = engine.borrow_mut();
            match eng.execute(sql) {
                Ok(result) => {
                    let elapsed = start.elapsed();
                    format!("{}\n({} us)\n", result.to_response(), elapsed.as_micros())
                }
                Err(e) => format!("Error: {}\n", e),
            }
        };

        buffered
            .get_mut()
            .write_all(response.as_bytes())
            .await
            .map_err(|e| {
                DbError::IoError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("{}", e),
                ))
            })?;
    }

    Ok(())
}
