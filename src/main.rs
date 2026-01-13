mod error;
mod query;
mod storage;

use std::io::Write;
use std::time::Instant;

use futures_lite::AsyncBufReadExt;
use glommio::io::stdin;
use glommio::{LocalExecutorBuilder, Placement};

use crate::error::{DbError, DbResult};
use crate::query::QueryEngine;

fn main() -> DbResult<()> {
    println!("Barq v0.1.0 - Thread-Per-Core SQL Engine");
    println!("Type SQL statements or 'exit' to quit.\n");

    LocalExecutorBuilder::new(Placement::Fixed(0))
        .name("barq-core-0")
        .spawn(|| async move { run_repl().await })
        .map_err(|e| DbError::IoError(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?
        .join()
        .map_err(|e| {
            DbError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{:?}", e),
            ))
        })??;

    Ok(())
}

async fn run_repl() -> DbResult<()> {
    let mut engine = QueryEngine::new();
    let mut sin = stdin();

    loop {
        print!("barq> ");
        std::io::stdout().flush().map_err(DbError::IoError)?;

        let mut input = String::new();
        let bytes_read = sin
            .read_line(&mut input)
            .await
            .map_err(DbError::IoError)?;

        if bytes_read == 0 {
            println!("\nGoodbye!");
            break;
        }

        let sql = input.trim();

        if sql.is_empty() {
            continue;
        }

        if sql.eq_ignore_ascii_case("exit") || sql.eq_ignore_ascii_case("quit") {
            println!("Goodbye!");
            break;
        }

        let start = Instant::now();

        match engine.execute(sql) {
            Ok(result) => {
                let elapsed = start.elapsed();
                result.display()?;
                println!("({} us)", elapsed.as_micros());
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }

    Ok(())
}
