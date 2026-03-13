mod aptos;
mod config;
mod db;
mod processor;

use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use tokio::time::sleep;

use crate::aptos::AptosClient;
use crate::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let cfg = Config::from_env()?;
    let pool = connect_with_retry(&cfg.database_url, 12, Duration::from_secs(5)).await?;
    init_schema_with_retry(&pool, 12, Duration::from_secs(3)).await?;
    let aptos = AptosClient::new(&cfg)?;

    println!(
        "AptosRoom Rust indexer started (processor={}, contract={})",
        cfg.processor_name, cfg.protocol_address
    );

    loop {
        match processor::process_next_batch(&cfg, &aptos, &pool).await {
            Ok(0) => sleep(Duration::from_millis(cfg.poll_interval_ms)).await,
            Ok(count) => {
                println!("Processed {count} transaction(s) in this batch");
            }
            Err(err) => {
                eprintln!("Indexer error: {err:#}");
                sleep(Duration::from_millis(cfg.poll_interval_ms)).await;
            }
        }
    }
}

async fn connect_with_retry(
    database_url: &str,
    max_attempts: u32,
    delay: Duration,
) -> Result<Pool<Postgres>> {
    let mut last_err: Option<anyhow::Error> = None;

    for attempt in 1..=max_attempts {
        let result = PgPoolOptions::new()
            .max_connections(1)
            .min_connections(0)
            .acquire_timeout(Duration::from_secs(60))
            .idle_timeout(Duration::from_secs(30))
            .max_lifetime(Duration::from_secs(300))
            .test_before_acquire(true)
            .connect(database_url)
            .await;

        match result {
            Ok(pool) => return Ok(pool),
            Err(err) => {
                last_err = Some(err.into());
                eprintln!(
                    "Postgres connect attempt {}/{} failed. Retrying in {}s...",
                    attempt,
                    max_attempts,
                    delay.as_secs()
                );
                sleep(delay).await;
            }
        }
    }

    Err(last_err
        .unwrap_or_else(|| anyhow::anyhow!("failed to connect to postgres")))
    .context("failed to connect to postgres after retries")
}

async fn init_schema_with_retry(
    pool: &Pool<Postgres>,
    max_attempts: u32,
    delay: Duration,
) -> Result<()> {
    let mut last_err: Option<anyhow::Error> = None;

    for attempt in 1..=max_attempts {
        match db::init_schema(pool).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                last_err = Some(err);
                eprintln!(
                    "Schema init attempt {}/{} failed. Retrying in {}s...",
                    attempt,
                    max_attempts,
                    delay.as_secs()
                );
                sleep(delay).await;
            }
        }
    }

    Err(last_err
        .unwrap_or_else(|| anyhow::anyhow!("failed to initialize schema")))
    .context("failed to initialize schema after retries")
}
