use anyhow::{Context, Result};
use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    pub aptos_indexer_url: String,
    pub aptos_node_url: String,
    pub aptos_api_key: Option<String>,
    pub protocol_address: String,
    pub batch_size: i64,
    pub poll_interval_ms: u64,
    pub processor_name: String,
    pub start_version: Option<i64>,
    pub rest_request_delay_ms: u64,
    pub max_retries: u32,
    pub retry_base_delay_ms: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let database_url = env::var("DATABASE_URL").context("DATABASE_URL is required")?;
        let aptos_indexer_url = env::var("APTOS_INDEXER_URL")
            .unwrap_or_else(|_| "https://api.testnet.aptoslabs.com/v1/graphql".to_string());
        let aptos_node_url = env::var("APTOS_NODE_URL")
            .unwrap_or_else(|_| "https://api.testnet.aptoslabs.com/v1".to_string());
        let aptos_api_key = env::var("APTOS_API_KEY")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let protocol_address = env::var("PROTOCOL_ADDRESS")
            .or_else(|_| env::var("CONTRACT_ADDRESS"))
            .context("PROTOCOL_ADDRESS (or CONTRACT_ADDRESS) is required")?
            .to_lowercase();
        let batch_size = env::var("BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(200);
        let poll_interval_ms = env::var("POLL_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1500);
        let processor_name = env::var("PROCESSOR_NAME")
            .unwrap_or_else(|_| "AptosRoomRustIndexer".to_string());
        let start_version = env::var("START_VERSION")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v > 0);
        let rest_request_delay_ms = env::var("REST_REQUEST_DELAY_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(120);
        let max_retries = env::var("MAX_RETRIES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(8);
        let retry_base_delay_ms = env::var("RETRY_BASE_DELAY_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(500);

        Ok(Self {
            database_url,
            aptos_indexer_url,
            aptos_node_url,
            aptos_api_key,
            protocol_address,
            batch_size,
            poll_interval_ms,
            processor_name,
            start_version,
            rest_request_delay_ms,
            max_retries,
            retry_base_delay_ms,
        })
    }

    pub fn protocol_prefix(&self) -> String {
        format!("{}::", self.protocol_address)
    }
}
