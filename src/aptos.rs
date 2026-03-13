use anyhow::{anyhow, Context, Result};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;

use crate::config::Config;

#[derive(Clone)]
pub struct AptosClient {
    http: Client,
    graphql_url: String,
    node_url: String,
    rest_request_delay_ms: u64,
    max_retries: u32,
    retry_base_delay_ms: u64,
}

#[derive(Debug, Deserialize)]
struct GraphQlResponse {
    data: Option<GraphQlData>,
    errors: Option<Vec<GraphQlError>>,
}

#[derive(Debug, Deserialize)]
struct GraphQlData {
    account_transactions: Vec<AccountTransactionRow>,
}

#[derive(Debug, Deserialize)]
struct GraphQlError {
    message: String,
}

#[derive(Debug, Deserialize)]
struct AccountTransactionRow {
    transaction_version: Value,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RestTransaction {
    pub timestamp: Option<String>,
    #[serde(default)]
    pub events: Vec<TransactionEvent>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TransactionEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: Value,
    pub guid: Option<EventGuid>,
    pub sequence_number: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EventGuid {
    pub account_address: String,
}

impl AptosClient {
    pub fn new(cfg: &Config) -> Result<Self> {
        let mut headers = HeaderMap::new();
        if let Some(api_key) = cfg.aptos_api_key.as_deref() {
            headers.insert("x-api-key", HeaderValue::from_str(api_key)?);
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {api_key}"))?,
            );
        }

        let http = Client::builder()
            .default_headers(headers)
            .build()
            .context("failed to build reqwest client")?;

        Ok(Self {
            http,
            graphql_url: cfg.aptos_indexer_url.clone(),
            node_url: cfg.aptos_node_url.clone(),
            rest_request_delay_ms: cfg.rest_request_delay_ms,
            max_retries: cfg.max_retries,
            retry_base_delay_ms: cfg.retry_base_delay_ms,
        })
    }

    pub async fn fetch_protocol_versions(
        &self,
        protocol_address: &str,
        after_version: i64,
        batch_size: i64,
    ) -> Result<Vec<i64>> {
        let query = r#"
            query GetProtocolTxns($addr: String!, $afterVersion: bigint!, $limit: Int!) {
              account_transactions(
                limit: $limit,
                order_by: { transaction_version: asc },
                where: {
                  account_address: { _eq: $addr },
                  transaction_version: { _gt: $afterVersion }
                }
              ) {
                transaction_version
              }
            }
        "#;

        let body = json!({
            "query": query,
            "variables": {
                "addr": protocol_address,
                "afterVersion": after_version,
                "limit": batch_size,
            }
        });

        let resp = self
            .http
            .post(&self.graphql_url)
            .json(&body)
            .send()
            .await
            .context("failed to call Aptos GraphQL indexer")?
            .error_for_status()
            .context("Aptos GraphQL indexer returned error status")?;

        let payload: GraphQlResponse = resp
            .json()
            .await
            .context("failed to decode GraphQL payload")?;

        if let Some(errors) = payload.errors {
            let joined = errors
                .into_iter()
                .map(|e| e.message)
                .collect::<Vec<_>>()
                .join("; ");
            return Err(anyhow!("GraphQL errors: {joined}"));
        }

        let Some(data) = payload.data else {
            return Ok(Vec::new());
        };

        let versions = data
            .account_transactions
            .into_iter()
            .map(|row| parse_i64_value(&row.transaction_version, "transaction_version"))
            .collect::<Result<Vec<_>>>()?;

        Ok(versions)
    }

    pub async fn fetch_transaction(&self, version: i64) -> Result<RestTransaction> {
        let url = format!(
            "{}/transactions/by_version/{}",
            self.node_url.trim_end_matches('/'),
            version
        );

        for attempt in 0..=self.max_retries {
            if self.rest_request_delay_ms > 0 {
                sleep(Duration::from_millis(self.rest_request_delay_ms)).await;
            }

            let resp = self
                .http
                .get(&url)
                .send()
                .await
                .with_context(|| format!("failed to fetch transaction {version}"))?;

            if resp.status().is_success() {
                let txn = resp
                    .json::<RestTransaction>()
                    .await
                    .with_context(|| format!("failed to decode transaction {version}"))?;
                return Ok(txn);
            }

            let status = resp.status();
            if should_retry(status) && attempt < self.max_retries {
                let retry_after_ms = parse_retry_after_ms(resp.headers().get("retry-after"));
                let backoff_ms = compute_backoff_ms(self.retry_base_delay_ms, attempt);
                let wait_ms = retry_after_ms.unwrap_or(backoff_ms);
                eprintln!(
                    "Rate-limited/error on tx {} (status {}), retrying in {} ms (attempt {}/{})",
                    version,
                    status,
                    wait_ms,
                    attempt + 1,
                    self.max_retries
                );
                sleep(Duration::from_millis(wait_ms)).await;
                continue;
            }

            return Err(anyhow!(
                "error response for transaction {}: HTTP {}",
                version,
                status
            ));
        }

        Err(anyhow!(
            "retry loop exhausted unexpectedly for transaction {}",
            version
        ))
    }
}

fn should_retry(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn parse_retry_after_ms(retry_after: Option<&HeaderValue>) -> Option<u64> {
    let seconds = retry_after
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())?;
    Some(seconds.saturating_mul(1000))
}

fn compute_backoff_ms(base_delay_ms: u64, attempt: u32) -> u64 {
    let shift = attempt.min(8);
    let multiplier = 1u64 << shift;
    base_delay_ms.saturating_mul(multiplier)
}

fn parse_i64_value(value: &Value, field: &str) -> Result<i64> {
    if let Some(v) = value.as_i64() {
        return Ok(v);
    }

    if let Some(v) = value.as_u64() {
        return i64::try_from(v).context("numeric conversion overflow");
    }

    if let Some(v) = value.as_str() {
        return v
            .parse::<i64>()
            .with_context(|| format!("failed parsing {field} as i64"));
    }

    Err(anyhow!("unsupported GraphQL type for field '{field}'"))
}
