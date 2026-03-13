use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{Pool, Postgres, Transaction};

use crate::aptos::{AptosClient, RestTransaction, TransactionEvent};
use crate::config::Config;

pub async fn process_next_batch(
    cfg: &Config,
    aptos: &AptosClient,
    pool: &Pool<Postgres>,
) -> Result<usize> {
    let checkpoint = load_checkpoint(pool, &cfg.processor_name).await?;
    let last_processed = if checkpoint == 0 {
        cfg.start_version
            .map(|v| v.saturating_sub(1))
            .unwrap_or(checkpoint)
    } else {
        checkpoint
    };

    let versions = aptos
        .fetch_protocol_versions(&cfg.protocol_address, last_processed, cfg.batch_size)
        .await?;

    if versions.is_empty() {
        return Ok(0);
    }

    let mut processed = 0usize;
    for version in versions {
        let txn = aptos.fetch_transaction(version).await?;
        process_single_tx(cfg, pool, version, &txn).await?;
        processed += 1;
    }

    Ok(processed)
}

async fn process_single_tx(
    cfg: &Config,
    pool: &Pool<Postgres>,
    version: i64,
    txn: &RestTransaction,
) -> Result<()> {
    let protocol_prefix = cfg.protocol_prefix();
    let tx_timestamp_usecs = txn
        .timestamp
        .as_deref()
        .and_then(|v| v.parse::<i64>().ok());

    let mut db_tx = pool.begin().await.context("failed to start db tx")?;

    for (idx, event) in txn.events.iter().enumerate() {
        let event_type = event.event_type.to_lowercase();
        if !event_type.starts_with(&protocol_prefix) {
            continue;
        }

        persist_raw_event(
            &mut db_tx,
            version,
            idx as i32,
            &event_type,
            event,
            tx_timestamp_usecs,
        )
        .await?;

        if let Err(err) = apply_projection(&mut db_tx, version, idx as i32, &event_type, event).await {
            eprintln!(
                "Projection warning for tx={} event_index={} type={}: {err:#}",
                version, idx, event_type
            );
        }
    }

    save_checkpoint(&mut db_tx, &cfg.processor_name, version).await?;
    db_tx
        .commit()
        .await
        .context("failed committing processed transaction")?;

    Ok(())
}

async fn load_checkpoint(pool: &Pool<Postgres>, processor_name: &str) -> Result<i64> {
    let row: Option<(i64,)> = sqlx::query_as(
        r#"
        SELECT last_processed_version
        FROM indexer_state
        WHERE processor_name = $1
        "#,
    )
    .bind(processor_name)
    .fetch_optional(pool)
    .await
    .context("failed loading checkpoint")?;

    Ok(row.map(|v| v.0).unwrap_or(0))
}

async fn save_checkpoint(
    tx: &mut Transaction<'_, Postgres>,
    processor_name: &str,
    version: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO indexer_state (processor_name, last_processed_version)
        VALUES ($1, $2)
        ON CONFLICT (processor_name)
        DO UPDATE SET
            last_processed_version = EXCLUDED.last_processed_version,
            updated_at = NOW()
        "#,
    )
    .bind(processor_name)
    .bind(version)
    .execute(&mut **tx)
    .await
    .context("failed to save checkpoint")?;

    Ok(())
}

async fn persist_raw_event(
    tx: &mut Transaction<'_, Postgres>,
    txn_version: i64,
    event_index: i32,
    event_type: &str,
    event: &TransactionEvent,
    tx_timestamp_usecs: Option<i64>,
) -> Result<()> {
    let account_address = event
        .guid
        .as_ref()
        .map(|g| g.account_address.to_lowercase())
        .unwrap_or_default();
    let sequence_number = event
        .sequence_number
        .as_deref()
        .and_then(|v| v.parse::<i64>().ok());

    sqlx::query(
        r#"
        INSERT INTO raw_events (
            txn_version,
            event_index,
            event_type,
            account_address,
            sequence_number,
            txn_timestamp_usecs,
            payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (txn_version, event_index) DO NOTHING
        "#,
    )
    .bind(txn_version)
    .bind(event_index)
    .bind(event_type)
    .bind(account_address)
    .bind(sequence_number)
    .bind(tx_timestamp_usecs)
    .bind(&event.data)
    .execute(&mut **tx)
    .await
    .context("failed to persist raw event")?;

    Ok(())
}

async fn apply_projection(
    tx: &mut Transaction<'_, Postgres>,
    txn_version: i64,
    event_index: i32,
    event_type: &str,
    event: &TransactionEvent,
) -> Result<()> {
    if event_type.ends_with("::room::roomcreated") {
        return handle_room_created(tx, txn_version, &event.data).await;
    }

    if event_type.ends_with("::room::roomstatechanged") {
        return handle_room_state_changed(tx, txn_version, event_index, &event.data).await;
    }

    if event_type.ends_with("::keycard::keycardminted") {
        return handle_keycard_minted(tx, txn_version, &event.data).await;
    }

    if event_type.ends_with("::keycard::keycardstatsupdated") {
        return handle_keycard_stats_updated(tx, txn_version, &event.data).await;
    }

    if event_type.ends_with("::settlement::settlementapproved") {
        return handle_settlement_approved(tx, txn_version, &event.data).await;
    }

    if event_type.ends_with("::settlement::roomsettled") {
        return handle_room_settled(tx, txn_version, &event.data).await;
    }

    Ok(())
}

async fn handle_room_created(
    tx: &mut Transaction<'_, Postgres>,
    txn_version: i64,
    data: &Value,
) -> Result<()> {
    let room_id = read_i64(data, "room_id")?;
    let creator_address = read_string(data, "client")?.to_lowercase();
    let category = read_string(data, "category")?;
    let task_reward = read_i64(data, "task_reward")?;
    let created_at_onchain = read_i64(data, "timestamp")?;

    sqlx::query(
        r#"
        INSERT INTO rooms (
            room_id,
            creator_address,
            category,
            task_reward,
            created_at_onchain,
            state,
            last_txn_version
        )
        VALUES ($1, $2, $3, $4, $5, 1, $6)
        ON CONFLICT (room_id)
        DO UPDATE SET
            creator_address = EXCLUDED.creator_address,
            category = EXCLUDED.category,
            task_reward = EXCLUDED.task_reward,
            created_at_onchain = EXCLUDED.created_at_onchain,
            last_txn_version = EXCLUDED.last_txn_version,
            updated_at = NOW()
        "#,
    )
    .bind(room_id)
    .bind(creator_address)
    .bind(category)
    .bind(task_reward)
    .bind(created_at_onchain)
    .bind(txn_version)
    .execute(&mut **tx)
    .await
    .context("failed handling RoomCreated")?;

    Ok(())
}

async fn handle_room_state_changed(
    tx: &mut Transaction<'_, Postgres>,
    txn_version: i64,
    event_index: i32,
    data: &Value,
) -> Result<()> {
    let room_id = read_i64(data, "room_id")?;
    let from_state = read_i64(data, "from_state")?;
    let to_state = read_i64(data, "to_state")?;
    let timestamp_onchain = read_i64(data, "timestamp")?;

    sqlx::query(
        r#"
        INSERT INTO room_state_history (
            txn_version,
            event_index,
            room_id,
            from_state,
            to_state,
            timestamp_onchain
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (txn_version, event_index) DO NOTHING
        "#,
    )
    .bind(txn_version)
    .bind(event_index)
    .bind(room_id)
    .bind(from_state as i32)
    .bind(to_state as i32)
    .bind(timestamp_onchain)
    .execute(&mut **tx)
    .await
    .context("failed writing room_state_history")?;

    sqlx::query(
        r#"
        UPDATE rooms
        SET state = $2,
            last_txn_version = $3,
            updated_at = NOW()
        WHERE room_id = $1
        "#,
    )
    .bind(room_id)
    .bind(to_state as i32)
    .bind(txn_version)
    .execute(&mut **tx)
    .await
    .context("failed applying RoomStateChanged to rooms")?;

    Ok(())
}

async fn handle_keycard_minted(
    tx: &mut Transaction<'_, Postgres>,
    txn_version: i64,
    data: &Value,
) -> Result<()> {
    let owner_address = read_string(data, "owner")?.to_lowercase();
    let keycard_id = read_i64(data, "keycard_id")?;
    let created_at_onchain = read_i64(data, "timestamp")?;

    sqlx::query(
        r#"
        INSERT INTO keycards (
            owner_address,
            keycard_id,
            created_at_onchain,
            last_txn_version
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (owner_address)
        DO UPDATE SET
            keycard_id = EXCLUDED.keycard_id,
            created_at_onchain = EXCLUDED.created_at_onchain,
            last_txn_version = EXCLUDED.last_txn_version,
            updated_at = NOW()
        "#,
    )
    .bind(owner_address)
    .bind(keycard_id)
    .bind(created_at_onchain)
    .bind(txn_version)
    .execute(&mut **tx)
    .await
    .context("failed handling KeycardMinted")?;

    Ok(())
}

async fn handle_keycard_stats_updated(
    tx: &mut Transaction<'_, Postgres>,
    txn_version: i64,
    data: &Value,
) -> Result<()> {
    let owner_address = read_string(data, "owner")?.to_lowercase();
    let tasks_completed = read_i64(data, "tasks_completed")?;
    let avg_score = read_i64(data, "avg_score")?;
    let jury_participations = read_i64(data, "jury_participations")?;
    let variance_flags = read_i64(data, "variance_flags")?;

    sqlx::query(
        r#"
        INSERT INTO keycards (
            owner_address,
            keycard_id,
            created_at_onchain,
            tasks_completed,
            avg_score,
            jury_participations,
            variance_flags,
            last_txn_version
        )
        VALUES ($1, 0, 0, $2, $3, $4, $5, $6)
        ON CONFLICT (owner_address)
        DO UPDATE SET
            tasks_completed = EXCLUDED.tasks_completed,
            avg_score = EXCLUDED.avg_score,
            jury_participations = EXCLUDED.jury_participations,
            variance_flags = EXCLUDED.variance_flags,
            last_txn_version = EXCLUDED.last_txn_version,
            updated_at = NOW()
        "#,
    )
    .bind(owner_address)
    .bind(tasks_completed)
    .bind(avg_score)
    .bind(jury_participations)
    .bind(variance_flags)
    .bind(txn_version)
    .execute(&mut **tx)
    .await
    .context("failed handling KeycardStatsUpdated")?;

    Ok(())
}

async fn handle_settlement_approved(
    tx: &mut Transaction<'_, Postgres>,
    txn_version: i64,
    data: &Value,
) -> Result<()> {
    let room_id = read_i64(data, "room_id")?;
    let client_address = read_string(data, "client")?.to_lowercase();
    let timestamp_onchain = read_i64(data, "timestamp")?;

    sqlx::query(
        r#"
        INSERT INTO settlement_approvals (
            txn_version,
            room_id,
            client_address,
            timestamp_onchain
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (txn_version) DO NOTHING
        "#,
    )
    .bind(txn_version)
    .bind(room_id)
    .bind(client_address)
    .bind(timestamp_onchain)
    .execute(&mut **tx)
    .await
    .context("failed handling SettlementApproved")?;

    Ok(())
}

async fn handle_room_settled(
    tx: &mut Transaction<'_, Postgres>,
    txn_version: i64,
    data: &Value,
) -> Result<()> {
    let room_id = read_i64(data, "room_id")?;
    let winner_address = read_string(data, "winner")?.to_lowercase();
    let final_score = read_i64(data, "final_score")?;
    let payout_amount = read_i64(data, "payout_amount")?;
    let timestamp_onchain = read_i64(data, "timestamp")?;

    sqlx::query(
        r#"
        INSERT INTO settlements (
            txn_version,
            room_id,
            winner_address,
            final_score,
            payout_amount,
            timestamp_onchain
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (txn_version)
        DO UPDATE SET
            room_id = EXCLUDED.room_id,
            winner_address = EXCLUDED.winner_address,
            final_score = EXCLUDED.final_score,
            payout_amount = EXCLUDED.payout_amount,
            timestamp_onchain = EXCLUDED.timestamp_onchain
        "#,
    )
    .bind(txn_version)
    .bind(room_id)
    .bind(winner_address)
    .bind(final_score)
    .bind(payout_amount)
    .bind(timestamp_onchain)
    .execute(&mut **tx)
    .await
    .context("failed handling RoomSettled")?;

    // The settled state is terminal in your state machine.
    sqlx::query(
        r#"
        UPDATE rooms
        SET state = 6,
            last_txn_version = $2,
            updated_at = NOW()
        WHERE room_id = $1
        "#,
    )
    .bind(room_id)
    .bind(txn_version)
    .execute(&mut **tx)
    .await
    .context("failed updating room terminal state")?;

    Ok(())
}

fn read_string(data: &Value, key: &str) -> Result<String> {
    let value = data
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing string field '{key}'"))?;
    Ok(value.to_string())
}

fn read_i64(data: &Value, key: &str) -> Result<i64> {
    let raw = data
        .get(key)
        .ok_or_else(|| anyhow::anyhow!("missing numeric field '{key}'"))?;

    if let Some(v) = raw.as_i64() {
        return Ok(v);
    }

    if let Some(v) = raw.as_u64() {
        return i64::try_from(v).context("numeric conversion overflow") ;
    }

    if let Some(v) = raw.as_str() {
        return v
            .parse::<i64>()
            .with_context(|| format!("failed to parse '{key}' as i64"));
    }

    Err(anyhow::anyhow!("invalid numeric field '{key}'"))
}
