use anyhow::{Context, Result};
use sqlx::{Pool, Postgres};

pub async fn init_schema(pool: &Pool<Postgres>) -> Result<()> {
    let mut conn = pool
        .acquire()
        .await
        .context("failed to acquire connection for schema init")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS indexer_state (
            processor_name TEXT PRIMARY KEY,
            last_processed_version BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#,
    )
    .execute(&mut *conn)
    .await
    .context("failed to create indexer_state")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS raw_events (
            txn_version BIGINT NOT NULL,
            event_index INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            account_address TEXT NOT NULL,
            sequence_number BIGINT,
            txn_timestamp_usecs BIGINT,
            payload JSONB NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (txn_version, event_index)
        );
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create raw_events")?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_raw_events_type ON raw_events (event_type);
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create idx_raw_events_type")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS rooms (
            room_id BIGINT PRIMARY KEY,
            creator_address TEXT NOT NULL,
            category TEXT NOT NULL,
            task_reward BIGINT NOT NULL,
            created_at_onchain BIGINT NOT NULL,
            state INTEGER NOT NULL,
            last_txn_version BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create rooms")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS room_state_history (
            txn_version BIGINT NOT NULL,
            event_index INTEGER NOT NULL,
            room_id BIGINT NOT NULL,
            from_state INTEGER NOT NULL,
            to_state INTEGER NOT NULL,
            timestamp_onchain BIGINT NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (txn_version, event_index)
        );
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create room_state_history")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS keycards (
            owner_address TEXT PRIMARY KEY,
            keycard_id BIGINT NOT NULL,
            created_at_onchain BIGINT NOT NULL,
            tasks_completed BIGINT,
            avg_score BIGINT,
            jury_participations BIGINT,
            variance_flags BIGINT,
            last_txn_version BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create keycards")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS settlements (
            txn_version BIGINT PRIMARY KEY,
            room_id BIGINT NOT NULL,
            winner_address TEXT NOT NULL,
            final_score BIGINT NOT NULL,
            payout_amount BIGINT NOT NULL,
            timestamp_onchain BIGINT NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create settlements")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS settlement_approvals (
            txn_version BIGINT PRIMARY KEY,
            room_id BIGINT NOT NULL,
            client_address TEXT NOT NULL,
            timestamp_onchain BIGINT NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create settlement_approvals")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS juror_registrations (
            txn_version BIGINT NOT NULL,
            event_index INTEGER NOT NULL,
            juror_address TEXT NOT NULL,
            category TEXT NOT NULL,
            action TEXT NOT NULL,
            timestamp_onchain BIGINT NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (txn_version, event_index)
        );
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create juror_registrations")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS submissions (
            txn_version BIGINT NOT NULL,
            event_index INTEGER NOT NULL,
            room_id BIGINT NOT NULL,
            contributor_address TEXT NOT NULL,
            submission_hash TEXT NOT NULL,
            timestamp_onchain BIGINT NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (txn_version, event_index)
        );
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create submissions")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS jury_assignments (
            txn_version BIGINT NOT NULL,
            event_index INTEGER NOT NULL,
            room_id BIGINT NOT NULL,
            jurors JSONB NOT NULL,
            timestamp_onchain BIGINT NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (txn_version, event_index)
        );
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create jury_assignments")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS votes (
            txn_version BIGINT NOT NULL,
            event_index INTEGER NOT NULL,
            room_id BIGINT NOT NULL,
            juror_address TEXT NOT NULL,
            vote_type TEXT NOT NULL,
            commit_hash TEXT,
            score BIGINT,
            timestamp_onchain BIGINT NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (txn_version, event_index)
        );
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create votes")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS variance_flags (
            txn_version BIGINT NOT NULL,
            event_index INTEGER NOT NULL,
            room_id BIGINT NOT NULL,
            juror_address TEXT NOT NULL,
            score BIGINT NOT NULL,
            min_distance BIGINT NOT NULL,
            timestamp_onchain BIGINT NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (txn_version, event_index)
        );
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create variance_flags")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS final_scores (
            txn_version BIGINT PRIMARY KEY,
            room_id BIGINT NOT NULL,
            jury_score BIGINT NOT NULL,
            timestamp_onchain BIGINT NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create final_scores")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS escrow_movements (
            txn_version BIGINT NOT NULL,
            event_index INTEGER NOT NULL,
            room_id BIGINT NOT NULL,
            address TEXT NOT NULL,
            movement_type TEXT NOT NULL,
            amount BIGINT NOT NULL,
            timestamp_onchain BIGINT NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (txn_version, event_index)
        );
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create escrow_movements")?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS zero_vote_refunds (
            txn_version BIGINT NOT NULL,
            event_index INTEGER NOT NULL,
            room_id BIGINT NOT NULL,
            client_address TEXT NOT NULL,
            refund_amount BIGINT NOT NULL,
            timestamp_onchain BIGINT NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (txn_version, event_index)
        );
        "#,
    )
        .execute(&mut *conn)
    .await
    .context("failed to create zero_vote_refunds")?;

    Ok(())
}
