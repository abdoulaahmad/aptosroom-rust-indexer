# AptosRoom Standard Indexer (Rust)

Production-oriented Rust indexer for AptosRoom that ingests all protocol events.

## What It Does

- Discovers protocol transaction versions using Aptos Indexer GraphQL.
- Fetches full transaction events from Aptos REST.
- Stores all protocol events in `raw_events`.
- Applies core projections (`rooms`, `keycards`, `settlements`).
- Tracks durable checkpoint in `indexer_state`.

## Ingestion Scope

The indexer stores every event whose type starts with:

- `{PROTOCOL_ADDRESS}::`

This allows adding new projections later without losing historical raw data.

## Tables

- `indexer_state`
- `raw_events`
- `rooms`
- `room_state_history`
- `keycards`
- `settlements`
- `settlement_approvals`

## Prerequisites

- Rust 1.78+
- PostgreSQL

## Setup

1. Create env file:

```powershell
cd room-indexer\aptosroom-standard-indexer
Copy-Item .env.example .env
```

2. Edit `.env` values.

3. Build and run:

```powershell
cargo check
cargo run
```

## Important Behavior

- Checkpoint is updated only after each transaction is fully persisted.
- Raw event insert is idempotent via primary key `(txn_version, event_index)`.
- Projection failures are logged as warnings and do not block raw ingestion.
- Existing TypeScript indexer remains untouched in `room-indexer/aptosroom-indexer`.
