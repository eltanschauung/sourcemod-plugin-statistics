# SourceMod Plugin Statistics

This is a statistics system for SourceMod server administrators familiar with programming. Use the API in your SourcePawn plugins to record whatever you want in a database that is easy to query. Statistics at this scale can become expensive on SourceMod's single game thread, so a Rust service performs durable, batched database writes instead.

The checked-in SourceMod plugin and Rust service are the complete production implementation. Site-specific deployments should differ through configuration, not source forks.

## Requirements

- SourceMod 1.11 or newer
- The SourceMod Socket extension and `socket.inc`
- Rust 1.85 or newer
- MySQL or MariaDB
- Linux and systemd for the supplied service unit

## SourceMod API

Include `plugin_statistics.inc` as a required plugin API:

```sourcepawn
#include <sourcemod>
#include <plugin_statistics>

public void RecordRoundWin(int winner)
{
    char message[64];
    Format(message, sizeof(message), "winner=%d", winner);
    PluginStats_Record("round_win", message);
}
```

```sourcepawn
native bool PluginStats_Record(const char[] eventName, const char[] message = "");
```

`eventName` must contain only lowercase ASCII letters, digits, and underscores. `message` is an optional application-defined payload of up to 511 bytes. The provider derives the calling plugin name and supplies all server, map-session, timestamp, and tickrate fields. The return value reports whether the event entered the in-memory queue; it does not wait for SQL.

There is deliberately no raw SQL API and no caller-controlled tickrate API.

## Build

Build the daemon:

```bash
cargo build --release
cargo test --all-targets
```

Build the SourceMod plugin with `SM_PATH` pointing to a SourceMod installation:

```bash
SM_PATH=/path/to/addons/sourcemod ./scripts/build_sourcemod.sh
```

The output is `build/plugin_statistics.smx`.

## Install

1. Copy `sourcemod/scripting/include/plugin_statistics.inc` into `addons/sourcemod/scripting/include/`.
2. Copy `build/plugin_statistics.smx` into `addons/sourcemod/plugins/`.
3. Copy `deploy/plugin-statisticsd.env.example` to `~/.config/plugin-statisticsd/service.env` and set the database values.
4. Copy `deploy/plugin-statisticsd.service` to `~/.config/systemd/user/`.
5. Enable the service:

```bash
systemctl --user daemon-reload
systemctl --user enable --now plugin-statisticsd.service
```

6. Set `sm_plugin_statistics_port` if the daemon is not using the default port, then load `plugin_statistics.smx` before plugins that require its API.

Multiple SourceMod servers can share one daemon port. Give each server a stable `sm_plugin_statistics_server_id`, or leave it blank to derive one from hostname and game port.

Operational commands:

- `sm_plugin_statistics_status`: queue, connection, map-session, and tickrate state.
- `sm_plugin_statistics_test`: queue a diagnostic event and request an immediate flush.

## Configuration

The daemon reads environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `PLUGIN_STATS_BIND` | `127.0.0.1:28019` | Listener address |
| `PLUGIN_STATS_DB_HOST` | `127.0.0.1` | MySQL host |
| `PLUGIN_STATS_DB_PORT` | `3306` | MySQL port |
| `PLUGIN_STATS_DB_NAME` | `sourcemod` | Database name |
| `PLUGIN_STATS_DB_USER` | `root` | Database user |
| `PLUGIN_STATS_DB_PASS` | empty | Database password |
| `PLUGIN_STATS_AUTH_TOKEN` | empty | Optional shared protocol token |
| `PLUGIN_STATS_FLUSH_INTERVAL_MS` | `500` | Maximum batching delay |
| `PLUGIN_STATS_MAX_BATCH_ROWS` | `500` | Rows per SQL transaction |
| `PLUGIN_STATS_QUEUE_LIMIT` | `50000` | Rust in-memory queue bound |
| `PLUGIN_STATS_PENDING_JOURNAL_PATH` | `statistics_pending_journal.log` | Durable pending journal |
| `PLUGIN_STATS_DEAD_LETTER_PATH` | `statistics_dead_letters.log` | Rejected journal records |
| `PLUGIN_STATS_REQUIRE_LOCALHOST` | `true` | Reject non-loopback peers |

SourceMod cvars are generated in `cfg/sourcemod/plugin_statistics.cfg`. The defaults use port `28019`, flush every five seconds, retain up to 5,000 queued events, send at most 128 records per batch, and sample tickrate every ten seconds.

## Database

The daemon applies versioned migrations at startup and writes these tables:

- `plugin_statistics_servers`: stable server identities.
- `plugin_statistics_map_sessions`: map, game mode, duration, and aggregate tickrate.
- `plugin_statistics_events`: caller events with mandatory tickrate fields.
- `plugin_statistics_tick_samples`: periodic observed tickrate samples.
- `plugin_statistics_schema_migrations`: applied schema versions.

Example query:

```sql
SELECT e.occurred_at,
       s.server_key,
       m.map_name,
       e.source_plugin,
       e.event_name,
       e.message,
       e.observed_tickrate
FROM plugin_statistics_events AS e
JOIN plugin_statistics_map_sessions AS m ON m.id = e.session_id
JOIN plugin_statistics_servers AS s ON s.id = m.server_id
ORDER BY e.occurred_at DESC
LIMIT 100;
```

Legacy `*_statistics_events` tables can be imported with `scripts/migrate_legacy_tables.py`. Existing tables are never dropped by the migration. Legacy rows use zero for tickrate fields because those values were not recorded; every event received through the current API has real tickrate stamps.

## Failure Behavior

SourceMod retains unsent events in a bounded queue and retries the connection. The daemon journals accepted events before acknowledgement, replays unfinished records after restart, deduplicates event IDs, and records unrecoverable journal entries in a dead-letter file. A database outage therefore delays writes without blocking the game thread.

License: GPL-3.0-or-later.
