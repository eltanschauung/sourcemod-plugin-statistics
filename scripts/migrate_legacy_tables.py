#!/usr/bin/env python3
"""Import shared-schema *_statistics_events tables into the canonical schema."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys


SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]{1,64}$")
CANONICAL_EVENTS_TABLE = "plugin_statistics_events"


def mysql_args() -> list[str]:
    return [
        os.environ.get("MYSQL_BIN", "mysql"),
        "--batch",
        "--skip-column-names",
        "--default-character-set=utf8mb4",
        "--host",
        os.environ.get("PLUGIN_STATS_DB_HOST", "127.0.0.1"),
        "--port",
        os.environ.get("PLUGIN_STATS_DB_PORT", "3306"),
        "--user",
        os.environ.get("PLUGIN_STATS_DB_USER", "root"),
        os.environ.get("PLUGIN_STATS_DB_NAME", "sourcemod"),
    ]


def run_sql(sql: str, *, capture: bool = False) -> str:
    env = os.environ.copy()
    password = env.get("PLUGIN_STATS_DB_PASS", "")
    if password:
        env["MYSQL_PWD"] = password

    result = subprocess.run(
        mysql_args(),
        input=sql,
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        print(result.stderr.rstrip(), file=sys.stderr)
        raise RuntimeError("mysql command failed")
    return result.stdout.strip() if capture else ""


def discover_tables() -> list[str]:
    output = run_sql(
        "SELECT tables.table_name FROM information_schema.tables AS tables "
        "JOIN information_schema.columns AS columns "
        "ON columns.table_schema = tables.table_schema "
        "AND columns.table_name = tables.table_name "
        "WHERE tables.table_schema = DATABASE() "
        "AND tables.table_name LIKE '%\\_statistics\\_events' "
        f"AND tables.table_name <> '{CANONICAL_EVENTS_TABLE}' "
        "GROUP BY tables.table_name "
        "HAVING COUNT(DISTINCT CASE WHEN columns.column_name IN "
        "('id', 'occurred_at', 'host_port', 'map_session_id', 'map_name', "
        "'gamemode', 'event_name', 'message') THEN columns.column_name END) = 8 "
        "ORDER BY tables.table_name;",
        capture=True,
    )
    tables = [line.strip() for line in output.splitlines() if line.strip()]
    unsafe = [table for table in tables if not SAFE_IDENTIFIER.fullmatch(table)]
    if unsafe:
        raise RuntimeError(f"unsafe table names returned by MySQL: {unsafe}")
    return tables


def source_name(table: str) -> str:
    return table.removesuffix("_statistics_events")


def session_expression(alias: str) -> str:
    return (
        f"IF({alias}.map_session_id = '', "
        f"CONCAT('legacy-', {alias}.host_port, '-', FLOOR({alias}.occurred_at / 3600)), "
        f"{alias}.map_session_id)"
    )


def session_migration_sql(table: str) -> str:
    session_key = session_expression("legacy")
    return f"""
START TRANSACTION;

INSERT INTO plugin_statistics_servers
    (server_key, hostname, host_port, created_at, last_seen_at)
SELECT CONCAT('legacy:', host_port), 'legacy-import', host_port,
       MIN(occurred_at), MAX(occurred_at)
FROM `{table}`
GROUP BY host_port
ON DUPLICATE KEY UPDATE
    host_port = VALUES(host_port),
    last_seen_at = GREATEST(last_seen_at, VALUES(last_seen_at));

INSERT INTO plugin_statistics_map_sessions
    (session_key, server_id, host_port, map_name, gamemode, started_at,
     ended_at, duration_seconds, end_reason, end_inferred, start_tick,
     end_tick, tick_interval_seconds, expected_tickrate,
     average_observed_tickrate, minimum_observed_tickrate, tick_sample_count,
     created_at, updated_at)
SELECT {session_key}, servers.id, legacy.host_port,
       COALESCE(NULLIF(MAX(legacy.map_name), ''), 'unknown'),
       COALESCE(NULLIF(MAX(legacy.gamemode), ''), 'other'),
       MIN(legacy.occurred_at), MAX(legacy.occurred_at),
       GREATEST(MAX(legacy.occurred_at) - MIN(legacy.occurred_at), 0),
       'legacy_import', 1, NULL, NULL, 0, 0, NULL, NULL, 0,
       MIN(legacy.occurred_at), MAX(legacy.occurred_at)
FROM `{table}` AS legacy
JOIN plugin_statistics_servers AS servers
  ON servers.server_key = CONCAT('legacy:', legacy.host_port)
GROUP BY servers.id, legacy.host_port, {session_key}
ON DUPLICATE KEY UPDATE
    map_name = IF(map_name = 'unknown', VALUES(map_name), map_name),
    gamemode = IF(gamemode = 'other', VALUES(gamemode), gamemode),
    started_at = LEAST(started_at, VALUES(started_at)),
    ended_at = GREATEST(COALESCE(ended_at, 0), VALUES(ended_at)),
    duration_seconds = GREATEST(
        GREATEST(COALESCE(ended_at, 0), VALUES(ended_at)) -
        LEAST(started_at, VALUES(started_at)),
        0
    ),
    updated_at = GREATEST(updated_at, VALUES(updated_at));

COMMIT;
"""


def event_migration_sql(table: str, first_id: int, last_id: int) -> str:
    source = source_name(table)
    session_key = session_expression("legacy")
    return f"""
START TRANSACTION;

INSERT IGNORE INTO plugin_statistics_events
    (event_id, session_id, source_plugin, event_name, message, occurred_at,
     server_tick, tick_interval_seconds, expected_tickrate, observed_tickrate,
     created_at)
SELECT CONCAT('legacy:{table}:', legacy.id), sessions.id, '{source}',
       legacy.event_name, legacy.message, legacy.occurred_at,
       0, 0, 0, 0, legacy.occurred_at
FROM `{table}` AS legacy
JOIN plugin_statistics_servers AS servers
  ON servers.server_key = CONCAT('legacy:', legacy.host_port)
JOIN plugin_statistics_map_sessions AS sessions
 ON sessions.server_id = servers.id
 AND sessions.session_key = CONVERT({session_key} USING utf8mb4) COLLATE utf8mb4_unicode_ci
WHERE legacy.id BETWEEN {first_id} AND {last_id};

COMMIT;
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="perform the import; without this flag only discovered tables are printed",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="legacy event rows imported per transaction (default: 5000)",
    )
    args = parser.parse_args()
    if args.chunk_size < 100 or args.chunk_size > 100_000:
        parser.error("--chunk-size must be between 100 and 100000")

    tables = discover_tables()
    if not tables:
        print("No legacy statistics tables found.")
        return 0

    print("Legacy tables:")
    for table in tables:
        print(f"  {table}")

    if not args.apply:
        print("Dry run only. Pass --apply to import them.")
        return 0

    before = int(run_sql("SELECT COUNT(*) FROM plugin_statistics_events;", capture=True) or 0)
    for table in tables:
        print(f"Importing {table}...")
        run_sql(session_migration_sql(table))
        last_id = int(run_sql(f"SELECT COALESCE(MAX(id), 0) FROM `{table}`;", capture=True) or 0)
        imported_id = int(
            run_sql(
                "SELECT COALESCE(MAX(CAST(SUBSTRING_INDEX(event_id, ':', -1) AS UNSIGNED)), 0) "
                "FROM plugin_statistics_events "
                f"WHERE source_plugin = '{source_name(table)}' "
                f"AND event_id LIKE 'legacy:{table}:%';",
                capture=True,
            )
            or 0
        )
        for first_id in range(imported_id + 1, last_id + 1, args.chunk_size):
            final_id = min(first_id + args.chunk_size - 1, last_id)
            run_sql(event_migration_sql(table, first_id, final_id))
            print(f"  rows through id {final_id}/{last_id}", flush=True)
    after = int(run_sql("SELECT COUNT(*) FROM plugin_statistics_events;", capture=True) or 0)
    print(f"Imported {after - before} event rows; canonical total is {after}.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError) as error:
        print(f"migration failed: {error}", file=sys.stderr)
        raise SystemExit(1)
