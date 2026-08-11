CREATE TABLE IF NOT EXISTS plugin_statistics_servers (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    server_key VARCHAR(128) NOT NULL,
    hostname VARCHAR(255) NOT NULL DEFAULT '',
    host_port INT NOT NULL DEFAULT 0,
    created_at BIGINT UNSIGNED NOT NULL,
    last_seen_at BIGINT UNSIGNED NOT NULL,
    UNIQUE KEY uniq_server_key (server_key),
    KEY idx_host_port (host_port),
    KEY idx_last_seen_at (last_seen_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS plugin_statistics_map_sessions (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    session_key VARCHAR(128) NOT NULL,
    server_id BIGINT UNSIGNED NOT NULL,
    host_port INT NOT NULL DEFAULT 0,
    map_name VARCHAR(128) NOT NULL DEFAULT '',
    gamemode VARCHAR(64) NOT NULL DEFAULT '',
    started_at BIGINT UNSIGNED NOT NULL,
    ended_at BIGINT UNSIGNED NULL,
    duration_seconds INT UNSIGNED NULL,
    end_reason VARCHAR(32) NULL,
    end_inferred TINYINT(1) NOT NULL DEFAULT 0,
    start_tick BIGINT NULL,
    end_tick BIGINT NULL,
    tick_interval_seconds DECIMAL(12,9) NOT NULL DEFAULT 0,
    expected_tickrate DECIMAL(9,3) NOT NULL DEFAULT 0,
    average_observed_tickrate DECIMAL(9,3) NULL,
    minimum_observed_tickrate DECIMAL(9,3) NULL,
    tick_sample_count INT UNSIGNED NOT NULL DEFAULT 0,
    created_at BIGINT UNSIGNED NOT NULL,
    updated_at BIGINT UNSIGNED NOT NULL,
    UNIQUE KEY uniq_server_session (server_id, session_key),
    KEY idx_server_started (server_id, started_at),
    KEY idx_host_started (host_port, started_at),
    KEY idx_map_started (map_name, started_at),
    KEY idx_open_sessions (server_id, ended_at),
    CONSTRAINT fk_plugin_stats_session_server
        FOREIGN KEY (server_id) REFERENCES plugin_statistics_servers(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS plugin_statistics_events (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(128) NOT NULL,
    session_id BIGINT UNSIGNED NOT NULL,
    source_plugin VARCHAR(64) NOT NULL,
    event_name VARCHAR(64) NOT NULL,
    message VARCHAR(512) NOT NULL DEFAULT '',
    occurred_at BIGINT UNSIGNED NOT NULL,
    server_tick BIGINT NOT NULL,
    tick_interval_seconds DECIMAL(12,9) NOT NULL,
    expected_tickrate DECIMAL(9,3) NOT NULL,
    observed_tickrate DECIMAL(9,3) NOT NULL,
    created_at BIGINT UNSIGNED NOT NULL,
    UNIQUE KEY uniq_event_id (event_id),
    KEY idx_session_occurred (session_id, occurred_at),
    KEY idx_source_event_time (source_plugin, event_name, occurred_at),
    KEY idx_event_time (event_name, occurred_at),
    CONSTRAINT fk_plugin_stats_event_session
        FOREIGN KEY (session_id) REFERENCES plugin_statistics_map_sessions(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS plugin_statistics_tick_samples (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(128) NOT NULL,
    session_id BIGINT UNSIGNED NOT NULL,
    sampled_at BIGINT UNSIGNED NOT NULL,
    server_tick BIGINT NOT NULL,
    tick_interval_seconds DECIMAL(12,9) NOT NULL,
    expected_tickrate DECIMAL(9,3) NOT NULL,
    observed_tickrate DECIMAL(9,3) NOT NULL,
    created_at BIGINT UNSIGNED NOT NULL,
    UNIQUE KEY uniq_tick_event_id (event_id),
    KEY idx_session_sampled (session_id, sampled_at),
    KEY idx_sampled_at (sampled_at),
    CONSTRAINT fk_plugin_stats_tick_session
        FOREIGN KEY (session_id) REFERENCES plugin_statistics_map_sessions(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
