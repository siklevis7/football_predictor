-- ============================================================
--  FOOTBALL PREDICTOR DATABASE SCHEMA
--  Run once: mysql -u root -p < db_setup.sql
-- ============================================================

CREATE DATABASE IF NOT EXISTS football_predictor
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE football_predictor;

-- ── API request log (tracks daily budget) ────────────────────
CREATE TABLE IF NOT EXISTS api_request_log (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    api         VARCHAR(50)  NOT NULL,
    endpoint    VARCHAR(200) NOT NULL,
    requested_at DATETIME    DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_date (requested_at)
);

-- ── Teams ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS teams (
    team_id     INT PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    short_name  VARCHAR(50),
    country     VARCHAR(50),
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ── Players ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS players (
    player_id   INT PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    team_id     INT,
    position    VARCHAR(10),
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

-- ── Matches (basic: goals, date) ─────────────────────────────
CREATE TABLE IF NOT EXISTS matches_basic (
    match_id        INT PRIMARY KEY,
    source          VARCHAR(20) NOT NULL DEFAULT 'football-data',
    season          INT NOT NULL,
    match_date      DATETIME NOT NULL,
    matchday        INT,
    home_team_id    INT,
    away_team_id    INT,
    home_team_name  VARCHAR(100) NOT NULL,
    away_team_name  VARCHAR(100) NOT NULL,
    home_goals      INT NOT NULL,
    away_goals      INT NOT NULL,
    ht_home_goals   INT,
    ht_away_goals   INT,
    status          VARCHAR(20) DEFAULT 'FINISHED',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_season   (season),
    INDEX idx_date     (match_date),
    INDEX idx_home     (home_team_name),
    INDEX idx_away     (away_team_name)
);

-- ── Match statistics (from api-football) ─────────────────────
CREATE TABLE IF NOT EXISTS matches_stats (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    match_id            INT NOT NULL,
    team_name           VARCHAR(100) NOT NULL,
    is_home             TINYINT(1) NOT NULL,
    shots_total         INT,
    shots_on_target     INT,
    shots_off_target    INT,
    shots_blocked       INT,
    possession_pct      FLOAT,
    passes_total        INT,
    passes_accurate     INT,
    pass_accuracy_pct   FLOAT,
    fouls               INT,
    yellow_cards        INT,
    red_cards           INT,
    corners             INT,
    offsides            INT,
    saves               INT,
    xg                  FLOAT,
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_match_team (match_id, team_name),
    INDEX idx_match     (match_id),
    INDEX idx_team      (team_name)
);

-- ── Match lineups ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS matches_lineups (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    match_id    INT NOT NULL,
    team_name   VARCHAR(100) NOT NULL,
    is_home     TINYINT(1) NOT NULL,
    player_id   INT,
    player_name VARCHAR(100) NOT NULL,
    position    VARCHAR(10),
    is_starter  TINYINT(1) DEFAULT 1,
    shirt_number INT,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_match  (match_id),
    INDEX idx_team   (team_name),
    INDEX idx_player (player_name)
);

-- ── Match events (goals, cards, times) ───────────────────────
CREATE TABLE IF NOT EXISTS match_events (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    match_id    INT NOT NULL,
    minute      INT,
    extra_time  INT DEFAULT 0,
    team_name   VARCHAR(100),
    player_name VARCHAR(100),
    event_type  VARCHAR(30),   -- GOAL, YELLOW_CARD, RED_CARD, SUBST
    detail      VARCHAR(100),  -- Normal Goal, Penalty, Own Goal, etc.
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_match  (match_id),
    INDEX idx_player (player_name),
    INDEX idx_type   (event_type)
);

-- ── Head-to-head cache ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS head_to_head (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    team_a          VARCHAR(100) NOT NULL,
    team_b          VARCHAR(100) NOT NULL,
    match_date      DATETIME,
    home_team       VARCHAR(100),
    away_team       VARCHAR(100),
    home_goals      INT,
    away_goals      INT,
    fetched_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_teams (team_a, team_b)
);

-- ── Injuries cache ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS injuries (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    team_name       VARCHAR(100) NOT NULL,
    player_name     VARCHAR(100) NOT NULL,
    injury_type     VARCHAR(100),
    reason          VARCHAR(200),
    expected_return VARCHAR(100),
    fetched_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_team  (team_name),
    INDEX idx_fetched (fetched_at)
);

-- ── Predictions log (links to Excel tracker) ─────────────────
CREATE TABLE IF NOT EXISTS predictions_log (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    prediction_date     DATETIME NOT NULL,
    home_team           VARCHAR(100) NOT NULL,
    away_team           VARCHAR(100) NOT NULL,
    home_xg             FLOAT,
    away_xg             FLOAT,
    home_win_pct        FLOAT,
    draw_pct            FLOAT,
    away_win_pct        FLOAT,
    over25_pct          FLOAT,
    under25_pct         FLOAT,
    over15_pct          FLOAT,
    over35_pct          FLOAT,
    btts_yes_pct        FLOAT,
    btts_no_pct         FLOAT,
    top_correct_score   VARCHAR(10),
    top_cs_pct          FLOAT,
    ht_home_win_pct     FLOAT,
    ht_draw_pct         FLOAT,
    ht_away_win_pct     FLOAT,
    expected_corners    FLOAT,
    expected_cards      FLOAT,
    best_market         VARCHAR(50),
    edge_pct            FLOAT,
    ev                  FLOAT,
    suggested_stake_rwf FLOAT,
    bankroll_rwf        FLOAT,
    odds_entered        VARCHAR(200),
    injury_notes_home   TEXT,
    injury_notes_away   TEXT,
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_date  (prediction_date),
    INDEX idx_teams (home_team, away_team)
);

-- ── Backfill progress tracker ─────────────────────────────────
CREATE TABLE IF NOT EXISTS backfill_progress (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    task        VARCHAR(100) NOT NULL UNIQUE,
    season      INT,
    match_id    INT,
    status      VARCHAR(20) DEFAULT 'pending',
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

SELECT 'Database schema created successfully.' AS status;
