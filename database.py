"""
database.py — Environment setup, MySQL connection wrapper, and schema bootstrap.
"""

import os, sys, getpass
import mysql.connector
from mysql.connector import Error as MySQLError
from dotenv import load_dotenv
import config


def setup_env():
    load_dotenv(config.ENV_FILE)
    keys = {
        "AFL_API_KEY"   : ("API-Football key (from dashboard.api-football.com)", False),
        "FD_API_KEY"    : ("football-data.org API key (free)", False),
        "MYSQL_HOST"    : ("MySQL host", False),
        "MYSQL_PORT"    : ("MySQL port", False),
        "MYSQL_USER"    : ("MySQL username", False),
        "MYSQL_PASSWORD": ("MySQL password", True),
        "MYSQL_DB"      : ("MySQL database name", False),
        "BANKROLL_RWF"  : ("Starting bankroll in RWF", False),
    }
    defaults = {
        "MYSQL_HOST": "localhost", "MYSQL_PORT": "3306",
        "MYSQL_USER": "root", "MYSQL_DB": "football_predictor",
        "BANKROLL_RWF": "20000",
    }
    env_lines = {}
    if config.ENV_FILE.exists():
        with open(config.ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    env_lines[k.strip()] = v.strip()
    changed = False
    for key, (label, is_secret) in keys.items():
        current = env_lines.get(key) or os.getenv(key) or defaults.get(key, "")
        if not current:
            print(f"\n  [{label}]")
            val = (getpass.getpass if is_secret else input)(f"  Enter {label}: ").strip()
            env_lines[key] = val
            changed = True
    if changed:
        with open(config.ENV_FILE, "w") as f:
            for k, v in env_lines.items():
                f.write(f"{k}={v}\n")
        load_dotenv(config.ENV_FILE, override=True)
        print("\n  [Setup] Credentials saved to .env\n")


def get_env(key, default=""):
    load_dotenv(config.ENV_FILE)
    return os.getenv(key, default)


# ══════════════════════════════════════════════════════════════════════════════
class DB:
    def __init__(self, db_name: str = None):
        self.conn    = None
        self.db_name = db_name or get_env("MYSQL_DB", "football_predictor")
        self._ensure_db()
        self.connect()

    def _ensure_db(self):
        try:
            tmp = mysql.connector.connect(
                host=get_env("MYSQL_HOST","localhost"),
                port=int(get_env("MYSQL_PORT","3306")),
                user=get_env("MYSQL_USER","root"),
                password=get_env("MYSQL_PASSWORD",""),
                charset="utf8mb4",
            )
            cur = tmp.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{self.db_name}` "
                        f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            tmp.commit(); cur.close(); tmp.close()
        except MySQLError:
            pass

    def connect(self):
        try:
            self.conn = mysql.connector.connect(
                host=get_env("MYSQL_HOST","localhost"),
                port=int(get_env("MYSQL_PORT","3306")),
                user=get_env("MYSQL_USER","root"),
                password=get_env("MYSQL_PASSWORD",""),
                database=self.db_name, charset="utf8mb4", autocommit=True,
            )
        except MySQLError as e:
            print(f"\n  [DB ERROR] Cannot connect to MySQL: {e}")
            sys.exit(1)

    def execute(self, sql, params=None, fetch=False, silent=False):
        try:
            if not self.conn.is_connected():
                self.connect()
            cur = self.conn.cursor(dictionary=True)
            cur.execute(sql, params or ())
            if fetch:
                result = cur.fetchall(); cur.close(); return result
            self.conn.commit()
            last_id = cur.lastrowid; cur.close(); return last_id
        except MySQLError as e:
            if not silent: print(f"  [DB] Query error: {e}")
            return [] if fetch else None

    def executemany(self, sql, data):
        try:
            if not self.conn.is_connected(): self.connect()
            cur = self.conn.cursor()
            cur.executemany(sql, data)
            self.conn.commit(); cur.close()
        except MySQLError as e:
            print(f"  [DB] Batch error: {e}")

    def fetchall(self, sql, params=None):
        return self.execute(sql, params, fetch=True)

    def fetchone(self, sql, params=None):
        rows = self.fetchall(sql, params)
        return rows[0] if rows else None

    def api_requests_today(self, api="afl"):
        row = self.fetchone(
            "SELECT COUNT(*) as n FROM api_request_log "
            "WHERE api=%s AND DATE(requested_at)=CURDATE()", (api,))
        return row["n"] if row else 0

    def log_api_request(self, api, endpoint):
        self.execute("INSERT INTO api_request_log (api,endpoint) VALUES (%s,%s)",
                     (api, endpoint))

    def close(self):
        if self.conn and self.conn.is_connected():
            self.conn.close()


# ══════════════════════════════════════════════════════════════════════════════
def _ensure_schema(db: "DB"):
    """
    Create all tables. Safe to run on existing DB — uses CREATE TABLE IF NOT EXISTS.
    Called once per session after connection.
    """
    statements = [
        """CREATE TABLE IF NOT EXISTS matches_basic (
            match_id        INT PRIMARY KEY,
            source          VARCHAR(30) DEFAULT 'api-football',
            season          INT, match_date DATETIME, matchday VARCHAR(10),
            home_team_name  VARCHAR(100), away_team_name VARCHAR(100),
            home_goals INT, away_goals INT,
            ht_home_goals INT, ht_away_goals INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_home (home_team_name), INDEX idx_away (away_team_name),
            INDEX idx_date (match_date),     INDEX idx_season (season)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        """CREATE TABLE IF NOT EXISTS matches_stats (
            match_id INT, team_name VARCHAR(100), is_home TINYINT(1),
            shots_total INT, shots_on_target INT, shots_off_target INT,
            shots_blocked INT, possession_pct FLOAT, passes_total INT,
            passes_accurate INT, pass_accuracy_pct FLOAT, fouls INT,
            yellow_cards INT, red_cards INT, corners INT, offsides INT,
            saves INT, xg FLOAT, npxg FLOAT, shot_quality FLOAT,
            penalties_awarded INT DEFAULT 0,
            UNIQUE KEY uq_match_team (match_id, is_home),
            INDEX idx_match (match_id), INDEX idx_team (team_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        """CREATE TABLE IF NOT EXISTS matches_lineups (
            id INT AUTO_INCREMENT PRIMARY KEY,
            match_id INT, team_name VARCHAR(100), is_home TINYINT(1),
            player_name VARCHAR(100), position VARCHAR(5),
            is_starter TINYINT(1), shirt_number INT,
            UNIQUE KEY uq_lineup (match_id, team_name, player_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        """CREATE TABLE IF NOT EXISTS match_events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            match_id INT, minute INT, extra_time INT DEFAULT 0,
            team_name VARCHAR(100), player_name VARCHAR(100),
            event_type VARCHAR(50), detail VARCHAR(100),
            INDEX idx_match (match_id), INDEX idx_type (event_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        """CREATE TABLE IF NOT EXISTS teams (
            team_id INT PRIMARY KEY, name VARCHAR(100),
            code VARCHAR(10), country VARCHAR(50),
            INDEX idx_name (name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        """CREATE TABLE IF NOT EXISTS injuries (
            id INT AUTO_INCREMENT PRIMARY KEY,
            team_name VARCHAR(100), player_name VARCHAR(100) NOT NULL,
            injury_type VARCHAR(100), reason VARCHAR(200),
            expected_return VARCHAR(50), position VARCHAR(5) DEFAULT 'MF',
            fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_team (team_name), INDEX idx_fetched (fetched_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        """CREATE TABLE IF NOT EXISTS head_to_head (
            id INT AUTO_INCREMENT PRIMARY KEY,
            team_a VARCHAR(100), team_b VARCHAR(100), match_date DATETIME,
            home_team VARCHAR(100), away_team VARCHAR(100),
            home_goals INT, away_goals INT,
            INDEX idx_teams (team_a, team_b)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        """CREATE TABLE IF NOT EXISTS predictions_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            prediction_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            league_code VARCHAR(5), home_team VARCHAR(100), away_team VARCHAR(100),
            home_xg FLOAT, away_xg FLOAT,
            home_win_pct FLOAT, draw_pct FLOAT, away_win_pct FLOAT,
            over25_pct FLOAT, under25_pct FLOAT, over15_pct FLOAT, over35_pct FLOAT,
            btts_yes_pct FLOAT, btts_no_pct FLOAT,
            top_correct_score VARCHAR(10), top_cs_pct FLOAT,
            ht_home_win_pct FLOAT, ht_draw_pct FLOAT, ht_away_win_pct FLOAT,
            expected_corners FLOAT, expected_cards FLOAT,
            best_market VARCHAR(50), edge_pct FLOAT, ev FLOAT,
            suggested_stake_rwf FLOAT, bankroll_rwf FLOAT,
            odds_entered TEXT, injury_notes_home TEXT, injury_notes_away TEXT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        """CREATE TABLE IF NOT EXISTS venues (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL UNIQUE, city VARCHAR(100),
            latitude DECIMAL(9,6), longitude DECIMAL(9,6),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_name (name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        """CREATE TABLE IF NOT EXISTS api_request_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            api VARCHAR(20) DEFAULT 'afl', endpoint VARCHAR(100),
            requested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_api_date (api, requested_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        """CREATE TABLE IF NOT EXISTS backfill_progress (
            match_id INT PRIMARY KEY,
            enriched_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        """CREATE TABLE IF NOT EXISTS match_player_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            match_id INT NOT NULL, team_name VARCHAR(100),
            player_id INT, player_name VARCHAR(100), position VARCHAR(5),
            minutes_played INT, goals INT DEFAULT 0, assists INT DEFAULT 0,
            shots_total INT, shots_on INT, xg FLOAT, key_passes INT, rating FLOAT,
            UNIQUE KEY uq_match_player (match_id, player_id),
            INDEX idx_match (match_id), INDEX idx_player (player_id),
            INDEX idx_team (team_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        """CREATE TABLE IF NOT EXISTS market_opening_odds (
            id INT AUTO_INCREMENT PRIMARY KEY,
            fixture_id INT NOT NULL, market VARCHAR(50) NOT NULL, odds FLOAT NOT NULL,
            fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_fixture_market (fixture_id, market),
            INDEX idx_fixture (fixture_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        """CREATE TABLE IF NOT EXISTS cross_competition_fixtures (
            id INT AUTO_INCREMENT PRIMARY KEY,
            team_name VARCHAR(100) NOT NULL, team_id INT,
            competition VARCHAR(100), league_id INT,
            match_date DATETIME NOT NULL, opponent VARCHAR(100),
            venue VARCHAR(10) DEFAULT 'home', season INT,
            fixture_id INT UNIQUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_team_date (team_name, match_date),
            INDEX idx_team_id   (team_id, match_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        """CREATE TABLE IF NOT EXISTS bet_tracker (
            id INT AUTO_INCREMENT PRIMARY KEY,
            match_id INT NOT NULL, match_date DATETIME,
            league_code VARCHAR(5), home_team VARCHAR(100), away_team VARCHAR(100),
            market VARCHAR(50) NOT NULL, model_prob FLOAT NOT NULL,
            odds_at_prediction FLOAT NOT NULL, implied_prob_at_prediction FLOAT NOT NULL,
            edge_at_prediction FLOAT, ev_at_prediction FLOAT, quality VARCHAR(20),
            stake_rwf FLOAT DEFAULT 0, closing_odds FLOAT DEFAULT NULL,
            clv_pct FLOAT DEFAULT NULL, result VARCHAR(10) DEFAULT NULL,
            profit_rwf FLOAT DEFAULT NULL,
            recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            closed_at DATETIME DEFAULT NULL,
            UNIQUE KEY uq_match_market (match_id, market),
            INDEX idx_match (match_id), INDEX idx_date (match_date),
            INDEX idx_league (league_code), INDEX idx_result (result),
            INDEX idx_clv (clv_pct)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

        # ── xG-Elo Ratings ────────────────────────────────────────────────────
        # One row per team per league DB. Computed from every historical match
        # in chronological order. Updated by DataManager.compute_xg_elo_ratings().
        #
        # Elo mechanics:
        #   Starting rating : 1500 (all teams begin equal)
        #   Home advantage  : +65 Elo points added to home expected score
        #   K-factor        : 32 (standard; produces stable but responsive ratings)
        #   Score signal    : xG share (npxG → xG → goals fallback per match row)
        #   No seasonal reset: ratings carry over between seasons so early-season
        #                      predictions benefit from prior-season final ratings.
        #
        # How ratings feed model.py:
        #   elo_delta = (home_elo - away_elo) / 400
        #   mu_h anchor = league_avg × exp(elo_delta × 0.30)
        #   mu_a anchor = league_avg × exp(-elo_delta × 0.30)
        #   These become the prior centers for attack/defense parameters,
        #   replacing the current hard-coded zero centers.
        """CREATE TABLE IF NOT EXISTS elo_ratings (
            team_name    VARCHAR(100) PRIMARY KEY,
            elo          FLOAT        NOT NULL DEFAULT 1500,
            n_matches    INT          NOT NULL DEFAULT 0,
            last_updated DATETIME     DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_elo (elo)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    ]

    for stmt in statements:
        db.execute(stmt)

    # Migrations — each attempted independently; duplicate column = already done
    for sql, _ in [
        ("ALTER TABLE injuries ADD COLUMN position VARCHAR(5) DEFAULT 'MF' AFTER expected_return", ""),
    ]:
        db.execute(sql, silent=True)
