"""
data_manager.py — DataManager, resolve_team, and input_lineup.
"""

import time
import numpy as np
import pandas as pd
from difflib import get_close_matches
from datetime import datetime, timedelta
import config
from database import DB
from api_client import APIFootball, WeatherFetcher


class DataManager:
    """
    Manages all data operations:
    - Loads match history from MySQL
    - Runs background backfill of rich stats from API-Football
    - Fetches live data (injuries, H2H) for each prediction
    - Shows progress messages as requested
    """

    def __init__(self, db: DB, afl: APIFootball):
        self.db      = db
        self.afl     = afl
        self.weather = WeatherFetcher(db=db)

    # ── Startup: sync new matches + show backfill status ──────────────────────
    def startup_sync(self):
        print("\n[Data] Checking for new matches …")
        self._sync_basic_matches()
        self._show_backfill_status()
        # Backfill now runs per-fixture (after you enter the teams)
        # so your daily budget goes to the teams you actually care about.

        # Recompute xG-Elo ratings once per session (no API cost — DB only).
        # This runs in <1s for 2000 matches and ensures every prediction
        # uses up-to-date ratings that reflect the most recent results.
        row = self.db.fetchone(
            "SELECT COUNT(*) as n FROM elo_ratings"
        )
        n_rated = row["n"] if row else 0
        print(f"[Elo]  Computing xG-Elo ratings …", end=" ", flush=True)
        n = self.compute_xg_elo_ratings()
        if n == 0 and n_rated > 0:
            print(f"using {n_rated} cached ratings ✓")

    def _sync_basic_matches(self, force_seasons: list = None):
        """
        Sync basic match data for all config.AFL_SEASONS.
        Uses per-league per-season expected match count to decide when a season
        is complete. COVID-shortened seasons have reduced expected counts.
        force_seasons: list of season years to force re-sync even if marked complete.
        """
        # Expected match counts per league per season.
        # Most seasons: full round count × 2.
        # COVID-affected seasons (2019/20) had fewer matches in some leagues.
        # Ligue 1 2019/20 was cancelled at ~252 matches (never finished).
        # Bundesliga, PL, Serie A, LaLiga did finish their 2019/20 seasons.
        # Values below are the REAL match counts — the model stops syncing
        # once these are reached and never wastes API requests re-checking.
        EXPECTED = {
            39: {   # Premier League (20 teams = 380/season)
                "default": 380,
                2019: 380,  # PL finished behind closed doors
            },
            61: {   # Ligue 1 (18 teams = 306/season normally)
                "default": 306,
                2019: 252,  # COVID: season abandoned after round 28 (28×18/2 = 252)
            },
            78: {   # Bundesliga (18 teams = 306/season)
                "default": 306,
                2019: 306,  # Bundesliga finished — first league to resume post-COVID
            },
            135: {  # Serie A (20 teams = 380/season)
                "default": 380,
                2019: 380,  # Serie A finished in August 2020
            },
            140: {  # LaLiga (20 teams = 380/season)
                "default": 380,
                2019: 380,  # LaLiga finished in July 2020
            },
        }
        league_exp  = EXPECTED.get(config.AFL_LEAGUE_ID, {"default": 380})
        force       = set(force_seasons or [])

        for season in config.AFL_SEASONS:
            # Get the correct expected count for this specific season
            expected = league_exp.get(season, league_exp["default"])
            nearly   = int(expected * 0.95)

            existing = self.db.fetchone(
                "SELECT COUNT(*) as n FROM matches_basic WHERE season=%s",
                (season,)
            )
            n_existing = existing["n"] if existing else 0

            # Season is fully complete — never fetch it again (unless forced)
            if n_existing >= expected and season not in force:
                print(f"  [Sync] Season {season}: complete ({n_existing} matches) ✓")
                continue
            # Nearly complete — skip if synced today (unless forced)
            if n_existing >= nearly and season not in force:
                last = self.db.fetchone(
                    "SELECT DATE(MAX(created_at)) as last_date FROM matches_basic "
                    "WHERE season=%s", (season,)
                )
                if last and last["last_date"]:
                    if str(last["last_date"]) == datetime.now().strftime("%Y-%m-%d"):
                        print(f"  [Sync] Season {season}: "
                              f"{n_existing} matches, synced today ✓")
                        continue

            # Fetch from API-Football — no code-side limit check
            print(f"  [Sync] Season {season} …", end=" ", flush=True)
            raw = self.afl.fetch_fixtures(season)
            if not raw:
                print(f"failed — using {n_existing} cached matches.")
                continue

            inserted = 0
            for m in raw:
                ft = m.get("score", {}).get("fulltime", {})
                ht = m.get("score", {}).get("halftime", {})
                if not ft or ft.get("home") is None:
                    continue

                fix    = m.get("fixture", {})
                teams  = m.get("teams", {})
                mid    = fix.get("id")
                if not mid:
                    continue

                exists = self.db.fetchone(
                    "SELECT match_id FROM matches_basic WHERE match_id=%s",
                    (mid,)
                )
                if exists:
                    continue

                match_date_raw = fix.get("date", "")
                try:
                    match_date = pd.to_datetime(match_date_raw).to_pydatetime().replace(tzinfo=None)
                except Exception:
                    continue

                home_name = config.normalize_team_name(
                    teams.get("home", {}).get("name", ""))
                away_name = config.normalize_team_name(
                    teams.get("away", {}).get("name", ""))

                self.db.execute(
                    """INSERT IGNORE INTO matches_basic
                       (match_id, source, season, match_date, matchday,
                        home_team_name, away_team_name,
                        home_goals, away_goals,
                        ht_home_goals, ht_away_goals)
                       VALUES (%s,'api-football',%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (mid, season, match_date,
                     m.get("league", {}).get("round", "").replace("Regular Season - ", ""),
                     home_name, away_name,
                     int(ft["home"]), int(ft["away"]),
                     int(ht["home"]) if ht and ht.get("home") is not None else None,
                     int(ht["away"]) if ht and ht.get("away") is not None else None)
                )
                inserted += 1

            total_now = n_existing + inserted
            if inserted > 0:
                print(f"+{inserted} new  (total: {total_now})")
            else:
                print(f"up to date ({total_now} matches) ✓")

            # Small pause between season requests
            if season != config.AFL_SEASONS[-1]:
                time.sleep(1.5)

    def _show_backfill_status(self):
        total    = self.db.fetchone("SELECT COUNT(*) as n FROM matches_basic")
        enriched = self.db.fetchone(
            "SELECT COUNT(DISTINCT match_id) as n FROM matches_stats"
        )
        n_total    = total["n"]    if total    else 0
        n_enriched = enriched["n"] if enriched else 0
        n_pending  = n_total - n_enriched
        season_rows = self.db.fetchall(
            "SELECT season, COUNT(*) as n FROM matches_basic GROUP BY season ORDER BY season"
        )
        season_str = "  ".join(
            f"{r['season']}:{r['n']}" for r in season_rows
        ) if season_rows else "none"

        used_today = self.afl.requests_used_today()
        print(f"\n[Data] Matches in DB: {n_total} total  ({season_str})")
        print(f"[Data] Enriched: {n_enriched}  |  Pending: {n_pending}")
        print(f"[API]  Requests used so far today: {used_today}")

    def _run_backfill_batch(self, priority_teams: list = None, limit: int = 50):
        """
        Enrich unenriched matches with rich stats.
        No code-side request limits — API stops us if needed.
        priority_teams: fetch those teams first (most relevant for prediction).
        limit: max matches to enrich per call (default 30 = ~90 requests).
        """
        if priority_teams and len(priority_teams) == 2:
            pending = self.db.fetchall(
                """SELECT b.match_id FROM matches_basic b
                   LEFT JOIN matches_stats s     ON b.match_id = s.match_id
                   LEFT JOIN backfill_progress p ON b.match_id = p.match_id
                   WHERE s.match_id IS NULL
                     AND p.match_id IS NULL
                     AND (b.home_team_name IN (%s,%s)
                          OR b.away_team_name IN (%s,%s))
                   ORDER BY b.match_date DESC
                   LIMIT %s""",
                (*priority_teams, *priority_teams, limit)
            )
            label = f"{priority_teams[0]} & {priority_teams[1]}"
        else:
            pending = self.db.fetchall(
                """SELECT b.match_id FROM matches_basic b
                   LEFT JOIN matches_stats s     ON b.match_id = s.match_id
                   LEFT JOIN backfill_progress p ON b.match_id = p.match_id
                   WHERE s.match_id IS NULL
                     AND p.match_id IS NULL
                   ORDER BY b.match_date DESC
                   LIMIT %s""",
                (limit,)
            )
            label = "all teams"

        if not pending:
            print(f"[Backfill] All matches for {label} fully enriched ✓")
            return

        n = len(pending)
        print(f"\n[Backfill] Enriching {n} matches for {label} "
              f"({n*3} requests) …")

        for row in pending:
            self._enrich_match(row["match_id"])
            time.sleep(1.2)

        enriched_now = self.db.fetchone(
            "SELECT COUNT(DISTINCT match_id) as n FROM matches_stats"
        )
        print(f"[Backfill] Done. Total enriched: "
              f"{enriched_now['n'] if enriched_now else 0}")

    def _enrich_match(self, match_id: int, skip_if_exists: bool = True):
        """Fetch and store stats, lineups, events for one match.
        skip_if_exists: if True, skips matches already in matches_stats (default).
        Set False only when you want to force re-enrich a specific match.

        Failed matches (API returns nothing) are logged in backfill_progress
        with status='failed' so they are never retried in the main backfill loop.
        Only the single-fixture option bypasses this with skip_if_exists=False.
        """
        # Verify this match exists in our DB
        mb = self.db.fetchone(
            "SELECT home_team_name FROM matches_basic WHERE match_id=%s", (match_id,)
        )
        if not mb:
            return   # match not in our DB

        # Skip if already enriched (saves API requests)
        if skip_if_exists:
            already = self.db.fetchone(
                "SELECT match_id FROM matches_stats WHERE match_id=%s LIMIT 1",
                (match_id,)
            )
            if already:
                return   # already done — no API call needed

            # Also skip if previously marked as failed — API had nothing for it
            failed = self.db.fetchone(
                "SELECT match_id FROM backfill_progress WHERE match_id=%s",
                (match_id,)
            )
            if failed:
                return   # already tried, API returned nothing — don't retry

        # Stats
        stats = self.afl.fetch_match_stats(match_id)
        if not stats:
            # API returned nothing — mark as attempted so we never retry
            # (retrying wastes requests; the data simply doesn't exist in the API)
            self.db.execute(
                "INSERT IGNORE INTO backfill_progress (match_id) VALUES (%s)",
                (match_id,)
            )
            return   # no stats = nothing else to fetch
        for team_data in stats:
            team_name = config.normalize_team_name(
                team_data.get("team", {}).get("name", ""))
            is_home = None
            home_row = self.db.fetchone(
                "SELECT home_team_name FROM matches_basic WHERE match_id=%s",
                (match_id,)
            )
            if home_row:
                is_home = 1 if team_name == home_row["home_team_name"] else 0

            s = {item["type"]: item.get("value")
                 for item in team_data.get("statistics", [])}

            def parse_stat(val):
                if val is None: return None
                if isinstance(val, str):
                    val = val.replace("%", "").strip()
                    try: return float(val)
                    except: return None
                return float(val) if val is not None else None

            xg_val      = parse_stat(s.get("expected_goals"))
            shots_total = parse_stat(s.get("Total Shots"))
            shots_on    = parse_stat(s.get("Shots on Goal"))
            penalties   = parse_stat(s.get("Penalty")) or 0

            npxg = (xg_val - penalties * 0.79) if xg_val is not None else None
            npxg = max(0.0, npxg) if npxg is not None else None
            shot_quality = (xg_val / shots_total) if (xg_val and shots_total and shots_total > 0) else None

            self.db.execute(
                """INSERT IGNORE INTO matches_stats
                   (match_id, team_name, is_home,
                    shots_total, shots_on_target, shots_off_target, shots_blocked,
                    possession_pct, passes_total, passes_accurate, pass_accuracy_pct,
                    fouls, yellow_cards, red_cards, corners, offsides, saves, xg,
                    npxg, shot_quality, penalties_awarded)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (match_id, team_name, is_home,
                 shots_total, shots_on,
                 parse_stat(s.get("Shots off Goal")),
                 parse_stat(s.get("Blocked Shots")),
                 parse_stat(s.get("Ball Possession")),
                 parse_stat(s.get("Total passes")),
                 parse_stat(s.get("Passes accurate")),
                 parse_stat(s.get("Passes %")),
                 parse_stat(s.get("Fouls")),
                 parse_stat(s.get("Yellow Cards")),
                 parse_stat(s.get("Red Cards")),
                 parse_stat(s.get("Corner Kicks")),
                 parse_stat(s.get("Offsides")),
                 parse_stat(s.get("Goalkeeper Saves")),
                 xg_val, npxg, shot_quality, int(penalties))
            )

        # Lineups
        lineups = self.afl.fetch_lineups(match_id)
        if lineups:
            home_row = self.db.fetchone(
                "SELECT home_team_name FROM matches_basic WHERE match_id=%s",
                (match_id,)
            )
            for team_data in lineups:
                team_name = team_data.get("team", {}).get("name", "")
                is_home = 1 if home_row and team_name == home_row["home_team_name"] else 0
                for p in team_data.get("startXI", []):
                    pi = p.get("player", {})
                    self.db.execute(
                        """INSERT IGNORE INTO matches_lineups
                           (match_id, team_name, is_home, player_name, position,
                            is_starter, shirt_number)
                           VALUES (%s,%s,%s,%s,%s,1,%s)""",
                        (match_id, team_name, is_home,
                         pi.get("name", ""), pi.get("pos", ""),
                         pi.get("number"))
                    )
                for p in team_data.get("substitutes", []):
                    pi = p.get("player", {})
                    self.db.execute(
                        """INSERT IGNORE INTO matches_lineups
                           (match_id, team_name, is_home, player_name, position,
                            is_starter, shirt_number)
                           VALUES (%s,%s,%s,%s,%s,0,%s)""",
                        (match_id, team_name, is_home,
                         pi.get("name", ""), pi.get("pos", ""),
                         pi.get("number"))
                    )

        # Events
        events = self.afl.fetch_events(match_id)
        if events:
            for ev in events:
                self.db.execute(
                    """INSERT IGNORE INTO match_events
                       (match_id, minute, extra_time, team_name, player_name,
                        event_type, detail)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (match_id,
                     ev.get("time", {}).get("elapsed"),
                     ev.get("time", {}).get("extra", 0),
                     ev.get("team", {}).get("name", ""),
                     ev.get("player", {}).get("name", ""),
                     ev.get("type", ""),
                     ev.get("detail", ""))
                )


    def team_non_goal_rates(self, team: str) -> dict:
        """
        Compute team-specific corners and cards rates from the last 40 enriched
        matches for this team (home or away), ordered by most recent first.
        Falls back to league averages if fewer than 5 enriched matches found.
        """
        rows = self.db.fetchall(
            """SELECT s.corners, s.yellow_cards
               FROM matches_stats s
               JOIN matches_basic b ON s.match_id = b.match_id
               WHERE (b.home_team_name = %s AND s.is_home = 1)
                  OR (b.away_team_name = %s AND s.is_home = 0)
               ORDER BY b.match_date DESC
               LIMIT 40""",
            (team, team)
        )
        if not rows or len(rows) < 5:
            return {}
        corners = [r["corners"] for r in rows if r["corners"] is not None]
        cards   = [r["yellow_cards"] for r in rows if r["yellow_cards"] is not None]
        return {
            "avg_corners": float(np.mean(corners)) if corners else None,
            "avg_cards"  : float(np.mean(cards))   if cards   else None,
            "n_matches"  : len(rows),
        }

    def referee_card_rate(self, referee_name: str) -> float:
        """
        Compute referee-specific card rate as a multiplier vs PL average.
        1.0 = average. >1.0 = card-happy referee. Falls back to 1.0 if unknown.
        """
        if not referee_name:
            return 1.0
        # Normalise against the active league's average, not always PL
        lg_avgs = config.get_league_avgs()
        league_avg_cards = lg_avgs["cards_h"] + lg_avgs["cards_a"]
        rows2 = self.db.fetchall(
            """SELECT AVG(card_count) as avg_cards FROM (
                   SELECT b.match_id, COUNT(e.id) as card_count
                   FROM matches_basic b
                   JOIN match_events e ON b.match_id = e.match_id
                   WHERE e.event_type IN ('Yellow Card','Red Card')
                     AND b.match_id IN (
                         SELECT DISTINCT match_id FROM match_events
                         WHERE player_name LIKE %s
                     )
                   GROUP BY b.match_id
               ) sub""",
            (f"%{referee_name.split()[-1]}%",)
        )
        if rows2 and rows2[0]["avg_cards"]:
            ref_avg = float(rows2[0]["avg_cards"])
            return float(np.clip(ref_avg / league_avg_cards, 0.6, 1.8))
        return 1.0

    def load_for_fixture(self, home: str, away: str) -> pd.DataFrame:
        """
        Pull all matches involving either team.
        Joins basic + stats tables for a richer feature set.
        """
        rows = self.db.fetchall(
            """SELECT b.match_id, b.match_date, b.season,
                      b.home_team_name, b.away_team_name,
                      b.home_goals, b.away_goals,
                      b.ht_home_goals, b.ht_away_goals,
                      sh.shots_on_target  as home_sot,
                      sh.xg               as home_xg,
                      sh.npxg             as home_npxg,
                      sh.shot_quality     as home_shot_quality,
                      sh.corners          as home_corners,
                      sh.yellow_cards     as home_yellows,
                      sh.possession_pct   as home_possession,
                      sa.shots_on_target  as away_sot,
                      sa.xg               as away_xg,
                      sa.npxg             as away_npxg,
                      sa.shot_quality     as away_shot_quality,
                      sa.corners          as away_corners,
                      sa.yellow_cards     as away_yellows,
                      sa.possession_pct   as away_possession
               FROM matches_basic b
               LEFT JOIN matches_stats sh
                      ON b.match_id = sh.match_id AND sh.is_home = 1
               LEFT JOIN matches_stats sa
                      ON b.match_id = sa.match_id AND sa.is_home = 0
               WHERE b.home_team_name IN (%s,%s)
                  OR b.away_team_name IN (%s,%s)
               ORDER BY b.match_date""",
            (home, away, home, away)
        )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["match_date"] = pd.to_datetime(df["match_date"])
        return df

    def fixture_data_completeness(self, home: str, away: str) -> dict:
        """
        Check how complete the data is for a given fixture.
        Returns a dict showing what is available vs missing.
        Run from MySQL Workbench or at prediction time to assess data quality.
        """
        # Count matches in DB for each team
        h_basic = self.db.fetchone(
            "SELECT COUNT(*) as n FROM matches_basic "
            "WHERE home_team_name=%s OR away_team_name=%s", (home, home)
        )
        a_basic = self.db.fetchone(
            "SELECT COUNT(*) as n FROM matches_basic "
            "WHERE home_team_name=%s OR away_team_name=%s", (away, away)
        )
        # Count enriched matches (have xG, shots, corners)
        h_enriched = self.db.fetchone(
            """SELECT COUNT(DISTINCT b.match_id) as n
               FROM matches_basic b
               JOIN matches_stats s ON b.match_id = s.match_id
               WHERE b.home_team_name=%s OR b.away_team_name=%s""",
            (home, home)
        )
        a_enriched = self.db.fetchone(
            """SELECT COUNT(DISTINCT b.match_id) as n
               FROM matches_basic b
               JOIN matches_stats s ON b.match_id = s.match_id
               WHERE b.home_team_name=%s OR b.away_team_name=%s""",
            (away, away)
        )
        # Check H2H from matches_basic (more reliable than head_to_head table)
        h2h_count = self.db.fetchone(
            """SELECT COUNT(*) as n FROM matches_basic
               WHERE (home_team_name=%s AND away_team_name=%s)
                  OR (home_team_name=%s AND away_team_name=%s)""",
            (home, away, away, home)
        )
        # Check lineups available
        h_lineups = self.db.fetchone(
            """SELECT COUNT(DISTINCT b.match_id) as n
               FROM matches_basic b
               JOIN matches_lineups l ON b.match_id = l.match_id
               WHERE b.home_team_name=%s OR b.away_team_name=%s""",
            (home, home)
        )

        h_b = h_basic["n"] if h_basic else 0
        a_b = a_basic["n"] if a_basic else 0
        h_e = h_enriched["n"] if h_enriched else 0
        a_e = a_enriched["n"] if a_enriched else 0
        h2h = h2h_count["n"] if h2h_count else 0
        h_l = h_lineups["n"] if h_lineups else 0

        result = {
            "home_team"         : home,
            "away_team"         : away,
            "home_basic_matches": h_b,
            "away_basic_matches": a_b,
            "home_enriched"     : h_e,
            "away_enriched"     : a_e,
            "home_enrich_pct"   : round(h_e/h_b*100, 1) if h_b else 0,
            "away_enrich_pct"   : round(a_e/a_b*100, 1) if a_b else 0,
            "h2h_records"       : h2h,
            "lineup_records"    : h_l,
            "has_full_data"     : h_e >= 10 and a_e >= 10,
            "data_quality"      : (
                "FULL"     if h_e >= 20 and a_e >= 20 and h2h >= 3 else
                "GOOD"     if h_e >= 10 and a_e >= 10 else
                "PARTIAL"  if h_e >= 5  or  a_e >= 5  else
                "MINIMAL"
            ),
        }
        return result

    def print_fixture_readiness(self, home: str, away: str):
        """Print a human-readable data readiness report for a fixture."""
        c = self.fixture_data_completeness(home, away)
        icons = {"FULL": "🟢", "GOOD": "🟡", "PARTIAL": "🟠", "MINIMAL": "🔴"}
        icon  = icons.get(c["data_quality"], "❓")
        print(f"\n  {icon} DATA READINESS: {c['data_quality']}  —  {home} vs {away}")
        print(f"  {home}: {c['home_enriched']}/{c['home_basic_matches']} "
              f"enriched ({c['home_enrich_pct']}%)")
        print(f"  {away}: {c['away_enriched']}/{c['away_basic_matches']} "
              f"enriched ({c['away_enrich_pct']}%)")
        print(f"  H2H records: {c['h2h_records']}   "
              f"Lineup records: {c['lineup_records']}")
        if c["data_quality"] == "MINIMAL":
            print("  ⚠  Run the script and let backfill enrich these teams first.")

    # ── Fetch injuries for prediction ──────────────────────────────────────────
    def fetch_injuries_for_team(self, team_name: str,
                                fixture_id: int = None) -> list:
        """
        Fetch current injuries for a team.

        Strategy (in priority order):
          1. Fixture-based endpoint (/injuries?fixture=ID) — most accurate.
             Returns only players confirmed absent for that specific match.
             No filtering, deduplication, or return-date guessing needed.
          2. Season-based endpoint (/injuries?season=Y&team=X) — fallback.
             Returns full season injury log. Requires dedup + date filtering.

        Uses today's DB cache to avoid redundant API calls.
        Hard cap: 8 players maximum (fixture endpoint) or 5 (season fallback).
        """
        cached = self.db.fetchall(
            """SELECT player_name, injury_type, reason, expected_return, position
               FROM injuries
               WHERE team_name=%s AND DATE(fetched_at)=CURDATE()""",
            (team_name,)
        )
        if cached:
            return cached

        # ── Path 1: Fixture-specific injuries ────────────────────────────────
        if fixture_id:
            injuries = self._fetch_fixture_injuries(
                team_name, fixture_id)
            if injuries is not None:   # None = endpoint failed; [] = genuinely 0
                self._persist_injuries(team_name, injuries)
                print(f"  [{len(injuries)} injured]  (fixture #{fixture_id})")
                return injuries

        # ── Path 2: Season-based fallback ─────────────────────────────────────
        injuries = self._fetch_season_injuries(team_name)
        self._persist_injuries(team_name, injuries)
        print(f"  [{len(injuries)} injured]  (season fallback)")
        return injuries

    def _fetch_fixture_injuries(self, team_name: str,
                                fixture_id: int) -> list | None:
        """
        Use /injuries?fixture=ID to get confirmed absentees for one match.
        Returns list of injury dicts, or None if the endpoint is unavailable.
        """
        print(f"  [API] Fetching fixture injuries ({team_name}) …",
              end=" ", flush=True)
        raw = self.afl.fetch_fixture_injuries(fixture_id)
        if not raw:
            # Endpoint returned nothing — could be too early before kickoff
            print("not yet available")
            return None

        team_id = self._get_team_id(team_name)
        result  = []
        seen    = set()

        for item in raw:
            p     = item.get("player", {}) or {}
            t     = item.get("team",   {}) or {}
            inj   = item.get("type",   "") or ""

            # Filter to this team only
            if team_id and t.get("id") and t["id"] != team_id:
                continue
            if not team_id:
                # Name-based fallback team filter
                tname = (t.get("name") or "").lower()
                if team_name.split()[0].lower() not in tname:
                    continue

            pname = (p.get("name") or "").strip()
            if not pname or pname.lower() in seen:
                continue
            seen.add(pname.lower())

            result.append({
                "player_name"    : pname,
                "injury_type"    : inj.strip() or "Unknown",
                "reason"         : (p.get("reason") or "").strip(),
                "expected_return": (p.get("missing_estimated_date") or "TBD").strip(),
                "position"       : "MF",   # enriched in a moment
            })

        if result:
            result = self._attach_positions(result, team_name)

        return result[:8]   # hard cap for fixture endpoint

    def _fetch_season_injuries(self, team_name: str) -> list:
        """
        Fallback: fetch season-long injury log and filter to current absences.

        PREVIOUS BUG: code dropped ALL TBD/missing return dates. This caused
        Bundesliga and Serie A to always return 0 injuries because those leagues
        report injuries without precise return dates far more often than the PL.

        NEW LOGIC:
          - Keep players with a FUTURE return date (clearly still out)
          - Keep players with TBD/missing date IF injury type is a physical
            injury that typically lasts more than 1 week (not suspension/ban)
          - Drop players with a PAST return date (back from injury)
          - Drop if injury type is "Suspension" or "Ban" (cleared after 1 match)
          - Cap at 8 players, sorted by position impact
        """
        team_id = self._get_team_id(team_name)
        if not team_id:
            return []

        print(f"  [API] Fetching season injuries ({team_name}) …",
              end=" ", flush=True)
        data = self.afl.fetch_injuries(team_id, season=config.AFL_SEASONS[-1])
        if not data:
            print("none found")
            return []

        now = pd.Timestamp.now()

        # Injury types that persist (keep even without return date)
        PHYSICAL_TYPES = {
            "muscle injury", "knee injury", "ankle injury", "hamstring",
            "thigh injury", "calf injury", "foot injury", "back injury",
            "shoulder injury", "achilles", "hip injury", "groin injury",
            "ligament injury", "fracture", "broken", "surgery", "operation",
            "illness", "covid", "concussion", "head injury", "eye injury",
            "torn", "strain", "sprain", "rupture", "dislocation",
            "muskelverletzung", "oberschenkelprobleme", "adduktorenprobleme",
            "knieprobleme", "sprunggelenkprobleme", "wadenprobleme",
            "rückenprobleme", "leistenprobleme", "blessure", "blessé",
            "infortunio", "lesión", "verletzt",
        }
        # Injury types to always DROP (temporary, one-match only)
        SKIP_TYPES = {"suspension", "ban", "red card", "sperre", "sospensione",
                      "sanción", "disciplinary"}

        # Deduplicate: last record wins (API returns oldest first usually)
        seen: dict = {}
        for item in data:
            p     = item.get("player", {}) or {}
            pname = (p.get("name") or p.get("firstname", "")).strip()
            if pname:
                seen[pname] = item

        current = []
        for pname, item in seen.items():
            p        = item.get("player", {}) or {}
            inj      = item.get("injury",  {}) or {}
            expected = (p.get("missing_estimated_date") or "").strip()
            inj_type = (inj.get("type") or inj.get("reason") or "").lower().strip()
            inj_reason = (inj.get("reason") or "").lower().strip()

            # Always skip suspensions — they're gone after 1 match
            if any(skip in inj_type or skip in inj_reason
                   for skip in SKIP_TYPES):
                continue

            keep = False

            if expected and expected.upper() not in ("TBD", "N/A", "-", ""):
                try:
                    ret_date = pd.to_datetime(expected)
                    if ret_date >= now:
                        keep = True   # future return date — still out
                    # else: past return date — player is back, skip
                except Exception:
                    # Unparseable date but has a value — treat as unknown
                    keep = True

            else:
                # TBD or missing return date — keep if it's a physical injury
                # This is the fix for Bundesliga/Serie A which rarely populate dates
                if inj_type or inj_reason:
                    is_physical = any(phys in inj_type or phys in inj_reason
                                      for phys in PHYSICAL_TYPES)
                    # If we can't identify the type at all, keep it conservatively
                    keep = True if (is_physical or not inj_type) else False
                else:
                    # No type info at all — keep conservatively
                    keep = True

            if keep:
                current.append({
                    "player_name"    : pname,
                    "injury_type"    : (inj.get("type") or "Unknown").strip(),
                    "reason"         : (inj.get("reason") or "").strip(),
                    "expected_return": expected or "TBD",
                    "position"       : "MF",
                })

        if current:
            current = self._attach_positions(current, team_name)

        # Sort by position impact: FW > MF/GK > DF
        pos_priority = {"FW": 0, "MF": 1, "GK": 1, "DF": 2}
        current.sort(key=lambda x: pos_priority.get(x.get("position", "MF"), 1))
        result = current[:8]
        print(f"{len(result)} current injuries ({len(seen)} unique in season log)")
        return result

    def _attach_positions(self, injuries: list, team_name: str) -> list:
        """
        Cross-reference the lineups table to add real positions to injury records.
        Uses a subquery to get the most recent position per player, avoiding all
        MySQL ONLY_FULL_GROUP_BY and DISTINCT+ORDER BY compatibility issues.
        """
        squad_rows = self.db.fetchall(
            """SELECT l.player_name, l.position
               FROM matches_lineups l
               INNER JOIN (
                   SELECT player_name, MAX(match_id) AS latest_match
                   FROM matches_lineups
                   WHERE team_name = %s AND position IS NOT NULL
                   GROUP BY player_name
               ) sub ON l.player_name = sub.player_name
                     AND l.match_id    = sub.latest_match
               WHERE l.team_name = %s""",
            (team_name, team_name)
        )
        pos_lookup: dict = {}
        for row in (squad_rows or []):
            key = (row.get("player_name") or "").lower()
            if key:
                pos_lookup[key] = row.get("position", "MF")

        for inj in injuries:
            pkey = inj["player_name"].lower()
            pos  = pos_lookup.get(pkey)
            if not pos:
                # Last-name fuzzy match
                last = pkey.split()[-1] if pkey else ""
                pos  = next(
                    (v for k, v in pos_lookup.items() if last and last in k),
                    None
                )
            if pos:
                inj["position"] = pos
        return injuries

    def _persist_injuries(self, team_name: str, injuries: list):
        """Save today's filtered injury list to DB."""
        self.db.execute("DELETE FROM injuries WHERE team_name=%s", (team_name,))
        for inj in injuries:
            self.db.execute(
                """INSERT INTO injuries
                   (team_name, player_name, injury_type, reason, expected_return, position)
                   VALUES (%s,%s,%s,%s,%s,%s)""",
                (team_name, inj["player_name"], inj["injury_type"],
                 inj["reason"], inj["expected_return"], inj["position"])
            )

    # ── Fetch H2H ──────────────────────────────────────────────────────────────
    def fetch_opening_odds_for_fixture(self, fixture_id: int) -> dict:
        """
        Fetch stored opening odds for a fixture from market_opening_odds table.
        Populated by weekly_update.py every Monday.
        Returns {market_key: odds} or empty dict if not found.
        """
        if not fixture_id:
            return {}
        rows = self.db.fetchall(
            "SELECT market, odds FROM market_opening_odds WHERE fixture_id=%s",
            (fixture_id,)
        )
        return {r["market"]: float(r["odds"]) for r in rows} if rows else {}

    def sync_cross_competition_fixtures(self, force: bool = False):
        """
        Sync fixture calendars for CL/EL/Cup competitions.
        Cost: ~6 API requests per run (one per competition).

        Skip condition: already synced within the last 7 days (not just today).
        This matches the weekly_update.py schedule — if weekly_update ran on
        Monday, main.py won't re-sync until next Monday. Without this, main.py
        would re-sync every single session because 'today' changes daily.

        Set force=True to bypass and always re-sync (used by weekly_update.py).
        """
        if not force:
            last = self.db.fetchone(
                "SELECT MAX(created_at) as last_sync FROM cross_competition_fixtures"
            )
            if last and last["last_sync"]:
                last_sync = pd.to_datetime(last["last_sync"])
                days_since = (pd.Timestamp.now() - last_sync).days
                if days_since < 7:
                    print(f"  [CrossComp] Synced {days_since}d ago — skipping (next sync in {7-days_since}d) ✓")
                    return

        comp_ids = config.get_cross_comp_ids()
        season   = config.AFL_SEASONS[-1]

        comp_names = {
            2: "Champions League", 3: "Europa League", 848: "Conference League",
            45: "FA Cup", 48: "Carabao Cup", 81: "DFB Pokal",
            66: "Coupe de France", 137: "Coppa Italia", 143: "Copa del Rey",
        }

        total_inserted = 0
        for league_id in comp_ids:
            name = comp_names.get(league_id, f"League {league_id}")
            print(f"  [CrossComp] Syncing {name} …", end=" ", flush=True)
            fixtures = self.afl.fetch_competition_fixtures(league_id, season)
            if not fixtures:
                print("no data"); continue

            inserted = 0
            for m in fixtures:
                fix   = m.get("fixture", {})
                teams = m.get("teams", {})
                fid   = fix.get("id")
                date_str = fix.get("date", "")
                if not fid or not date_str:
                    continue

                status = fix.get("status", {}).get("short", "")
                if status in ("CANC", "PST", "ABD"):
                    continue

                try:
                    match_dt = pd.to_datetime(date_str).replace(tzinfo=None)
                except Exception:
                    continue

                home_name = teams.get("home", {}).get("name", "")
                away_name = teams.get("away", {}).get("name", "")
                home_id   = teams.get("home", {}).get("id")
                away_id   = teams.get("away", {}).get("id")

                for team_name, team_id, venue in [
                    (home_name, home_id, "home"),
                    (away_name, away_id, "away"),
                ]:
                    if not team_name:
                        continue
                    self.db.execute(
                        """INSERT IGNORE INTO cross_competition_fixtures
                           (team_name, team_id, competition, league_id,
                            match_date, opponent, venue, season, fixture_id)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (team_name, team_id, name, league_id,
                         match_dt,
                         away_name if venue == "home" else home_name,
                         venue, season, fid)
                    )
                    inserted += 1

            total_inserted += inserted
            print(f"{inserted // 2} matches")

        print(f"  [CrossComp] Done. {total_inserted // 2} total fixtures stored.")

    def enrich_match_player_stats(self, match_id: int,
                                   skip_if_exists: bool = True) -> bool:
        """
        Fetch and store per-player stats for one match using /fixtures/players.
        Returns True if new data was stored, False if skipped or failed.

        This is separate from _enrich_match (which fetches team-level stats).
        Cost: 1 additional API request per match.
        """
        if skip_if_exists:
            already = self.db.fetchone(
                "SELECT id FROM match_player_stats WHERE match_id=%s LIMIT 1",
                (match_id,)
            )
            if already:
                return False   # already done

        mb = self.db.fetchone(
            "SELECT home_team_name, away_team_name FROM matches_basic WHERE match_id=%s",
            (match_id,)
        )
        if not mb:
            return False

        raw = self.afl.fetch_fixture_player_stats(match_id)
        if not raw:
            return False

        inserted = 0
        for team_data in raw:
            team_name = team_data.get("team", {}).get("name", "")
            players   = team_data.get("players", [])
            for p_entry in players:
                p     = p_entry.get("player", {})
                s_list = p_entry.get("statistics", [{}])
                s     = s_list[0] if s_list else {}

                pid   = p.get("id")
                pname = (p.get("name") or "").strip()
                if not pid or not pname:
                    continue

                games    = s.get("games", {})
                goals    = s.get("goals", {})
                shots    = s.get("shots", {})
                passes   = s.get("passes", {})

                def _f(val):
                    try: return float(val) if val is not None else None
                    except: return None
                def _i(val):
                    try: return int(val) if val is not None else None
                    except: return None

                self.db.execute(
                    """INSERT IGNORE INTO match_player_stats
                       (match_id, team_name, player_id, player_name, position,
                        minutes_played, goals, assists, shots_total, shots_on,
                        xg, key_passes, rating)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (match_id, team_name, pid, pname,
                     games.get("position", "MF"),
                     _i(games.get("minutes")),
                     _i(goals.get("total")) or 0,
                     _i(goals.get("assists")) or 0,
                     _i(shots.get("total")),
                     _i(shots.get("on")),
                     _f(shots.get("on")),    # API sometimes puts xG here
                     _i(passes.get("key")),
                     _f(games.get("rating")))
                )
                inserted += 1

        return inserted > 0

    def count_player_stats_coverage(self) -> dict:
        """Return player stats enrichment coverage vs total enriched matches."""
        total = self.db.fetchone(
            "SELECT COUNT(DISTINCT match_id) as n FROM matches_stats"
        )
        covered = self.db.fetchone(
            "SELECT COUNT(DISTINCT match_id) as n FROM match_player_stats"
        )
        n_t = total["n"]   if total   else 0
        n_c = covered["n"] if covered else 0
        return {"total_enriched": n_t, "player_stats": n_c,
                "pending": n_t - n_c, "pct": round(n_c/max(n_t,1)*100, 1)}

    # ── xG-Elo Ratings ────────────────────────────────────────────────────────

    def compute_xg_elo_ratings(self) -> int:
        """
        Compute xG-Elo ratings for every team from all historical matches
        and write them to the elo_ratings table.

        Algorithm (sequential, chronological):
          1. Load every match in matches_basic JOIN matches_stats, ordered by date.
          2. For each match:
             a. Look up current Elo for home and away (default 1500).
             b. Compute expected xG share using the Elo formula with home advantage.
             c. Compute actual xG share from npxG → xG → goals fallback.
             d. Update Elo for both teams using K-factor × (actual - expected).
          3. Upsert final ratings to elo_ratings table.

        Parameters:
          HOME_ADVANTAGE : 65   — Elo points added to home expected score.
                                  Empirical value from Club Elo / football literature.
                                  Equivalent to ~0.09 xG share advantage at home.
          K_FACTOR       : 32   — Learning rate per match. Calibrated so one
                                  dominant performance (0.8 xG share) moves a team
                                  ~6 Elo points. Too high = noisy; too low = stale.
          ELO_START      : 1500 — Universal starting point. Newly promoted teams
                                  begin at 1500 which is below the established-team
                                  average (~1530), reflecting appropriate uncertainty.

        xG share vs goals share:
          Using xG share (rather than win/draw/loss outcome) is the key improvement.
          Goals-based Elo (Club Elo, FiveThirtyEight SPI) still contains finishing
          luck. xG share removes that variance — a team that dominates possession
          and shots but loses 1-0 to a worldie still gets a positive Elo update.

        Returns the number of teams whose ratings were updated.
        """
        HOME_ADVANTAGE = 65
        K_FACTOR       = 32
        ELO_START      = 1500.0

        # Load all historical matches with xG data, chronological order
        rows = self.db.fetchall(
            """SELECT b.match_id, b.match_date,
                      b.home_team_name, b.away_team_name,
                      b.home_goals,     b.away_goals,
                      sh.npxg AS home_npxg, sh.xg AS home_xg,
                      sa.npxg AS away_npxg, sa.xg AS away_xg
               FROM matches_basic b
               LEFT JOIN matches_stats sh ON b.match_id = sh.match_id AND sh.is_home = 1
               LEFT JOIN matches_stats sa ON b.match_id = sa.match_id AND sa.is_home = 0
               WHERE b.home_goals IS NOT NULL AND b.away_goals IS NOT NULL
               ORDER BY b.match_date ASC"""
        )

        if not rows:
            print("  [Elo] No match data found — ratings not computed.")
            return 0

        # Rolling ratings dict: team_name → float
        ratings: dict[str, float] = {}
        match_counts: dict[str, int] = {}

        def get_elo(team: str) -> float:
            return ratings.get(team, ELO_START)

        def xg_share(home_xg_val, away_xg_val, home_goals, away_goals) -> float:
            """
            Resolve xG share from best available signal.
            Returns home_xg / (home_xg + away_xg), clamped to [0.05, 0.95].
            Falls back: npxG → xG → goals.
            """
            hv = home_xg_val if (home_xg_val is not None and home_xg_val >= 0) else None
            av = away_xg_val if (away_xg_val is not None and away_xg_val >= 0) else None
            if hv is None or av is None:
                hv = float(home_goals)
                av = float(away_goals)
            total = hv + av
            if total < 0.01:
                return 0.5   # scoreless / no data → neutral
            return float(np.clip(hv / total, 0.05, 0.95))

        for row in rows:
            home = row["home_team_name"]
            away = row["away_team_name"]
            if not home or not away:
                continue

            elo_h = get_elo(home)
            elo_a = get_elo(away)

            # Expected xG share for home team (with home advantage)
            # Formula: E_home = 1 / (1 + 10^(-(elo_h + HA - elo_a) / 400))
            exp_h = 1.0 / (1.0 + 10.0 ** (-(elo_h + HOME_ADVANTAGE - elo_a) / 400.0))

            # Actual xG share — prefer npxG, fall back to xG, then goals
            home_signal = row["home_npxg"] if row.get("home_npxg") is not None else row.get("home_xg")
            away_signal = row["away_npxg"] if row.get("away_npxg") is not None else row.get("away_xg")
            act_h = xg_share(home_signal, away_signal,
                             row["home_goals"], row["away_goals"])

            # Elo update
            delta = K_FACTOR * (act_h - exp_h)
            ratings[home] = get_elo(home) + delta
            ratings[away] = get_elo(away) - delta
            match_counts[home] = match_counts.get(home, 0) + 1
            match_counts[away] = match_counts.get(away, 0) + 1

        # Upsert all ratings to DB
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for team, elo in ratings.items():
            self.db.execute(
                """INSERT INTO elo_ratings (team_name, elo, n_matches, last_updated)
                   VALUES (%s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                     elo          = VALUES(elo),
                     n_matches    = VALUES(n_matches),
                     last_updated = VALUES(last_updated)""",
                (team, round(float(elo), 4), match_counts.get(team, 0), now)
            )

        n_teams = len(ratings)
        avg_elo = sum(ratings.values()) / max(n_teams, 1)
        top5 = sorted(ratings.items(), key=lambda x: x[1], reverse=True)[:5]
        bot5 = sorted(ratings.items(), key=lambda x: x[1])[:5]
        print(f"  [Elo] Computed ratings for {n_teams} teams "
              f"from {len(rows)} matches  (avg={avg_elo:.0f})")
        print(f"  [Elo] Top 5: " +
              "  ".join(f"{t} {e:.0f}" for t, e in top5))
        print(f"  [Elo] Bot 5: " +
              "  ".join(f"{t} {e:.0f}" for t, e in bot5))
        return n_teams

    def get_xg_elo(self, team_name: str) -> float:
        """
        Return the current xG-Elo rating for a team.
        Falls back to 1500 (league average) if the team has no rating yet.

        Called by model.py FixtureModel to set attack/defense prior centers:
          elo_delta = (home_elo - away_elo) / 400
          attack_prior_center[home]  = +elo_delta * 0.30
          defense_prior_center[home] = -elo_delta * 0.15  (defense = opponent attack)

        A 200-point Elo gap → elo_delta = 0.5 → attack prior shift of +0.15 log-units
        ≈ 16% higher expected goals for the stronger team. This is calibrated to
        roughly match observed xG differences between top-4 and bottom-4 PL teams.
        """
        row = self.db.fetchone(
            "SELECT elo FROM elo_ratings WHERE team_name = %s", (team_name,)
        )
        if row:
            return float(row["elo"])
        # Fuzzy fallback: first-word match (handles "Man City" vs "Manchester City")
        first = team_name.split()[0] if team_name else ""
        if first:
            row = self.db.fetchone(
                "SELECT elo FROM elo_ratings WHERE team_name LIKE %s LIMIT 1",
                (f"{first}%",)
            )
            if row:
                return float(row["elo"])
        return 1500.0   # league average — safe default for unknown teams

    def get_all_elo_ratings(self) -> dict:
        """
        Return all xG-Elo ratings as {team_name: elo_float}.
        Called by main.py to pass a pre-built dict to FixtureModel.
        Falls back to empty dict if the elo_ratings table is empty,
        which causes FixtureModel to use zero-centered priors (safe default).
        """
        rows = self.db.fetchall("SELECT team_name, elo FROM elo_ratings")
        return {r["team_name"]: float(r["elo"]) for r in rows} if rows else {}

    def get_last_match_date_any_comp(self, team_name: str,
                                     before_date: pd.Timestamp) -> pd.Timestamp | None:
        """
        Return the most recent match date for a team across ALL competitions
        (domestic league + CL/EL/Cups), before a given cutoff date.
        Used by _compute_fatigue to detect cross-competition congestion.

        Strategy:
          1. Check cross_competition_fixtures table (CL/EL/Cup matches)
          2. Check matches_basic (domestic league matches)
          3. Return the more recent of the two
        """
        # Ensure before_date is tz-naive so MySQL comparison works correctly
        if hasattr(before_date, 'tzinfo') and before_date.tzinfo is not None:
            before_date = before_date.tz_convert(None)
        cutoff_str = before_date.strftime("%Y-%m-%d %H:%M:%S")

        # Cross-competition: try both exact name and team_id lookup
        team_id = self._get_team_id(team_name)

        if team_id:
            cross = self.db.fetchone(
                """SELECT MAX(match_date) as last_date
                   FROM cross_competition_fixtures
                   WHERE (team_id=%s OR team_name LIKE %s)
                     AND match_date < %s""",
                (team_id, f"%{team_name.split()[0]}%", cutoff_str)
            )
        else:
            cross = self.db.fetchone(
                """SELECT MAX(match_date) as last_date
                   FROM cross_competition_fixtures
                   WHERE team_name LIKE %s
                     AND match_date < %s""",
                (f"%{team_name.split()[0]}%", cutoff_str)
            )

        # Domestic league (from matches_basic)
        domestic = self.db.fetchone(
            """SELECT MAX(match_date) as last_date
               FROM matches_basic
               WHERE (home_team_name=%s OR away_team_name=%s)
                 AND match_date < %s""",
            (team_name, team_name, cutoff_str)
        )

        dates = []
        if cross   and cross.get("last_date"):
            dates.append(pd.to_datetime(cross["last_date"]))
        if domestic and domestic.get("last_date"):
            dates.append(pd.to_datetime(domestic["last_date"]))

        return max(dates) if dates else None

    def fetch_h2h(self, home: str, away: str) -> pd.DataFrame:
        """Get head-to-head history from DB or API."""
        cached = self.db.fetchall(
            """SELECT match_date, home_team, away_team, home_goals, away_goals
               FROM head_to_head
               WHERE (team_a=%s AND team_b=%s)
                  OR (team_a=%s AND team_b=%s)
               ORDER BY match_date DESC LIMIT 20""",
            (home, away, away, home)
        )
        if cached and len(cached) >= 5:
            return pd.DataFrame(cached)

        # Try to get team IDs
        th_id = self._get_team_id(home)
        ta_id = self._get_team_id(away)
        if not th_id or not ta_id:
            return pd.DataFrame()

        print(f"  [API] Fetching H2H {home} vs {away} …", end=" ", flush=True)
        data = self.afl.fetch_h2h(th_id, ta_id)
        if not data:
            print("none")
            return pd.DataFrame()

        rows = []
        for m in data:
            ft = m.get("score", {}).get("fulltime", {})
            if not ft or ft.get("home") is None:
                continue
            self.db.execute(
                """INSERT INTO head_to_head
                   (team_a, team_b, match_date, home_team, away_team,
                    home_goals, away_goals)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (home, away,
                 pd.to_datetime(m.get("fixture", {}).get("date")).replace(tzinfo=None),
                 m.get("teams", {}).get("home", {}).get("name", ""),
                 m.get("teams", {}).get("away", {}).get("name", ""),
                 int(ft["home"]), int(ft["away"]))
            )
            rows.append({
                "match_date": m.get("fixture", {}).get("date"),
                "home_team" : m.get("teams", {}).get("home", {}).get("name", ""),
                "away_team" : m.get("teams", {}).get("away", {}).get("name", ""),
                "home_goals": int(ft["home"]),
                "away_goals": int(ft["away"]),
            })
        print(f"{len(rows)} historical meetings")
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    # ── Next fixtures ──────────────────────────────────────────────────────────
    def fetch_upcoming(self) -> list:
        league_name = config.ACTIVE_LEAGUE.get("code", "League")
        print(f"  [API] Fetching upcoming {league_name} fixtures …", end=" ", flush=True)
        data = self.afl.fetch_next_fixtures()
        if not data:
            print("unavailable")
            return []
        fixtures = []
        for m in data:
            status = m.get("fixture", {}).get("status", {}).get("short", "")
            if status in ("FT", "AET", "PEN"):
                continue   # skip already finished
            fixtures.append({
                "date"    : m.get("fixture", {}).get("date", "")[:10],
                "time"    : m.get("fixture", {}).get("date", "")[11:16],
                "home"    : m.get("teams", {}).get("home", {}).get("name", ""),
                "away"    : m.get("teams", {}).get("away", {}).get("name", ""),
                "venue"   : m.get("fixture", {}).get("venue", {}).get("name", ""),
                "matchday": m.get("league", {}).get("round", ""),
            })
        print(f"{len(fixtures)} upcoming fixtures")
        return fixtures

    # ── Load teams registry ────────────────────────────────────────────────────
    def ensure_teams(self):
        """Fetch and cache team registry only if not done yet."""
        count = self.db.fetchone("SELECT COUNT(*) as n FROM teams")
        if count and count["n"] > 0:
            return  # already cached — never fetch again
        print("  [API] Fetching team registry (one-time) …", end=" ", flush=True)
        data = self.afl.fetch_teams()
        if not data:
            print("failed")
            return
        for item in data:
            t = item.get("team", {})
            self.db.execute(
                """INSERT IGNORE INTO teams (team_id, name, code, country)
                   VALUES (%s,%s,%s,%s)""",
                (t.get("id"), t.get("name", ""),
                 t.get("code", ""), t.get("country", ""))
            )
        print(f"{len(data)} teams cached ✓")


    # ── Fixture auto-lookup ────────────────────────────────────────────────────

    def _get_team_id(self, team_name: str) -> int | None:
        """
        Robust team ID lookup with 6 strategies.
        Handles: FC prefixes, numeric prefixes (1. FC), special chars,
        and the Manchester City / Manchester United ambiguity.
        """
        # Strip common prefixes for cleaner matching
        def strip_prefix(n):
            for p in ("1. ", "FC ", "SC ", "AC ", "AS ", "RC ", "SV ", "VfB ",
                      "VfL ", "TSG ", "RB ", "SSC ", "US ", "OGC "):
                if n.startswith(p): return n[len(p):]
            return n

        # 1. Exact match
        row = self.db.fetchone("SELECT team_id FROM teams WHERE name=%s LIMIT 1", (team_name,))
        if row: return row["team_id"]

        # 2. Case-insensitive exact
        row = self.db.fetchone("SELECT team_id FROM teams WHERE LOWER(name)=LOWER(%s) LIMIT 1", (team_name,))
        if row: return row["team_id"]

        # 3. DB name contains our query (e.g. "Arsenal FC" matches "Arsenal")
        row = self.db.fetchone("SELECT team_id FROM teams WHERE name LIKE %s LIMIT 1", (f"%{team_name}%",))
        if row: return row["team_id"]

        # 4. Try with prefix stripped (e.g. "1. FC Köln" → "Köln")
        stripped = strip_prefix(team_name)
        if stripped != team_name:
            row = self.db.fetchone("SELECT team_id FROM teams WHERE name LIKE %s LIMIT 1", (f"%{stripped}%",))
            if row: return row["team_id"]

        # 5. All significant words must appear — prevents Man City / Man Utd confusion
        words = [w for w in team_name.split() if len(w) > 2 and w not in ("FC","SC","AC","AS","RC","SV","VfB","VfL","TSG","RB","SSC","US","OGC")]
        if len(words) >= 2:
            conditions = " AND ".join(["name LIKE %s" for _ in words])
            params     = tuple(f"%{w}%" for w in words)
            row = self.db.fetchone(f"SELECT team_id FROM teams WHERE {conditions} LIMIT 1", params)
            if row: return row["team_id"]

        # 6. Last resort — first significant word only (block ambiguous ones)
        sig_words = [w for w in team_name.split() if len(w) > 3
                     and w.lower() not in ("manchester","west","brighton","nottingham",
                                           "wolverhampton","crystal","aston","sheffield",
                                           "sporting","athletic","union","saint")]
        if not sig_words:
            return None
        first = sig_words[0]
        row = self.db.fetchone("SELECT team_id FROM teams WHERE name LIKE %s LIMIT 1", (f"%{first}%",))
        return row["team_id"] if row else None

    def find_fixture(self, home: str, away: str) -> dict:
        """
        Find the upcoming scheduled fixture between two teams.
        Looks up team IDs from the teams table, then queries API-Football
        for the next match between them within 60 days.
        Returns fixture info dict or empty dict if not found / no budget.
        """
        # Use robust lookup — prevents Manchester City / Manchester United confusion
        home_id = self._get_team_id(home)
        away_id = self._get_team_id(away)
        if not home_id or not away_id:
            print(f"  [Fixture] Could not resolve IDs: "
                  f"{home}→{home_id}  {away}→{away_id}")
            return {}

        print(f"  [API] Looking up fixture: {home}(id={home_id}) vs "
              f"{away}(id={away_id}) …", end=" ", flush=True)
        fix = self.afl.find_upcoming_fixture(home_id, away_id)
        if fix and fix.get("fixture_id"):
            dt_str = fix["date"][:16].replace("T", " ") if fix.get("date") else "TBD"
            print(f"found  (ID:{fix['fixture_id']}  {dt_str}  {fix.get('venue','')})")
            # Auto-save venue to DB so weather lookup works even for new venues
            vname = fix.get("venue", "")
            vcity = fix.get("city", "")
            if vname:
                exists = self.db.fetchone(
                    "SELECT id FROM venues WHERE name=%s", (vname,)
                )
                if not exists:
                    # Try to geocode from fallback dict
                    coords = config.VENUE_COORDS_FALLBACK.get(vname)
                    if coords:
                        self.db.execute(
                            "INSERT IGNORE INTO venues (name,city,latitude,longitude)"
                            " VALUES (%s,%s,%s,%s)",
                            (vname, vcity, coords[0], coords[1])
                        )
                    else:
                        # Save without coords — admin can add lat/lng later
                        self.db.execute(
                            "INSERT IGNORE INTO venues (name,city) VALUES (%s,%s)",
                            (vname, vcity)
                        )
                        print(f"  [Venue] '{vname}' saved to DB (no coords yet — add manually)")
        else:
            print("not found in next 60 days")
        return fix

    # ── Automatic odds fetching ────────────────────────────────────────────────
    def fetch_auto_odds(self, fixture_id: int) -> dict:
        """
        Fetch pre-match Bet365 odds and map to our market key format.
        Returns dict compatible with the existing mkt_probs / EV system.
        """
        if not fixture_id:
            return {}

        print(f"  [API] Fetching Bet365 odds (fixture {fixture_id}) …",
              end=" ", flush=True)
        raw = self.afl.fetch_prematch_odds(fixture_id)
        if not raw:
            print("unavailable")
            return {}

        odds = {}

        # ── 1X2 ───────────────────────────────────────────────────────────────
        mw = raw.get("Match Winner", [])
        for v in mw:
            val = v.get("value", ""); odd = float(v.get("odd", 0))
            if odd <= 1.0: continue
            if val == "Home":  odds["home_win"]  = odd
            elif val == "Draw": odds["draw"]      = odd
            elif val == "Away": odds["away_win"]  = odd

        # ── Over/Under goals ──────────────────────────────────────────────────
        for bet_name, values in raw.items():
            if "Goals Over/Under" in bet_name or "Over/Under" in bet_name:
                for v in values:
                    val = v.get("value", ""); odd = float(v.get("odd", 0))
                    if odd <= 1.0: continue
                    if val == "Over 1.5":  odds["over_1.5"]  = odd
                    elif val == "Under 1.5": odds["under_1.5"] = odd
                    elif val == "Over 2.5":  odds["over_2.5"]  = odd
                    elif val == "Under 2.5": odds["under_2.5"] = odd
                    elif val == "Over 3.5":  odds["over_3.5"]  = odd
                    elif val == "Under 3.5": odds["under_3.5"] = odd
                    elif val == "Over 4.5":  odds["over_4.5"]  = odd
                    elif val == "Under 4.5": odds["under_4.5"] = odd

        # ── BTTS ──────────────────────────────────────────────────────────────
        btts = raw.get("Both Teams Score", [])
        for v in btts:
            val = v.get("value", ""); odd = float(v.get("odd", 0))
            if odd <= 1.0: continue
            if val == "Yes": odds["btts_yes"] = odd
            elif val == "No": odds["btts_no"]  = odd

        # ── Double Chance ─────────────────────────────────────────────────────
        dc = raw.get("Double Chance", [])
        for v in dc:
            val = v.get("value", ""); odd = float(v.get("odd", 0))
            if odd <= 1.0: continue
            if val == "Home/Draw":  odds["dc_1x"] = odd
            elif val == "Draw/Away": odds["dc_x2"] = odd
            elif val == "Home/Away": odds["dc_12"] = odd

        # ── Asian Handicap ────────────────────────────────────────────────────
        for bet_name, values in raw.items():
            if "Asian Handicap" in bet_name:
                for v in values:
                    val = v.get("value", ""); odd = float(v.get("odd", 0))
                    if odd <= 1.0: continue
                    if val == "Home -0.5":  odds["ah_-0.5"] = odd
                    elif val == "Away +0.5": odds["ah_+0.5"] = odd
                    elif val == "Home -1.5": odds["ah_-1.5"] = odd
                    elif val == "Away +1.5": odds["ah_+1.5"] = odd
                    elif val == "Home +0.5": odds["ah_h+0.5"] = odd
                    elif val == "Away -0.5": odds["ah_a-0.5"] = odd

        # ── Corners ───────────────────────────────────────────────────────────
        for bet_name, values in raw.items():
            if "Corner" in bet_name and "Over/Under" in bet_name:
                for v in values:
                    val = v.get("value", ""); odd = float(v.get("odd", 0))
                    if odd <= 1.0: continue
                    for line in ("7.5", "8.5", "9.5", "10.5", "11.5", "12.5"):
                        if val == f"Over {line}":  odds[f"cor_ov_{line}"] = odd
                        elif val == f"Under {line}": odds[f"cor_un_{line}"] = odd

        # ── Cards ─────────────────────────────────────────────────────────────
        for bet_name, values in raw.items():
            if "Card" in bet_name and "Over/Under" in bet_name:
                for v in values:
                    val = v.get("value", ""); odd = float(v.get("odd", 0))
                    if odd <= 1.0: continue
                    for line in ("1.5", "2.5", "3.5", "4.5", "5.5"):
                        if val == f"Over {line}":  odds[f"crd_ov_{line}"] = odd
                        elif val == f"Under {line}": odds[f"crd_un_{line}"] = odd

        n = len(odds)
        print(f"{n} markets fetched")
        return odds

    # ── Automatic lineup fetching ──────────────────────────────────────────────
    def fetch_auto_lineups(self, fixture_id: int,
                           home: str, away: str) -> tuple:
        """
        Fetch confirmed lineups from API-Football.
        Only available ~60 minutes before kickoff.
        Returns (players_home, players_away) each as list of dicts.
        """
        if not fixture_id:
            return [], []

        print(f"  [API] Checking lineups …", end=" ", flush=True)
        raw = self.afl.fetch_live_lineups(fixture_id)
        if not raw:
            print("not released yet")
            return [], []

        players_h, players_a = [], []
        for team_data in raw:
            team_name = team_data.get("team", {}).get("name", "")
            is_home   = home.lower() in team_name.lower() or                         team_name.lower() in home.lower()
            target    = players_h if is_home else players_a

            for p in team_data.get("startXI", []):
                pi  = p.get("player", {})
                pos = pi.get("pos", "MF")
                # Map API position codes to our GK/DF/MF/FW
                pos_map = {"G": "GK", "D": "DF", "M": "MF", "F": "FW"}
                pos     = pos_map.get(pos[0] if pos else "M", "MF")
                target.append({
                    "name"    : pi.get("name", ""),
                    "position": pos,
                    "number"  : pi.get("number", 0),
                    "id"      : pi.get("id", 0),
                })

        h_n = len(players_h); a_n = len(players_a)
        if h_n or a_n:
            print(f"✓  {home}: {h_n} players  |  {away}: {a_n} players")
        else:
            print("not released yet")
        return players_h, players_a

    # ── Player xG/90 from API stats ────────────────────────────────────────────
    def enrich_players_with_xg(self, players: list,
                                team_name: str, season: int = None) -> list:
        season = season or config.AFL_SEASONS[-1]
        """
        Match lineup players to their historical xG/90 from the API stats.
        Replaces position-weight estimates with actual player data.
        Returns players list with added 'xg_per90' key.
        """
        if not players:
            return players

        team_id = self._get_team_id(team_name)
        if not team_id:
            return players
        team_row = {"team_id": team_id}

        print(f"  [API] Fetching player xG stats for {team_name} …",
              end=" ", flush=True)
        raw = self.afl.fetch_player_stats(team_row["team_id"], season)
        if not raw:
            print("unavailable")
            return players

        # Build lookup: player name → xG per 90
        xg_lookup = {}
        for item in raw:
            p     = item.get("player", {})
            stats = item.get("statistics", [{}])[0]
            goals = stats.get("goals", {})
            games = stats.get("games", {})
            xg_val   = goals.get("assists")  # API sometimes puts xG here
            shots    = goals.get("total", 0) or 0
            minutes  = games.get("minutes", 0) or 0
            # xG per 90 proxy: shots on target * conversion rate
            sot      = goals.get("saves", 0) or 0   # reuse field
            actual_xg = goals.get("rating")          # sometimes xG rating
            # Best approximation from free tier: goals/90
            goals_n  = goals.get("total", 0) or 0
            xg90     = (goals_n / max(minutes, 1)) * 90 if minutes > 0 else 0
            name     = p.get("name", "")
            if name:
                xg_lookup[name.lower()] = float(xg90)

        enriched = []
        for p in players:
            p_copy = p.copy()
            pname  = p.get("name", "").lower()
            # Fuzzy match player name
            xg90 = xg_lookup.get(pname)
            if xg90 is None:
                # Try last name match
                last = pname.split()[-1] if pname else ""
                for k, v in xg_lookup.items():
                    if last and last in k:
                        xg90 = v
                        break
            p_copy["xg_per90"] = xg90 if xg90 is not None else None
            enriched.append(p_copy)

        matched = sum(1 for p in enriched if p.get("xg_per90") is not None)
        print(f"{matched}/{len(enriched)} players matched")
        return enriched

    # ── Standings context ──────────────────────────────────────────────────────
    def standings_context(self, home: str, away: str,
                          season: int = None) -> dict:
        """
        Fetch current league standings to provide context.
        Returns dict with position, points, form string for each team.

        Matching strategy (in priority order):
          1. team_id from DB (_get_team_id) — avoids ALL name confusion
          2. All significant words present in candidate (len>2, non-generic)
          3. Reject first-word-only match for known ambiguous cities/words
        This fixes Man City vs Man Utd, PSG vs Paris FC, etc.
        """
        print(f"  [API] Fetching standings …", end=" ", flush=True)
        table = self.afl.fetch_standings(season)
        if not table:
            print("unavailable")
            return {}

        # Resolve team IDs up front — primary match key
        home_id = self._get_team_id(home)
        away_id = self._get_team_id(away)

        # Words that must NOT be used alone for matching (too many teams share them)
        _AMBIGUOUS = {
            "manchester","paris","west","brighton","nottingham","wolverhampton",
            "crystal","aston","sheffield","sporting","athletic","union","saint",
            "real","inter","dynamo","lokomotiv","olympique","stade","racing",
            "sporting","atletico","deportivo","fc","sc","ac","as",
        }

        def _name_match(query: str, candidate: str) -> bool:
            """True when candidate is a genuine match for query."""
            q = query.lower().replace("-", " ").replace("'", "").strip()
            c = candidate.lower().replace("-", " ").replace("'", "").strip()
            if q == c:
                return True
            # All significant words from query must appear in candidate
            sig = [w for w in q.split()
                   if len(w) > 2 and w not in _AMBIGUOUS]
            if len(sig) >= 2:
                return all(w in c for w in sig)
            if len(sig) == 1:
                # Only allow single-word match if it covers most of the candidate
                return sig[0] in c and len(sig[0]) / max(len(c.replace(" ","")), 1) > 0.5
            return False

        result = {}
        for entry in table:
            team      = entry.get("team", {})
            api_id    = team.get("id")
            api_name  = team.get("name", "")
            data = {
                "team"  : api_name,
                "pos"   : entry.get("rank", 0),
                "pts"   : entry.get("points", 0),
                "played": entry.get("all", {}).get("played", 0),
                "gd"    : entry.get("goalsDiff", 0),
                "form"  : entry.get("form", ""),
            }
            # Primary: ID match (exact — no ambiguity possible)
            if "home" not in result and home_id and api_id == home_id:
                result["home"] = data
            elif "away" not in result and away_id and api_id == away_id:
                result["away"] = data
            # Fallback: only if ID lookup failed for that side
            elif "home" not in result and not home_id and _name_match(home, api_name):
                result["home"] = data
            elif "away" not in result and not away_id and _name_match(away, api_name):
                result["away"] = data

        n = len(result)
        if n == 2:
            h = result["home"]; a = result["away"]
            print(f"  {h['team']} (#{h['pos']})  vs  {a['team']} (#{a['pos']})")
        else:
            found = [k for k in ("home","away") if k in result]
            missing_home = home if "home" not in result else None
            missing_away = away if "away" not in result else None
            missing = [t for t in (missing_home, missing_away) if t]
            if missing:
                print(f"{n}/2 found — not in standings: {', '.join(missing)}")
            else:
                print(f"{n} teams found in table")
        return result

    def all_team_names(self) -> list:
        rows = self.db.fetchall(
            "SELECT DISTINCT home_team_name FROM matches_basic"
        )
        names = [r["home_team_name"] for r in rows]
        rows2 = self.db.fetchall(
            "SELECT DISTINCT away_team_name FROM matches_basic"
        )
        names += [r["away_team_name"] for r in rows2]
        return sorted(set(names))


# ══════════════════════════════════════════════════════════════════════════════
# TEAM RESOLVER
# ══════════════════════════════════════════════════════════════════════════════


def resolve_team(query: str, known: list) -> str:
    q = query.lower().strip()
    for alias, canonical in config.ALIASES.items():
        if q == alias:
            matches = [t for t in known if canonical.lower() in t.lower()]
            if matches:
                return matches[0]
    for t in known:
        if t.lower() == q:
            return t
    hits = [t for t in known if q in t.lower()]
    if len(hits) == 1:
        return hits[0]
    if hits:
        close = get_close_matches(q, [t.lower() for t in hits], n=1, cutoff=0.3)
        if close:
            return next(t for t in hits if t.lower() == close[0])
        return hits[0]
    close = get_close_matches(q, [t.lower() for t in known], n=3, cutoff=0.3)
    raise ValueError(
        f"Cannot find '{query}'. "
        f"Closest: {get_close_matches(q, [t.lower() for t in known], n=3)}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# TIME-DECAY WEIGHTS
# ══════════════════════════════════════════════════════════════════════════════


def input_lineup(team_name: str) -> list:
    """
    Ask user to enter starting XI with positions.
    Format per player: Name,POS  (e.g. Saka,FW or White,DF)
    Returns list of dicts.
    """
    print(f"\n  ── Starting XI for {team_name} ──")
    print("  Format: Player Name,POS  (POS = GK/DF/MF/FW)")
    print("  Enter 11 players, one per line. Press Enter to skip lineups.\n")
    players = []
    for i in range(1, 12):
        raw = input(f"    {i:>2}. ").strip()
        if not raw:
            break
        parts = raw.rsplit(",", 1)
        name = parts[0].strip()
        pos  = parts[1].strip().upper() if len(parts) == 2 else "MF"
        if pos not in ("GK","DF","MF","FW"):
            pos = "MF"
        players.append({"name": name, "position": pos})
    return players


# ══════════════════════════════════════════════════════════════════════════════
# EXCEL TRACKER
# ══════════════════════════════════════════════════════════════════════════════
