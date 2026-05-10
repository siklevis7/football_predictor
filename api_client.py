"""
api_client.py — APIFootball and WeatherFetcher clients.
"""

import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import config
from database import DB, get_env


class APIFootball:
    """Client for api-football.com. No artificial limits — the API stops itself."""

    def __init__(self, db: DB):
        self.db  = db
        self.key = get_env("AFL_API_KEY", "")
        self.session = requests.Session()
        self.session.headers.update({
            "x-apisports-key": self.key,
            "x-rapidapi-key" : self.key,
        })

    def requests_used_today(self) -> int:
        row = self.db.fetchone(
            "SELECT COUNT(*) as n FROM api_request_log "
            "WHERE api='afl' AND DATE(requested_at)=CURDATE()"
        )
        return row["n"] if row else 0

    def _get(self, endpoint, params=None):
        url = f"{config.AFL_BASE}/{endpoint}"
        try:
            r = self.session.get(url, params=params, timeout=30)
            r.raise_for_status()
            self.db.log_api_request("afl", endpoint)
            data = r.json()
            if data.get("errors"):
                # Let caller decide what to do — just report the error
                err = data["errors"]
                print(f"  [API] {err}")
                return None
            return data.get("response", [])
        except requests.RequestException as e:
            print(f"  [API] Request failed: {e}")
            return None

    def fetch_fixtures(self, season):
        print(f"    Fetching fixtures season {season} …", end=" ", flush=True)
        # Do NOT filter by status="FT" — AET (extra time) and PEN (penalties)
        # matches are excluded by that filter. We filter in code instead
        # by checking ft.get("home") is not None.
        data = self._get("fixtures", {"league": config.AFL_LEAGUE_ID,
                                      "season": season})
        if data is None:
            print("failed")
            return []
        # Filter in Python: keep only finished matches (have a fulltime score)
        finished = [m for m in data
                    if m.get("score", {}).get("fulltime", {}).get("home") is not None]
        print(f"{len(finished)} finished matches ({len(data)} total)")
        return finished

    def fetch_match_stats(self, fixture_id):
        return self._get("fixtures/statistics", {"fixture": fixture_id})

    def fetch_lineups(self, fixture_id):
        return self._get("fixtures/lineups", {"fixture": fixture_id})

    def fetch_events(self, fixture_id):
        return self._get("fixtures/events", {"fixture": fixture_id})

    def fetch_injuries(self, team_id, season=None):
        season = season or config.AFL_SEASONS[-1]
        return self._get("injuries", {"league": config.AFL_LEAGUE_ID,
                                      "season": season, "team": team_id})

    def fetch_fixture_injuries(self, fixture_id: int) -> list:
        """
        Fetch injuries specific to one fixture using /injuries?fixture=ID.
        Returns only players confirmed absent for that match.
        This is more accurate than the season-based endpoint, which returns
        every injury that occurred all season including recovered players.
        Only available once the fixture is in the API system (typically
        24-72h before kickoff).
        """
        return self._get("injuries", {"fixture": fixture_id}) or []

    def fetch_h2h(self, team_a_id, team_b_id, last=20):
        return self._get("fixtures/headtohead",
                         {"h2h": f"{team_a_id}-{team_b_id}", "last": last})

    def fetch_next_fixtures(self, next_n=10):
        """
        Fetch upcoming PL fixtures. Uses 'next' parameter (paid plan).
        Falls back to date range if that fails.
        """
        data = self._get("fixtures", {
            "league" : config.AFL_LEAGUE_ID,
            "next"   : next_n,
        })
        if data:
            return data
        # Fallback: date range
        today  = datetime.now().strftime("%Y-%m-%d")
        future = (datetime.now() + timedelta(days=21)).strftime("%Y-%m-%d")
        return self._get("fixtures", {
            "league" : config.AFL_LEAGUE_ID,
            "season" : config.AFL_SEASONS[-1],
            "from"   : today,
            "to"     : future,
        })

    def fetch_fixture_player_stats(self, fixture_id: int) -> list:
        """
        Fetch per-player statistics for a specific fixture.
        Uses /fixtures/players endpoint — returns xG, shots, goals,
        assists, key passes, rating for every player in the match.
        Cost: 1 request per match.
        """
        return self._get("fixtures/players", {"fixture": fixture_id}) or []

    def fetch_competition_fixtures(self, league_id: int,
                                    season: int = None) -> list:
        """
        Fetch all fixtures for a given competition (CL, EL, FA Cup, etc.).
        Used to populate cross_competition_fixtures table for fatigue detection.
        Cost: 1 API request per competition per season.
        """
        season = season or config.AFL_SEASONS[-1]
        return self._get("fixtures", {
            "league": league_id,
            "season": season,
        }) or []

    def fetch_teams(self, season: int = None) -> list:
        """
        Fetch the team registry for the active league/season.
        Called once by DataManager.ensure_teams() to populate the teams table.
        Cost: 1 API request.
        """
        season = season or config.AFL_SEASONS[-1]
        return self._get("teams", {"league": config.AFL_LEAGUE_ID, "season": season}) or []

    def find_upcoming_fixture(self, home_team_id: int, away_team_id: int,
                              season: int = None) -> dict:
        """
        Find the next scheduled fixture between two teams.
        Strategy:
          1. Use H2H endpoint — most direct, returns all fixtures between exactly these two teams
          2. Filter to upcoming matches only
        This avoids the season-guessing problem entirely.
        """
        today = datetime.now().strftime("%Y-%m-%d")
        future = (datetime.now() + timedelta(days=90)).strftime("%Y-%m-%d")

        # H2H endpoint for upcoming fixtures — season required by API
        h2h_data = self._get("fixtures/headtohead", {
            "h2h"    : f"{home_team_id}-{away_team_id}",
            "season" : config.AFL_SEASONS[-1],
            "from"   : today,
            "to"     : future,
            "league" : config.AFL_LEAGUE_ID,
        })

        if h2h_data:
            for m in h2h_data:
                teams = m.get("teams", {})
                h_id  = teams.get("home", {}).get("id")
                a_id  = teams.get("away", {}).get("id")
                if h_id == home_team_id and a_id == away_team_id:
                    fix = m.get("fixture", {})
                    return {
                        "fixture_id": fix.get("id"),
                        "date"      : fix.get("date", ""),
                        "venue"     : fix.get("venue", {}).get("name", ""),
                        "city"      : fix.get("venue", {}).get("city", ""),
                        "referee"   : fix.get("referee", "") or "",
                        "status"    : fix.get("status", {}).get("short", ""),
                        "round"     : m.get("league", {}).get("round", ""),
                    }

        # Fallback: search each season for team fixtures, filter by opponent
        seasons_to_try = sorted(set(config.AFL_SEASONS), reverse=True)
        for try_season in seasons_to_try:
            data = self._get("fixtures", {
                "league" : config.AFL_LEAGUE_ID,
                "season" : try_season,
                "team"   : home_team_id,
                "from"   : today,
                "to"     : future,
            })
            if not data:
                continue
            for m in data:
                teams = m.get("teams", {})
                h_id  = teams.get("home", {}).get("id")
                a_id  = teams.get("away", {}).get("id")
                if h_id == home_team_id and a_id == away_team_id:
                    fix = m.get("fixture", {})
                    return {
                        "fixture_id": fix.get("id"),
                        "date"      : fix.get("date", ""),
                        "venue"     : fix.get("venue", {}).get("name", ""),
                        "city"      : fix.get("venue", {}).get("city", ""),
                        "referee"   : fix.get("referee", "") or "",
                        "status"    : fix.get("status", {}).get("short", ""),
                        "round"     : m.get("league", {}).get("round", ""),
                    }
        return {}

    def fetch_prematch_odds(self, fixture_id: int) -> dict:
        """
        Fetch pre-match odds from Bet365 (bookmaker_id=8) for a fixture.
        Returns raw bets list from API response.
        """
        data = self._get("odds", {
            "fixture"    : fixture_id,
            "bookmaker"  : config.BET365_ID,
        })
        if not data:
            return {}
        # Response is list of bookmaker objects
        for bm in data:
            for bookmaker in bm.get("bookmakers", []):
                if bookmaker.get("id") == config.BET365_ID:
                    return {b["name"]: b["values"]
                            for b in bookmaker.get("bets", [])}
        return {}

    def fetch_live_lineups(self, fixture_id: int) -> list:
        """
        Fetch confirmed lineups for a fixture.
        Returns empty list if lineups not yet released.
        """
        return self._get("fixtures/lineups", {"fixture": fixture_id}) or []

    def fetch_player_stats(self, team_id: int, season: int = None) -> list:
        season = season or config.AFL_SEASONS[-1]
        """
        Fetch player statistics for a team/season.
        Returns list of player dicts with goals, shots, xG per game.
        Uses league filter to get PL-specific stats.
        """
        data = self._get("players", {
            "team"   : team_id,
            "season" : season,
            "league" : config.AFL_LEAGUE_ID,
        })
        return data or []

    def fetch_standings(self, season: int = None) -> list:
        season = season or config.AFL_SEASONS[-1]
        """Fetch current PL standings table."""
        data = self._get("standings", {
            "league" : config.AFL_LEAGUE_ID,
            "season" : season,
        })
        if not data:
            return []
        try:
            return data[0]["league"]["standings"][0]
        except (IndexError, KeyError):
            return []


class WeatherFetcher:
    """
    Fetches match-day weather from Open-Meteo (free, no key).
    Looks up venue coordinates from DB first, then fallback dict.
    Add new venues: INSERT INTO venues (name,latitude,longitude) VALUES (...);
    """

    def __init__(self, db=None):
        self.session = requests.Session()
        self.db      = db

    def get_match_weather(self, venue_name: str, match_datetime_str: str) -> dict:
        """
        Returns weather adjustment factors for a given venue and match time.
        match_datetime_str: ISO format e.g. "2025-04-19T15:00:00+01:00"

        Returns dict:
          goal_factor    : float multiplier for lambda (default 1.0)
          corner_factor  : float multiplier for corners (default 1.0)
          description    : human-readable weather summary
        """
        default = {"goal_factor": 1.0, "corner_factor": 1.0,
                   "description": "Weather unavailable"}

        # DB lookup first, then fallback dict, then partial match
        coords = None
        if hasattr(self, 'db') and self.db:
            row = self.db.fetchone(
                "SELECT latitude, longitude FROM venues WHERE name=%s LIMIT 1",
                (venue_name,)
            )
            if not row:
                row = self.db.fetchone(
                    "SELECT latitude, longitude, name FROM venues "
                    "WHERE name LIKE %s LIMIT 1",
                    (f"%{venue_name.split()[0]}%",)
                )
            if row and row.get("latitude"):
                coords = (float(row["latitude"]), float(row["longitude"]))
                # Auto-save new venues found from API to DB
        if not coords:
            coords = config.VENUE_COORDS_FALLBACK.get(venue_name)
        if not coords:
            for k, v in config.VENUE_COORDS_FALLBACK.items():
                if any(w in venue_name for w in k.split()[:2]):
                    coords = v
                    break
        if not coords:
            return default

        try:
            match_dt = pd.to_datetime(match_datetime_str).tz_localize(None)
            date_str = match_dt.strftime("%Y-%m-%d")

            resp = self.session.get(
                config.WEATHER_BASE,
                params={
                    "latitude"      : coords[0],
                    "longitude"     : coords[1],
                    "hourly"        : "precipitation,wind_speed_10m",
                    "daily"         : "wind_speed_10m_max,precipitation_sum",
                    "forecast_days" : 7,
                    "timezone"      : "Europe/London",
                },
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()

            # Find the match hour index
            hourly_times = data.get("hourly", {}).get("time", [])
            match_hour   = match_dt.strftime("%Y-%m-%dT%H:00")
            try:
                idx = hourly_times.index(match_hour)
            except ValueError:
                # Fall back to closest hour
                idx = 0

            precip    = data["hourly"]["precipitation"][idx]    # mm/h
            wind      = data["hourly"]["wind_speed_10m"][idx]   # km/h

            # Compute factors
            goal_factor   = 1.0
            corner_factor = 1.0
            parts         = []

            if precip >= 5.0:
                goal_factor   *= 0.92
                corner_factor *= 0.94
                parts.append(f"Heavy rain {precip:.1f}mm/h → goals −8%")
            elif precip >= 2.0:
                goal_factor   *= 0.96
                corner_factor *= 0.97
                parts.append(f"Light rain {precip:.1f}mm/h → goals −4%")

            if wind >= 40.0:
                goal_factor   *= 0.95
                corner_factor *= 1.03
                parts.append(f"Strong wind {wind:.0f}km/h → goals −5%")
            elif wind >= 25.0:
                goal_factor   *= 0.98
                parts.append(f"Moderate wind {wind:.0f}km/h → goals −2%")

            description = "  ·  ".join(parts) if parts else \
                          f"Clear  rain:{precip:.1f}mm  wind:{wind:.0f}km/h"

            return {
                "goal_factor"  : round(goal_factor, 4),
                "corner_factor": round(corner_factor, 4),
                "description"  : description,
                "precip_mm"    : precip,
                "wind_kmh"     : wind,
            }

        except Exception as e:
            return default


