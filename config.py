"""
config.py — All constants, league configuration, and runtime globals.
Every other module imports from here. Nothing in this file imports
from any other project module — that keeps the dependency graph clean.
"""

import warnings
import numpy as np
from pathlib import Path

warnings.filterwarnings("ignore")
np.random.seed(42)

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).parent
ENV_FILE     = SCRIPT_DIR / ".env"
TRACKER_FILE = SCRIPT_DIR / "predictions_tracker.xlsx"
LOGS_DIR     = SCRIPT_DIR / "fixture_logs"   # league subfolders created automatically

# ── Model constants ────────────────────────────────────────────────────────────
N_POSTERIOR     = 2_000
N_SIM           = 10_000
DECAY_HALF_LIFE = 60
RECENT_DAYS     = 35
QUARTER_KELLY   = 0.25
MAX_KELLY_PCT   = 0.05

# ── Betting thresholds ────────────────────────────────────────────────────────
# A bet is only worth placing when BOTH are satisfied:
#   edge >= MIN_EDGE  AND  model_prob >= MIN_PROB_BY_MARKET (or MIN_PROB_DEFAULT)
# Premium (🔥): edge >= PREMIUM_EDGE  AND  model_prob >= PREMIUM_PROB

MIN_EDGE         = 0.03
MIN_PROB_DEFAULT = 0.40
PREMIUM_EDGE     = 0.07
PREMIUM_PROB     = 0.50

# EV cap: bets with EV > 0.40 are likely model overconfidence, not real edge.
# Your data shows: EV 0.065–0.669 = profits. EV > 0.669 = large losses.
# A real betting edge of EV > 0.40 would imply >40% return on a single bet —
# no sustainable market offers this. Values above this threshold almost always
# indicate the model probability is badly wrong, not that you found a huge edge.
MAX_EV_THRESHOLD = 0.40

# ── Market efficiency tiers ───────────────────────────────────────────────────
# Under markets are sharp (set by syndicates) and the NB model structurally
# overestimates Under probability. They need double the standard edge floor.
UNDER_MARKET_MIN_EDGE = 0.06
UNDER_MARKETS = {
    "under_0.5", "under_1.5", "under_2.5", "under_3.5",
    "under_4.5", "under_5.5", "under_6.5",
}

# Under 1.5 — NEVER recommend regardless of apparent edge.
# True probability: ~29% in PL, ~24% in Bundesliga.
# Model is structurally incapable of pricing this market reliably before
# Platt scaling calibration. Show in ticket for information only.
NEVER_BET_MARKETS = {"under_1.5", "under_0.5"}

# League-specific minimum probability floors for Under 2.5.
# True Under 2.5 frequency varies significantly by league:
#   PL: ~51%   Ligue1: ~52%   BL: ~44%   SerieA: ~52%   LaLiga: ~52%
# A model floor of 45% (PL-calibrated) is dangerously low for Bundesliga.
# These floors require the model to be MORE confident before recommending
# an Under 2.5 bet in high-scoring leagues.
LEAGUE_UNDER25_FLOOR = {
    39 : 0.52,   # Premier League  — true rate ~51%, floor 52%
    61 : 0.53,   # Ligue 1         — true rate ~52%, floor 53%
    78 : 0.57,   # Bundesliga      — true rate ~44%, floor 57% (extra margin)
    135: 0.53,   # Serie A         — true rate ~52%, floor 53%
    140: 0.53,   # LaLiga          — true rate ~52%, floor 53%
}

# Per-market minimum probability floors
MIN_PROB_BY_MARKET = {
    "home_win"   : 0.35,
    "draw"       : 0.25,
    "away_win"   : 0.30,
    "over_2.5"   : 0.45,
    "under_2.5"  : 0.45,
    "over_1.5"   : 0.45,
    "over_3.5"   : 0.40,
    "btts_yes"   : 0.40,
    "btts_no"    : 0.40,
    "dc_1x"      : 0.55,
    "dc_x2"      : 0.55,
    "dnb_home"   : 0.55,   # Only bet DNB when clearly favouring a team.
    "dnb_away"   : 0.52,   # DNB fires refund when draw — bad on draw-prone fixtures.
    "wtn_home"   : 0.30,   # Raised from 0.25 — win-to-nil needs strong favourite
    "wtn_away"   : 0.30,
    "ah_-0.5"    : 0.45,
    "ah_+0.5"    : 0.45,
    "ah_-1.5"    : 0.40,
    "ah_+1.5"    : 0.40,
    "cor_ov_7.5" : 0.45, "cor_ov_8.5" : 0.45,
    "cor_ov_9.5" : 0.45, "cor_ov_10.5": 0.45,
    "cor_un_7.5" : 0.45, "cor_un_8.5" : 0.45,
    "cor_un_9.5" : 0.45, "cor_un_10.5": 0.45,
    "crd_ov_1.5" : 0.45, "crd_ov_2.5" : 0.45,
    "crd_ov_3.5" : 0.45, "crd_ov_4.5" : 0.40,
    "crd_un_1.5" : 0.45, "crd_un_2.5" : 0.45,
    "crd_un_3.5" : 0.45,
    "bp_ov_20.5" : 0.45, "bp_ov_30.5" : 0.45,
    "bp_ov_40.5" : 0.40,
}

# ── Cross-competition league IDs for fatigue detection ───────────────────────
# Maps each domestic league to the cup/European competitions its teams play in.
# These are fetched weekly to populate cross_competition_fixtures.
# UEFA competitions are shared across all leagues.
UEFA_COMP_IDS = [2, 3, 848]   # Champions League, Europa League, Conference League

CROSS_COMPETITION_IDS = {
    39 : [2, 3, 848, 45, 48],    # PL → CL, EL, ECL, FA Cup, Carabao Cup
    61 : [2, 3, 848, 66],        # Ligue 1 → CL, EL, ECL, Coupe de France
    78 : [2, 3, 848, 81],        # Bundesliga → CL, EL, ECL, DFB Pokal
    135: [2, 3, 848, 137],       # Serie A → CL, EL, ECL, Coppa Italia
    140: [2, 3, 848, 143],       # LaLiga → CL, EL, ECL, Copa del Rey
}

def get_cross_comp_ids() -> list:
    """Return list of competition IDs to check for the active league."""
    return CROSS_COMPETITION_IDS.get(AFL_LEAGUE_ID, UEFA_COMP_IDS)


# ── Leagues restricted to observe-only ───────────────────────────────────────
# Based on tracker data (83 bets): Bundesliga -311 RWF/bet, Ligue1 -521 RWF/bet.
# These leagues show negative CLV and systematic losses. Model parameters are
# not calibrated for them yet. All predictions are shown but NO bets recommended
# until positive CLV is confirmed over 30+ bets in each league separately.
# Remove a league ID from this set once it shows positive average CLV.
OBSERVE_ONLY_LEAGUES: set = {}
AFL_BASE    = "https://v3.football.api-sports.io"
WEATHER_BASE= "https://api.open-meteo.com/v1/forecast"
BET365_ID   = 8

# ── League configuration ──────────────────────────────────────────────────────
LEAGUES = {
    "1": {
        "name"    : "🏴󠁧󠁢󠁥󠁮󠁧󠁿  Premier League (England)",
        "id"      : 39,
        "db"      : "fp_premier_league",
        "seasons" : [2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025],
        "code"    : "PL",
        "country" : "England",
        "currency": "RWF",
    },
    "2": {
        "name"    : "🇫🇷  Ligue 1 (France)",
        "id"      : 61,
        "db"      : "fp_ligue1",
        "seasons" : [2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025],
        "code"    : "L1",
        "country" : "France",
        "currency": "RWF",
    },
    "3": {
        "name"    : "🇩🇪  Bundesliga (Germany)",
        "id"      : 78,
        "db"      : "fp_bundesliga",
        "seasons" : [2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025],
        "code"    : "BL",
        "country" : "Germany",
        "currency": "RWF",
    },
    "4": {
        "name"    : "🇮🇹  Serie A (Italy)",
        "id"      : 135,
        "db"      : "fp_serie_a",
        "seasons" : [2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025],
        "code"    : "SA",
        "country" : "Italy",
        "currency": "RWF",
    },
    "5": {
        "name"    : "🇪🇸  LaLiga (Spain)",
        "id"      : 140,
        "db"      : "fp_laliga",
        "seasons" : [2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025],
        "code"    : "LL",
        "country" : "Spain",
        "currency": "RWF",
    },
}

# ── Mutable runtime globals (modified by select_league() in main.py) ──────────
# All modules that read these must do `import config` then `config.AFL_LEAGUE_ID`
# so they always see the current value after a league switch.
AFL_LEAGUE_ID : int  = 39
AFL_SEASONS   : list = [2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025]
ACTIVE_LEAGUE : dict = LEAGUES["1"]

# ── League-specific match averages ───────────────────────────────────────────
# Used as fallback when team-specific DB data is unavailable.
# Each league has meaningfully different corner rates, card rates, and
# home advantage — using PL figures for all leagues was systematically wrong.
#
# Sources: historical averages 2018-2024 across each league.
#
# home_adv_prior : mean for Normal prior on home_adv in the Bayesian model.
#   Higher = stronger structural home advantage in that league.
#   PL has weakened post-COVID; LaLiga and Serie A remain stronger.
#
# corners_h/a   : home/away corners per game (league average)
# cards_h/a     : home/away yellow cards per game (referee culture differs significantly)
#   Bundesliga referees are the most lenient in Europe (~3.2 cards/game).
#   Serie A referees are the strictest (~4.5 cards/game).
#
LEAGUE_AVERAGES = {
    39: {   # Premier League
        "name"           : "Premier League",
        "home_adv_prior" : 0.27,
        "corners_h"      : 5.4,
        "corners_a"      : 4.7,
        "cards_h"        : 1.75,
        "cards_a"        : 2.10,
        "goals_per_game" : 2.74,
        "n_teams"        : 20,
        "total_games"    : 38,
        "cl_spots"       : 4,
        "relegation_zone": 3,
        # Shots on target per goal — used as fallback signal when xG is missing.
        # SoT is available for all seasons; goals are noisier at low rates.
        # Scale: goals_per_team / avg_sot_per_team  ≈ 1.37 / 4.5
        "sot_per_goal"   : 3.28,   # avg SoT needed per goal scored
    },
    61: {   # Ligue 1
        "name"           : "Ligue 1",
        "home_adv_prior" : 0.25,
        "corners_h"      : 4.8,
        "corners_a"      : 4.4,
        "cards_h"        : 2.00,
        "cards_a"        : 2.20,
        "goals_per_game" : 2.58,
        "n_teams"        : 18,
        "total_games"    : 34,
        "cl_spots"       : 2,
        "relegation_zone": 3,
        "sot_per_goal"   : 3.45,   # slightly less clinical than PL
    },
    78: {   # Bundesliga
        "name"           : "Bundesliga",
        "home_adv_prior" : 0.24,
        "corners_h"      : 5.1,
        "corners_a"      : 4.7,
        "cards_h"        : 1.55,
        "cards_a"        : 1.65,
        "goals_per_game" : 3.08,
        "n_teams"        : 18,
        "total_games"    : 34,
        "cl_spots"       : 4,
        "relegation_zone": 2,
        "sot_per_goal"   : 3.05,   # highest-scoring league — more clinical
    },
    135: {  # Serie A
        "name"           : "Serie A",
        "home_adv_prior" : 0.30,
        "corners_h"      : 5.0,
        "corners_a"      : 4.6,
        "cards_h"        : 2.10,
        "cards_a"        : 2.40,
        "goals_per_game" : 2.62,
        "n_teams"        : 20,
        "total_games"    : 38,
        "cl_spots"       : 4,
        "relegation_zone": 3,
        "sot_per_goal"   : 3.52,   # defensive league, fewer conversions
    },
    140: {  # LaLiga
        "name"           : "LaLiga",
        "home_adv_prior" : 0.30,
        "corners_h"      : 4.9,
        "corners_a"      : 4.5,
        "cards_h"        : 1.90,
        "cards_a"        : 2.10,
        "goals_per_game" : 2.62,
        "n_teams"        : 20,
        "total_games"    : 38,
        "cl_spots"       : 4,
        "relegation_zone": 3,
        "sot_per_goal"   : 3.38,   # similar to PL conversion rate
    },
}

def get_league_avgs() -> dict:
    """Return the averages dict for the currently active league.
    Falls back to Premier League values if the league ID is unrecognised.
    Always call as get_league_avgs() — reads AFL_LEAGUE_ID at call time
    so league switches in mid-session are handled correctly.
    """
    return LEAGUE_AVERAGES.get(AFL_LEAGUE_ID, LEAGUE_AVERAGES[39])


# Legacy single-league constants — kept for any code that still references
# them directly; new code should use get_league_avgs() instead.
PL_CORNERS_H = 5.4
PL_CORNERS_A = 4.7
PL_CARDS_H   = 1.75
PL_CARDS_A   = 2.10

# ── Canonical team names ──────────────────────────────────────────────────────
# API-Football changes team names between seasons (e.g. "FC Bayern München" in
# 2016 becomes "Bayern Munich" in 2022). Every unique spelling creates a
# separate team in the model — the model then estimates separate attack/defense
# parameters for what is actually the same team, wasting data and producing
# wrong predictions.
#
# This dict maps EVERY known API variant → one canonical name.
# The canonical name is what gets stored in matches_basic and used everywhere.
# Applied at INSERT time so new data is always clean.
# Run Option 6 in backfill_all.py to fix historical data already in the DB.
#
# Format:  "api variant (lowercase)" : "Canonical Name"
TEAM_CANONICAL: dict = {
    # ── Bundesliga ─────────────────────────────────────────────────────────────
    "fc bayern münchen"            : "Bayern Munich",
    "fc bayern munchen"            : "Bayern Munich",
    "bayern münchen"               : "Bayern Munich",
    "bayern munchen"               : "Bayern Munich",
    "fc bayern"                    : "Bayern Munich",
    "bvb 09 borussia dortmund"     : "Borussia Dortmund",
    "borussia dortmund"            : "Borussia Dortmund",
    "bayer 04 leverkusen"          : "Bayer Leverkusen",
    "bayer leverkusen"             : "Bayer Leverkusen",
    "rasenballsport leipzig"       : "RB Leipzig",
    "rb leipzig"                   : "RB Leipzig",
    "red bull leipzig"             : "RB Leipzig",
    "borussia m'gladbach"          : "Borussia Monchengladbach",
    "borussia mönchengladbach"     : "Borussia Monchengladbach",
    "borussia monchengladbach"     : "Borussia Monchengladbach",
    "tsg 1899 hoffenheim"          : "TSG Hoffenheim",
    "tsg hoffenheim"               : "TSG Hoffenheim",
    "1899 hoffenheim"              : "TSG Hoffenheim",
    "vfl wolfsburg"                : "Wolfsburg",
    "wolfsburg"                    : "Wolfsburg",
    "sv werder bremen"             : "Werder Bremen",
    "werder bremen"                : "Werder Bremen",
    "1. fc union berlin"           : "Union Berlin",
    "fc union berlin"              : "Union Berlin",
    "union berlin"                 : "Union Berlin",
    "hertha bsc"                   : "Hertha Berlin",
    "hertha berlin"                : "Hertha Berlin",
    "1. fsv mainz 05"              : "Mainz 05",
    "fsv mainz 05"                 : "Mainz 05",
    "mainz 05"                     : "Mainz 05",
    "1. fc köln"                   : "FC Köln",
    "1. fc koln"                   : "FC Köln",
    "fc köln"                      : "FC Köln",
    "fc koln"                      : "FC Köln",
    "vfl bochum"                   : "Bochum",
    "vfl bochum 1848"              : "Bochum",
    "bochum"                       : "Bochum",
    "sv darmstadt 98"              : "Darmstadt 98",
    "darmstadt 98"                 : "Darmstadt 98",
    "1. fc heidenheim 1846"        : "Heidenheim",
    "1. fc heidenheim"             : "Heidenheim",
    "heidenheim"                   : "Heidenheim",
    "fc augsburg"                  : "FC Augsburg",
    "augsburg"                     : "FC Augsburg",
    "sport-club freiburg"          : "SC Freiburg",
    "sc freiburg"                  : "SC Freiburg",
    "freiburg"                     : "SC Freiburg",
    "vfb stuttgart"                : "VfB Stuttgart",
    "stuttgart"                    : "VfB Stuttgart",
    "eintracht frankfurt"          : "Eintracht Frankfurt",
    "fc schalke 04"                : "Schalke 04",
    "schalke 04"                   : "Schalke 04",
    "hamburger sv"                 : "Hamburg",
    "hamburg sv"                   : "Hamburg",
    "hamburg"                      : "Hamburg",
    "fc st. pauli"                 : "St. Pauli",
    "st. pauli"                    : "St. Pauli",
    "holstein kiel"                : "Holstein Kiel",
    "sportgemeinschaft eintracht paderborn"  : "Paderborn",
    "sc paderborn 07"              : "Paderborn",
    "fortuna düsseldorf"           : "Fortuna Dusseldorf",
    "fortuna dusseldorf"           : "Fortuna Dusseldorf",
    "1. fc nürnberg"               : "Nurnberg",
    "1. fc nurnberg"               : "Nurnberg",
    # ── Premier League ─────────────────────────────────────────────────────────
    "manchester city"              : "Manchester City",
    "manchester united"            : "Manchester United",
    "man city"                     : "Manchester City",
    "man utd"                      : "Manchester United",
    "man united"                   : "Manchester United",
    "tottenham hotspur"            : "Tottenham",
    "wolverhampton wanderers"      : "Wolverhampton Wanderers",
    "wolverhampton"                : "Wolverhampton Wanderers",
    "nottingham forest"            : "Nottingham Forest",
    "newcastle united"             : "Newcastle United",
    "brighton & hove albion"       : "Brighton",
    "brighton and hove albion"     : "Brighton",
    "west ham united"              : "West Ham United",
    "aston villa"                  : "Aston Villa",
    "sheffield united"             : "Sheffield Utd",
    "sheffield utd"                : "Sheffield Utd",
    "crystal palace"               : "Crystal Palace",
    "leicester city"               : "Leicester City",
    "ipswich town"                 : "Ipswich",
    "luton town"                   : "Luton",
    # ── Ligue 1 ────────────────────────────────────────────────────────────────
    "paris saint-germain"          : "Paris Saint-Germain",
    "paris sg"                     : "Paris Saint-Germain",
    "psg"                          : "Paris Saint-Germain",
    "paris fc"                     : "Paris FC",
    "olympique de marseille"       : "Marseille",
    "olympique marseille"          : "Marseille",
    "olympique lyonnais"           : "Lyon",
    "ol lyon"                      : "Lyon",
    "losc lille"                   : "Lille",
    "losc"                         : "Lille",
    "as monaco"                    : "Monaco",
    "ogc nice"                     : "Nice",
    "stade rennais fc"             : "Rennes",
    "stade rennais"                : "Rennes",
    "rc lens"                      : "Lens",
    "montpellier hsc"              : "Montpellier",
    "montpellier hérault sc"       : "Montpellier",
    "stade brestois 29"            : "Brest",
    "rc strasbourg alsace"         : "Strasbourg",
    "toulouse fc"                  : "Toulouse",
    "fc lorient"                   : "Lorient",
    "nantes"                       : "Nantes",
    "fc nantes"                    : "Nantes",
    "stade de reims"               : "Reims",
    "havre ac"                     : "Le Havre",
    "le havre"                     : "Le Havre",
    "fc metz"                      : "Metz",
    "clermont foot 63"             : "Clermont",
    # ── Serie A ────────────────────────────────────────────────────────────────
    "fc internazionale"            : "Inter",
    "inter milan"                  : "Inter",
    "internazionale"               : "Inter",
    "ac milan"                     : "AC Milan",
    "fc juventus"                  : "Juventus",
    "juventus fc"                  : "Juventus",
    "as roma"                      : "Roma",
    "ssc napoli"                   : "Napoli",
    "acf fiorentina"               : "Fiorentina",
    "ss lazio"                     : "Lazio",
    "atalanta bc"                  : "Atalanta",
    "torino fc"                    : "Torino",
    "fc bologna"                   : "Bologna",
    "bologna fc 1909"              : "Bologna",
    "udinese calcio"               : "Udinese",
    "cagliari calcio"              : "Cagliari",
    "lecce"                        : "Lecce",
    "us lecce"                     : "Lecce",
    "hellas verona"                : "Hellas Verona",
    "ac monza"                     : "Monza",
    "us sassuolo calcio"           : "Sassuolo",
    "us salernitana 1919"          : "Salernitana",
    "empoli fc"                    : "Empoli",
    "genoa cfc"                    : "Genoa",
    "venezia fc"                   : "Venezia",
    "parma calcio 1913"            : "Parma",
    "como 1907"                    : "Como",
    # ── LaLiga ─────────────────────────────────────────────────────────────────
    "real madrid cf"               : "Real Madrid",
    "real madrid"                  : "Real Madrid",
    "fc barcelona"                 : "Barcelona",
    "club atletico de madrid"      : "Atletico Madrid",
    "atletico madrid"              : "Atletico Madrid",
    "atlético de madrid"           : "Atletico Madrid",
    "atletico de madrid"           : "Atletico Madrid",
    "sevilla fc"                   : "Sevilla",
    "real betis balompie"          : "Real Betis",
    "real betis"                   : "Real Betis",
    "real sociedad"                : "Real Sociedad",
    "athletic club bilbao"         : "Athletic Club",
    "athletic bilbao"              : "Athletic Club",
    "athletic club"                : "Athletic Club",
    "villarreal cf"                : "Villarreal",
    "villarreal"                   : "Villarreal",
    "valencia cf"                  : "Valencia",
    "getafe cf"                    : "Getafe",
    "celta de vigo"                : "Celta Vigo",
    "celta vigo"                   : "Celta Vigo",
    "rcd espanyol"                 : "Espanyol",
    "rcd mallorca"                 : "Mallorca",
    "ud almeria"                   : "Almeria",
    "granada cf"                   : "Granada",
    "ud las palmas"                : "Las Palmas",
    "deportivo alaves"             : "Alaves",
    "deportivo alavés"             : "Alaves",
    "ca osasuna"                   : "Osasuna",
    "rayo vallecano"               : "Rayo Vallecano",
    "leganes"                      : "Leganes",
    "cd leganes"                   : "Leganes",
    "real valladolid cf"           : "Valladolid",
    "girona fc"                    : "Girona",
}


def normalize_team_name(name: str) -> str:
    """
    Return the canonical team name for a given API-Football team name.
    Applies TEAM_CANONICAL lookup (case-insensitive).
    Falls back to the original name if no mapping exists.
    Called at INSERT time so every match is stored with consistent names.
    """
    if not name:
        return name
    canonical = TEAM_CANONICAL.get(name.lower().strip())
    return canonical if canonical else name
ALIASES = {
    "man city": "Manchester City", "man utd": "Manchester United",
    "man united": "Manchester United", "arsenal": "Arsenal",
    "chelsea": "Chelsea", "liverpool": "Liverpool",
    "tottenham": "Tottenham", "spurs": "Tottenham",
    "newcastle": "Newcastle United", "villa": "Aston Villa",
    "aston villa": "Aston Villa", "wolves": "Wolverhampton Wanderers",
    "west ham": "West Ham United", "brighton": "Brighton",
    "everton": "Everton", "brentford": "Brentford",
    "fulham": "Fulham", "crystal palace": "Crystal Palace",
    "bournemouth": "Bournemouth", "nottm forest": "Nottingham Forest",
    "forest": "Nottingham Forest", "leicester": "Leicester City",
    "ipswich": "Ipswich", "southampton": "Southampton",
    "luton": "Luton", "burnley": "Burnley",
    "sheffield utd": "Sheffield Utd",
}

# ── Venue coordinates for weather lookup ─────────────────────────────────────
# Primary source is the DB venues table. This dict is the fallback.
VENUE_COORDS_FALLBACK = {
    # Premier League
    "Emirates Stadium"             : (51.5550, -0.1084),
    "Anfield"                      : (53.4308, -2.9608),
    "Old Trafford"                 : (53.4631, -2.2913),
    "Etihad Stadium"               : (53.4831, -2.2004),
    "Stamford Bridge"              : (51.4816, -0.1910),
    "Tottenham Hotspur Stadium"    : (51.6042, -0.0666),
    "St. James' Park"             : (54.9754, -1.6218),
    "Villa Park"                   : (52.5092, -1.8847),
    "Goodison Park"                : (53.4388, -2.9666),
    "Amex Stadium"                 : (50.8609, -0.0832),
    "Molineux Stadium"             : (52.5902, -2.1302),
    "London Stadium"               : (51.5386, -0.0162),
    "Selhurst Park"                : (51.3983, -0.0855),
    "Gtech Community Stadium"      : (51.4882, -0.2866),
    "Craven Cottage"               : (51.4749, -0.2218),
    "City Ground"                  : (52.9399, -1.1323),
    "Vitality Stadium"             : (50.7352, -1.8382),
    "Bramall Lane"                 : (53.3703, -1.4706),
    "King Power Stadium"           : (52.6204, -1.1424),
    "Portman Road"                 : (52.0545,  1.1446),
    "St Mary's Stadium"           : (50.9058, -1.3914),
    "Stadium of Light"             : (54.9147, -1.3883),
    "Kenilworth Road"              : (51.8839, -0.4317),
    "Turf Moor"                    : (53.7889, -2.2302),
    # Ligue 1
    "Parc des Princes"             : (48.8414,  2.2530),
    "Stade Vélodrome"              : (43.2697,  5.3960),
    "Groupama Stadium"             : (45.7653,  4.9825),
    "Stade Louis II"               : (43.7279,  7.4153),
    "Stade Pierre-Mauroy"          : (50.6121,  3.1305),
    "Stade de la Mosson"           : (43.6223,  3.8183),
    "Stade Bollaert-Delelis"       : (50.4369,  2.8154),
    "Stade de l'Abbé-Deschamps"    : (47.7983,  3.5622),
    "Stade Auguste-Delaune"        : (49.2601,  4.0358),
    "Stade du Roudourou"           : (48.5635, -3.1455),
    "Stadium de Toulouse"          : (43.5833,  1.4342),
    "Stade de la Licorne"          : (49.8942,  2.2954),
    "Stade Raymond-Kopa"           : (47.4733, -0.5556),
    "Stade Francis-Le Blé"         : (48.3825, -4.4853),
    "Stade Geoffroy-Guichard"      : (45.4609,  4.3901),
    "Stade de Nice"                : (43.7059,  7.1921),
    "Stade de la Beaujoire"        : (47.2561, -1.5243),
    # Bundesliga
    "Allianz Arena"                : (48.2188, 11.6247),
    "Signal Iduna Park"            : (51.4926,  7.4518),
    "Deutsche Bank Park"           : (50.0686,  8.6452),
    "RheinEnergieStadion"          : (50.9333,  6.8753),
    "Volksparkstadion"             : (53.5876,  9.8986),
    "BayArena"                     : (51.0383,  7.0023),
    "Schwarzwald-Stadion"          : (47.9875,  7.8946),
    "MHPArena"                     : (48.7924,  9.2322),
    "WWK Arena"                    : (48.3240, 10.8869),
    "Sportpark Ronhof"             : (49.5025, 10.9453),
    "PreZero Arena"                : (49.2330,  8.8903),
    "MEWA Arena"                   : (49.9842,  8.2236),
    "Vonovia Ruhrstadion"          : (51.4853,  7.2192),
    "Millerntor-Stadion"           : (53.5544,  9.9654),
    "Holstein-Stadion"             : (54.3547, 10.1326),
    "Olympiastadion"               : (52.5147, 13.2393),
    "Weserstadion"                 : (53.0665,  8.8381),
    # Serie A
    "Stadio Giuseppe Meazza"       : (45.4781,  9.1240),
    "Stadio Olimpico"              : (41.9340, 12.4549),
    "Stadio Diego Armando Maradona": (40.8279, 14.1931),
    "Allianz Stadium"              : (45.1096,  7.6414),
    "Stadio Atleti Azzurri d'Italia": (45.7090, 9.6798),
    "Stadio Franchi"               : (43.7806, 11.2817),
    "Stadio Marc'Antonio Bentegodi": (45.4349, 10.9797),
    "Stadio Via del Mare"          : (40.3555, 18.1657),
    "Stadio Pino Zaccheria"        : (41.4635, 15.5434),
    "Stadio Renato Dall'Ara"       : (44.4922, 11.3063),
    "Stadio Ferraris"              : (44.4163,  8.9518),
    "Stadio Arechi"                : (40.6681, 14.7920),
    "Stadio Olimpico di Torino"    : (45.0406,  7.6499),
    "Stadio Adriatico"             : (42.4667, 14.2500),
    "Stadio Friuli"                : (46.0735, 13.2013),
    "Stadio Brianteo"              : (45.6223,  9.2688),
    "Stadio Castellani"            : (43.7174, 10.6678),
    "Stadio Tardini"               : (44.7918, 10.3443),
    # LaLiga
    "Estadio Santiago Bernabéu"    : (40.4531, -3.6883),
    "Spotify Camp Nou"             : (41.3809,  2.1228),
    "Estadio Cívitas Metropolitano": (40.4361, -3.5996),
    "Estadio de la Cerámica"       : (39.9445, -0.1036),
    "Estadio Manuel Martínez Valero": (38.2652, -0.6982),
    "Estadio de Mestalla"          : (39.4742, -0.3583),
    "Estadio San Mamés"            : (43.2640, -2.9499),
    "Reale Arena"                  : (43.3013, -1.9740),
    "RCDE Stadium"                 : (41.3474,  2.0773),
    "Estadio Nuevo Los Cármenes"   : (37.1523, -3.5988),
    "Estadio de Montilivi"         : (41.9836,  2.8254),
    "Estadio Nuevo Mirandilla"     : (36.5075, -6.2735),
    "Estadio El Alcoraz"           : (42.1347, -0.4103),
    "Estadio Municipal de Ipurúa"  : (43.1792, -2.4739),
    "Estadio de Gran Canaria"      : (28.1011, -15.4507),
    "Estadio Balaídos"             : (42.2120, -8.7343),
    "Estadio El Sadar"             : (42.7966, -1.6376),
    "Estadio José Zorrilla"        : (41.6464, -4.7478),
    "Estadio Ramón Sánchez-Pizjuán": (37.3838, -5.9706),
}
