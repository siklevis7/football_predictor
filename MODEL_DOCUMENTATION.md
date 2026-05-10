# Bayesian Multi-League Football Prediction Engine
## Project Documentation — v11.0

---

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Database Design](#3-database-design)
4. [Mathematical Model](#4-mathematical-model)
5. [Code Walkthrough — Every Class and Method](#5-code-walkthrough)
6. [Prediction Pipeline](#6-prediction-pipeline)
7. [Markets and EV Analysis](#7-markets-and-ev-analysis)
8. [Multi-League System](#8-multi-league-system)
9. [Excel Tracker](#9-excel-tracker)
10. [How to Interpret Results](#10-how-to-interpret-results)

---

## 1. Project Overview

This system predicts football match outcomes across five European leagues using a Bayesian Hierarchical Poisson model enhanced with Dixon-Coles correction, non-penalty xG, league form, weather, injuries, fatigue, and momentum.

**Core philosophy:** The model does not try to predict the score. It estimates the probability of each outcome more accurately than the bookmaker's implied probability. When the model probability exceeds the implied probability by a sufficient margin AND the model has high enough confidence, a value bet exists.

**Supported leagues:**
- Premier League (England) — API ID 39 — Database: fp_premier_league
- Ligue 1 (France) — API ID 61 — Database: fp_ligue1
- Bundesliga (Germany) — API ID 78 — Database: fp_bundesliga
- Serie A (Italy) — API ID 135 — Database: fp_serie_a
- LaLiga (Spain) — API ID 140 — Database: fp_laliga

**Data source:** API-Football (api-football.com). Paid plan required for season 2025 and historical seasons before 2022.

---

## 2. Architecture

```
football_predictor.py
│
├── Constants & Config
│   ├── LEAGUES dict — all league configs in one place
│   ├── AFL_LEAGUE_ID, AFL_SEASONS — set at runtime from selected league
│   ├── ACTIVE_LEAGUE — current session's league
│   ├── Betting thresholds (MIN_EDGE, MIN_PROB_BY_MARKET, etc.)
│   └── VENUE_COORDS_FALLBACK — stadium coordinates for weather
│
├── Core Classes
│   ├── DB — MySQL connection wrapper with per-league database support
│   ├── APIFootball — all API-Football calls, no artificial request limits
│   ├── WeatherFetcher — Open-Meteo free weather API
│   ├── DataManager — orchestrates DB + API, all data operations
│   ├── FixtureModel — Bayesian Poisson model + MAP inference
│   └── Sim — Monte Carlo simulator, all market probabilities
│
├── Excel Tracker
│   └── ExcelTracker — shared across all leagues, one row per prediction
│
├── Presentation
│   ├── print_ticket() — the full prediction output
│   └── get_odds() — manual odds entry across all markets
│
└── main()
    ├── select_league() — picks league, sets globals, connects DB
    ├── _ensure_schema() — creates all tables in selected DB
    ├── Startup sync — fetches and caches all season fixtures
    ├── Backfill — enriches matches with xG, shots, corners
    └── Main loop — fixture input → model → ticket → tracker
```

---

## 3. Database Design

Each league has its own MySQL database. All databases have identical schemas created by `_ensure_schema()`. This separation means:
- Querying Arsenal's form never touches Bundesliga data
- Team names can overlap across leagues without confusion
- Each DB can be backed up independently

### Table: matches_basic
Stores one row per completed match. The foundation of all model training.

| Column | Type | Purpose |
|---|---|---|
| match_id | INT PK | API-Football's unique fixture ID |
| source | VARCHAR | Always 'api-football' |
| season | INT | Year the season started (2024 = 2024/25) |
| match_date | DATETIME | Kickoff datetime (UTC) |
| matchday | VARCHAR | "1" through "38" — used for fatigue calculation |
| home_team_name | VARCHAR | Exact team name as returned by API |
| away_team_name | VARCHAR | Exact team name as returned by API |
| home_goals | INT | Full-time goals |
| away_goals | INT | Full-time goals |
| ht_home_goals | INT | Half-time goals — used for HT predictions |
| ht_away_goals | INT | Half-time goals |

**Why separate from matches_stats?** Because basic data (goals, date, teams) is fetched in one API request per season (380 matches, 1 request). Rich stats (xG, corners, shots) require one request per match. Separating them lets us use the basic data immediately while enrichment runs gradually.

### Table: matches_stats
One row per team per match (two rows per match: is_home=1 and is_home=0).

| Column | Type | Purpose |
|---|---|---|
| xg | FLOAT | Expected Goals — probability-weighted shot value |
| npxg | FLOAT | Non-Penalty xG — xG minus penalty attempts × 0.79 |
| shot_quality | FLOAT | xG / total_shots — average quality per attempt |
| corners | INT | Corner kicks — used for corners model |
| yellow_cards | INT | Used for cards model |
| possession_pct | FLOAT | Ball possession % |

**npxG reasoning:** A penalty has 0.79 xG regardless of team quality. Including it in xG inflates a team's attacking strength artificially. npxG removes this noise and gives a purer measure of open-play attacking threat. The model uses npxG when at least 30% of a team's matches have it available; otherwise falls back to xG, then goals.

**shot_quality reasoning:** A team scoring 2 goals from 20 shots (0.10 xG/shot) is less dangerous than one scoring 2 from 5 shots (0.40 xG/shot). Shot quality captures this. High shot quality teams are more clinical; low shot quality teams are more wasteful.

### Table: venues
Stores stadium coordinates for weather lookup.

| Column | Type | Purpose |
|---|---|---|
| name | VARCHAR | Stadium name (matched against API fixture response) |
| latitude | DECIMAL | For Open-Meteo weather API call |
| longitude | DECIMAL | For Open-Meteo weather API call |

When a fixture is found via API, the venue name is auto-saved to this table. New stadiums (promoted teams, relocated clubs) are saved automatically. Weather cannot be fetched without coordinates — the admin can add lat/lng via SQL for new venues.

### Table: predictions_log
Every prediction made by the model is stored here permanently. Used for calibration analysis.

The `league_code` column (PL, L1, BL, SA, LL) lets you analyse performance per league. After 200+ predictions per league, you can run a calibration check: if your 60% predictions win 60% of the time, the model is calibrated. If they win only 50%, the model is overconfident and you need to adjust thresholds.

---

## 4. Mathematical Model

### 4.1 Hierarchical Poisson Model

Football goals are modelled as Poisson random variables. Each team has a latent attack strength and defense strength. The expected goals scored by each team follows:

```
λ_home = exp(α + β_home + attack_home - defense_away) × adjustments
λ_away = exp(α + attack_away - defense_home) × adjustments
```

Where:
- **α (intercept):** Log of the baseline league goal rate. Warm-started at 0.26 ≈ log(1.3), the approximate average goals per team per PL match.
- **β_home (home advantage):** Log multiplier for playing at home. Warm-started at 0.25 ≈ 22% home advantage, which is the empirical PL average. Teams at home score ~22% more goals than the same teams would score away.
- **attack[t]:** Team t's attack parameter. Positive = above-average attack.
- **defense[t]:** Team t's defense parameter. Positive = below-average defense (concedes more). Confusingly named but mathematically it appears with a negative sign — a team with a high defense parameter concedes more.

### 4.2 Inference: MAP + Laplace Approximation

Full Bayesian inference (MCMC) would take hours. We use:

1. **MAP (Maximum A Posteriori):** Find the parameter values that maximise the posterior probability (likelihood × prior). This is done with L-BFGS-B optimisation — a gradient-based method that converges in seconds for ~150 parameters.

2. **Laplace Approximation:** Approximate the posterior distribution as a multivariate Gaussian centred at the MAP estimate. The covariance is the inverse Hessian (second derivative of the log posterior). This gives us uncertainty around the MAP estimate, which we sample from to propagate uncertainty through the simulation.

**Why not MCMC?** With 150 parameters and needing a fresh model fit for every prediction, MCMC would take 3-10 minutes per fixture. MAP + Laplace takes under 5 seconds.

### 4.3 Prior Distributions

```
intercept  ~ Normal(0.0, 0.5)     — centred at zero, allows wide range
home_adv   ~ Normal(0.3, 0.2)     — prior belief: positive home advantage
attack[t]  ~ Normal(0.0, 1.0)     — each team's attack starts at league average
defense[t] ~ Normal(0.0, 1.0)     — each team's defense starts at league average
rho        ~ Normal(-0.1, 0.2)    — Dixon-Coles, prior: slightly negative
```

The priors prevent overfitting on small samples. If Arsenal only has 5 matches in the DB, their attack/defense parameters are pulled towards zero (league average) by the prior, preventing extreme estimates from noise.

### 4.4 Dixon-Coles Correction

Standard Poisson assumes home and away goals are independent. This is wrong for low-scoring games. The Dixon-Coles correction applies a multiplicative adjustment τ to the joint probability of four specific scorelines that Poisson systematically misprices:

```
τ(0,0) = max(1 − λ_home × λ_away × ρ, ε)  ← 0-0 draws
τ(1,0) = max(1 + λ_away × ρ, ε)            ← 1-0 home wins
τ(0,1) = max(1 + λ_home × ρ, ε)            ← 0-1 away wins
τ(1,1) = max(1 − ρ, ε)                     ← 1-1 draws
τ(h,a) = 1   for all other scorelines
```

The parameter ρ (rho) is learned from data. In practice it is slightly negative (around −0.10 to −0.15), meaning 0-0 and 1-1 draws are slightly more probable than pure Poisson predicts. This matters most for correct score and 1X2 markets.

**Why does this improve 1X2 accuracy?** Because draws are over-represented among low-scoring games. Without the DC correction, the model under-predicts draws. With it, draw probability improves, which flows directly into Double Chance and DNB markets too.

### 4.5 Adjustment Factors

All adjustment factors are multiplicative — they scale the expected goals lambda.

**Time Decay:** Older matches receive exponentially less weight in the log-likelihood.
```
weight(match) = exp(−log(2) / 60 × days_ago)
```
Half-life of 60 days means a match from 2 months ago has half the influence of yesterday's match. Matches within the last 35 days get a ×1.5 bonus ("recency boost").

**Momentum:** Compares recent xG performance to season average.
```
factor = 1.0 + (recent_avg_xg − season_avg_xg) × 0.12
clipped to [0.90, 1.10]
```
Computed separately for home and away contexts, because many teams have very different home/away profiles. A team in good form at home but poor form away would show different momentum factors for each context.

**League Form:** Computed from actual points in last 6 matches.
```
factor = 1.0 + (points_pct − 0.33) × 0.20
clipped to [0.90, 1.10]
```
0.33 corresponds to 1 point per game (draw every match). Above this = positive factor. This captures points-based form which affects team psychology and confidence.

**Fatigue:** Based on matchday number and fixture congestion.
```
matchday >= 35: factor = 0.94   (last 4 games of season — heavy fatigue)
matchday >= 30: factor = 0.97   (late season — mild fatigue)
last match < 4 days ago: additional × 0.97 (midweek turnaround)
```

**Injury Factor:** Each key player absence reduces lambda.
```
GK absent: −4%  |  FW absent: −8%  |  MF absent: −4%  |  DF absent: −3%
```
Minimum factor: 0.75 (even with many injuries, teams can still score).

**H2H Factor:** Based on historical win rate in this specific matchup.
```
factor = 1.0 + (home_win_rate − 0.45) × 0.15
clipped to [0.92, 1.08]
```
0.45 is the neutral reference (slightly below 50% due to draws). Requires at least 3 historical meetings; returns 1.0 otherwise.

**Weather Factor:** From Open-Meteo free API.
```
Heavy rain (>5mm/h):  goals × 0.92, corners × 0.94
Light rain (>2mm/h):  goals × 0.96, corners × 0.97
Strong wind (>40km/h): goals × 0.95, corners × 1.03
```

---

## 5. Code Walkthrough

### `class DB`

**Purpose:** MySQL connection wrapper. Handles auto-reconnect, cursor management, and per-league database selection.

**`__init__(db_name)`:** Takes an optional database name. If provided, it overrides the .env MYSQL_DB setting. This is how multi-league works — each league passes its own `db_name` (e.g., "fp_premier_league"). Calls `_ensure_db()` first to create the database if it doesn't exist.

**`_ensure_db()`:** Connects to MySQL without specifying a database, then runs `CREATE DATABASE IF NOT EXISTS`. This is why the first run works without manual DB creation — the script creates everything automatically.

**`execute(sql, params, fetch)`:** The single method used for all queries. Uses `cursor(dictionary=True)` so results come back as dicts (column_name: value) not tuples, which is far more readable throughout the codebase.

### `_ensure_schema(db)`

**Purpose:** Creates all 10 tables in the connected database using `CREATE TABLE IF NOT EXISTS`. Safe to run on any existing database — it only creates missing tables, never drops or modifies existing ones. Called once per session, right after DB connection.

**Why a function not a SQL file?** Because the script needs to work across 5 databases, and requiring users to run a SQL file 5 times (once per league) would be error-prone. Instead, the script creates the schema automatically when a new league database is used for the first time.

### `class APIFootball`

**Purpose:** All API-Football HTTP calls. No artificial request limits — the API returns error codes when limits are hit, and the code handles those gracefully.

**`requests_used_today()`:** Queries the `api_request_log` table to count how many API calls were made today. This is informational only — it's printed at session end so you can track your usage without the script ever blocking you.

**`_get(endpoint, params)`:** The core HTTP method. Logs every successful request to `api_request_log`. On error, prints the API's own error message and returns None. The caller decides whether to continue or skip.

**`find_upcoming_fixture(home_id, away_id)`:** Uses the H2H endpoint with `status=NS` (Not Started) to find the next scheduled match between exactly these two teams. This is more reliable than querying by season because it works regardless of which season the match falls in. Falls back to season-by-season search if H2H returns nothing.

**`_get_team_id(team_name)`:** Robust team lookup with five fallback levels:
1. Exact match
2. Case-insensitive exact
3. Full name LIKE search
4. All significant words must match (prevents "Manchester" matching both City and United)
5. First significant word (with a blocklist for ambiguous words: "Manchester", "West", etc.)

### `class WeatherFetcher`

**Purpose:** Fetches match-day weather from Open-Meteo — completely free, no API key. Adjusts expected goals and corners based on precipitation and wind.

**`get_match_weather(venue_name, match_datetime_str)`:** Looks up venue coordinates from the `venues` DB table first, then from `VENUE_COORDS_FALLBACK`. Calls Open-Meteo's hourly forecast API and extracts precipitation and wind speed for the match hour. Returns a dict with `goal_factor` and `corner_factor` multipliers.

**Why weather?** Studies of European football show that heavy rain reduces total goals by approximately 6-9% due to poorer ball control, more conservative tactics, and physical difficulty. Strong wind reduces long passing and aerial quality. These are real effects that bookmakers account for roughly — if our model accounts for them more precisely, that creates edge.

### `class DataManager`

**Purpose:** Orchestrates all data operations. The bridge between the API, the database, and the model.

**`_sync_basic_matches()`:** For each season in AFL_SEASONS:
- If 380 matches already exist → skip forever (season is complete)
- If 370+ matches exist and synced today → skip
- Otherwise fetch from API (1 request per season)

This logic ensures that once a historical season is fully loaded, it costs zero API requests on every subsequent run.

**`_run_backfill_batch(priority_teams, limit)`:** Fetches rich stats (xG, shots, corners) for unenriched matches. The `priority_teams` parameter ensures that when you enter "Arsenal vs Man City", Arsenal's and Man City's matches get enriched first — before other teams — so the model has the best possible data for that specific prediction. Each match costs 3 API requests (stats + lineups + events).

**`_enrich_match(match_id)`:** For one match, fetches stats, lineups, and events, then stores them in the DB. Verifies the match exists in `matches_basic` first — this prevents enriching matches from other leagues that somehow got into the DB.

**`fixture_data_completeness(home, away)`:** Checks the database to assess prediction readiness:
- FULL: 20+ enriched matches per team, 3+ H2H records
- GOOD: 10+ enriched matches per team
- PARTIAL: 5+ enriched for at least one team
- MINIMAL: less than 5 enriched matches for either team

### `class FixtureModel`

**Purpose:** The statistical heart of the system. Fits the Bayesian Poisson model to historical data and returns posterior samples for simulation.

**`__init__`:** Prepares all data arrays. Uses npxG if available (>30% coverage), otherwise xG, otherwise raw goals. Computes all adjustment factors (H2H, injuries, momentum, fatigue, form, weather) upfront so inference is not slowed down by repeated DB calls.

**`_log_likelihood(theta)`:** The weighted sum of Poisson log-probabilities plus the Dixon-Coles tau correction. The weights are the time-decay weights — recent matches contribute more to the likelihood than old matches.

**`_log_prior(theta)`:** Normal distribution priors on all parameters. Prevents overfitting.

**`fit()`:** Runs L-BFGS-B optimisation to find MAP estimate (typically 500-800 iterations), then computes the Hessian for the Laplace covariance. Draws 2,000 samples from the posterior multivariate Gaussian.

**`lambdas()`:** Using the 2,000 posterior samples, computes home and away expected goals for this specific fixture, applying all adjustment factors.

### `class Sim`

**Purpose:** Monte Carlo simulation. Given the posterior lambda samples, simulate 10,000 matches and compute probabilities for every market.

**`__init__`:** Draws 10,000 bootstrapped samples from the 2,000 posterior lambda samples. For each, simulates home and away goals from Poisson distributions.

**`correct_score()`:** Applies the Dixon-Coles tau correction to the raw simulation counts. This makes 0-0, 1-0, 0-1, 1-1 probabilities match the corrected model rather than raw Poisson simulation.

**`corners(home_avg, away_avg, weather_factor)`:** Uses team-specific corner rates from the DB when available (minimum 5 enriched matches). Falls back to league averages scaled by the team's relative xG dominance in this match. Weather factor scales the result.

**`cards(home_avg, away_avg, referee_factor)`:** Uses team-specific yellow card rates. The `referee_factor` comes from the `referee_card_rate()` method which queries historical card rates for the assigned referee from match events data.

**`anytime_goalscorer(players_h, players_a)`:** When player stats are available from the API:
- Uses actual historical goals/90 as the individual player rate
- P(scores at least once) = 1 − exp(−player_lambda)

When stats are unavailable:
- Uses position weights (FW=35%, MF=12%, DF=4%, GK=1%) applied to team lambda
- This is the fallback and is less accurate

---

## 6. Prediction Pipeline

When you type "Arsenal vs Man City" and press Enter:

```
1. resolve_team() — converts your input to exact DB team name
   Uses ALIASES dict + fuzzy matching against known teams in DB

2. find_fixture() — looks up the upcoming scheduled match
   Uses _get_team_id() (robust, multi-word matching)
   Calls API H2H endpoint to get fixture ID, date, venue, referee

3. _run_backfill_batch() — enriches unenriched matches
   Arsenal and Man City's matches prioritised
   Fetches stats + lineups + events for unenriched historical matches

4. fetch_injuries_for_team() — current injuries from API
   Fetches current season injuries only (no historical players)
   Skips null player names

5. fetch_h2h() — head-to-head history
   Checks DB cache first (if 5+ records exist, skip API)
   Otherwise fetches from API and caches

6. standings_context() — current league table positions
   For display purposes and context in ticket header

7. WeatherFetcher.get_match_weather() — match day weather
   Looks up venue in DB, calls Open-Meteo hourly forecast

8. load_for_fixture() — pulls all historical matches involving either team
   Returns DataFrame with basic + stats data joined
   Used to train the model

9. FixtureModel.fit() — Bayesian inference
   L-BFGS-B MAP optimisation (~2 seconds)
   Laplace covariance computation (~1 second)
   2,000 posterior samples drawn

10. Sim() — 10,000 Monte Carlo simulations
    All markets computed in under 1 second

11. get_odds() — manual odds entry by section
    You enter only the markets your bookmaker offers

12. print_ticket() — full prediction output
    Value analysis compares model probabilities to implied probabilities
    Only recommends bets meeting BOTH edge AND probability thresholds

13. tracker.append_prediction() — saves to Excel
    db.execute predictions_log INSERT — saves to MySQL
```

---

## 7. Markets and EV Analysis

### Dual-Condition Bet Filter

A bet is only recommended when BOTH conditions are simultaneously true:

```
Condition 1: model_probability − implied_probability ≥ MIN_EDGE (4%)
Condition 2: model_probability ≥ MIN_PROB_BY_MARKET
```

**Why both conditions?** Edge alone is insufficient because:
- A 10% edge on a 5% event (longshot) requires ~2,000 bets to confirm statistically
- A 10% edge on a 60% event requires ~200 bets to confirm
- High-edge longshots have enormous variance — you can go bankrupt before the edge pays off

**Premium bet:** Both conditions met AND edge ≥ 7% AND model_prob ≥ 50%.

### Kelly Criterion (Quarter-Kelly)

```
Kelly fraction = (b×p − (1−p)) / b
Stake = bankroll × Kelly_fraction × 0.25
```

Where b = decimal_odds − 1, p = model probability.

Quarter-Kelly (×0.25) is used instead of full Kelly because:
1. The model's probability estimates have uncertainty
2. Full Kelly produces very large swings even when correct
3. Quarter-Kelly reduces variance by 75% with only ~10% reduction in expected growth rate

Maximum stake is capped at 5% of bankroll regardless of Kelly output.

### Market Coverage

The model computes probabilities for 80+ markets organized in 5 sections:
- **Result:** 1X2, DC, DNB, WtN, AH, HT, HT/FT
- **Goals:** O/U 0.5-5.5, BTTS, Home/Away to score, 1st/2nd half goals, team goals, combos
- **Corners:** O/U 7.5-11.5
- **Cards:** O/U 1.5-4.5, booking points, red card
- **Correct Score:** All scores 0-0 through 5-5

---

## 8. Multi-League System

### How League Selection Works

At startup, `select_league()` prompts you to choose a league (1-5). It sets three global variables:
- `AFL_LEAGUE_ID` — the API-Football league ID used in all API calls
- `AFL_SEASONS` — the list of seasons to sync for this league
- `ACTIVE_LEAGUE` — the full config dict (name, DB name, code, etc.)

Then `DB(db_name=league["db"])` connects to that league's specific database. If the database doesn't exist yet, `_ensure_db()` creates it. `_ensure_schema(db)` creates all tables.

### Why Separate Databases?

**Data integrity:** PSG's matches in Ligue 1 should never influence Arsenal's model in the Premier League. The team strength parameters are league-relative — a "strong" team in the Championship would have terrible parameters if compared to Premier League teams. Separate databases guarantee complete isolation.

**Team name conflicts:** "Lyon" appears in Ligue 1 but not in the PL. With a shared DB, you'd need to filter by league on every query. With separate DBs, every query is implicitly filtered.

**Independent backfill:** Each league enriches its own matches independently. You can run deep enrichment on the Bundesliga without affecting PL data collection.

### Shared Tracker

The Excel tracker (`predictions_tracker.xlsx`) is shared across all leagues. It has a "League" column so you can filter and analyse performance per league. After 300+ predictions across leagues, you can compare: which league does this model predict best?

---

## 9. Excel Tracker

### Purpose

The tracker is your calibration and profitability measurement tool. Without tracking predictions vs results, you cannot know if the model has genuine edge or if wins/losses are just variance.

### Column Structure

**Auto-filled (blue headers):** Everything the model calculates — xG, probabilities, expected corners, model factors, fixture ID, weather, referee factor.

**Manual fill (orange headers):** You fill these in after the match — actual score, goals, corners, cards, bet placed Y/N, stake, odds taken, result W/L/P, profit/loss.

**Formula columns (green headers):**
- **1X2 Correct?** — Was the model's top predicted outcome correct?
- **Corners/Cards Correct?** — Was the over/under direction correct?
- **Closing Odds** — Fill in the closing line from your bookmaker
- **CLV (Closing Line Value)** — `(odds_taken / closing_odds − 1) × 100%`
- **Bankroll After** — Running bankroll after each bet
- **Running ROI%** — Total profit / total staked × 100

### CLV — The Most Important Column

Closing Line Value tells you whether you found genuine edge. If your average CLV is positive (you consistently bet at better odds than the closing line), you are a winning bettor long-term regardless of short-term results. Professional bettors use CLV as their primary performance metric because:
- Match results have high variance (noise)
- CLV measures process quality (signal)
- A model with positive average CLV will be profitable over 500+ bets

### Performance Sheet

The Performance sheet summarises:
- Total predictions, bets placed, strike rate
- Total staked, returns, net profit, ROI%
- Accuracy per market: 1X2%, O/U 2.5%, BTTS%, Corners%, Cards%
- Average CLV — the key long-run health metric
- Current bankroll

---

## 10. How to Interpret Results

### What Good Numbers Look Like

| Metric | Concerning | Acceptable | Good |
|---|---|---|---|
| 1X2 Accuracy | <45% | 45-50% | >52% |
| O/U 2.5 Accuracy | <52% | 52-57% | >58% |
| BTTS Accuracy | <52% | 52-56% | >57% |
| Running ROI% | <-10% | -5% to +5% | >5% |
| Average CLV% | <-2% | -2% to +2% | >2% |

**Important:** Do not judge the model before 200 bets per market. Football has high variance. A 60% model will lose 40% of the time — that means losing streaks of 5-8 bets are completely normal even when the model is working correctly.

### When to Trust the Model More

- Fixture has FULL data quality (20+ enriched matches per team)
- Both teams have been in the selected league for multiple seasons
- Model probability ≥ 50% (model genuinely favours this outcome)
- Edge ≥ 5% (meaningful mispricing)
- Market is O/U goals or BTTS (more efficient markets than 1X2)

### When to Be Cautious

- Fixture has MINIMAL or PARTIAL data quality
- Newly promoted team (limited history)
- Very early season (< matchday 5)
- Large injury list with key players absent
- Late season for a team with nothing to play for vs a title chaser

### Long-Run Projection

Based on historical performance of calibrated Poisson models with Dixon-Coles on European top-flight leagues:

- Expected bettable matches per season (edge ≥ 4%): 60-80 per league
- Expected ROI on bettable selections: 7-13%
- Bankroll growth on 20,000 RWF starting balance: 5,000-12,000 RWF net year 1
- Compounding effect year 2 (if starting from larger base): significantly higher

Running predictions across 5 leagues gives you ~300-400 bettable opportunities per season rather than 60-80. This accelerates calibration (you reach 200 bets in one season rather than two) and diversifies the variance across different tactical systems and refereeing cultures.

---

## Setup Reference

```cmd
# Install Python dependencies (Anaconda Prompt)
pip install numpy pandas scipy requests openpyxl mysql-connector-python python-dotenv

# First run — script creates all databases automatically
python football_predictor.py

# Useful commands during a session
teams          → list all teams in selected league's DB
check Arsenal vs Man City   → show data readiness for this fixture
quit           → exit and show requests used today
```

---

*Document version: v11.0 | Generated for football_predictor.py*
