"""
betting.py — Edge/EV/Kelly functions, bet classification, odds entry prompts,
             and CLV tracking (save snapshot → update closing → resolve result).
"""

import config


def implied(odds):   return 1/odds if odds>1 else 0.0
def edge(p, odds):   return p - implied(odds)
def ev(p, odds):     return p*(odds-1) - (1-p)

def kelly_stake(p, odds, bankroll):
    b = odds-1.0
    if b<=0 or p<=0: return 0.0
    f = max(0.0, (b*p-(1-p))/b) * config.QUARTER_KELLY
    return round(bankroll * min(f, config.MAX_KELLY_PCT), 0)

def bet_quality(market: str, model_prob: float, edge_val: float,
                ev_val: float = None) -> str:
    """
    Classify a bet using dual conditions — edge AND probability floors.

    Additional guards from tracker data analysis (83 bets):
      - EV > 0.40: return 'ev_cap' — model overconfidence, not real edge.
        Data shows EV > 0.669 produced large losses. Cap at 0.40 for safety.
      - Observe-only leagues (BL/L1): return 'observe_league' — no bets
        until positive CLV confirmed. Based on -311 and -521 RWF/bet avg.
      - under_0.5, under_1.5: return 'never' — disabled always.
      - under_2.5: league-specific probability floor.
      - dnb_home/dnb_away: raised floors (55%/52%) — stop betting draws as DNB.

    Returns: 'premium', 'acceptable', 'low_prob', 'low_edge', 'weak',
             'never', 'ev_cap', 'observe_league'
    """
    # Hard league restriction — observe only, no bets
    if config.AFL_LEAGUE_ID in config.OBSERVE_ONLY_LEAGUES:
        return "observe_league"

    # Hard market ban
    if market in config.NEVER_BET_MARKETS:
        return "never"

    # EV cap — overconfident model output
    if ev_val is not None and ev_val > config.MAX_EV_THRESHOLD:
        return "ev_cap"

    if market == "under_2.5":
        min_prob = config.LEAGUE_UNDER25_FLOOR.get(
            config.AFL_LEAGUE_ID, 0.52)
    else:
        min_prob = config.MIN_PROB_BY_MARKET.get(market, config.MIN_PROB_DEFAULT)

    min_edge_mkt = config.UNDER_MARKET_MIN_EDGE if market in config.UNDER_MARKETS else config.MIN_EDGE
    prob_ok = model_prob >= min_prob
    edge_ok = edge_val   >= min_edge_mkt

    if (model_prob >= config.PREMIUM_PROB and edge_val >= config.PREMIUM_EDGE
            and prob_ok and (ev_val is None or ev_val <= config.MAX_EV_THRESHOLD)):
        return "premium"
    if prob_ok and edge_ok:
        return "acceptable"
    if edge_ok and not prob_ok:
        return "low_prob"
    if prob_ok and not edge_ok:
        return "low_edge"
    return "weak"

def quality_icon(quality: str) -> str:
    return {
        "premium"        : "🔥",
        "acceptable"     : "✅",
        "low_prob"       : "⚠️",
        "low_edge"       : "⚠️",
        "weak"           : "⚠️",
        "never"          : "🚫",
        "ev_cap"         : "🔴",
        "observe_league" : "👁️",
    }.get(quality, "⚠️")

def should_bet(quality: str) -> bool:
    """Only premium and acceptable qualities warrant placing a bet."""
    return quality in ("premium", "acceptable")


# ══════════════════════════════════════════════════════════════════════════════
# ODDS ENTRY  —  section-by-section prompt style
# Each section: y/n to enter, then one prompt per market.
# Press Enter on any market to skip it.
# ══════════════════════════════════════════════════════════════════════════════

# ── Section 1: 1X2 + Double Chance ───────────────────────────────────────────
ODDS_PROMPTS_RESULT = [
    ("home_win",   "  Home Win (1)                    "),
    ("draw",       "  Draw (X)                        "),
    ("away_win",   "  Away Win (2)                    "),
    ("dc_1x",      "  Double Chance  Home or Draw (1X)"),
    ("dc_x2",      "  Double Chance  Draw or Away (X2)"),
    ("dc_12",      "  Double Chance  No Draw (12)     "),
]

# ── Section 2: Match Goals Under ─────────────────────────────────────────────
ODDS_PROMPTS_GOALS_UNDER = [
    ("under_0.5",  "  Match Goals   Under 0.5         "),
    ("under_1.5",  "  Match Goals   Under 1.5         "),
    ("under_2.5",  "  Match Goals   Under 2.5         "),
    ("under_3.5",  "  Match Goals   Under 3.5         "),
    ("under_4.5",  "  Match Goals   Under 4.5         "),
    ("under_5.5",  "  Match Goals   Under 5.5         "),
    ("under_6.5",  "  Match Goals   Under 6.5         "),
]

# ── Section 3: Match Goals Over ──────────────────────────────────────────────
ODDS_PROMPTS_GOALS_OVER = [
    ("over_0.5",   "  Match Goals   Over 0.5          "),
    ("over_1.5",   "  Match Goals   Over 1.5          "),
    ("over_2.5",   "  Match Goals   Over 2.5          "),
    ("over_3.5",   "  Match Goals   Over 3.5          "),
    ("over_4.5",   "  Match Goals   Over 4.5          "),
    ("over_5.5",   "  Match Goals   Over 5.5          "),
    ("over_6.5",   "  Match Goals   Over 6.5          "),
]

# ── Section 4: BTTS ───────────────────────────────────────────────────────────
ODDS_PROMPTS_BTTS = [
    ("btts_yes",   "  BTTS          Yes               "),
    ("btts_no",    "  BTTS          No                "),
]

# ── Section 5: Home Team Total Goals ─────────────────────────────────────────
ODDS_PROMPTS_HOME_GOALS = [
    ("home_un_0.5","  Home Goals    Under 0.5         "),
    ("home_un_1.5","  Home Goals    Under 1.5         "),
    ("home_un_2.5","  Home Goals    Under 2.5         "),
    ("home_un_3.5","  Home Goals    Under 3.5         "),
    ("home_ov_0.5","  Home Goals    Over 0.5          "),
    ("home_ov_1.5","  Home Goals    Over 1.5          "),
    ("home_ov_2.5","  Home Goals    Over 2.5          "),
    ("home_ov_3.5","  Home Goals    Over 3.5          "),
]

# ── Section 6: Away Team Total Goals ─────────────────────────────────────────
ODDS_PROMPTS_AWAY_GOALS = [
    ("away_un_0.5","  Away Goals    Under 0.5         "),
    ("away_un_1.5","  Away Goals    Under 1.5         "),
    ("away_un_2.5","  Away Goals    Under 2.5         "),
    ("away_un_3.5","  Away Goals    Under 3.5         "),
    ("away_ov_0.5","  Away Goals    Over 0.5          "),
    ("away_ov_1.5","  Away Goals    Over 1.5          "),
    ("away_ov_2.5","  Away Goals    Over 2.5          "),
    ("away_ov_3.5","  Away Goals    Over 3.5          "),
]

# ── Section 7: Match Corners ──────────────────────────────────────────────────
ODDS_PROMPTS_CORNERS = [
    ("cor_un_6.5",  "  Corners       Under 6.5         "),
    ("cor_un_7.5",  "  Corners       Under 7.5         "),
    ("cor_un_8.5",  "  Corners       Under 8.5         "),
    ("cor_un_9.5",  "  Corners       Under 9.5         "),
    ("cor_un_10.5", "  Corners       Under 10.5        "),
    ("cor_un_11.5", "  Corners       Under 11.5        "),
    ("cor_un_12.5", "  Corners       Under 12.5        "),
    ("cor_ov_6.5",  "  Corners       Over 6.5          "),
    ("cor_ov_7.5",  "  Corners       Over 7.5          "),
    ("cor_ov_8.5",  "  Corners       Over 8.5          "),
    ("cor_ov_9.5",  "  Corners       Over 9.5          "),
    ("cor_ov_10.5", "  Corners       Over 10.5         "),
    ("cor_ov_11.5", "  Corners       Over 11.5         "),
    ("cor_ov_12.5", "  Corners       Over 12.5         "),
]

# ── Section 8: Draw No Bet ────────────────────────────────────────────────────
# High-confidence market: derived directly from 1X2, removes draw variance.
# Strong signal when model has a clear team preference.
ODDS_PROMPTS_DNB = [
    ("dnb_home",   "  Draw No Bet   Home              "),
    ("dnb_away",   "  Draw No Bet   Away              "),
]

# ── Section 9: Half Time Result ───────────────────────────────────────────────
# Model uses a dedicated halftime sub-model (mu × 0.45).
# Reliable when one team dominates early (high press, fast starters).
ODDS_PROMPTS_HT = [
    ("ht_hw",      "  HT Result     Home Win          "),
    ("ht_d",       "  HT Result     Draw              "),
    ("ht_aw",      "  HT Result     Away Win          "),
    ("ht_ov_0.5",  "  HT Goals      Over 0.5          "),
    ("ht_ov_1.5",  "  HT Goals      Over 1.5          "),
    ("ht_un_0.5",  "  HT Goals      Under 0.5         "),
    ("ht_un_1.5",  "  HT Goals      Under 1.5         "),
]

# ── Section 10: Cards ────────────────────────────────────────────────────────
# Model has a dedicated NB cards sub-model using team rates + referee factor.
# Most reliable for high-line: Over 2.5 and Over 3.5 cards.
ODDS_PROMPTS_CARDS = [
    ("crd_ov_1.5", "  Cards         Over 1.5          "),
    ("crd_ov_2.5", "  Cards         Over 2.5          "),
    ("crd_ov_3.5", "  Cards         Over 3.5          "),
    ("crd_ov_4.5", "  Cards         Over 4.5          "),
    ("crd_un_1.5", "  Cards         Under 1.5         "),
    ("crd_un_2.5", "  Cards         Under 2.5         "),
    ("crd_un_3.5", "  Cards         Under 3.5         "),
]


def _collect_odds_section(prompts: list, section_label: str) -> dict:
    """Prompt once per section (y/n), then one prompt per market. Enter = skip."""
    odds = {}
    raw = input(f"\n  Enter {section_label} odds? (y/n): ").strip().lower()
    if raw != "y":
        return odds
    for key, label in prompts:
        raw2 = input(label + ": ").strip()
        if raw2:
            try:
                v = float(raw2)
                if v > 1.0:
                    odds[key] = v
            except ValueError:
                pass
    return odds


def get_odds() -> dict:
    print()
    print("╔" + "═"*56 + "╗")
    print("║" + "  BOOKMAKER ODDS  (decimal, e.g. 1.85 · Enter=skip)  ".center(56) + "║")
    print("║" + "  Answer y/n for each section then fill what you have ".center(56) + "║")
    print("╚" + "═"*56 + "╝")
    odds = {}
    odds.update(_collect_odds_section(ODDS_PROMPTS_RESULT,       "Result & Double Chance"))
    odds.update(_collect_odds_section(ODDS_PROMPTS_GOALS_UNDER,  "Match Goals Under"))
    odds.update(_collect_odds_section(ODDS_PROMPTS_GOALS_OVER,   "Match Goals Over"))
    odds.update(_collect_odds_section(ODDS_PROMPTS_BTTS,         "BTTS"))
    odds.update(_collect_odds_section(ODDS_PROMPTS_HOME_GOALS,   "Home Team Goals"))
    odds.update(_collect_odds_section(ODDS_PROMPTS_AWAY_GOALS,   "Away Team Goals"))
    odds.update(_collect_odds_section(ODDS_PROMPTS_CORNERS,      "Match Corners"))
    odds.update(_collect_odds_section(ODDS_PROMPTS_DNB,          "Draw No Bet"))
    odds.update(_collect_odds_section(ODDS_PROMPTS_HT,           "Half Time Result & Goals"))
    odds.update(_collect_odds_section(ODDS_PROMPTS_CARDS,        "Cards"))
    return odds


# ══════════════════════════════════════════════════════════════════════════════
# LINEUP INPUT
# ══════════════════════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════════════════════
# CLV TRACKING
# ══════════════════════════════════════════════════════════════════════════════
#
# FLOW:
#   1. Prediction time  → save_bet_snapshot()   called for every should_bet market
#   2. Kickoff          → update_clv()           called by weekly_update.py
#                         (fetches closing odds from market_opening_odds table)
#   3. Match settled    → resolve_bet()          called by weekly_update.py
#                         (evaluates result and writes profit_rwf)
#
# HOW TO CALL save_bet_snapshot IN main.py:
#   After the model processes each market and decides should_bet, call:
#
#     from betting import save_bet_snapshot
#     if should_bet(quality):
#         save_bet_snapshot(
#             db           = db,
#             match_id     = fixture_id,   # int from API
#             match_date   = match_date,   # datetime
#             league_code  = config.AFL_LEAGUE_CODE,
#             home_team    = home_team,
#             away_team    = away_team,
#             market       = market_key,   # e.g. "under_2.5"
#             model_prob   = model_prob,
#             odds         = bookmaker_odds,
#             quality      = quality,
#             stake_rwf    = stake,
#             ev_val       = ev_val,
#         )
#
# HOW TO CALL update_clv IN weekly_update.py:
#   For each tracked bet where match kicks off today and closing_odds is NULL:
#
#     from betting import update_clv_from_db
#     update_clv_from_db(db)   # handles all pending CLV updates automatically
#
# HOW TO CALL resolve_bet IN weekly_update.py:
#   After fetching match results:
#
#     from betting import resolve_bet
#     resolve_bet(db, match_id=fixture_id, home_goals=hg, away_goals=ag)
# ══════════════════════════════════════════════════════════════════════════════


def save_bet_snapshot(db, match_id: int, match_date, league_code: str,
                      home_team: str, away_team: str, market: str,
                      model_prob: float, odds: float, quality: str,
                      stake_rwf: float, ev_val: float) -> int | None:
    """
    Record a bet recommendation at the moment odds are entered.

    Uses ON DUPLICATE KEY UPDATE so running the predictor twice for the same
    fixture updates the existing row rather than creating a duplicate.
    The UNIQUE KEY on (match_id, market) enforces one entry per market per match.

    Returns the bet_tracker row ID, or None on failure.
    """
    edge_val     = edge(model_prob, odds)
    implied_prob = implied(odds)

    row_id = db.execute(
        """INSERT INTO bet_tracker
               (match_id, match_date, league_code, home_team, away_team,
                market, model_prob, odds_at_prediction, implied_prob_at_prediction,
                edge_at_prediction, ev_at_prediction, quality, stake_rwf)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON DUPLICATE KEY UPDATE
               model_prob                 = VALUES(model_prob),
               odds_at_prediction         = VALUES(odds_at_prediction),
               implied_prob_at_prediction = VALUES(implied_prob_at_prediction),
               edge_at_prediction         = VALUES(edge_at_prediction),
               ev_at_prediction           = VALUES(ev_at_prediction),
               quality                    = VALUES(quality),
               stake_rwf                  = VALUES(stake_rwf),
               recorded_at                = NOW()
        """,
        (match_id, match_date, league_code, home_team, away_team,
         market, model_prob, odds, implied_prob,
         edge_val, ev_val, quality, stake_rwf)
    )
    return row_id


def update_clv(db, match_id: int, market: str, closing_odds: float) -> bool:
    """
    Populate closing_odds and compute CLV for one specific bet.

    CLV formula:  clv_pct = (odds_at_prediction / closing_odds - 1) * 100
      Positive  = we got better odds than market closed at = genuine edge signal
      Negative  = market moved against us after we bet

    Called by update_clv_from_db() and can be called directly from weekly_update.py
    if you already have closing odds in hand.
    Returns True if a row was updated, False if not found.
    """
    row = db.fetchone(
        """SELECT id, odds_at_prediction
           FROM bet_tracker
           WHERE match_id = %s AND market = %s AND closing_odds IS NULL""",
        (match_id, market)
    )
    if not row:
        return False

    our_odds = row["odds_at_prediction"]
    clv_pct  = round((our_odds / closing_odds - 1) * 100, 4) if closing_odds > 1 else None

    db.execute(
        """UPDATE bet_tracker
           SET closing_odds = %s, clv_pct = %s, closed_at = NOW()
           WHERE id = %s""",
        (closing_odds, clv_pct, row["id"])
    )
    return True


def update_clv_from_db(db) -> int:
    """
    Automatic CLV update: for every tracked bet where:
      - closing_odds is still NULL
      - the match kicks off within the next 2 hours (i.e. closing line is set)
      - a closing odds value exists in market_opening_odds

    Uses market_opening_odds as the proxy for closing odds. This is an
    approximation — true closing odds require a live odds feed. In practice,
    the odds fetched closest to kickoff are the best available proxy.

    Returns the count of bets updated.
    """
    pending = db.fetchall(
        """SELECT bt.id, bt.match_id, bt.market, bt.odds_at_prediction,
                  mo.odds AS closing_odds
           FROM bet_tracker bt
           JOIN market_opening_odds mo
             ON mo.fixture_id = bt.match_id AND mo.market = bt.market
           WHERE bt.closing_odds IS NULL
             AND bt.match_date <= NOW() + INTERVAL 2 HOUR
        """
    )

    updated = 0
    for row in pending:
        closing = row["closing_odds"]
        our_odds = row["odds_at_prediction"]
        clv_pct  = round((our_odds / closing - 1) * 100, 4) if closing > 1 else None

        db.execute(
            """UPDATE bet_tracker
               SET closing_odds = %s, clv_pct = %s, closed_at = NOW()
               WHERE id = %s""",
            (closing, clv_pct, row["id"])
        )
        updated += 1

    return updated


def resolve_bet(db, match_id: int, home_goals: int, away_goals: int) -> int:
    """
    Evaluate and record the result for every unresolved tracked bet for this match.

    Handles all market types defined in the odds sections above.
    Half-time markets (ht_*) are skipped here — they require ht score input,
    which must be passed separately if needed.

    Returns the count of bets resolved.
    """
    bets = db.fetchall(
        """SELECT id, market, odds_at_prediction, stake_rwf
           FROM bet_tracker
           WHERE match_id = %s AND result IS NULL""",
        (match_id,)
    )

    resolved = 0
    for bet in bets:
        result = _evaluate_result(bet["market"], home_goals, away_goals)
        if result == "void":
            profit = 0.0
        elif result == "won":
            profit = round(bet["stake_rwf"] * (bet["odds_at_prediction"] - 1), 0)
        else:  # lost
            profit = -round(bet["stake_rwf"], 0)

        db.execute(
            """UPDATE bet_tracker
               SET result = %s, profit_rwf = %s
               WHERE id = %s""",
            (result, profit, bet["id"])
        )
        resolved += 1

    return resolved


def _evaluate_result(market: str, hg: int, ag: int) -> str:
    """
    Evaluate bet outcome from market key and final score.
    Returns 'won', 'lost', or 'void'.
    'void' is used for unknown markets and DNB push (draw outcome).
    Half-time markets (ht_*) return 'void' here — resolve separately with HT score.
    """
    total = hg + ag
    m     = market

    # ── Exact market map ────────────────────────────────────────────────────
    fixed = {
        "home_win" : "won" if hg > ag          else "lost",
        "draw"     : "won" if hg == ag          else "lost",
        "away_win" : "won" if ag > hg           else "lost",
        "btts_yes" : "won" if hg > 0 and ag > 0 else "lost",
        "btts_no"  : "won" if hg == 0 or ag == 0 else "lost",
        "dc_1x"    : "won" if hg >= ag          else "lost",   # Home or Draw
        "dc_x2"    : "won" if ag >= hg          else "lost",   # Draw or Away
        "dc_12"    : "won" if hg != ag          else "lost",   # No Draw
        "dnb_home" : ("won" if hg > ag else ("void" if hg == ag else "lost")),
        "dnb_away" : ("won" if ag > hg else ("void" if hg == ag else "lost")),
    }
    if m in fixed:
        return fixed[m]

    # Half-time markets — cannot resolve without HT score, mark void
    if m.startswith("ht_"):
        return "void"

    # ── Pattern-based markets ────────────────────────────────────────────────
    try:
        # Match totals: over_2.5, under_3.5, etc.
        if m.startswith("over_"):
            line = float(m[5:])
            return "won" if total > line else "lost"
        if m.startswith("under_"):
            line = float(m[6:])
            return "won" if total < line else "lost"

        # Home team goals: home_ov_1.5, home_un_0.5, etc.
        if m.startswith("home_ov_"):
            line = float(m[8:])
            return "won" if hg > line else "lost"
        if m.startswith("home_un_"):
            line = float(m[8:])
            return "won" if hg < line else "lost"

        # Away team goals: away_ov_1.5, away_un_0.5, etc.
        if m.startswith("away_ov_"):
            line = float(m[8:])
            return "won" if ag > line else "lost"
        if m.startswith("away_un_"):
            line = float(m[8:])
            return "won" if ag < line else "lost"

        # Corners and cards — totals only, actual value passed as hg+ag proxy
        # NOTE: for corner/card markets caller should pass corner_total, card_total
        # as hg and ag respectively (combined total in hg, 0 in ag).
        if m.startswith("cor_ov_") or m.startswith("crd_ov_"):
            line = float(m[7:])
            return "won" if total > line else "lost"
        if m.startswith("cor_un_") or m.startswith("crd_un_"):
            line = float(m[7:])
            return "won" if total < line else "lost"

    except (ValueError, IndexError):
        pass

    return "void"   # unknown market — safe default


# ── CLV Summary & Report ──────────────────────────────────────────────────────

def clv_summary(db) -> dict:
    """
    Aggregate CLV statistics for the report.
    Returns a dict with keys: overall, by_market, by_league, trend.
    Only includes rows where clv_pct is populated (i.e. past kickoff).
    """
    overall = db.fetchone(
        """SELECT
               COUNT(*)                                              AS n,
               ROUND(AVG(clv_pct), 3)                               AS avg_clv,
               ROUND(SUM(profit_rwf), 0)                            AS total_profit,
               ROUND(SUM(stake_rwf), 0)                             AS total_staked,
               SUM(CASE WHEN result = 'won'  THEN 1 ELSE 0 END)     AS wins,
               SUM(CASE WHEN result = 'lost' THEN 1 ELSE 0 END)     AS losses,
               SUM(CASE WHEN clv_pct > 0     THEN 1 ELSE 0 END)     AS clv_positive,
               ROUND(STDDEV(clv_pct), 3)                            AS clv_stddev
           FROM bet_tracker
           WHERE clv_pct IS NOT NULL"""
    )

    by_market = db.fetchall(
        """SELECT
               market,
               COUNT(*)                    AS n,
               ROUND(AVG(clv_pct), 3)      AS avg_clv,
               ROUND(SUM(profit_rwf), 0)   AS profit,
               ROUND(SUM(stake_rwf), 0)    AS staked
           FROM bet_tracker
           WHERE clv_pct IS NOT NULL
           GROUP BY market
           ORDER BY avg_clv DESC"""
    )

    by_league = db.fetchall(
        """SELECT
               league_code,
               COUNT(*)                    AS n,
               ROUND(AVG(clv_pct), 3)      AS avg_clv,
               ROUND(SUM(profit_rwf), 0)   AS profit,
               ROUND(SUM(stake_rwf), 0)    AS staked
           FROM bet_tracker
           WHERE clv_pct IS NOT NULL
           GROUP BY league_code
           ORDER BY avg_clv DESC"""
    )

    trend = db.fetchall(
        """SELECT
               DATE_FORMAT(match_date, '%Y-%m')   AS month,
               COUNT(*)                            AS n,
               ROUND(AVG(clv_pct), 3)              AS avg_clv,
               ROUND(SUM(profit_rwf), 0)           AS profit
           FROM bet_tracker
           WHERE clv_pct IS NOT NULL
           GROUP BY month
           ORDER BY month"""
    )

    return {
        "overall"   : overall   or {},
        "by_market" : by_market or [],
        "by_league" : by_league or [],
        "trend"     : trend     or [],
    }


def print_clv_report(db):
    """
    Print a formatted CLV report to stdout.
    Call from main.py or backfill_all.py's status check.
    """
    data = clv_summary(db)
    ov   = data["overall"]

    print("\n" + "═"*62)
    print("  CLOSING LINE VALUE  (CLV)  REPORT")
    print("═"*62)

    n = ov.get("n") or 0
    if n == 0:
        print("\n  No CLV data yet.")
        print("  CLV is populated automatically at kickoff for every tracked bet.")
        print("  Bets are recorded the moment you enter odds in the predictor.")
        print("═"*62)
        return

    avg_clv  = ov.get("avg_clv")  or 0.0
    stddev   = ov.get("clv_stddev") or 0.0
    staked   = ov.get("total_staked")  or 0
    profit   = ov.get("total_profit")  or 0
    roi      = (profit / staked * 100) if staked else 0
    wins     = ov.get("wins")    or 0
    losses   = ov.get("losses")  or 0
    clv_pos  = ov.get("clv_positive") or 0
    clv_icon = "✅" if avg_clv > 0 else "🔴"

    print(f"\n  Bets tracked          : {n}")
    print(f"  Avg CLV               : {clv_icon}  {avg_clv:+.3f}%  (σ = {stddev:.3f}%)")
    print(f"  CLV positive rate     : {clv_pos}/{n}  ({clv_pos/n*100:.0f}%)")
    print(f"  ROI                   : {roi:+.1f}%  ({profit:+,.0f} RWF on {staked:,.0f} staked)")
    print(f"  Record                : {wins}W / {losses}L")

    # ── By market ──────────────────────────────────────────────────────────
    if data["by_market"]:
        print(f"\n  {'Market':<22} {'N':>4}  {'Avg CLV':>9}  {'ROI':>7}  {'Profit':>10}")
        print("  " + "─"*58)
        for r in data["by_market"]:
            clv  = r.get("avg_clv") or 0.0
            prof = r.get("profit")  or 0
            stk  = r.get("staked")  or 0
            roi_m = (prof / stk * 100) if stk else 0
            icon  = "✅" if clv > 0 else "🔴"
            print(f"  {r['market']:<22} {r['n']:>4}  "
                  f"{icon} {clv:>+7.3f}%  {roi_m:>+6.1f}%  {prof:>+10,.0f}")

    # ── By league ──────────────────────────────────────────────────────────
    if data["by_league"]:
        print(f"\n  {'League':<10} {'N':>4}  {'Avg CLV':>9}  {'ROI':>7}  {'Profit':>10}")
        print("  " + "─"*46)
        for r in data["by_league"]:
            clv  = r.get("avg_clv") or 0.0
            prof = r.get("profit")  or 0
            stk  = r.get("staked")  or 0
            roi_l = (prof / stk * 100) if stk else 0
            icon  = "✅" if clv > 0 else "🔴"
            print(f"  {r['league_code']:<10} {r['n']:>4}  "
                  f"{icon} {clv:>+7.3f}%  {roi_l:>+6.1f}%  {prof:>+10,.0f}")

    # ── Monthly trend ──────────────────────────────────────────────────────
    if data["trend"]:
        print(f"\n  Monthly CLV trend:")
        for r in data["trend"]:
            clv  = r.get("avg_clv") or 0.0
            prof = r.get("profit")  or 0
            icon = "▲" if clv > 0 else "▼"
            bar_len = min(16, max(1, int(abs(clv) * 3)))
            bar = ("█" * bar_len) if clv > 0 else ("░" * bar_len)
            print(f"    {r['month']}  {icon} {bar:<16}  {clv:>+.3f}%  "
                  f"n={r['n']}  {prof:>+,.0f} RWF")

    # ── Interpretation ─────────────────────────────────────────────────────
    print()
    if n < 30:
        verdict = f"⏳  Sample too small ({n}/30 minimum). Accumulate more data before conclusions."
    elif avg_clv > 2.0:
        verdict = "✅  Strong CLV signal. Model has genuine edge. Maintain discipline."
    elif avg_clv > 0:
        verdict = "✅  Positive CLV. Early evidence of edge. Keep tracking."
    elif avg_clv > -1.0:
        verdict = "⚠️   Near-zero CLV. Edge is marginal. Review weak markets and reduce stakes."
    else:
        verdict = "🔴  Negative CLV. Model has no real edge here. Halt betting and diagnose signal."

    print(f"  {verdict}")
    print("═"*62)
