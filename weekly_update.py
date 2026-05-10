#!/usr/bin/env python3
"""
weekly_update.py  —  Weekly Automation Script
──────────────────────────────────────────────
Run this every Monday morning to:
  1. Sync cross-competition fixtures (CL/EL/Cups) — 6 API requests
  2. Fetch opening odds for upcoming fixtures — ~10 requests per league
  3. Resolve settled bets from last week and update profit_rwf
  4. Populate CLV for all tracked bets using stored odds
  5. Print a CLV performance report across all leagues

HOW TO SCHEDULE ON WINDOWS (Task Scheduler):
─────────────────────────────────────────────
  1. Open Task Scheduler (search in Start menu)
  2. Click "Create Basic Task"
  3. Name it: "Football Predictor Weekly Update"
  4. Trigger: Weekly, Monday at 08:00
  5. Action: "Start a program"
  6. Program: C:/path/to/your/anaconda/python.exe
  7. Arguments: C:/path/to/your/scripts/weekly_update.py
  8. Start in: C:/path/to/your/scripts/
  9. Finish and enable the task

HOW TO SCHEDULE ON MAC/LINUX (cron):
──────────────────────────────────────
  Run in terminal: crontab -e
  Add this line (adjust paths):
  0 8 * * 1 /path/to/python /path/to/weekly_update.py >> /tmp/fp_weekly.log 2>&1

HOW TO RUN MANUALLY:
────────────────────
  python weekly_update.py
"""

import sys, time
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

try:
    import config
    from database import DB, _ensure_schema, setup_env
    from api_client import APIFootball
    from data_manager import DataManager
    from betting import resolve_bet, print_clv_report
except ImportError as e:
    print(f"\n❌  Cannot import modules: {e}")
    print(f"    Make sure weekly_update.py is in the same folder as the other scripts.")
    sys.exit(1)

LEAGUES = config.LEAGUES


def fmt_time(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:   return f"{seconds}s"
    if seconds < 3600: return f"{seconds//60}m {seconds%60}s"
    h = seconds // 3600; m = (seconds % 3600) // 60
    return f"{h}h {m}m"


# ── Task 1: Cross-competition fixtures ───────────────────────────────────────

def sync_cross_competition(league: dict, db: DB, dm: DataManager):
    """Sync CL/EL/Cup fixture calendar for this league. ~6 requests."""
    print(f"\n  [{league['code']}] Syncing cross-competition fixtures …")
    dm.sync_cross_competition_fixtures(force=True)


# ── Task 2: Opening odds ──────────────────────────────────────────────────────

def fetch_opening_odds(league: dict, db: DB, afl: APIFootball):
    """
    Fetch opening odds for upcoming fixtures and store in market_opening_odds.
    Covers 1X2, Over/Under 2.5, BTTS — the most commonly bet markets.
    ~1 request per fixture (odds endpoint).
    """
    print(f"\n  [{league['code']}] Fetching opening odds for upcoming fixtures …")

    upcoming = afl.fetch_next_fixtures(next_n=20)
    if not upcoming:
        print(f"    No upcoming fixtures found.")
        return 0

    saved = 0
    for m in upcoming:
        fix    = m.get("fixture", {})
        fid    = fix.get("id")
        status = fix.get("status", {}).get("short", "")
        if not fid or status in ("FT", "AET", "PEN"):
            continue

        raw = afl.fetch_prematch_odds(fid)
        if not raw:
            continue

        market_map = {
            "home_win" : None, "draw": None, "away_win": None,
            "over_2.5" : None, "under_2.5": None,
            "btts_yes" : None, "btts_no": None,
        }

        for v in raw.get("Match Winner", []):
            val = v.get("value", ""); odd = float(v.get("odd", 0) or 0)
            if odd <= 1.0: continue
            if val == "Home":   market_map["home_win"] = odd
            elif val == "Draw": market_map["draw"]     = odd
            elif val == "Away": market_map["away_win"] = odd

        for bet_name, values in raw.items():
            if "Over/Under" in bet_name:
                for v in values:
                    val = v.get("value", ""); odd = float(v.get("odd", 0) or 0)
                    if odd <= 1.0: continue
                    if val == "Over 2.5":    market_map["over_2.5"]  = odd
                    elif val == "Under 2.5": market_map["under_2.5"] = odd

        for v in raw.get("Both Teams Score", []):
            val = v.get("value", ""); odd = float(v.get("odd", 0) or 0)
            if odd <= 1.0: continue
            if val == "Yes":   market_map["btts_yes"] = odd
            elif val == "No":  market_map["btts_no"]  = odd

        for market, odds_val in market_map.items():
            if odds_val is None:
                continue
            db.execute(
                """INSERT INTO market_opening_odds (fixture_id, market, odds)
                   VALUES (%s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                     odds = IF(fetched_at < DATE_SUB(NOW(), INTERVAL 12 HOUR),
                               VALUES(odds), odds),
                     fetched_at = IF(fetched_at < DATE_SUB(NOW(), INTERVAL 12 HOUR),
                                     NOW(), fetched_at)""",
                (fid, market, odds_val)
            )
            saved += 1

        time.sleep(1.0)

    print(f"    {saved} odds entries stored for {league['code']}.")
    return saved


# ── Task 3: Resolve settled bets ─────────────────────────────────────────────

def resolve_settled_bets(league: dict, db: DB) -> int:
    """
    Find all tracked bets from this league where:
      - result is still NULL (unresolved)
      - match_date is in the past (match has kicked off)
      - the final score exists in matches_basic

    Calls resolve_bet() for each, which evaluates all markets for that
    fixture in one pass and writes result + profit_rwf to bet_tracker.

    Returns the count of individual bet rows resolved.
    """
    print(f"\n  [{league['code']}] Resolving settled bets …")

    # Find distinct match_ids needing resolution where the score is available
    pending = db.fetchall(
        """SELECT DISTINCT bt.match_id,
                  mb.home_goals, mb.away_goals,
                  bt.home_team, bt.away_team,
                  bt.match_date
           FROM bet_tracker bt
           JOIN matches_basic mb ON mb.match_id = bt.match_id
           WHERE bt.result IS NULL
             AND bt.match_date < NOW()
             AND mb.home_goals IS NOT NULL
             AND mb.away_goals IS NOT NULL
             AND bt.league_code = %s
           ORDER BY bt.match_date ASC""",
        (league["code"],)
    )

    if not pending:
        print(f"    No settled bets to resolve.")
        return 0

    total_resolved = 0
    for row in pending:
        mid  = row["match_id"]
        hg   = row["home_goals"]
        ag   = row["away_goals"]
        date = str(row.get("match_date", "?"))[:10]
        home = row.get("home_team", "?")
        away = row.get("away_team", "?")

        n = resolve_bet(db, match_id=mid, home_goals=hg, away_goals=ag)
        total_resolved += n

        if n > 0:
            # Show the outcome for each resolved market
            resolved_rows = db.fetchall(
                """SELECT market, result, odds_at_prediction,
                          stake_rwf, profit_rwf
                   FROM bet_tracker
                   WHERE match_id = %s AND result IS NOT NULL""",
                (mid,)
            )
            print(f"\n    {date}  {home} {hg}-{ag} {away}")
            for r in resolved_rows:
                icon   = "✅" if r["result"] == "won" else ("↩" if r["result"] == "void" else "❌")
                profit = r.get("profit_rwf") or 0
                print(f"      {icon}  {r['market']:<22}  @{r['odds_at_prediction']:.2f}  "
                      f"stake {r['stake_rwf']:,.0f}  →  {profit:+,.0f} RWF")

    print(f"\n    {total_resolved} bet(s) resolved for {league['code']}.")
    return total_resolved


# ── Task 4: Populate CLV ──────────────────────────────────────────────────────

def backfill_clv_from_odds(league: dict, db: DB) -> int:
    """
    Populate closing_odds and clv_pct for ALL bet_tracker rows where:
      - closing_odds is still NULL
      - market_opening_odds has an entry for that fixture + market

    This is intentionally NOT gated on match timing (unlike update_clv_from_db
    which requires match to be within 2 hours of kickoff). Running on Monday
    morning we need to catch Saturday/Sunday matches that have already settled.

    The odds stored in market_opening_odds are the best proxy for closing line
    available without a live feed. They were fetched on Monday morning of the
    match week — typically 5-7 days before kickoff — so they represent the
    opening/early line rather than the true closing line.

    CLV interpretation note:
    ─────────────────────────
    True CLV = (our_odds / closing_odds - 1) × 100
    Here we approximate closing_odds with the stored market_opening_odds value.
    This UNDERSTATES true CLV if the market moved in our favour after we bet,
    and OVERSTATES it if the market shortened. Until a live odds feed is
    integrated, this is the best measurable signal available.

    Returns the count of CLV rows populated.
    """
    print(f"\n  [{league['code']}] Populating CLV from stored odds …")

    pending = db.fetchall(
        """SELECT bt.id, bt.match_id, bt.market,
                  bt.odds_at_prediction,
                  mo.odds AS stored_odds
           FROM bet_tracker bt
           JOIN market_opening_odds mo
             ON mo.fixture_id = bt.match_id AND mo.market = bt.market
           WHERE bt.closing_odds IS NULL
             AND bt.league_code = %s""",
        (league["code"],)
    )

    if not pending:
        print(f"    No CLV entries to populate.")
        return 0

    updated = 0
    for row in pending:
        our_odds  = row["odds_at_prediction"]
        stored    = row["stored_odds"]
        if not stored or stored <= 1.0:
            continue

        clv_pct = round((our_odds / stored - 1) * 100, 4)

        db.execute(
            """UPDATE bet_tracker
               SET closing_odds = %s,
                   clv_pct      = %s,
                   closed_at    = NOW()
               WHERE id = %s""",
            (stored, clv_pct, row["id"])
        )
        updated += 1

    print(f"    {updated} CLV value(s) populated for {league['code']}.")
    return updated


# ── Task 5: CLV report ────────────────────────────────────────────────────────

def print_combined_clv_report(league_dbs: list):
    """
    Print a combined CLV report aggregated across all leagues.

    Because each league has its own DB, we union the bet_tracker rows
    by reading each DB individually and combining in Python.
    """
    print(f"\n{'═'*62}")
    print("  CLOSING LINE VALUE  (CLV)  REPORT  —  ALL LEAGUES")
    print(f"{'═'*62}")

    all_rows = []
    for (league, db) in league_dbs:
        rows = db.fetchall(
            """SELECT league_code, market, match_date,
                      clv_pct, odds_at_prediction, closing_odds,
                      result, profit_rwf, stake_rwf
               FROM bet_tracker
               WHERE clv_pct IS NOT NULL"""
        )
        all_rows.extend(rows or [])

    if not all_rows:
        print("\n  No CLV data yet across any league.")
        print("  CLV is populated each Monday for last week's tracked bets.")
        print(f"{'═'*62}")
        return

    n          = len(all_rows)
    avg_clv    = sum(r["clv_pct"] for r in all_rows) / n
    clv_pos    = sum(1 for r in all_rows if r["clv_pct"] > 0)
    total_prof = sum((r["profit_rwf"] or 0) for r in all_rows)
    total_stk  = sum((r["stake_rwf"] or 0) for r in all_rows)
    wins       = sum(1 for r in all_rows if r.get("result") == "won")
    losses     = sum(1 for r in all_rows if r.get("result") == "lost")
    roi        = (total_prof / total_stk * 100) if total_stk else 0
    clv_icon   = "✅" if avg_clv > 0 else "🔴"

    print(f"\n  Bets tracked          : {n}")
    print(f"  Avg CLV               : {clv_icon}  {avg_clv:+.3f}%")
    print(f"  CLV positive rate     : {clv_pos}/{n}  ({clv_pos/n*100:.0f}%)")
    print(f"  ROI                   : {roi:+.1f}%  ({total_prof:+,.0f} RWF on {total_stk:,.0f} staked)")
    print(f"  Record                : {wins}W / {losses}L")

    # ── By league ─────────────────────────────────────────────────────────────
    league_agg: dict = {}
    for r in all_rows:
        lc = r.get("league_code", "?")
        if lc not in league_agg:
            league_agg[lc] = {"n": 0, "clv_sum": 0.0, "profit": 0.0, "staked": 0.0}
        league_agg[lc]["n"]       += 1
        league_agg[lc]["clv_sum"] += r["clv_pct"]
        league_agg[lc]["profit"]  += r.get("profit_rwf") or 0
        league_agg[lc]["staked"]  += r.get("stake_rwf")  or 0

    if league_agg:
        print(f"\n  {'League':<10} {'N':>4}  {'Avg CLV':>9}  {'ROI':>7}  {'Profit':>10}")
        print("  " + "─"*46)
        for lc, agg in sorted(league_agg.items(),
                               key=lambda x: x[1]["clv_sum"]/max(x[1]["n"],1),
                               reverse=True):
            avg  = agg["clv_sum"] / max(agg["n"], 1)
            roi_ = (agg["profit"] / agg["staked"] * 100) if agg["staked"] else 0
            icon = "✅" if avg > 0 else "🔴"
            print(f"  {lc:<10} {agg['n']:>4}  "
                  f"{icon} {avg:>+7.3f}%  {roi_:>+6.1f}%  {agg['profit']:>+10,.0f}")

    # ── By market ──────────────────────────────────────────────────────────────
    market_agg: dict = {}
    for r in all_rows:
        mk = r.get("market", "?")
        if mk not in market_agg:
            market_agg[mk] = {"n": 0, "clv_sum": 0.0, "profit": 0.0, "staked": 0.0}
        market_agg[mk]["n"]       += 1
        market_agg[mk]["clv_sum"] += r["clv_pct"]
        market_agg[mk]["profit"]  += r.get("profit_rwf") or 0
        market_agg[mk]["staked"]  += r.get("stake_rwf")  or 0

    if market_agg:
        print(f"\n  {'Market':<22} {'N':>4}  {'Avg CLV':>9}  {'ROI':>7}  {'Profit':>10}")
        print("  " + "─"*58)
        for mk, agg in sorted(market_agg.items(),
                               key=lambda x: x[1]["clv_sum"]/max(x[1]["n"],1),
                               reverse=True):
            avg  = agg["clv_sum"] / max(agg["n"], 1)
            roi_ = (agg["profit"] / agg["staked"] * 100) if agg["staked"] else 0
            icon = "✅" if avg > 0 else "🔴"
            print(f"  {mk:<22} {agg['n']:>4}  "
                  f"{icon} {avg:>+7.3f}%  {roi_:>+6.1f}%  {agg['profit']:>+10,.0f}")

    # ── Verdict ────────────────────────────────────────────────────────────────
    print()
    if n < 30:
        verdict = f"⏳  Sample too small ({n}/30 minimum). Keep tracking."
    elif avg_clv > 2.0:
        verdict = "✅  Strong CLV. Genuine edge confirmed. Maintain discipline."
    elif avg_clv > 0:
        verdict = "✅  Positive CLV. Early edge signal. Keep accumulating data."
    elif avg_clv > -1.0:
        verdict = "⚠️   Near-zero CLV. Marginal edge. Review weakest markets."
    else:
        verdict = "🔴  Negative CLV. No real edge. Halt betting and diagnose."

    print(f"  {verdict}")
    print(f"{'═'*62}\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║         FOOTBALL PREDICTOR — WEEKLY UPDATE                  ║")
    print(f"║         {datetime.now().strftime('%A %d %B %Y  %H:%M'):<52}║")
    print("╚══════════════════════════════════════════════════════════════╝")

    setup_env()

    start          = time.time()
    total_requests = 0
    open_dbs       = []   # (league, db) pairs kept open for combined CLV report

    for key, league in LEAGUES.items():
        print(f"\n{'─'*60}")
        print(f"  {league['name']}")
        print(f"{'─'*60}")

        try:
            db = DB(db_name=league["db"])
            _ensure_schema(db)
            config.AFL_LEAGUE_ID = league["id"]
            config.AFL_SEASONS   = league["seasons"]
            afl = APIFootball(db)
            dm  = DataManager(db, afl)
        except Exception as e:
            print(f"  ❌  Could not connect to {league['db']}: {e}")
            continue

        # ── Task 1: Cross-competition fixtures ────────────────────────────────
        try:
            sync_cross_competition(league, db, dm)
            total_requests += 6
        except Exception as e:
            print(f"  ⚠  Cross-competition sync failed: {e}")

        # ── Task 2: Opening odds for upcoming fixtures ─────────────────────────
        try:
            saved = fetch_opening_odds(league, db, afl)
            total_requests += max(saved // 7, 1)
        except Exception as e:
            print(f"  ⚠  Opening odds fetch failed: {e}")

        # ── Task 3: Resolve settled bets ──────────────────────────────────────
        # Matches in matches_basic with a score are used directly.
        # No extra API requests — scores were already synced by startup_sync.
        try:
            resolve_settled_bets(league, db)
        except Exception as e:
            print(f"  ⚠  Bet resolution failed: {e}")

        # ── Task 4: Populate CLV ───────────────────────────────────────────────
        # Uses stored market_opening_odds as closing line proxy.
        # Covers all unresolved CLV rows regardless of match timing.
        try:
            backfill_clv_from_odds(league, db)
        except Exception as e:
            print(f"  ⚠  CLV population failed: {e}")

        # Keep DB open for the combined report — will be closed below
        open_dbs.append((league, db))

    # ── Task 5: Combined CLV report ───────────────────────────────────────────
    try:
        print_combined_clv_report(open_dbs)
    except Exception as e:
        print(f"  ⚠  CLV report failed: {e}")

    # Close all DBs
    for _, db in open_dbs:
        try:
            db.close()
        except Exception:
            pass

    elapsed = time.time() - start
    print(f"{'═'*60}")
    print(f"  Weekly update complete in {fmt_time(elapsed)}")
    print(f"  Approximate API requests used: ~{total_requests}")
    print(f"  Next run: next Monday at the same time")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    main()
