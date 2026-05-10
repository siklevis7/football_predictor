#!/usr/bin/env python3
"""
backfill_all.py  —  Data Enrichment Tool
"""
import os, sys, time
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

try:
    import config
    from database import DB, _ensure_schema, setup_env
    from api_client import APIFootball
    from data_manager import DataManager
except ImportError as e:
    print(f"\n❌  Cannot import modules: {e}")
    print(f"    Make sure backfill_all.py is in the same folder as:")
    print(f"    config.py, database.py, api_client.py, data_manager.py")
    sys.exit(1)

LEAGUES = config.LEAGUES


def fmt_time(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:   return f"{seconds}s"
    if seconds < 3600: return f"{seconds//60}m {seconds%60}s"
    h = seconds // 3600; m = (seconds % 3600) // 60
    return f"{h}h {m}m"


def connect_league(league: dict) -> tuple:
    db = DB(db_name=league["db"])
    _ensure_schema(db)
    config.AFL_LEAGUE_ID = league["id"]
    config.AFL_SEASONS   = league["seasons"]
    afl = APIFootball(db)
    dm  = DataManager(db, afl)
    return db, afl, dm


def pick_league() -> dict | None:
    print()
    for key, lg in LEAGUES.items():
        print(f"    {key}.  {lg['name']}")
    print()
    choice = input(f"  Choose league (1-{len(LEAGUES)}): ").strip()
    if choice not in LEAGUES:
        print("  Invalid choice.")
        return None
    return LEAGUES[choice]


def check_pending(league: dict) -> tuple:
    """
    Returns (total, enriched, skipped, truly_pending).

    - total         : all rows in matches_basic
    - enriched      : rows that have a matches_stats entry
    - skipped       : rows in backfill_progress that are NOT yet enriched
                      (permanently failed — won't be attempted again)
    - truly_pending : total - enriched - skipped  (actually actionable)
    """
    try:
        db = DB(db_name=league["db"])
        t = db.fetchone("SELECT COUNT(*) as n FROM matches_basic")
        e = db.fetchone("SELECT COUNT(DISTINCT match_id) as n FROM matches_stats")

        # Skipped = in backfill_progress but still not enriched
        s = db.fetchone(
            """SELECT COUNT(*) as n
               FROM backfill_progress p
               LEFT JOIN matches_stats s ON s.match_id = p.match_id
               WHERE s.match_id IS NULL"""
        )

        n_t = t["n"] if t else 0
        n_e = e["n"] if e else 0
        n_s = s["n"] if s else 0
        n_p = max(0, n_t - n_e - n_s)   # truly actionable pending
        db.close()
        return n_t, n_e, n_s, n_p
    except Exception:
        return 0, 0, 0, 0


# ── Option 1: Full league enrichment ─────────────────────────────────────────

def run_full_league_backfill():
    print("\n  ── Full League Enrichment ───────────────────────────────────────")
    print("  Enriches every historical match missing xG/stats data.")
    print("  Already-enriched matches are SKIPPED — no duplicate API requests.")
    print()
    print("  Available leagues:")
    for key, lg in LEAGUES.items():
        n_t, n_e, n_s, n_p = check_pending(lg)
        done_pct = n_e / max(n_t, 1) * 100
        bar = "█" * int(done_pct/5) + "░" * (20 - int(done_pct/5))
        skip_tag = f"  ⚠ {n_s} skipped" if n_s else ""
        print(f"    {key}.  {lg['name']:<36} {bar} {done_pct:.0f}%  "
              f"({n_p:,} pending{skip_tag})")

    print()
    print("  Enter league numbers separated by spaces (e.g. 1 3), or Enter for ALL:")
    choice = input("  Choice: ").strip()

    if not choice:
        selected = list(LEAGUES.values())
    else:
        selected = []
        for k in choice.split():
            if k in LEAGUES:
                selected.append(LEAGUES[k])
            else:
                print(f"  ⚠  Unknown key '{k}' — skipped")

    if not selected:
        print("  Nothing selected.")
        return

    # Use truly_pending (index 3) — not the old total-enriched figure
    grand_pending = sum(check_pending(lg)[3] for lg in selected)
    grand_skipped = sum(check_pending(lg)[2] for lg in selected)

    if grand_pending == 0:
        if grand_skipped > 0:
            print(f"\n  ⚠️   No truly pending matches, but {grand_skipped} matches were "
                  f"permanently skipped due to API failures.")
            print(f"      Use Option 7 (Clear skipped / retry failed) to attempt them again.")
        else:
            print("\n  ✅  Everything is already enriched. Nothing to do.")
        return

    est = grand_pending * 3 * 1.0
    print(f"\n  Will enrich: {', '.join(lg['code'] for lg in selected)}")
    print(f"  Pending matches : {grand_pending:,}  (~{grand_pending*3:,} API requests)")
    if grand_skipped:
        print(f"  Skipped matches : {grand_skipped:,}  (use Option 7 to retry)")
    print(f"  Estimated time  : ~{fmt_time(est)}")

    if input("\n  Start? (y/n): ").strip().lower() != "y":
        print("  Aborted.")
        return

    overall_start = time.time()
    for league in selected:
        _enrich_league(league)
    print(f"\n  ✅  All done in {fmt_time(time.time() - overall_start)}")


def _enrich_league(league: dict, batch_size: int = 200):
    print(f"\n{'═'*62}")
    print(f"  {league['name']}  (db: {league['db']})")
    print(f"{'═'*62}")
    try:
        db, afl, dm = connect_league(league)
    except Exception as e:
        print(f"  ❌  Could not connect: {e}"); return

    total_row    = db.fetchone("SELECT COUNT(*) as n FROM matches_basic")
    enriched_row = db.fetchone("SELECT COUNT(DISTINCT match_id) as n FROM matches_stats")
    skipped_row  = db.fetchone(
        """SELECT COUNT(*) as n
           FROM backfill_progress p
           LEFT JOIN matches_stats s ON s.match_id = p.match_id
           WHERE s.match_id IS NULL"""
    )

    total    = total_row["n"]    if total_row    else 0
    enriched = enriched_row["n"] if enriched_row else 0
    skipped  = skipped_row["n"]  if skipped_row  else 0
    pending  = max(0, total - enriched - skipped)

    if pending == 0:
        if skipped > 0:
            pct = enriched / max(total, 1) * 100
            print(f"  ⚠️   No pending matches, but {skipped} are permanently skipped.")
            print(f"       Enrichment: {enriched:,}/{total:,} ({pct:.1f}%)")
            print(f"       Use Option 7 (Clear skipped / retry failed) to attempt them again.")
        else:
            print(f"  ✅  All {total:,} matches already enriched.")
        db.close(); return

    print(f"  Total: {total:,}  |  Done: {enriched:,}  |  Skipped: {skipped:,}  |  Pending: {pending:,}")
    done = 0; start_time = time.time()

    while True:
        batch = db.fetchall(
            """SELECT b.match_id, b.match_date, b.home_team_name, b.away_team_name
               FROM matches_basic b
               LEFT JOIN matches_stats s    ON b.match_id = s.match_id
               LEFT JOIN backfill_progress p ON b.match_id = p.match_id
               WHERE s.match_id IS NULL
                 AND p.match_id IS NULL
               ORDER BY b.match_date ASC LIMIT %s""",
            (batch_size,)
        )
        if not batch:
            break

        for row in batch:
            done += 1
            elapsed   = time.time() - start_time
            rate      = done / elapsed if elapsed > 0 else 0.01
            remaining = (pending - done) / rate if rate > 0 else 0
            pct       = (enriched + done) / total * 100
            h = row["home_team_name"][:18]; a = row["away_team_name"][:18]
            print(
                f"  [{done:>5}/{pending}]  {str(row['match_date'])[:10]}"
                f"  {h:<18} vs {a:<18}  {pct:.1f}%  ETA {fmt_time(remaining)}",
                end="\r", flush=True
            )
            try:
                dm._enrich_match(row["match_id"])
            except KeyboardInterrupt:
                print(f"\n\n  ⏸  Stopped at {done}/{pending}. Run again to continue.")
                db.close(); sys.exit(0)
            except Exception as ex:
                print(f"\n  ⚠  match {row['match_id']} failed: {ex}")
            time.sleep(1.0)

    print()

    # Final accurate summary
    final_enriched = db.fetchone("SELECT COUNT(DISTINCT match_id) as n FROM matches_stats")
    final_skipped  = db.fetchone(
        """SELECT COUNT(*) as n
           FROM backfill_progress p
           LEFT JOIN matches_stats s ON s.match_id = p.match_id
           WHERE s.match_id IS NULL"""
    )
    n_enriched = final_enriched["n"] if final_enriched else 0
    n_skipped  = final_skipped["n"]  if final_skipped  else 0
    pct        = n_enriched / max(total, 1) * 100

    print(f"\n  ✅  Run complete.")
    print(f"     Enriched : {n_enriched:,} / {total:,}  ({pct:.1f}%)")
    print(f"     Skipped  : {n_skipped:,}  (API failures logged in backfill_progress)")
    if n_skipped > 0:
        print(f"     ⚠️  True enrichment rate is {pct:.1f}%, NOT 100%.")
        print(f"        Use Option 7 to retry skipped matches after investigating the cause.")
    print(f"     Time     : {fmt_time(time.time() - start_time)}")
    db.close()


# ── Option 2: Single fixture enrichment ──────────────────────────────────────

def run_single_fixture_backfill():
    print("\n  ── Single Fixture Enrichment ────────────────────────────────────")
    print("  Enrich one specific match by fixture ID.")
    print("  You can find the fixture ID in the predictor output, e.g.:")
    print("    'found  (ID:1387929  2026-04-22 17:00  Parc des Princes)'")

    league = pick_league()
    if not league:
        return

    try:
        db, afl, dm = connect_league(league)
    except Exception as e:
        print(f"  ❌  Could not connect: {e}"); return

    fixture_input = input(
        "\n  Enter fixture ID, or 'search' to find by team name: "
    ).strip().lower()

    if fixture_input == "search":
        team_name = input("  Team name (partial OK): ").strip()
        rows = db.fetchall(
            """SELECT b.match_id, b.match_date, b.home_team_name, b.away_team_name,
                      b.season,
                      (SELECT COUNT(*) FROM matches_stats s
                       WHERE s.match_id = b.match_id) as enriched
               FROM matches_basic b
               WHERE home_team_name LIKE %s OR away_team_name LIKE %s
               ORDER BY match_date DESC LIMIT 20""",
            (f"%{team_name}%", f"%{team_name}%")
        )
        if not rows:
            print(f"  No matches found for '{team_name}'")
            db.close(); return

        print(f"\n  {'ID':<12} {'Date':<12} {'Home':<24} {'Away':<24} {'xG?'}")
        print("  " + "─"*80)
        for r in rows:
            status = "✅ yes" if r["enriched"] else "⬜ no "
            print(f"  {r['match_id']:<12} {str(r['match_date'])[:10]:<12} "
                  f"{r['home_team_name'][:24]:<24} {r['away_team_name'][:24]:<24} {status}")
        print()
        fixture_input = input("  Fixture ID to enrich (Enter to cancel): ").strip()
        if not fixture_input:
            db.close(); return

    try:
        fixture_id = int(fixture_input)
    except ValueError:
        print("  Invalid fixture ID."); db.close(); return

    row = db.fetchone(
        "SELECT home_team_name, away_team_name, match_date "
        "FROM matches_basic WHERE match_id=%s", (fixture_id,)
    )
    if not row:
        print(f"\n  ❌  Fixture {fixture_id} not found in {league['code']} database.")
        print(f"     Try option 4 (re-sync) first if you believe this match should exist.")
        db.close(); return

    already = db.fetchone(
        "SELECT match_id FROM matches_stats WHERE match_id=%s", (fixture_id,)
    )
    print(f"\n  Match  : {row['home_team_name']} vs {row['away_team_name']}")
    print(f"  Date   : {str(row['match_date'])[:10]}")
    print(f"  Status : {'Already enriched ✅' if already else 'Not yet enriched ⬜'}")

    if already:
        if input("  Already enriched. Force re-enrich anyway? (y/n): ").strip().lower() != "y":
            db.close(); return

    print(f"\n  Enriching … (3 API requests)")
    start = time.time()
    try:
        dm._enrich_match(fixture_id, skip_if_exists=False)
        print(f"  ✅  Done in {fmt_time(time.time() - start)}")
    except Exception as e:
        print(f"  ❌  Failed: {e}")
    db.close()


# ── Option 3: Status check ────────────────────────────────────────────────────

def run_status_check():
    print("\n  ── Database Status  (no API requests) ───────────────────────────")
    for lg in LEAGUES.values():
        print(f"\n  {lg['name']}")
        try:
            db = DB(db_name=lg["db"])
            config.AFL_LEAGUE_ID = lg["id"]
            rows = db.fetchall(
                """SELECT b.season, COUNT(*) as total,
                          COUNT(DISTINCT s.match_id) as enriched
                   FROM matches_basic b
                   LEFT JOIN matches_stats s ON b.match_id = s.match_id
                   GROUP BY b.season ORDER BY b.season"""
            )
            if not rows:
                print("    (no data yet)"); db.close(); continue

            # Skipped count (league-wide, not per season — backfill_progress has no season col)
            skipped_row = db.fetchone(
                """SELECT COUNT(*) as n
                   FROM backfill_progress p
                   LEFT JOIN matches_stats s ON s.match_id = p.match_id
                   WHERE s.match_id IS NULL"""
            )
            total_skipped = skipped_row["n"] if skipped_row else 0

            total_t = 0; total_e = 0
            for r in rows:
                pct = r["enriched"] / max(r["total"], 1) * 100
                bar = "█" * int(pct/5) + "░" * (20 - int(pct/5))
                total_t += r["total"]; total_e += r["enriched"]
                print(f"    {r['season']}:  {bar} {pct:>5.1f}%  "
                      f"({r['enriched']:>3}/{r['total']:>3})")

            overall      = total_e / max(total_t, 1) * 100
            truly_pending = max(0, total_t - total_e - total_skipped)
            print(f"    {'TOTAL':<6}  overall {overall:.1f}%  "
                  f"({total_e:,}/{total_t:,})")
            if total_skipped:
                print(f"    ⚠️   {total_skipped:,} permanently skipped (API failures)  "
                      f"|  {truly_pending:,} truly pending")
                print(f"        Use Option 7 to retry skipped matches.")
            else:
                pending = total_t - total_e
                print(f"           {pending:,} pending  (~{pending*3:,} API requests to complete)")
            db.close()
        except Exception as e:
            print(f"    Could not connect: {e}")


# ── Option 4: Re-sync missing seasons ────────────────────────────────────────

def run_resync_seasons():
    print("\n  ── Re-sync Season Match List ────────────────────────────────────")
    print("  Use this if a team has fewer matches than expected.")
    print("  Common cause: API used a different team name in older seasons.")
    print("  Example: 'Bayern Munich' in 2022 vs 'FC Bayern München' in 2015.")
    print("  This ONLY fetches the match list (1 request/season) — NOT stats.")
    print()

    league = pick_league()
    if not league:
        return

    print(f"\n  Available seasons: {league['seasons']}")
    print("  Enter years to re-sync (e.g. 2015 2016 2017), or 'all':")
    season_input = input("  Seasons: ").strip()

    if season_input.lower() == "all":
        seasons = league["seasons"]
    else:
        try:
            seasons = [int(s) for s in season_input.split()]
        except ValueError:
            print("  Invalid input."); return

    if not seasons:
        print("  No seasons selected."); return

    try:
        db, afl, dm = connect_league(league)
    except Exception as e:
        print(f"  ❌  Could not connect: {e}"); return

    print(f"\n  Will re-sync: {seasons}")
    print(f"  API requests: {len(seasons)} (very cheap)")
    if input("  Proceed? (y/n): ").strip().lower() != "y":
        db.close(); return

    dm._sync_basic_matches(force_seasons=seasons)
    db.close()
    print(f"\n  ✅  Re-sync complete.")
    print(f"     Run option 3 to check match counts, then option 1 to enrich new matches.")


# ── Option 5: Player stats enrichment ────────────────────────────────────────

def run_player_stats_enrichment():
    """
    Fetches per-player xG, shots, goals, assists for every match.
    Uses the /fixtures/players endpoint — 1 extra request per match.
    Already-enriched matches are skipped automatically.

    This enables real goalscorer market pricing instead of position estimates.
    A full Premier League season (380 matches) costs 380 API requests.
    """
    print("\n  ── Player Stats Enrichment ──────────────────────────────────────")
    print("  Fetches per-player xG, shots, goals for each match.")
    print("  1 API request per match. Already-done matches are skipped.")
    print()
    print("  Available leagues:")
    for key, lg in LEAGUES.items():
        try:
            db = DB(db_name=lg["db"])
            config.AFL_LEAGUE_ID = lg["id"]
            t = db.fetchone("SELECT COUNT(DISTINCT match_id) as n FROM matches_stats")
            c = db.fetchone("SELECT COUNT(DISTINCT match_id) as n FROM match_player_stats")
            n_t = t["n"] if t else 0
            n_c = c["n"] if c else 0
            pct = n_c / max(n_t, 1) * 100
            bar = "█" * int(pct/5) + "░" * (20 - int(pct/5))
            print(f"    {key}.  {lg['name']:<36} {bar} {pct:.0f}%  ({n_t-n_c:,} pending)")
            db.close()
        except Exception:
            print(f"    {key}.  {lg['name']:<36} (could not connect)")

    print()
    print("  Enter league numbers (e.g. 1 2), or Enter for ALL:")
    choice = input("  Choice: ").strip()

    if not choice:
        selected = list(LEAGUES.values())
    else:
        selected = []
        for k in choice.split():
            if k in LEAGUES: selected.append(LEAGUES[k])

    if not selected:
        print("  Nothing selected.")
        return

    for league in selected:
        _enrich_player_stats_league(league)


def _enrich_player_stats_league(league: dict, batch_size: int = 200):
    print(f"\n{'═'*62}")
    print(f"  {league['name']}  — Player Stats")
    print(f"{'═'*62}")
    try:
        db, afl, dm = connect_league(league)
    except Exception as e:
        print(f"  ❌  Could not connect: {e}"); return

    # Matches that have team stats but NOT player stats yet
    pending_rows = db.fetchall(
        """SELECT DISTINCT s.match_id
           FROM matches_stats s
           LEFT JOIN match_player_stats p ON s.match_id = p.match_id
           WHERE p.match_id IS NULL
           ORDER BY s.match_id DESC"""
    )

    total   = len(pending_rows)
    if total == 0:
        print("  ✅  Player stats already complete for this league.")
        db.close(); return

    print(f"  Pending: {total:,} matches  (~{total:,} API requests)")
    if input("  Start? (y/n): ").strip().lower() != "y":
        db.close(); return

    done = 0; failed = 0; start = time.time()
    for row in pending_rows:
        done += 1
        elapsed   = time.time() - start
        rate      = done / elapsed if elapsed > 0 else 0.01
        remaining = (total - done) / rate if rate > 0 else 0
        pct       = done / total * 100
        print(
            f"  [{done:>5}/{total}]  {pct:.1f}%  ETA {fmt_time(remaining)}",
            end="\r", flush=True
        )
        try:
            ok = dm.enrich_match_player_stats(row["match_id"])
            if not ok: failed += 1
        except KeyboardInterrupt:
            print(f"\n\n  ⏸  Stopped. Run again to continue.")
            db.close(); sys.exit(0)
        except Exception as ex:
            print(f"\n  ⚠  match {row['match_id']}: {ex}")
            failed += 1
        time.sleep(1.0)

    print()
    coverage = dm.count_player_stats_coverage()
    print(f"\n  ✅  Done. Player stats: {coverage['player_stats']:,}/{coverage['total_enriched']:,}"
          f"  ({coverage['pct']}%)")
    if failed:
        print(f"  ⚠  {failed} matches had no player data in the API")
    db.close()


# ── Option 6: Fix duplicate team names ───────────────────────────────────────

def run_fix_team_names():
    """
    One-time fix: rename variant team names in the DB to their canonical form.
    Applies TEAM_CANONICAL from config.py to all rows in matches_basic,
    matches_stats, cross_competition_fixtures, head_to_head, and injuries.

    Safe to run multiple times — changes only rows that need updating.
    Run this once after updating config.TEAM_CANONICAL.
    """
    print("\n  ── Fix Duplicate Team Names ─────────────────────────────────────")
    print("  Renames API variant names to canonical names in your databases.")
    print("  Example: 'FC Bayern München' and 'Bayern Munich' → 'Bayern Munich'")
    print("  This is a ONE-TIME fix for historical data already in the DB.")
    print("  After this, new data is normalised automatically at import time.")
    print()

    import config
    canonical_map = config.TEAM_CANONICAL  # lowercase variant → canonical

    league = pick_league()
    if not league:
        return

    try:
        db, afl, dm = connect_league(league)
    except Exception as e:
        print(f"  ❌  Could not connect: {e}"); return

    # Find all distinct team names in this DB
    home_names = db.fetchall("SELECT DISTINCT home_team_name as name FROM matches_basic")
    away_names = db.fetchall("SELECT DISTINCT away_team_name as name FROM matches_basic")
    all_names  = set(r["name"] for r in (home_names or []) + (away_names or []) if r.get("name"))

    # Identify which names need renaming
    to_fix = {}
    for name in all_names:
        canonical = canonical_map.get(name.lower().strip())
        if canonical and canonical != name:
            to_fix[name] = canonical

    if not to_fix:
        print("  ✅  No duplicate team names found — database is already clean.")
        db.close(); return

    print(f"  Found {len(to_fix)} team name(s) to fix:\n")
    for old, new in sorted(to_fix.items()):
        print(f"    '{old}'  →  '{new}'")

    print()
    confirm = input("  Apply these fixes? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("  Cancelled.")
        db.close(); return

    total_updated = 0
    for old_name, canonical in to_fix.items():
        tables_and_cols = [
            ("matches_basic",              ["home_team_name", "away_team_name"]),
            ("matches_stats",              ["team_name"]),
            ("matches_lineups",            ["team_name"]),
            ("match_events",               ["team_name"]),
            ("head_to_head",               ["team_a", "team_b", "home_team", "away_team"]),
            ("injuries",                   ["team_name"]),
            ("cross_competition_fixtures", ["team_name", "opponent"]),
            ("match_player_stats",         ["team_name"]),
        ]
        for table, cols in tables_and_cols:
            for col in cols:
                try:
                    db.execute(
                        f"UPDATE {table} SET {col}=%s WHERE {col}=%s",
                        (canonical, old_name)
                    )
                    total_updated += 1
                except Exception:
                    pass   # table/column may not exist in all DBs

    print(f"\n  ✅  Done. {total_updated} UPDATE statements applied.")
    print(f"     Run Option 3 (Status check) to verify match counts improved.")

    # Show the updated name list
    home_after = db.fetchall("SELECT DISTINCT home_team_name as n FROM matches_basic ORDER BY 1")
    print(f"\n  Teams now in DB ({len(home_after)}):")
    for r in (home_after or []):
        print(f"    {r['n']}")
    db.close()


# ── Option 7: Clear skipped matches and retry ─────────────────────────────────

def run_clear_skipped():
    """
    Clears backfill_progress entries for a league so failed matches are retried.

    Use this after:
      - Fixing a team name inconsistency (e.g. Bayern Munich API rename)
      - Investigating why matches were failing (rate limits, bad fixture IDs)
      - A re-sync that added new match IDs that previously didn't exist

    Safe to run multiple times. Only clears unenriched failures — already
    enriched matches that happen to be in backfill_progress are left alone.
    """
    print("\n  ── Clear Skipped Matches (Retry Failed) ─────────────────────────")
    print("  Removes permanently-skipped matches from the failure log.")
    print("  On the next Option 1 run, those matches will be attempted again.")
    print()

    league = pick_league()
    if not league:
        return

    try:
        db = DB(db_name=league["db"])
    except Exception as e:
        print(f"  ❌  Could not connect: {e}"); return

    # Count how many skipped entries exist for unenriched matches
    skipped_row = db.fetchone(
        """SELECT COUNT(*) as n
           FROM backfill_progress p
           LEFT JOIN matches_stats s ON s.match_id = p.match_id
           WHERE s.match_id IS NULL"""
    )
    n_skipped = skipped_row["n"] if skipped_row else 0

    if n_skipped == 0:
        print("  ✅  No skipped matches found for this league. Nothing to clear.")
        db.close(); return

    # Show a sample of what will be retried
    sample = db.fetchall(
        """SELECT p.match_id, b.match_date, b.home_team_name, b.away_team_name
           FROM backfill_progress p
           LEFT JOIN matches_stats s  ON s.match_id = p.match_id
           LEFT JOIN matches_basic b  ON b.match_id = p.match_id
           WHERE s.match_id IS NULL
           ORDER BY b.match_date ASC
           LIMIT 10"""
    )

    print(f"  Found {n_skipped} skipped match(es) for {league['name']}.")
    print(f"  Sample (up to 10):\n")
    print(f"  {'ID':<12} {'Date':<12} {'Home':<24} {'Away'}")
    print("  " + "─"*70)
    for r in (sample or []):
        h = (r.get("home_team_name") or "?")[:24]
        a = (r.get("away_team_name") or "?")[:24]
        d = str(r.get("match_date") or "?")[:10]
        print(f"  {r['match_id']:<12} {d:<12} {h:<24} {a}")

    print()
    confirm = input(f"  Clear all {n_skipped} skipped entries and allow retry? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("  Cancelled.")
        db.close(); return

    # Delete only unenriched failure entries
    db.execute(
        """DELETE p FROM backfill_progress p
           LEFT JOIN matches_stats s ON s.match_id = p.match_id
           WHERE s.match_id IS NULL"""
    )
    db.close()

    print(f"\n  ✅  Cleared {n_skipped} skipped entries for {league['name']}.")
    print(f"     Run Option 1 to attempt these matches again.")


# ── Main menu ─────────────────────────────────────────────────────────────────

def main():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║         FOOTBALL PREDICTOR — DATA ENRICHMENT TOOL           ║")
    print("║  Fetches xG, shots, corners, cards for match predictions     ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    setup_env()

    while True:
        print()
        print("  ┌─────────────────────────────────────────────────────────┐")
        print("  │  MENU                                                   │")
        print("  ├─────────────────────────────────────────────────────────┤")
        print("  │  1.  Enrich a full league (all pending matches)         │")
        print("  │      Already-enriched matches are skipped automatically │")
        print("  │                                                         │")
        print("  │  2.  Enrich one specific fixture                        │")
        print("  │      Search by team name or enter a fixture ID directly │")
        print("  │                                                         │")
        print("  │  3.  Check database status (NO API requests)            │")
        print("  │      Shows enrichment coverage per season               │")
        print("  │                                                         │")
        print("  │  4.  Re-sync season match list (fix missing teams)      │")
        print("  │      Fixes teams with fewer matches than expected       │")
        print("  │                                                         │")
        print("  │  5.  Enrich player stats (xG, shots per player)        │")
        print("  │      Powers goalscorer market pricing — 1 req/match    │")
        print("  │                                                         │")
        print("  │  6.  Fix duplicate team names (one-time DB fix)        │")
        print("  │      Renames API variants to canonical names            │")
        print("  │                                                         │")
        print("  │  7.  Clear skipped matches / retry failed               │")
        print("  │      Resets API failures so matches are attempted again │")
        print("  │                                                         │")
        print("  │  0.  Exit                                               │")
        print("  └─────────────────────────────────────────────────────────┘")
        print()

        choice = input("  Choice (0-7): ").strip()
        if   choice == "0": print("\n  Goodbye!\n"); break
        elif choice == "1": run_full_league_backfill()
        elif choice == "2": run_single_fixture_backfill()
        elif choice == "3": run_status_check()
        elif choice == "4": run_resync_seasons()
        elif choice == "5": run_player_stats_enrichment()
        elif choice == "6": run_fix_team_names()
        elif choice == "7": run_clear_skipped()
        else: print("  Invalid choice. Enter 0–7.")


if __name__ == "__main__":
    main()
