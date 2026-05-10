"""
main.py — Entry point. Run: python main.py
"""

import sys, json
import pandas as pd
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
import config
from database import DB, _ensure_schema, setup_env
from api_client import APIFootball
from data_manager import DataManager, resolve_team, input_lineup
from model import FixtureModel, Sim
from betting import get_odds
from display import ExcelTracker, print_ticket, print_upcoming


# ── Fixture output logging ────────────────────────────────────────────────────
class _Tee:
    """Writes to both terminal and a log file simultaneously."""
    def __init__(self, file_obj):
        self._file   = file_obj
        self._stdout = sys.stdout
    def write(self, text):
        self._stdout.write(text)
        self._file.write(text)
    def flush(self):
        self._stdout.flush()
        try: self._file.flush()
        except Exception: pass
    def isatty(self): return False


@contextmanager
def fixture_log(home: str, away: str, match_date: str = ""):
    """
    Context manager: mirrors all terminal output inside the block to a
    .txt file at:  fixture_logs/<LEAGUE_CODE>/YYYY-MM-DD_Home_vs_Away.txt
    """
    league_code = config.ACTIVE_LEAGUE.get("code", "UNKNOWN")
    league_dir  = config.LOGS_DIR / league_code
    league_dir.mkdir(parents=True, exist_ok=True)
    def _safe(n): return n.replace(" ", "_").replace("/", "-").replace("\\", "-")
    date_str  = match_date[:10] if match_date else datetime.now().strftime("%Y-%m-%d")
    fname     = f"{date_str}_{_safe(home)}_vs_{_safe(away)}.txt"
    log_path  = league_dir / fname
    log_file  = open(log_path, "w", encoding="utf-8")
    log_file.write(f"{'='*72}\n  {league_code} | {home} vs {away} | {date_str}\n"
                   f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                   f"{'='*72}\n\n")
    log_file.flush()
    tee = _Tee(log_file)
    sys.stdout = tee
    try:
        yield log_path
    finally:
        sys.stdout = tee._stdout
        log_file.write(f"\n{'='*72}\n  END OF FIXTURE LOG\n{'='*72}\n")
        log_file.close()
        print(f"  [Log] Saved → fixture_logs/{league_code}/{fname}")

# ── Persistent bankroll ───────────────────────────────────────────────────────
BANKROLL_FILE = config.SCRIPT_DIR / "bankroll.json"

def load_bankroll() -> float:
    """Load saved bankroll from file. Returns 20,000 if file doesn't exist."""
    try:
        if BANKROLL_FILE.exists():
            data = json.loads(BANKROLL_FILE.read_text())
            return float(data.get("bankroll", 20000.0))
    except Exception:
        pass
    return 20000.0

def save_bankroll(amount: float):
    """Save current bankroll to file so it persists across sessions."""
    try:
        BANKROLL_FILE.write_text(json.dumps({
            "bankroll"   : round(float(amount), 2),
            "updated_at" : datetime.now().strftime("%Y-%m-%d %H:%M"),
        }, indent=2))
    except Exception:
        pass

def update_bankroll_after_bet(bankroll: float, stake: float,
                               odds: float) -> float:
    """
    Prompt the user for the bet result and update the bankroll.
    Called after every prediction where a bet was recommended.
    Returns the updated bankroll.
    """
    print()
    print("  ╔══════════════════════════════════════════════════════╗")
    print(f"  ║  BET RESULT  (stake: {stake:,.0f} RWF  odds: {odds:.2f})  ║")
    print("  ╠══════════════════════════════════════════════════════╣")
    print("  ║  w = Won     l = Lost     v = Void/refund            ║")
    print("  ║  Enter = skip (did not bet or result unknown yet)    ║")
    print("  ╚══════════════════════════════════════════════════════╝")

    result = input("  Result (w/l/v/Enter): ").strip().lower()

    if result == "w":
        profit   = round(stake * (odds - 1), 0)
        bankroll = bankroll + profit
        save_bankroll(bankroll)
        print(f"  ✅  Won {profit:,.0f} RWF  →  New bankroll: {bankroll:,.0f} RWF")
    elif result == "l":
        bankroll = bankroll - stake
        save_bankroll(bankroll)
        print(f"  📉  Lost {stake:,.0f} RWF  →  New bankroll: {bankroll:,.0f} RWF")
        if bankroll < load_bankroll() * 0.70:
            print(f"  ⚠  Bankroll down >30% from saved peak — consider halving stakes")
    elif result == "v":
        print(f"  ↩  Void/refund — bankroll unchanged: {bankroll:,.0f} RWF")
        save_bankroll(bankroll)
    else:
        print(f"  Skipped — bankroll unchanged: {bankroll:,.0f} RWF")

    return bankroll


def select_league() -> dict:
    """
    Show league menu at startup. User picks a league.
    Sets global config.AFL_LEAGUE_ID, config.AFL_SEASONS, config.ACTIVE_LEAGUE.
    Returns the chosen league config dict.
    """
    # Assign into config module so all other modules see the update

    print()
    print("╔" + "═"*54 + "╗")
    print("║" + "  SELECT LEAGUE  ".center(54) + "║")
    print("╠" + "═"*54 + "╣")
    for key, cfg in config.LEAGUES.items():
        print(f"║  {key}.  {cfg['name']:<48}║")
    print("╚" + "═"*54 + "╝")

    while True:
        choice = input("  Enter number (1-5): ").strip()
        if choice in config.LEAGUES:
            league = config.LEAGUES[choice]
            config.AFL_LEAGUE_ID = league["id"]
            config.AFL_SEASONS   = league["seasons"]
            config.ACTIVE_LEAGUE = league
            print(f"\n  ✓ Selected: {league['name']}  |  DB: {league['db']}")
            return league
        print("  Invalid choice. Enter 1-5.")



def main():
    print(f"""
╔{'═'*74}╗
║{'':74}║
║{'  ⚽  BAYESIAN MULTI-LEAGUE PREDICTION ENGINE  v11.0  ⚽':^74}║
║{'  xG · H2H · Injuries · All Markets · MySQL · Excel Tracker':^74}║
║{'':74}║
╠{'═'*74}╣
║  Select league at startup — each has its own database                ║
║  Tracker (predictions_tracker.xlsx) is shared across all leagues      ║
╚{'═'*74}╝
""")

    # ── First-run setup ────────────────────────────────────────────────────────
    setup_env()

    # ── League selection ───────────────────────────────────────────────────────
    league = select_league()

    # ── Database — one DB per league, auto-created if missing ─────────────────
    print(f"[DB] Connecting to {league['db']} …", end=" ", flush=True)
    db = DB(db_name=league["db"])
    print("connected ✓")
    _ensure_schema(db)   # create tables if new DB

    # ── API clients ────────────────────────────────────────────────────────────
    afl = APIFootball(db)
    dm  = DataManager(db, afl)

    # ── Excel tracker ──────────────────────────────────────────────────────────
    tracker = ExcelTracker(config.TRACKER_FILE)

    # ── Startup sync & backfill ────────────────────────────────────────────────
    dm.ensure_teams()
    dm.startup_sync()
    dm.sync_cross_competition_fixtures()   # ~6 requests, skips if done today

    # ── Upcoming fixtures ──────────────────────────────────────────────────────
    upcoming = dm.fetch_upcoming()
    if upcoming:
        print_upcoming(upcoming)

    # ── Team list ──────────────────────────────────────────────────────────────
    all_teams = dm.all_team_names()
    if not all_teams:
        print("❌  No match data found. Check your API keys and MySQL connection.")
        sys.exit(1)

    print(f"✅  {len(all_teams)} teams in database. Ready.")
    print("    'teams' → list all  |  'quit' → exit\n")

    # ── Main loop ──────────────────────────────────────────────────────────────
    while True:
        try:
            raw = input("🎯  Fixture (e.g. Arsenal vs Man City): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye! 🍀"); break

        if not raw: continue
        if raw.lower() in ("quit","exit","q"):
            used = afl.requests_used_today()
            print(f"[Session] API requests used today: {used}")
            print("Stay disciplined. 🍀"); break
        if raw.lower() == "teams":
            print()
            for i, t in enumerate(all_teams, 1):
                print(f"  {i:>2}. {t}")
            print(); continue
        if raw.lower().startswith("check "):
            # e.g. "check Arsenal vs Man City"
            check_raw = raw[6:].strip()
            for d in (" vs ", " VS ", " v "):
                if d in check_raw:
                    cp = check_raw.split(d, 1)
                    try:
                        ch = resolve_team(cp[0].strip(), all_teams)
                        ca = resolve_team(cp[1].strip(), all_teams)
                        dm.print_fixture_readiness(ch, ca)
                    except ValueError as e:
                        print(f"  ❌ {e}")
                    break
            continue

        # Parse fixture
        delim = None
        for d in (" vs "," VS "," Vs "," v "," V "):
            if d in raw: delim = d; break
        if not delim:
            print("  ❌  Format: 'Team A vs Team B'\n"); continue

        parts = raw.split(delim, 1)
        try:
            home = resolve_team(parts[0].strip(), all_teams)
            away = resolve_team(parts[1].strip(), all_teams)
        except ValueError as e:
            print(f"  ❌  {e}\n"); continue

        if home == away:
            print("  ❌  Teams must be different.\n"); continue

        print(f"\n  ✓  {home}  vs  {away}")

        # ── Bankroll — load from file, allow override ─────────────────────────
        saved_br = load_bankroll()
        try:
            br = input(f"  💰  Bankroll (RWF, Enter = {saved_br:,.0f}): ").strip()
            bankroll = float(br.replace(",","")) if br else saved_br
            if br:   # user manually entered a value — save it
                save_bankroll(bankroll)
        except (ValueError, NameError):
            bankroll = saved_br

        # ── Auto fixture lookup ────────────────────────────────────────────────
        fixture_info = dm.find_fixture(home, away)
        fixture_id   = fixture_info.get("fixture_id") if fixture_info else None
        match_date   = (fixture_info or {}).get("date", "")

        # All output below is mirrored to fixture_logs/<LEAGUE>/<date>_Home_vs_Away.txt
        with fixture_log(home, away, match_date):

            # ── Opening odds from weekly_update.py store ──────────────────────────
            opening_odds = dm.fetch_opening_odds_for_fixture(fixture_id) if fixture_id else {}
            if opening_odds:
                print(f"  [Odds] Opening line found for fixture {fixture_id} "
                      f"({len(opening_odds)} markets)")
            else:
                print(f"  [Odds] No stored opening line — run weekly_update.py on Monday")

            # ── Fixture-first backfill ─────────────────────────────────────────────
            print(f"\n[Backfill] Fetching missing stats for {home} & {away} …")
            dm._run_backfill_batch(priority_teams=[home, away])

            # ── Fetch supporting data ──────────────────────────────────────────────
            print()
            injuries_h = dm.fetch_injuries_for_team(home, fixture_id=fixture_id)
            injuries_a = dm.fetch_injuries_for_team(away, fixture_id=fixture_id)

            # ── Cross-competition last match dates (for fatigue) ───────────────────
            # tz_localize(None) strips timezone so arithmetic with DB timestamps works
            if fixture_info and fixture_info.get("date"):
                fixture_dt = pd.to_datetime(fixture_info["date"]).tz_localize(None) \
                             if pd.to_datetime(fixture_info["date"]).tzinfo is None \
                             else pd.to_datetime(fixture_info["date"]).tz_convert(None)
            else:
                fixture_dt = pd.Timestamp.now()

            last_match_dates = {
                "home": dm.get_last_match_date_any_comp(home, before_date=fixture_dt),
                "away": dm.get_last_match_date_any_comp(away, before_date=fixture_dt),
            }
            for side, lmd in last_match_dates.items():
                team = home if side == "home" else away
                if lmd:
                    try:
                        lmd_naive = lmd.tz_localize(None) if lmd.tzinfo else lmd
                        days = (fixture_dt - lmd_naive).days
                        print(f"  [Fatigue] {team}: last match {days} day(s) ago (any competition)")
                    except Exception:
                        pass
            h2h_df     = dm.fetch_h2h(home, away)

            # ── Standings context ──────────────────────────────────────────────────
            standings = dm.standings_context(home, away)

            # ── Team-specific non-goal rates ───────────────────────────────────────
            home_rates = dm.team_non_goal_rates(home)
            away_rates = dm.team_non_goal_rates(away)
            if home_rates:
                print(f"  [Rates] {home}: {home_rates['n_matches']} matches — "
                      f"avg corners {home_rates.get('avg_corners',0):.1f}  "
                      f"avg cards {home_rates.get('avg_cards',0):.1f}")
            else:
                print(f"  [Rates] {home}: using PL averages")
            if away_rates:
                print(f"  [Rates] {away}: {away_rates['n_matches']} matches — "
                      f"avg corners {away_rates.get('avg_corners',0):.1f}  "
                      f"avg cards {away_rates.get('avg_cards',0):.1f}")
            else:
                print(f"  [Rates] {away}: using PL averages")

            # ── Referee ────────────────────────────────────────────────────────────
            # Use auto-detected referee from fixture if available
            auto_ref = (fixture_info or {}).get("referee", "")
            if auto_ref:
                print(f"  [Referee] Auto-detected: {auto_ref}")
                referee_name   = auto_ref
                referee_factor = dm.referee_card_rate(referee_name)
                if referee_factor != 1.0:
                    print(f"  [Referee] {referee_name}: ×{referee_factor:.2f} vs PL avg")
            else:
                referee_name = input("  Referee name (Enter to skip): ").strip()
                referee_factor = dm.referee_card_rate(referee_name) \
                                 if referee_name else 1.0
                if referee_name and referee_factor != 1.0:
                    print(f"  [Referee] {referee_name}: ×{referee_factor:.2f} vs PL avg")

            # ── Weather ────────────────────────────────────────────────────────────
            venue    = (fixture_info or {}).get("venue", "")
            match_dt = (fixture_info or {}).get("date", "")
            # If no venue from fixture API, look up home team's known ground from DB
            if not venue:
                vrow = dm.db.fetchone(
                    "SELECT v.name FROM venues v "
                    "JOIN teams t ON t.name LIKE CONCAT('%', SUBSTRING_INDEX(v.name,' ',1), '%') "
                    "WHERE t.name LIKE %s LIMIT 1",
                    (f"%{home.split()[0]}%",)
                )
                if not vrow:
                    # Last resort: check fallback dict for home team keyword
                    for vname in config.VENUE_COORDS_FALLBACK:
                        if home.split()[0].lower() in vname.lower():
                            venue = vname
                            break
                elif vrow:
                    venue = vrow["name"]
            weather = {}
            if venue and match_dt:
                print(f"  [Weather] Fetching for {venue} …", end=" ", flush=True)
                weather = dm.weather.get_match_weather(venue, match_dt)
                print(weather.get("description", "unavailable"))
            elif venue:
                print(f"  [Weather] {venue} — no match date, skipping weather.")
            else:
                print(f"  [Weather] No venue found for {home} — skipping.")

            # ── Auto lineups (available ~60 min before kickoff) ───────────────────
            players_h, players_a = [], []
            lineups_auto = False
            if fixture_id:
                players_h, players_a = dm.fetch_auto_lineups(
                    fixture_id, home, away
                )
                if players_h or players_a:
                    lineups_auto = True
                    # Enrich with actual player xG/90 stats
                    players_h = dm.enrich_players_with_xg(
                            players_h, home)
                    players_a = dm.enrich_players_with_xg(
                            players_a, away)

            # Manual lineup entry if auto failed or no fixture ID
            if not lineups_auto:
                print("\n  Lineups not yet released (or no fixture found).")
                do_manual = input(
                    "  Enter lineups manually for goalscorer markets? (y/n): "
                ).strip().lower() == "y"
                if do_manual:
                    players_h = input_lineup(home)
                    players_a = input_lineup(away)

            # ── Injury notes for tracker ───────────────────────────────────────────
            inj_notes_h = "; ".join(
                f"{i.get('player_name','?')} ({i.get('injury_type','')})"
                for i in injuries_h if i.get("player_name")
            ) if injuries_h else "None"
            inj_notes_a = "; ".join(
                f"{i.get('player_name','?')} ({i.get('injury_type','')})"
                for i in injuries_a if i.get("player_name")
            ) if injuries_a else "None"

            # ── Load match data ────────────────────────────────────────────────────
            df_fix = dm.load_for_fixture(home, away)
            if df_fix.empty:
                print("  ❌  No match data found for these teams.\n"); continue
            print(f"  [Data] {len(df_fix)} relevant matches loaded.")

            # ── Fit model ─────────────────────────────────────────────────────────
            # Fetch pre-computed xG-Elo ratings (DB only, no API cost).
            # These anchor the attack/defense prior centers so the MAP optimizer
            # starts from an Elo-informed position rather than zero.
            elo_ratings = dm.get_all_elo_ratings()
            print(f"  [Fitting] MAP + Laplace …")
            mdl = FixtureModel(df_fix, home, away, h2h_df,
                               injuries_h, injuries_a, weather,
                               standings=standings,
                               last_match_dates=last_match_dates,
                               elo_ratings=elo_ratings)
            mdl.fit()

            # ── Simulate ──────────────────────────────────────────────────────────
            mu_h, mu_a = mdl.lambdas()
            print(f"  [Simulate] {config.N_SIM:,} draws … ", end="", flush=True)
            s = Sim(mu_h, mu_a, rho=mdl.rho, r_goals=mdl.r_goals)
            print("done ✓")

            # ── Manual odds entry ─────────────────────────────────────────────────
            odds = get_odds()

            # Print ticket + get value summary
            val = print_ticket(home, away, mdl, s, odds, bankroll,
                               injuries_h, injuries_a, players_h, players_a, h2h_df,
                               home_rates=home_rates, away_rates=away_rates,
                               referee_factor=referee_factor,
                               weather=weather, standings=standings,
                               fixture_info=fixture_info,
                               opening_odds=opening_odds)

            # Build tracker data dict — use same team rates as print_ticket
            home_cor = home_rates.get("avg_corners") if home_rates else None
            away_cor = away_rates.get("avg_corners") if away_rates else None
            home_crd = home_rates.get("avg_cards")   if home_rates else None
            away_crd = away_rates.get("avg_cards")   if away_rates else None

            res  = s.result(); ou = s.ou(); bt = s.btts()
            cs   = s.correct_score(); ht = s.halftime()
            hmg  = s.half_most_goals(); htft_d = s.htft()
            wtn  = s.win_to_nil(); dnb = s.draw_no_bet()
            w_corner = float((weather or {}).get("corner_factor", 1.0))
            cor  = s.corners(home_avg=home_cor, away_avg=away_cor,
                             weather_factor=w_corner)
            crd  = s.cards(home_avg=home_crd, away_avg=away_crd,
                           referee_factor=referee_factor)
            xg   = s.xg()

            top_cs    = cs[cs["score"] != "other"].iloc[0]
            htft_best = max(htft_d, key=htft_d.get)

            tracker_data = {
                "date"             : datetime.now().strftime("%Y-%m-%d %H:%M"),
                "league_code"      : config.ACTIVE_LEAGUE.get("code", ""),
                "home_team"        : home, "away_team": away,
                "xg_h"             : xg["xg_h"], "xg_a": xg["xg_a"],
                "home_win_pct"     : res["home_win"],
                "draw_pct"         : res["draw"],
                "away_win_pct"     : res["away_win"],
                "over25_pct"       : ou["over_2.5"],
                "under25_pct"      : ou["under_2.5"],
                "over15_pct"       : ou["over_1.5"],
                "over35_pct"       : ou["over_3.5"],
                "btts_yes_pct"     : bt["btts_yes"],
                "btts_no_pct"      : bt["btts_no"],
                "top_correct_score": top_cs["score"],
                "top_cs_pct"       : top_cs["prob"],
                "ht_hw_pct"        : ht["ht_hw"],
                "ht_d_pct"         : ht["ht_d"],
                "ht_aw_pct"        : ht["ht_aw"],
                "first_half_pct"   : hmg["first_half"],
                "second_half_pct"  : hmg["second_half"],
                "equal_pct"        : hmg["equal"],
                "htft"             : htft_d,
                "home_wtn_pct"     : wtn["home_wtn"],
                "away_wtn_pct"     : wtn["away_wtn"],
                "dnb_home_pct"     : dnb["dnb_home"],
                "dnb_away_pct"     : dnb["dnb_away"],
                "exp_corners"      : cor["mean_t"],
                "exp_cards"        : crd["mean_t"],
                "exp_booking_pts"  : crd["mean_booking_pts"],
                "best_market"      : val["best_market"],
                "edge_pct"         : val["best_edge"],
                "ev"               : val["best_ev"],
                "kelly_stake_rwf"  : val["best_stake"],
                "bankroll_rwf"     : bankroll,
                "odds_entered"     : json.dumps(odds),
                "h2h_factor"       : mdl.h2h_factor,
                "inj_h_factor"     : mdl.inj_h_factor,
                "inj_a_factor"     : mdl.inj_a_factor,
                "mom_h"            : mdl.mom_h,
                "mom_a"            : mdl.mom_a,
                "fatigue_h"        : mdl.fatigue_h,
                "fatigue_a"        : mdl.fatigue_a,
                "form_h"           : mdl.form_h,
                "form_a"           : mdl.form_a,
                "dc_rho"           : mdl.rho,
                "nb_r_goals"       : mdl.r_goals,
                "motiv_h"          : mdl.motiv_h,
                "motiv_a"          : mdl.motiv_a,
                "nb_r_goals"       : mdl.r_goals,
                "referee_factor"   : referee_factor,
                "corners_src"      : "team" if cor.get("using_team_data") else "PL_avg",
                "cards_src"        : "team" if crd.get("using_team_data") else "PL_avg",
                "weather_goal"     : mdl.weather_goal_factor,
                "xg_signal_pct"    : round(float((mdl.hxg != mdl.hg).sum() / max(len(mdl.df), 1) * 100), 1),
                "weather_corner"   : mdl.weather_corner_factor,
                "weather_desc"     : (weather or {}).get("description", ""),
                "venue"            : (fixture_info or {}).get("venue", ""),
                "referee_auto"     : (fixture_info or {}).get("referee", ""),
                "fixture_id"       : fixture_id or 0,
                "injury_notes_home": inj_notes_h,
                "injury_notes_away": inj_notes_a,
            }

            # Save to Excel tracker
            tracker.append_prediction(tracker_data)

            # Save to DB predictions log
            db.execute(
                """INSERT INTO predictions_log
                   (prediction_date, league_code, home_team, away_team,
                    home_xg, away_xg, home_win_pct, draw_pct, away_win_pct,
                    over25_pct, under25_pct, over15_pct, over35_pct,
                    btts_yes_pct, btts_no_pct,
                    top_correct_score, top_cs_pct,
                    ht_home_win_pct, ht_draw_pct, ht_away_win_pct,
                    expected_corners, expected_cards,
                    best_market, edge_pct, ev,
                    suggested_stake_rwf, bankroll_rwf,
                    odds_entered, injury_notes_home, injury_notes_away)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                           %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (tracker_data["date"], config.ACTIVE_LEAGUE.get("code","?"), home, away,
                 xg["xg_h"], xg["xg_a"],
                 res["home_win"], res["draw"], res["away_win"],
                 ou["over_2.5"], ou["under_2.5"], ou["over_1.5"], ou["over_3.5"],
                 bt["btts_yes"], bt["btts_no"],
                 top_cs["score"], float(top_cs["prob"]),
                 ht["ht_hw"], ht["ht_d"], ht["ht_aw"],
                 cor["mean_t"], crd["mean_t"],
                 val["best_market"], val["best_edge"], val["best_ev"],
                 val["best_stake"], bankroll,
                 json.dumps(odds), inj_notes_h, inj_notes_a)
            )

            # ── Bet result entry — update bankroll immediately ────────────────────
            if val.get("best_stake", 0) > 0:
                bankroll = update_bankroll_after_bet(
                    bankroll,
                    stake=val["best_stake"],
                    odds=val.get("best_odds", 2.0),
                )

        try:
                again = input("  🔄  Another fixture? (y)es / (s)witch league / (q)uit: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            used = afl.requests_used_today()
            print(f"\n[Session] API requests used today: {used}")
            print("Goodbye! 🍀"); break
        if again == "s":
            # Switch league — re-run selection, reconnect DB, reload teams
            db.close()
            league = select_league()
            db  = DB(db_name=league["db"])
            _ensure_schema(db)
            afl = APIFootball(db)
            dm  = DataManager(db, afl)
            tracker = ExcelTracker(config.TRACKER_FILE)
            dm.ensure_teams()
            dm.startup_sync()
            dm.sync_cross_competition_fixtures()
            upcoming = dm.fetch_upcoming()
            if upcoming:
                print_upcoming(upcoming)
            all_teams = dm.all_team_names()
            print(f"✅  {len(all_teams)} teams ready in {league['name']}\n")
            continue
        if again != "y":
            used = afl.requests_used_today()
            print(f"\n[Session] API requests used today: {used}")
            print("  The edge is in the process. Stay disciplined. 🍀\n")
            db.close(); break

    db.close()


if __name__ == "__main__":
    main()
