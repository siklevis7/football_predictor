"""
match_preview.py — Football Predictor Match Preview
────────────────────────────────────────────────────
Standalone Flask app. Does NOT modify any existing script.
Accesses the same MySQL databases and runs the same simulation.

Run: python match_preview.py
Open: http://localhost:5050
"""

import sys, json, os
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

# ── Import from existing split modules ───────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

from flask import Flask, jsonify, request, render_template_string
import config
from database import DB, _ensure_schema, setup_env
from api_client import APIFootball
from data_manager import DataManager
from model import FixtureModel, Sim, time_weights

app = Flask(__name__)
setup_env()

# ── API-Football logo URLs ────────────────────────────────────────────────────
# Pattern: https://media.api-sports.io/football/teams/{team_id}.png
# League:  https://media.api-sports.io/football/leagues/{league_id}.png
LOGO_BASE  = "https://media.api-sports.io/football/teams"
LG_LOGO    = "https://media.api-sports.io/football/leagues"

def team_logo(team_id):
    if not team_id: return ""
    return f"{LOGO_BASE}/{team_id}.png"

def league_logo(league_id):
    return f"{LG_LOGO}/{league_id}.png"

# ── Database helpers ──────────────────────────────────────────────────────────

def get_db(league_id: int):
    league = next((v for v in config.LEAGUES.values() if v["id"] == league_id), None)
    if not league: return None, None
    config.AFL_LEAGUE_ID = league["id"]
    config.AFL_SEASONS   = league["seasons"]
    db = DB(db_name=league["db"])
    return db, league

def get_team_id_from_db(db, team_name: str):
    row = db.fetchone(
        "SELECT team_id FROM teams WHERE name=%s LIMIT 1", (team_name,)
    )
    return row["team_id"] if row else None

# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/api/leagues")
def api_leagues():
    return jsonify([{
        "id"  : int(v["id"]),
        "name": v["name"].split("\xa0")[-1].strip() if "\xa0" in v["name"] else v["name"],
        "code": v["code"],
        "logo": league_logo(v["id"]),
    } for v in config.LEAGUES.values()])


@app.route("/api/teams/<int:league_id>")
def api_teams(league_id):
    db, league = get_db(league_id)
    if not db:
        return jsonify([])
    rows = db.fetchall(
        """SELECT DISTINCT home_team_name as name FROM matches_basic
           UNION SELECT DISTINCT away_team_name FROM matches_basic
           ORDER BY name"""
    )
    teams = []
    for r in (rows or []):
        name = r["name"]
        tid  = get_team_id_from_db(db, name)
        teams.append({"name": name, "logo": team_logo(tid), "team_id": tid})
    db.close()
    return jsonify(teams)


@app.route("/api/predict", methods=["POST"])
def api_predict():
    body = request.json or {}
    league_id = int(body.get("league_id", 39))
    home      = body.get("home", "").strip()
    away      = body.get("away", "").strip()

    if not home or not away:
        return jsonify({"error": "Home and away team required"}), 400

    db, league = get_db(league_id)
    if not db:
        return jsonify({"error": "League not found"}), 400

    afl = APIFootball(db)
    dm  = DataManager(db, afl)

    # Load historical data
    df = dm.load_for_fixture(home, away)
    if df.empty or len(df) < 10:
        db.close()
        return jsonify({"error": f"Insufficient data for {home} vs {away}"}), 400

    # Fetch live context
    try:
        standings = dm.standings_context(home, away)
    except Exception:
        standings = {}
    try:
        injuries_h = dm.fetch_injuries_for_team(home)
        injuries_a = dm.fetch_injuries_for_team(away)
    except Exception:
        injuries_h = injuries_a = []

    # Fit model (no odds needed)
    mdl = FixtureModel(
        df, home, away,
        home_injuries=injuries_h,
        away_injuries=injuries_a,
        standings=standings,
    )
    mdl.fit()
    mu_h, mu_a = mdl.lambdas()
    s = Sim(mu_h, mu_a, rho=mdl.rho, r_goals=mdl.r_goals)

    # ── Results ───────────────────────────────────────────────────────────────
    res       = s.result()
    ou        = s.ou()
    btts      = s.btts()
    cs_df     = s.correct_score()
    ht        = s.halftime()
    corners   = s.corners()

    home_id = get_team_id_from_db(db, home)
    away_id = get_team_id_from_db(db, away)

    # Recent form (last 5)
    def recent_form(team, n=5):
        mask = (df["home_team_name"]==team)|(df["away_team_name"]==team)
        rows = df[mask].sort_values("match_date", ascending=False).head(n)
        form = []
        for _, row in rows.iterrows():
            is_home = row["home_team_name"] == team
            gh = int(row["home_goals"]); ga = int(row["away_goals"])
            tg = gh if is_home else ga; og = ga if is_home else gh
            result = "W" if tg > og else ("D" if tg == og else "L")
            opp = row["away_team_name"] if is_home else row["home_team_name"]
            form.append({
                "result": result,
                "score": f"{gh}-{ga}",
                "opponent": opp,
                "date": str(row["match_date"])[:10],
            })
        return form

    # H2H
    h2h_mask = (
        ((df["home_team_name"]==home)&(df["away_team_name"]==away)) |
        ((df["home_team_name"]==away)&(df["away_team_name"]==home))
    )
    h2h_rows = df[h2h_mask].sort_values("match_date", ascending=False).head(8)
    h2h_list = []
    for _, row in h2h_rows.iterrows():
        h2h_list.append({
            "home": row["home_team_name"],
            "away": row["away_team_name"],
            "score": f"{int(row['home_goals'])}-{int(row['away_goals'])}",
            "date": str(row["match_date"])[:10],
        })

    # Injuries
    def fmt_injuries(inj_list):
        return [{"name": i.get("player_name","?"),
                 "type": i.get("injury_type","?"),
                 "pos": i.get("position","MF")} for i in inj_list]

    # Match count
    home_count = int((df["home_team_name"]==home).sum() + (df["away_team_name"]==home).sum())
    away_count = int((df["home_team_name"]==away).sum() + (df["away_team_name"]==away).sum())

    # Top correct scores
    cs_top = cs_df.head(8)[["score","prob"]].to_dict("records")
    for c in cs_top: c["prob"] = round(float(c["prob"]) * 100, 1)

    db.close()

    return jsonify({
        "home": {"name": home, "logo": team_logo(home_id),
                 "matches": home_count, "form": recent_form(home),
                 "injuries": fmt_injuries(injuries_h),
                 "standings": standings.get("home", {})},
        "away": {"name": away, "logo": team_logo(away_id),
                 "matches": away_count, "form": recent_form(away),
                 "injuries": fmt_injuries(injuries_a),
                 "standings": standings.get("away", {})},
        "league": {"id": league_id, "name": league["name"],
                   "logo": league_logo(league_id), "code": league["code"]},
        "model": {
            "home_win":   round(float(res.get("home_win", 0)) * 100, 1),
            "draw":       round(float(res.get("draw", 0))     * 100, 1),
            "away_win":   round(float(res.get("away_win", 0)) * 100, 1),
            "over_2_5":   round(float(ou.get("over_2.5", 0))  * 100, 1),
            "under_2_5":  round(float(ou.get("under_2.5", 0)) * 100, 1),
            "over_1_5":   round(float(ou.get("over_1.5", 0))  * 100, 1),
            "over_3_5":   round(float(ou.get("over_3.5", 0))  * 100, 1),
            "btts_yes":   round(float(btts.get("btts_yes", 0)) * 100, 1),
            "btts_no":    round(float(btts.get("btts_no", 0))  * 100, 1),
            "exp_home_goals": round(float(mu_h.mean()), 2),
            "exp_away_goals": round(float(mu_a.mean()), 2),
            "rho":        round(float(mdl.rho), 4),
            "r_goals":    round(float(mdl.r_goals), 2),
            "motiv_h":    round(float(getattr(mdl, "motiv_h", 1.0)), 3),
            "motiv_a":    round(float(getattr(mdl, "motiv_a", 1.0)), 3),
            "fatigue_h":  round(float(getattr(mdl, "fatigue_h", 1.0)), 3),
            "fatigue_a":  round(float(getattr(mdl, "fatigue_a", 1.0)), 3),
            "inj_h":      round(float(getattr(mdl, "inj_h_factor", 1.0)), 3),
            "inj_a":      round(float(getattr(mdl, "inj_a_factor", 1.0)), 3),
        },
        "correct_scores": cs_top,
        "h2h": h2h_list,
        "corners": {k: round(float(v)*100,1) for k,v in corners.items()},
        "halftime": {k: round(float(v)*100,1) for k,v in ht.items()},
    })


# ── HTML frontend ─────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Football Predictor</title>
<style>
  :root {
    --bg: #0a0e1a; --card: #111827; --card2: #1a2236;
    --border: #1e2d45; --accent: #3b82f6; --accent2: #6366f1;
    --green: #22c55e; --red: #ef4444; --amber: #f59e0b;
    --text: #f1f5f9; --muted: #94a3b8; --pill: #1e3a5f;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Inter', system-ui, sans-serif; min-height: 100vh; }

  /* Header */
  header { background: linear-gradient(135deg,#0f172a 0%,#1e3a5f 100%);
           border-bottom: 1px solid var(--border); padding: 16px 24px;
           display: flex; align-items: center; gap: 12px; }
  header svg { width:28px; height:28px; }
  header h1 { font-size: 1.25rem; font-weight: 700; background: linear-gradient(90deg,#60a5fa,#818cf8);
              -webkit-background-clip: text; -webkit-text-fill-color: transparent; }

  /* Form area */
  .form-wrap { max-width: 900px; margin: 32px auto; padding: 0 16px; }
  .form-card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 24px 28px; }
  .form-card h2 { font-size: 1rem; color: var(--muted); margin-bottom: 20px; letter-spacing: .05em; text-transform: uppercase; font-size:.75rem; }
  .form-row { display: grid; grid-template-columns: 1fr 1fr 1fr auto; gap: 14px; align-items: end; }
  label { display: block; font-size: .75rem; color: var(--muted); margin-bottom: 6px; font-weight: 500; letter-spacing:.04em; text-transform:uppercase; }
  select, input { width: 100%; background: var(--card2); border: 1px solid var(--border); border-radius: 10px;
                  color: var(--text); padding: 10px 14px; font-size: .9rem; appearance: none;
                  transition: border-color .2s; outline: none; }
  select:focus, input:focus { border-color: var(--accent); }
  .select-wrap { position: relative; }
  .select-wrap::after { content:"▾"; position: absolute; right:12px; top:50%; transform:translateY(-50%); color:var(--muted); pointer-events:none; }
  .btn-predict { background: linear-gradient(135deg,#3b82f6,#6366f1); border: none;
                 color: #fff; padding: 10px 24px; border-radius: 10px; font-size: .9rem;
                 font-weight: 600; cursor: pointer; white-space: nowrap; height: 42px;
                 transition: opacity .2s, transform .1s; }
  .btn-predict:hover { opacity: .9; transform: translateY(-1px); }
  .btn-predict:disabled { opacity: .5; cursor: not-allowed; transform: none; }

  /* Loading */
  .loading { display: none; text-align: center; padding: 60px; color: var(--muted); }
  .spinner { width:40px; height:40px; border:3px solid var(--border); border-top-color:var(--accent);
             border-radius:50%; animation: spin 0.8s linear infinite; margin: 0 auto 16px; }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* Error */
  .error-box { background: #1a0a0a; border: 1px solid var(--red); border-radius: 12px;
               padding: 16px 20px; color: var(--red); margin: 16px 0; display:none; }

  /* Results */
  #results { display: none; max-width: 900px; margin: 0 auto 48px; padding: 0 16px; }

  /* Match header */
  .match-header { background: linear-gradient(135deg,#111827,#1a2236);
                  border: 1px solid var(--border); border-radius: 16px;
                  padding: 28px; margin-bottom: 16px; }
  .league-badge { display: flex; align-items: center; gap: 8px; margin-bottom: 20px;
                  color: var(--muted); font-size:.8rem; font-weight:600; letter-spacing:.05em; text-transform:uppercase; }
  .league-badge img { width:20px; height:20px; object-fit:contain; }
  .teams-row { display: grid; grid-template-columns: 1fr auto 1fr; align-items: center; gap: 20px; }
  .team-block { display:flex; flex-direction:column; align-items:center; gap:10px; }
  .team-block.away { align-items:flex-end; }
  .team-logo { width:72px; height:72px; object-fit:contain; filter:drop-shadow(0 4px 12px rgba(0,0,0,.5)); }
  .team-logo-placeholder { width:72px; height:72px; background:var(--card2); border-radius:50%;
                           display:flex; align-items:center; justify-content:center; font-size:1.5rem; }
  .team-name { font-size:1.1rem; font-weight:700; text-align:center; }
  .team-away .team-name { text-align:right; }
  .vs-block { text-align:center; }
  .vs { font-size:1.5rem; font-weight:800; color:var(--muted); }
  .exp-goals { font-size:.8rem; color:var(--muted); margin-top:4px; }

  /* Grid */
  .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }
  .grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; margin-bottom: 16px; }

  /* Card */
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 20px; }
  .card-title { font-size: .7rem; color: var(--muted); font-weight: 700; letter-spacing: .08em;
                text-transform: uppercase; margin-bottom: 16px; }

  /* Probability bars */
  .prob-row { margin-bottom: 12px; }
  .prob-label { display: flex; justify-content: space-between; font-size:.85rem; margin-bottom:5px; }
  .prob-label .val { font-weight: 700; color: var(--accent); }
  .bar-bg { background: var(--card2); border-radius: 99px; height: 8px; overflow: hidden; }
  .bar-fill { height: 100%; border-radius: 99px; transition: width .6s cubic-bezier(.4,0,.2,1); }
  .bar-home { background: linear-gradient(90deg, #3b82f6, #60a5fa); }
  .bar-draw { background: linear-gradient(90deg, #f59e0b, #fbbf24); }
  .bar-away { background: linear-gradient(90deg, #ef4444, #f87171); }
  .bar-green { background: linear-gradient(90deg, #22c55e, #4ade80); }
  .bar-purple { background: linear-gradient(90deg, #8b5cf6, #a78bfa); }
  .bar-orange { background: linear-gradient(90deg, #f97316, #fb923c); }

  /* 1X2 big display */
  .result-trio { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; }
  .result-box { background: var(--card2); border-radius: 12px; padding: 14px;
                text-align: center; border: 1px solid var(--border); }
  .result-box.top { border-color: var(--accent); }
  .result-box .pct { font-size: 1.6rem; font-weight: 800; }
  .result-box .lbl { font-size: .7rem; color: var(--muted); margin-top: 3px; text-transform: uppercase; letter-spacing:.05em; }
  .pct-home { color: #60a5fa; }
  .pct-draw { color: #fbbf24; }
  .pct-away { color: #f87171; }

  /* Form dots */
  .form-dots { display: flex; gap: 5px; flex-wrap: wrap; }
  .dot { width:28px; height:28px; border-radius:6px; display:flex; align-items:center;
         justify-content:center; font-size:.7rem; font-weight:800; }
  .dot-W { background:#14532d; color:#4ade80; }
  .dot-D { background:#451a03; color:#fbbf24; }
  .dot-L { background:#450a0a; color:#f87171; }

  /* Team split layout */
  .team-split { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .side-label { font-size:.7rem; font-weight:700; color:var(--muted); text-transform:uppercase;
                letter-spacing:.06em; margin-bottom: 8px; }

  /* Correct scores */
  .cs-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; }
  .cs-item { background: var(--card2); border-radius: 10px; padding: 10px 6px; text-align: center;
             border: 1px solid var(--border); }
  .cs-item.top1 { border-color: #fbbf24; background: #1a1400; }
  .cs-score { font-size: .95rem; font-weight: 700; }
  .cs-pct { font-size: .75rem; color: var(--muted); margin-top: 2px; }

  /* H2H */
  .h2h-row { display: flex; align-items: center; justify-content: space-between;
             padding: 8px 12px; border-radius: 8px; margin-bottom: 6px; background:var(--card2); font-size:.85rem; }
  .h2h-date { color: var(--muted); font-size: .75rem; min-width: 80px; }
  .h2h-score { font-weight: 700; background: var(--pill); padding: 3px 10px; border-radius: 99px; font-size:.85rem; }

  /* Injury pill */
  .inj-pill { display:inline-flex; align-items:center; gap:5px; background:#1a0a0a;
              border:1px solid #7f1d1d; border-radius:6px; padding:4px 9px; font-size:.75rem;
              color:#fca5a5; margin:3px; }
  .inj-pos { background:#7f1d1d; border-radius:4px; padding:1px 5px; font-size:.65rem; font-weight:700; }

  /* Standings mini */
  .standing-badge { display:inline-flex; align-items:center; gap:6px; background:var(--card2);
                    border:1px solid var(--border); border-radius:8px; padding:6px 12px; font-size:.8rem; }
  .pos-num { font-size:1.1rem; font-weight:800; color:var(--accent); min-width:24px; text-align:center; }

  /* Factor pills */
  .factors { display:flex; flex-wrap:wrap; gap:6px; }
  .factor { background:var(--card2); border:1px solid var(--border); border-radius:8px;
            padding:5px 10px; font-size:.75rem; }
  .factor .f-val { font-weight:700; color:var(--accent); margin-left:4px; }
  .factor.down .f-val { color:var(--red); }
  .factor.neutral .f-val { color:var(--muted); }

  /* Responsive */
  @media (max-width:640px) {
    .form-row { grid-template-columns: 1fr; }
    .grid-2, .grid-3 { grid-template-columns: 1fr; }
    .cs-grid { grid-template-columns: repeat(2,1fr); }
    .team-split { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>

<header>
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
    <circle cx="12" cy="12" r="10"/><path d="M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20"/>
    <path d="M2 12h20"/>
  </svg>
  <h1>Football Predictor</h1>
</header>

<div class="form-wrap">
  <div class="form-card">
    <h2>Match Preview</h2>
    <div class="form-row">
      <div>
        <label>League</label>
        <div class="select-wrap">
          <select id="sel-league" onchange="loadTeams()">
            <option value="">Loading…</option>
          </select>
        </div>
      </div>
      <div>
        <label>Home Team</label>
        <div class="select-wrap">
          <select id="sel-home"><option value="">Select league first</option></select>
        </div>
      </div>
      <div>
        <label>Away Team</label>
        <div class="select-wrap">
          <select id="sel-away"><option value="">Select league first</option></select>
        </div>
      </div>
      <div>
        <label>&nbsp;</label>
        <button class="btn-predict" onclick="runPredict()" id="btn-go">Predict</button>
      </div>
    </div>
  </div>

  <div class="error-box" id="err-box"></div>
  <div class="loading" id="loading">
    <div class="spinner"></div>
    <div>Running simulation…</div>
  </div>
</div>

<div id="results"></div>

<script>
let teamsCache = {};

async function loadLeagues() {
  const res = await fetch("/api/leagues");
  const data = await res.json();
  const sel = document.getElementById("sel-league");
  sel.innerHTML = data.map(l =>
    `<option value="${l.id}">${l.name}</option>`
  ).join("");
  loadTeams();
}

async function loadTeams() {
  const lgId = document.getElementById("sel-league").value;
  if (!lgId) return;
  const selH = document.getElementById("sel-home");
  const selA = document.getElementById("sel-away");
  selH.innerHTML = `<option value="">Loading…</option>`;
  selA.innerHTML = `<option value="">Loading…</option>`;

  const res = await fetch(`/api/teams/${lgId}`);
  const teams = await res.json();
  teamsCache[lgId] = teams;

  const opts = teams.map(t => `<option value="${t.name}">${t.name}</option>`).join("");
  selH.innerHTML = opts;
  selA.innerHTML = opts;
}

async function runPredict() {
  const lgId = document.getElementById("sel-league").value;
  const home  = document.getElementById("sel-home").value;
  const away  = document.getElementById("sel-away").value;
  const err   = document.getElementById("err-box");
  const res   = document.getElementById("results");

  err.style.display = "none";
  res.style.display = "none";
  document.getElementById("loading").style.display = "block";
  document.getElementById("btn-go").disabled = true;

  try {
    const r = await fetch("/api/predict", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({league_id: parseInt(lgId), home, away})
    });
    const data = await r.json();
    document.getElementById("loading").style.display = "none";
    document.getElementById("btn-go").disabled = false;

    if (data.error) {
      err.textContent = "⚠  " + data.error;
      err.style.display = "block";
      return;
    }
    renderResults(data);
  } catch(e) {
    document.getElementById("loading").style.display = "none";
    document.getElementById("btn-go").disabled = false;
    err.textContent = "⚠  Connection error: " + e.message;
    err.style.display = "block";
  }
}

function pbar(val, cls) {
  return `<div class="bar-bg"><div class="bar-fill ${cls}" style="width:${Math.min(val,100)}%"></div></div>`;
}

function formDots(form) {
  return form.map(f =>
    `<div class="dot dot-${f.result}" title="${f.opponent} ${f.score} (${f.date})">${f.result}</div>`
  ).join("");
}

function standingBadge(st) {
  if (!st || !st.pos) return "";
  return `<div class="standing-badge"><span class="pos-num">${st.pos}</span>
    <div><div style="font-size:.85rem;font-weight:700">${st.pts} pts</div>
    <div style="font-size:.7rem;color:var(--muted)">${st.played} played · GD ${st.gd>0?'+':''}${st.gd}</div></div></div>`;
}

function factorChip(label, val) {
  const diff = val - 1;
  const cls  = diff < -0.01 ? "down" : (diff > 0.01 ? "" : "neutral");
  const sign = diff >= 0 ? "+" : "";
  return `<div class="factor ${cls}">${label}<span class="f-val">${sign}${(diff*100).toFixed(1)}%</span></div>`;
}

function injPills(inj) {
  if (!inj.length) return `<span style="color:var(--muted);font-size:.8rem">None reported</span>`;
  return inj.map(i =>
    `<span class="inj-pill"><span class="inj-pos">${i.pos}</span>${i.name} <span style="color:var(--muted)">(${i.type})</span></span>`
  ).join("");
}

function renderResults(d) {
  const m = d.model;
  const top1X2 = Math.max(m.home_win, m.draw, m.away_win);

  const homeLogoHtml = d.home.logo
    ? `<img class="team-logo" src="${d.home.logo}" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'" alt="${d.home.name}"><div class="team-logo-placeholder" style="display:none">⚽</div>`
    : `<div class="team-logo-placeholder">⚽</div>`;
  const awayLogoHtml = d.away.logo
    ? `<img class="team-logo" src="${d.away.logo}" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'" alt="${d.away.name}"><div class="team-logo-placeholder" style="display:none">⚽</div>`
    : `<div class="team-logo-placeholder">⚽</div>`;

  const html = `
  <!-- Match Header -->
  <div class="match-header">
    <div class="league-badge">
      <img src="${d.league.logo}" onerror="this.style.display='none'" width="20" height="20">
      ${d.league.name}
    </div>
    <div class="teams-row">
      <div class="team-block">
        ${homeLogoHtml}
        <div class="team-name">${d.home.name}</div>
        ${standingBadge(d.home.standings)}
      </div>
      <div class="vs-block">
        <div class="vs">VS</div>
        <div class="exp-goals" style="margin-top:8px">
          <span style="color:#60a5fa">${m.exp_home_goals}</span>
          <span style="color:var(--muted)"> xG </span>
          <span style="color:#f87171">${m.exp_away_goals}</span>
        </div>
      </div>
      <div class="team-block" style="align-items:flex-end">
        ${awayLogoHtml}
        <div class="team-name" style="text-align:right">${d.away.name}</div>
        ${standingBadge(d.away.standings)}
      </div>
    </div>
  </div>

  <!-- 1X2 Probabilities -->
  <div class="card" style="margin-bottom:16px">
    <div class="card-title">Match Result Probabilities</div>
    <div class="result-trio">
      <div class="result-box ${m.home_win===top1X2?'top':''}">
        <div class="pct pct-home">${m.home_win}%</div>
        <div class="lbl">Home Win</div>
      </div>
      <div class="result-box ${m.draw===top1X2?'top':''}">
        <div class="pct pct-draw">${m.draw}%</div>
        <div class="lbl">Draw</div>
      </div>
      <div class="result-box ${m.away_win===top1X2?'top':''}">
        <div class="pct pct-away">${m.away_win}%</div>
        <div class="lbl">Away Win</div>
      </div>
    </div>
  </div>

  <div class="grid-2">
    <!-- Goals Markets -->
    <div class="card">
      <div class="card-title">Goals Markets</div>
      ${[
        ["Over 1.5", m.over_1_5, "bar-green"],
        ["Over 2.5", m.over_2_5, "bar-green"],
        ["Under 2.5", m.under_2_5, "bar-purple"],
        ["Over 3.5", m.over_3_5, "bar-orange"],
        ["BTTS Yes", m.btts_yes, "bar-home"],
        ["BTTS No",  m.btts_no,  "bar-draw"],
      ].map(([label,val,cls]) => `
        <div class="prob-row">
          <div class="prob-label"><span>${label}</span><span class="val">${val}%</span></div>
          ${pbar(val, cls)}
        </div>`).join("")}
    </div>

    <!-- Correct Scores -->
    <div class="card">
      <div class="card-title">Most Likely Correct Scores</div>
      <div class="cs-grid">
        ${d.correct_scores.map((cs,i) =>
          `<div class="cs-item ${i===0?'top1':''}">
            <div class="cs-score">${cs.score}</div>
            <div class="cs-pct">${cs.prob}%</div>
          </div>`).join("")}
      </div>
    </div>
  </div>

  <div class="grid-2">
    <!-- Form -->
    <div class="card">
      <div class="card-title">Recent Form (last 5)</div>
      <div class="team-split">
        <div>
          <div class="side-label">${d.home.name}</div>
          <div class="form-dots">${formDots(d.home.form)}</div>
        </div>
        <div>
          <div class="side-label">${d.away.name}</div>
          <div class="form-dots">${formDots(d.away.form)}</div>
        </div>
      </div>
    </div>

    <!-- HT Markets -->
    <div class="card">
      <div class="card-title">Half Time</div>
      ${d.halftime.ht_hw !== undefined ? [
        ["HT Home Win", d.halftime.ht_hw||0, "bar-home"],
        ["HT Draw",     d.halftime.ht_d||0,  "bar-draw"],
        ["HT Away Win", d.halftime.ht_aw||0, "bar-away"],
      ].map(([label,val,cls]) => `
        <div class="prob-row">
          <div class="prob-label"><span>${label}</span><span class="val">${val.toFixed(1)}%</span></div>
          ${pbar(val, cls)}
        </div>`).join("") : "<div style='color:var(--muted);font-size:.85rem'>No HT data</div>"}
    </div>
  </div>

  <!-- H2H -->
  ${d.h2h.length ? `<div class="card" style="margin-bottom:16px">
    <div class="card-title">Head to Head (last ${d.h2h.length})</div>
    ${d.h2h.map(h => `
      <div class="h2h-row">
        <span class="h2h-date">${h.date}</span>
        <span>${h.home}</span>
        <span class="h2h-score">${h.score}</span>
        <span>${h.away}</span>
      </div>`).join("")}
  </div>` : ""}

  <!-- Injuries -->
  <div class="grid-2">
    <div class="card">
      <div class="card-title">Injuries — ${d.home.name}</div>
      <div>${injPills(d.home.injuries)}</div>
    </div>
    <div class="card">
      <div class="card-title">Injuries — ${d.away.name}</div>
      <div>${injPills(d.away.injuries)}</div>
    </div>
  </div>

  <!-- Adjustment Factors -->
  <div class="card" style="margin-top:16px">
    <div class="card-title">Model Adjustment Factors</div>
    <div class="team-split" style="margin-top:4px">
      <div>
        <div class="side-label">${d.home.name}</div>
        <div class="factors">
          ${factorChip("Motivation", m.motiv_h)}
          ${factorChip("Fatigue", m.fatigue_h)}
          ${factorChip("Injuries", m.inj_h)}
        </div>
      </div>
      <div>
        <div class="side-label">${d.away.name}</div>
        <div class="factors">
          ${factorChip("Motivation", m.motiv_a)}
          ${factorChip("Fatigue", m.fatigue_a)}
          ${factorChip("Injuries", m.inj_a)}
        </div>
      </div>
    </div>
    <div style="margin-top:12px;font-size:.75rem;color:var(--muted)">
      Data: ${d.home.matches} matches for ${d.home.name} · ${d.away.matches} matches for ${d.away.name}
      &nbsp;·&nbsp; NB dispersion r=${m.r_goals} · Dixon-Coles ρ=${m.rho}
    </div>
  </div>
  `;

  const el = document.getElementById("results");
  el.innerHTML = html;
  el.style.display = "block";
  el.scrollIntoView({behavior:"smooth"});
}

loadLeagues();
</script>
</body>
</html>"""

@app.route("/")
def index():
    return render_template_string(HTML)

if __name__ == "__main__":
    print("\n  ⚽  Football Predictor — Match Preview")
    print("  Open your browser at:  http://localhost:5050\n")
    app.run(host="0.0.0.0", port=5050, debug=False)
