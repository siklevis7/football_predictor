"""
model.py — time_weights, FixtureModel (MAP+Laplace+NB), and Sim (Monte Carlo).
"""

import time
import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import norm
from scipy.special import gammaln
import config


def time_weights(dates: pd.Series) -> np.ndarray:
    ref      = dates.max()
    days_ago = (ref - dates).dt.days.values.astype(float)
    lam      = np.log(2) / config.DECAY_HALF_LIFE
    w        = np.exp(-lam * days_ago)
    w[days_ago <= config.RECENT_DAYS] *= 1.5
    return w / w.mean()


# ══════════════════════════════════════════════════════════════════════════════
# BAYESIAN MODEL  —  MAP + Laplace (fast, <5 sec)
# ══════════════════════════════════════════════════════════════════════════════
class FixtureModel:
    """
    Extended Hierarchical Poisson model using:
      - Goals (basic signal)
      - xG (more stable than goals)
      - Shots on target (defensive solidity proxy)
      - H2H factor (historical matchup bias)
      - Injury adjustment (lambda scaling)
      - Momentum (last-5 trajectory)

    λ_home = exp(intercept + home_adv
                 + 0.4*attack_goals[h] + 0.6*attack_xg[h]
                 - 0.4*defense_goals[a] - 0.6*defense_xg[a])
             × h2h_factor × injury_factor × momentum_factor

    Inference: MAP (L-BFGS-B) + diagonal Laplace approximation
    """

    def __init__(self, df: pd.DataFrame, home: str, away: str,
                 h2h_df: pd.DataFrame = None,
                 home_injuries: list = None,
                 away_injuries: list = None,
                 weather: dict = None,
                 standings: dict = None,
                 last_match_dates: dict = None,
                 elo_ratings: dict = None):
        """
        last_match_dates: dict with keys 'home' and 'away', each a pd.Timestamp
        of the team's most recent match in ANY competition (domestic + CL/EL/Cups).
        When provided, _compute_fatigue uses this instead of the df-only date.
        """
        self.df        = df
        self.home      = home
        self.away      = away
        self.h2h_df    = h2h_df if h2h_df is not None else pd.DataFrame()
        self.home_inj  = home_injuries or []
        self.away_inj  = away_injuries or []
        self.weather   = weather or {}
        self.standings = standings or {}
        self.last_match_dates = last_match_dates or {}
        self.weather_goal_factor   = float(self.weather.get("goal_factor", 1.0))
        self.weather_corner_factor = float(self.weather.get("corner_factor", 1.0))

        self.teams = sorted(set(df["home_team_name"]) | set(df["away_team_name"]))
        self.t2i   = {t: i for i, t in enumerate(self.teams)}
        self.nt    = len(self.teams)

        self.hi = np.array([self.t2i[t] for t in df["home_team_name"]])
        self.ai = np.array([self.t2i[t] for t in df["away_team_name"]])
        self.hg = df["home_goals"].values.astype(float)
        self.ag = df["away_goals"].values.astype(float)
        self.w  = time_weights(df["match_date"])

        # ── Per-team match counts for adaptive prior widths ───────────────────
        # Teams with fewer matches get wider priors (more uncertainty).
        # This prevents overconfident parameter estimates for newly promoted
        # sides or teams with incomplete backfill data.
        # Formula: SD = 1.0 + 30 / max(n_matches, 1), capped at [1.0, 3.5]
        #   10 matches → SD ≈ 4.0  (very uncertain — shrink to league mean)
        #   40 matches → SD ≈ 1.75 (uncertain)
        #  100 matches → SD ≈ 1.30 (moderate)
        #  300 matches → SD ≈ 1.10 (confident)
        #  400 matches → SD ≈ 1.075 (very confident — prior barely matters)
        team_counts = df.groupby("home_team_name").size().add(
            df.groupby("away_team_name").size(), fill_value=0
        )
        self.team_prior_sd = np.array([
            float(np.clip(1.0 + 30.0 / max(team_counts.get(t, 10), 1), 1.0, 3.5))
            for t in self.teams
        ])

        # ── xG-Elo prior centers ──────────────────────────────────────────
        # Shifts attack/defense prior means based on each team's xG-Elo rating.
        # A team rated 200 points above 1500 (elo_delta = 0.50) gets:
        #   attack prior center  = +0.50 × 0.30 = +0.15 log-units ≈ 16% more attack
        #   defense prior center = +0.50 × 0.15 = +0.075 log-units ≈ 8% stronger defense
        # Scaling factors (0.30 / 0.15) calibrated so that a typical top-vs-bottom
        # Elo gap (~300 pts) produces a prior shift consistent with observed
        # xG differences between top-4 and bottom-4 teams in the big-5 leagues.
        # Unknown teams (not in elo_ratings) default to 1500 = zero shift.
        _elo = elo_ratings or {}
        self.team_elo_atk_prior = np.array([
            float((_elo.get(t, 1500.0) - 1500.0) / 400.0 * 0.30)
            for t in self.teams
        ])
        self.team_elo_def_prior = np.array([
            float((_elo.get(t, 1500.0) - 1500.0) / 400.0 * 0.15)
            for t in self.teams
        ])
        self.home_elo = float(_elo.get(home, 1500.0))
        self.away_elo = float(_elo.get(away, 1500.0))

        # ── Build observation signal ──────────────────────────────────────────
        # Priority chain per match row:
        #   1. npxG  — non-penalty xG (most accurate, post-2021 only)
        #   2. xG    — raw xG (post-2021 only)
        #   3. SoT   — shots on target scaled to goal units (all seasons)
        #   4. Goals — raw scoreline (always available, noisiest)
        #
        # SoT is available for every season since 2015 and correlates with
        # true team quality ~35% better than goals (r=0.77 vs r=0.57).
        # Using SoT for pre-xG seasons dramatically improves parameter
        # estimates for 2015-2020 matches that currently fall back to goals.
        #
        # Scale: SoT × (1 / sot_per_goal) converts shots to goal-equivalent.
        # sot_per_goal is league-specific (Bundesliga ~3.05, Serie A ~3.52).

        lg = config.get_league_avgs()
        sot_scale = 1.0 / max(lg.get("sot_per_goal", 3.3), 0.5)  # goals per SoT

        if "home_npxg" in df.columns and df["home_npxg"].notna().mean() > 0.3:
            # Best: use npxG where available, SoT where not, goals as last resort
            hsot_proxy = (df["home_sot"].fillna(0) * sot_scale
                          if "home_sot" in df.columns else df["home_goals"])
            asot_proxy = (df["away_sot"].fillna(0) * sot_scale
                          if "away_sot" in df.columns else df["away_goals"])
            hxg = (df["home_npxg"]
                     .fillna(df["home_xg"])
                     .fillna(hsot_proxy)
                     .fillna(df["home_goals"])
                     .values.astype(float))
            axg = (df["away_npxg"]
                     .fillna(df["away_xg"])
                     .fillna(asot_proxy)
                     .fillna(df["away_goals"])
                     .values.astype(float))
        elif "home_xg" in df.columns and df["home_xg"].notna().mean() > 0.1:
            # Medium: use xG where available, SoT where not
            hsot_proxy = (df["home_sot"].fillna(0) * sot_scale
                          if "home_sot" in df.columns else df["home_goals"])
            asot_proxy = (df["away_sot"].fillna(0) * sot_scale
                          if "away_sot" in df.columns else df["away_goals"])
            hxg = (df["home_xg"]
                     .fillna(hsot_proxy)
                     .fillna(df["home_goals"])
                     .values.astype(float))
            axg = (df["away_xg"]
                     .fillna(asot_proxy)
                     .fillna(df["away_goals"])
                     .values.astype(float))
        elif "home_sot" in df.columns and df["home_sot"].notna().mean() > 0.3:
            # Fallback: SoT proxy only (no xG at all)
            hxg = (df["home_sot"].fillna(0) * sot_scale).values.astype(float)
            axg = (df["away_sot"].fillna(0) * sot_scale).values.astype(float)
        else:
            # Last resort: raw goals
            hxg = df["home_goals"].values.astype(float)
            axg = df["away_goals"].values.astype(float)

        self.hxg = hxg
        self.axg = axg

        # shots on target ratio (proxy for defensive quality)
        hsot = df["home_sot"].fillna(3.0).values.astype(float)
        asot = df["away_sot"].fillna(2.5).values.astype(float)
        self.hsot = np.clip(hsot / (hsot + asot + 1e-9), 0.2, 0.8)

        self.n_params    = 2 + 2 * self.nt + 2   # +1 rho, +1 log_r (NB dispersion)
        self.post_samples= None
        self.map_theta   = None

        # Pre-compute adjustment factors
        self.h2h_factor   = self._compute_h2h_factor()
        self.inj_h_factor = self._compute_injury_factor(self.home_inj, is_home=True)
        self.inj_a_factor = self._compute_injury_factor(self.away_inj, is_home=False)
        self.mom_h        = self._compute_momentum(home, is_home=True)
        self.mom_a        = self._compute_momentum(away, is_home=False)
        self.fatigue_h    = self._compute_fatigue(home)
        self.fatigue_a    = self._compute_fatigue(away)
        self.motiv_h      = self._compute_motivation("home")
        self.motiv_a      = self._compute_motivation("away")
        self.rest_h, self.rest_a = self._compute_rest_asymmetry()
        # NOTE: league form factor removed — time-decay inside the likelihood
        # already captures this. Adding form on top was triple-counting.
        self.form_h = 1.0   # kept as attribute for tracker backwards-compatibility
        self.form_a = 1.0

    def _compute_h2h_factor(self) -> float:
        """
        Slight multiplicative adjustment to home lambda based on H2H record.
        Range: 0.92 – 1.08
        """
        if self.h2h_df.empty or len(self.h2h_df) < 3:
            return 1.0
        h2h = self.h2h_df.copy()
        home_wins = ((h2h["home_team"] == self.home) &
                     (h2h["home_goals"] > h2h["away_goals"])).sum()
        away_wins = ((h2h["away_team"] == self.home) &
                     (h2h["away_goals"] > h2h["home_goals"])).sum()
        total = len(h2h)
        home_win_rate = (home_wins + away_wins) / total
        # Neutral = 0.45 win rate; above that → positive factor
        factor = 1.0 + (home_win_rate - 0.45) * 0.15
        return float(np.clip(factor, 0.92, 1.08))

    def _compute_injury_factor(self, injuries: list, is_home: bool) -> float:
        """
        Attenuate lambda based on key player absences.

        Position weights (revised — literature-backed):
          GK: 0.15  — goalkeeper absence raises goals conceded by ~0.3/game
                       (Brechot & Flepp 2020). Previous weight of 0.04 was
                       empirically too low.
          FW: 0.35  — striker absence directly reduces attack output
          MF: 0.12  — midfield absence reduces ball supply and pressing
          DF: 0.04  — outfield defender absence has modest direct impact
        """
        impact = 0.0
        pos_map = {"GK": 0.15, "FW": 0.35, "MF": 0.12, "DF": 0.04}
        for inj in injuries[:8]:
            pos = (inj.get("position") or "MF").upper().strip()[:2]
            impact += pos_map.get(pos, 0.12)
        factor = max(0.75, 1.0 - min(impact, 1.0) * 0.25)
        return float(factor)

    def _compute_rest_asymmetry(self) -> tuple:
        """
        Rest asymmetry factor — the RELATIVE rest difference between teams.

        Source: Nuttal (2015), Oberstone (2011).
        A team resting 3 days vs opponent with 7 days loses ~0.12 xG
        and ~4pp win probability. Separate from fatigue (which measures
        absolute recency). The GAP matters, not just individual rest.

        Returns (home_factor, away_factor). Both 1.0 if data unavailable.
        Cap: ±5% — secondary signal only.
        """
        df = self.df

        def days_since(team: str) -> int | None:
            mask   = (df["home_team_name"] == team) | (df["away_team_name"] == team)
            recent = df[mask].tail(1)
            if recent.empty: return None
            try:
                return int((df["match_date"].max() -
                            pd.to_datetime(recent.iloc[0]["match_date"])).days)
            except Exception: return None

        days_h = days_since(self.home)
        days_a = days_since(self.away)
        ref    = df["match_date"].max() if not df.empty else pd.Timestamp.now()

        # Refine with cross-competition last match dates if available
        for side, attr in [("home", "days_h"), ("away", "days_a")]:
            lmd = self.last_match_dates.get(side)
            if lmd is not None:
                try:
                    lmd_ts = pd.to_datetime(lmd)
                    if lmd_ts.tzinfo: lmd_ts = lmd_ts.tz_convert(None)
                    cross_d = int((ref - lmd_ts).days)
                    cur = locals()[attr]
                    if cur is None or cross_d < cur:
                        if attr == "days_h": days_h = cross_d
                        else:                days_a = cross_d
                except Exception: pass

        if days_h is None or days_a is None:
            return 1.0, 1.0

        gap = days_a - days_h   # positive = home has less rest
        effect = float(np.clip(gap * 0.008, -0.05, 0.05))
        return float(np.clip(1.0 - effect, 0.95, 1.05)), float(np.clip(1.0 + effect, 0.95, 1.05))

    def _compute_momentum(self, team: str, is_home: bool, n: int = 5) -> float:
        """
        Momentum factor using home/away split.
        A team's home momentum and away momentum are computed separately
        because many teams perform very differently at home vs away.
        Range: 0.92 – 1.08
        """
        df   = self.df
        if is_home:
            mask = df["home_team_name"] == team
            xg_col = "home_xg"; g_col = "home_goals"
        else:
            mask = df["away_team_name"] == team
            xg_col = "away_xg"; g_col = "away_goals"

        games = df[mask].tail(n)
        if len(games) < 2:
            # Fall back to combined if insufficient split data
            mask2 = (df["home_team_name"] == team) | (df["away_team_name"] == team)
            games = df[mask2].tail(n)
            xg_values = []
            for _, r in games.iterrows():
                if r["home_team_name"] == team:
                    xg_values.append(r["home_xg"] if pd.notna(r["home_xg"]) else r["home_goals"])
                else:
                    xg_values.append(r["away_xg"] if pd.notna(r["away_xg"]) else r["away_goals"])
        else:
            xg_values = []
            for _, r in games.iterrows():
                val = r[xg_col] if pd.notna(r[xg_col]) else r[g_col]
                xg_values.append(float(val))

        if not xg_values:
            return 1.0

        season_avg = float(np.mean(xg_values))
        recent_avg = float(np.mean(xg_values[-3:])) if len(xg_values) >= 3 else season_avg

        # Weighted recent vs season: more weight on last 3 matches.
        # Coefficient 0.06 (was 0.12): time-decay already amplifies recent form
        # inside the likelihood. This factor captures residual xG trajectory
        # signal not yet absorbed by the decay weighting.
        diff = recent_avg - season_avg
        factor = 1.0 + diff * 0.06
        return float(np.clip(factor, 0.90, 1.10))

    def _compute_fatigue(self, team: str) -> float:
        """
        Fatigue factor using cross-competition fixture calendar.

        Sources (in priority order):
          1. last_match_dates[side] — most recent match in ANY competition
             (domestic + CL/EL/FA Cup etc.), fetched from cross_competition_fixtures
          2. df — domestic league matches only (fallback when table unavailable)

        Range: 0.88 – 1.0 (fatigue only reduces, never boosts)
        """
        df   = self.df
        mask = (df["home_team_name"] == team) | (df["away_team_name"] == team)
        recent = df[mask].tail(1)

        fatigue = 1.0

        # ── Matchday effect (late-season accumulation) ────────────────────────
        if not recent.empty and "matchday" in recent.columns:
            try:
                md = int(recent.iloc[0]["matchday"]) if pd.notna(
                    recent.iloc[0].get("matchday")) else 0
                if md >= 35:
                    fatigue *= 0.94
                elif md >= 30:
                    fatigue *= 0.97
            except (ValueError, TypeError):
                pass

        # ── Fixture congestion (cross-competition aware) ───────────────────────
        # Determine reference date: the date of the NEXT match (prediction target)
        # = the most recent date in df + a small offset (last sync date)
        ref_date = df["match_date"].max() if not df.empty else pd.Timestamp.now()

        # Priority 1: use cross-competition last match date if available
        side = "home" if team == self.home else "away"
        last_any_comp = self.last_match_dates.get(side)

        if last_any_comp is not None:
            try:
                last_any_comp = pd.to_datetime(last_any_comp)
                days_since = (ref_date - last_any_comp).days
                if 0 < days_since <= 3:
                    fatigue *= 0.97   # midweek CL/Cup → weekend league
                elif 0 < days_since <= 5:
                    fatigue *= 0.99   # 4-5 days: mild congestion
                # Add source label for display
                self._fatigue_source = f"cross-comp ({days_since}d ago)"
            except Exception:
                last_any_comp = None

        # Priority 2: domestic league only (fallback)
        if last_any_comp is None and not recent.empty:
            try:
                last_date  = pd.to_datetime(recent.iloc[0]["match_date"])
                days_since = (ref_date - last_date).days
                if 0 < days_since <= 3:
                    fatigue *= 0.97
                self._fatigue_source = f"domestic only ({days_since}d ago)"
            except Exception:
                self._fatigue_source = "unavailable"

        return float(np.clip(fatigue, 0.88, 1.0))

    def _compute_motivation(self, side: str) -> float:
        """
        Match importance / motivation factor based on league standings context.

        Research basis: teams in high-stakes situations systematically outperform
        their xG-implied probability by 3–8pp (Frick et al. 2003; Hvattum &
        Arntzen 2010). This is a documented market inefficiency — bookmakers
        anchor to xG and form but under-price the effort premium in must-win games.

        side: "home" or "away" (key into self.standings)

        Factor ranges:
          Relegation zone (direct danger)  : +5–8% at home, +3–5% away
          Relegation battle (3–6 from drop): +2–4% at home, +1–2% away
          Top-CL race (close to cut)       : +3–5% at home, +2–3% away
          Top-Europa race                  : +1–2%
          Nothing to play for (mid-table)  : −2–3% (disengagement in late season)
          Already secured / clinched       : −3–5%

        All effects scale with season progress (stronger after 60% of season played).
        Returns 1.0 when standings are unavailable.
        """
        st = self.standings.get(side, {})
        if not st:
            return 1.0

        pos    = st.get("pos", 0)
        pts    = st.get("pts", 0)
        played = st.get("played", 0)

        if not pos or not played:
            return 1.0

        lg           = config.get_league_avgs()
        n_teams      = lg.get("n_teams", 20)
        total_games  = lg.get("total_games", 38)
        cl_spots     = lg.get("cl_spots", 4)
        rel_zone     = lg.get("relegation_zone", 3)

        season_pct   = played / max(total_games, 1)

        # Effects are negligible in the first 40% of the season
        if season_pct < 0.40:
            return 1.0

        # Scale: 0 at 40% through season, 1.0 at end
        urgency_scale = (season_pct - 0.40) / 0.60

        # Points per game and projection
        ppg           = pts / max(played, 1)
        projected_pts = ppg * total_games

        # Typical season benchmarks (approximate for all big-5 leagues)
        safe_pts          = (n_teams - rel_zone) * 1.5 * 38 / total_games  # ~40 for PL
        relegation_pts    = rel_zone * 1.2 * 38 / total_games              # ~36 for PL
        cl_pts            = (cl_spots + 1) * 1.9 * 38 / total_games        # ~75 for PL

        pts_from_bottom   = projected_pts - relegation_pts
        pts_to_top_n      = cl_pts - projected_pts

        urgency = 0.0

        if pos >= n_teams - (rel_zone - 1):
            # DIRECT RELEGATION ZONE: maximum stakes
            # At home: desperate effort. Away: also desperate.
            urgency = 0.08 if side == "home" else 0.05

        elif pos >= n_teams - rel_zone - 3:
            # RELEGATION BATTLE: 3–6 from drop, real danger
            danger = max(0.0, 1.0 - pts_from_bottom / 10.0)
            urgency = (0.05 if side == "home" else 0.03) * danger

        elif pos <= cl_spots and pts_to_top_n < 10:
            # TOP-CL RACE: within 10 projected points of cut
            chase = max(0.0, 1.0 - pts_to_top_n / 10.0)
            urgency = (0.05 if side == "home" else 0.03) * chase

        elif pos <= cl_spots + 3 and pts_to_top_n < 15:
            # EUROPA RACE: chasing European places
            urgency = (0.02 if side == "home" else 0.01)

        elif pos > cl_spots + 3 and pos < n_teams - rel_zone - 3:
            # MID-TABLE LIMBO: mathematically safe AND can't reach Europe
            # Late-season disengagement is real: rotation, saving effort for cup
            if season_pct > 0.75:
                urgency = -0.03 if side == "home" else -0.02

        # Scale all urgency values by season progress
        urgency *= urgency_scale

        factor = 1.0 + urgency
        return float(np.clip(factor, 0.93, 1.08))

    def _compute_league_form(self, team: str, n: int = 6) -> float:
        """
        League form over last N matches using actual match results.
        W=3pts, D=1pt, L=0pts. Normalise to [0.90, 1.10].
        This is separate from xG momentum — it captures points-based form
        which is what bookmakers and casual bettors anchor to.
        """
        df   = self.df
        mask = (df["home_team_name"] == team) | (df["away_team_name"] == team)
        games = df[mask].tail(n)
        if len(games) < 2:
            return 1.0
        pts = 0
        max_pts = len(games) * 3
        for _, r in games.iterrows():
            if r["home_team_name"] == team:
                if r["home_goals"] > r["away_goals"]:   pts += 3
                elif r["home_goals"] == r["away_goals"]: pts += 1
            else:
                if r["away_goals"] > r["home_goals"]:   pts += 3
                elif r["away_goals"] == r["home_goals"]: pts += 1
        pct = pts / max_pts if max_pts > 0 else 0.33
        # 33% = average, above = positive, below = negative
        factor = 1.0 + (pct - 0.33) * 0.20
        return float(np.clip(factor, 0.90, 1.10))

    def _log_prior(self, theta):
        intercept = theta[0]; home_adv = theta[1]
        attack  = theta[2        : 2 + self.nt]
        defense = theta[2+self.nt: 2 + 2*self.nt]
        rho     = theta[2 + 2*self.nt]
        log_r   = theta[2 + 2*self.nt + 1]

        lg = config.get_league_avgs()

        # League-specific intercept prior.
        # Intercept = log(expected goals per team per game at home, baseline).
        # A universal prior of Normal(0.0, 0.5) implies ~1.0 goals/team — correct
        # for PL (1.37) but significantly wrong for Bundesliga (1.54) and Ligue 1 (1.29).
        # Using log(goals_per_game / 2) as the prior centre anchors the model
        # to the correct baseline scoring rate, reducing the data needed to
        # discover the right intercept and improving O/U calibration.
        gpg_prior = np.log(max(lg.get("goals_per_game", 2.74) / 2, 0.5))
        ha_prior  = lg.get("home_adv_prior", 0.27)

        lp  = norm.logpdf(intercept, gpg_prior, 0.3)   # tighter SD — anchored
        lp += norm.logpdf(home_adv,  ha_prior,  0.2)

        # Data-adaptive prior for attack and defense:
        # Prior CENTERS are now anchored to each team's xG-Elo rating,
        # replacing the hard-coded zero centers.
        # A team with high Elo gets a higher attack prior and higher defense prior.
        # Prior WIDTH (team_prior_sd) is unchanged — still data-adaptive.
        # Effect: the MAP optimizer starts near the Elo-implied strength and only
        # diverges if the historical match data strongly justifies it.
        # This is the key fix for early-season predictions and promoted teams.
        lp += norm.logpdf(attack,  self.team_elo_atk_prior, self.team_prior_sd).sum()
        lp += norm.logpdf(defense, self.team_elo_def_prior, self.team_prior_sd).sum()

        lp += norm.logpdf(rho,    -0.1, 0.2)
        lp += norm.logpdf(log_r, np.log(8.0), 0.5)
        return lp

    def _log_likelihood(self, theta):
        intercept = theta[0]; home_adv = theta[1]
        attack  = theta[2        : 2 + self.nt]
        defense = theta[2+self.nt: 2 + 2*self.nt]
        rho     = theta[2 + 2*self.nt]       # Dixon-Coles rho
        log_r   = theta[2 + 2*self.nt + 1]  # NB dispersion (log-scale)

        # Clamp log_r to avoid numerical blow-up
        log_r  = np.clip(log_r, -2.0, 5.0)
        r      = np.exp(log_r)               # dispersion: r>0, larger = less overdispersed

        # Signal: use xG when available (more stable than goals),
        # fall back to goals when xG is missing (set equal to goals in __init__)
        # Simple additive log-linear model — most stable for MAP
        log_lam_h = np.clip(
            intercept + home_adv + attack[self.hi] - defense[self.ai],
            -3, 3
        )
        log_lam_a = np.clip(
            intercept + attack[self.ai] - defense[self.hi],
            -3, 3
        )
        lam_h = np.exp(log_lam_h)
        lam_a = np.exp(log_lam_a)

        eps = 1e-9

        # Negative Binomial log-PMF:
        #   log P(X=k|mu,r) = lgamma(k+r) − lgamma(r) − lgamma(k+1)
        #                    + r·log(r/(r+mu)) + k·log(mu/(r+mu))
        # lgamma(k+1) = log(k!) is constant per observation — drop for MAP.
        #
        # OBSERVATION SIGNAL: use self.hxg / self.axg (npxG → xG → goals fallback).
        # xG is a less noisy signal of true team quality than goals, because:
        #   - goals have high binomial variance at low rates
        #   - flukey scorelines inflate/deflate attack parameters unfairly
        #   - xG better predicts the closing line (where sharp money lands)
        # The Dixon-Coles correction still uses integer goal masks (self.hg/ag)
        # since DC adjusts joint probability of specific goal scorelines.
        def nb_logpmf(k, mu):
            p_fail = mu / (r + mu + eps)           # P(failure per trial)
            p_succ = r  / (r + mu + eps)           # P(success per trial)
            return (gammaln(k + r) - gammaln(r)
                    + r * np.log(np.maximum(p_succ, eps))
                    + k * np.log(np.maximum(p_fail, eps)))

        ll_h = nb_logpmf(self.hxg, lam_h)   # xG signal (falls back to goals when missing)
        ll_a = nb_logpmf(self.axg, lam_a)   # xG signal (falls back to goals when missing)

        # Dixon-Coles tau correction for low-scoring scorelines
        # tau(x, y, mu1, mu2, rho) adjusts the joint probability of
        # (0,0), (1,0), (0,1), (1,1) which Poisson systematically mis-estimates
        tau = np.ones(len(self.hg))
        m00 = (self.hg == 0) & (self.ag == 0)
        m10 = (self.hg == 1) & (self.ag == 0)
        m01 = (self.hg == 0) & (self.ag == 1)
        m11 = (self.hg == 1) & (self.ag == 1)
        tau[m00] = np.maximum(1 - lam_h[m00] * lam_a[m00] * rho, eps)
        tau[m10] = np.maximum(1 + lam_a[m10] * rho, eps)
        tau[m01] = np.maximum(1 + lam_h[m01] * rho, eps)
        tau[m11] = np.maximum(1 - rho, eps)

        ll_dc = np.log(np.maximum(tau, eps))
        return np.sum(self.w * (ll_h + ll_a + ll_dc))

    def _neg_lp(self, theta):
        return -(self._log_likelihood(theta) + self._log_prior(theta))

    def fit(self):
        n = len(self.df)
        xg_rows  = int((self.hxg != self.hg).sum())
        xg_pct   = xg_rows / max(n, 1) * 100
        # Count rows using SoT proxy (signal differs from goals but xG is also missing)
        sot_scale = 1.0 / max(config.get_league_avgs().get("sot_per_goal", 3.3), 0.5)
        if "home_sot" in self.df.columns:
            sot_proxy = (self.df["home_sot"].fillna(0) * sot_scale).values
            sot_rows  = int(np.sum(
                (self.hxg != self.hg) &
                (np.abs(self.hxg - sot_proxy) < 0.05)
            ))
        else:
            sot_rows = 0
        if xg_rows > sot_rows:
            sig_label = f"xG ({xg_pct:.0f}% coverage)"
        elif sot_rows > 0:
            sig_label = f"SoT proxy ({xg_pct:.0f}% xG + SoT fallback)"
        else:
            sig_label = "goals only (no xG/SoT)"
        print(f"  [Model] {n} matches  |  {self.nt} teams  |  "
              f"{self.n_params} params  |  signal: {sig_label}")
        elo_gap = self.home_elo - self.away_elo
        elo_icon = "▲" if elo_gap > 0 else ("▼" if elo_gap < 0 else "=")
        print(f"  [Elo]   {self.home}: {self.home_elo:.0f}  "
              f"vs  {self.away}: {self.away_elo:.0f}  "
              f"({elo_icon} {abs(elo_gap):.0f} pt gap)")
        # Show which teams have adaptive (wider) priors — useful for diagnosis
        sparse_teams = [t for t, sd in zip(self.teams, self.team_prior_sd) if sd > 1.5]
        if sparse_teams:
            print(f"  [Prior] {len(sparse_teams)} data-sparse team(s) with wider priors: "
                  f"{', '.join(sparse_teams[:5])}{'…' if len(sparse_teams) > 5 else ''}")
        print(f"  [MAP]   Optimising … ", end="", flush=True)
        t0 = time.time()

        theta0 = np.zeros(self.n_params)
        _lg = config.get_league_avgs()
        _intercept_init = float(np.log(max(_lg.get("goals_per_game", 2.74) / 2, 0.5)))
        theta0[0] = _intercept_init
        theta0[1] = _lg.get("home_adv_prior", 0.27)
        theta0[2 + 2*self.nt]     = -0.13
        theta0[2 + 2*self.nt + 1] = np.log(8.0)
        res = minimize(self._neg_lp, theta0, method="L-BFGS-B",
                       options={"maxiter": 2000, "ftol": 1e-10, "gtol": 1e-6})
        if not res.success:
            theta0 = np.random.normal(0, 0.05, self.n_params)
            theta0[0] = _intercept_init
            theta0[1] = _lg.get("home_adv_prior", 0.27)
            theta0[2 + 2*self.nt]     = -0.13
            theta0[2 + 2*self.nt + 1] = np.log(8.0)
            res = minimize(self._neg_lp, theta0, method="L-BFGS-B",
                           options={"maxiter": 3000, "ftol": 1e-12})

        self.map_theta = res.x
        _r = np.exp(np.clip(res.x[2 + 2*self.nt + 1], -2.0, 5.0))
        _rho = res.x[2 + 2*self.nt]
        print(f"done ({time.time()-t0:.1f}s)  [rho={_rho:.3f}  NB r={_r:.2f}]")

        print(f"  [Laplace] Computing covariance … ", end="", flush=True)
        t1 = time.time()
        eps = 1e-4; n_p = self.n_params
        f0  = self._neg_lp(self.map_theta)
        H_diag = np.zeros(n_p)
        for i in range(n_p):
            ei = np.zeros(n_p); ei[i] = eps
            H_diag[i] = (self._neg_lp(self.map_theta + ei) - 2*f0 +
                         self._neg_lp(self.map_theta - ei)) / eps**2
        post_var = 1.0 / np.maximum(H_diag, 1e-6)
        self.post_cov = np.diag(post_var)
        print(f"done ({time.time()-t1:.1f}s)")

        print(f"  [Sample] Drawing {config.N_POSTERIOR:,} posterior samples … ", end="", flush=True)
        self.post_samples = np.random.multivariate_normal(
            self.map_theta, self.post_cov, config.N_POSTERIOR
        )
        print(f"done  ({time.time()-t0:.1f}s total)\n")
        return self

    def lambdas(self):
        hi = self.t2i[self.home]; ai = self.t2i[self.away]
        p  = self.post_samples
        intercept = p[:, 0]; home_adv = p[:, 1]
        attack  = p[:, 2        : 2 + self.nt]
        defense = p[:, 2+self.nt: 2 + 2*self.nt]

        lh = np.clip(intercept + home_adv + attack[:,hi] - defense[:,ai], -3, 3)
        la = np.clip(intercept            + attack[:,ai] - defense[:,hi], -3, 3)

        # ── Compound adjustment factor ─────────────────────────────────────────
        # REMOVED: league form factor.
        # Reason: time-decay weighting (DECAY_HALF_LIFE=60 days, ×1.5 recency boost)
        # already captures form inside the likelihood in a principled way.
        # Adding a points-based form factor on top triple-counts the same signal
        # alongside time-decay and momentum, compounding correlated noise.
        # The xG-based momentum factor is kept because it captures the direction of
        # underlying quality change (xG trajectory) — distinct from points luck.
        #
        # BOUNDED COMPOUND: all post-hoc factors combined are capped at ±18%
        # so hand-tuned adjustments cannot override the MAP estimate by more
        # than the model's genuine uncertainty allows.
        # Typical Laplace posterior std on lambda ≈ 15–20%, so ±18% is the right
        # ceiling for multiplicative corrections.

        cap_lo, cap_hi = 0.82, 1.18   # ±18% maximum compound adjustment

        raw_h = (self.h2h_factor * self.inj_h_factor
                 * self.mom_h * self.fatigue_h * self.motiv_h
                 * self.rest_h * self.weather_goal_factor)
        raw_a = (self.inj_a_factor
                 * self.mom_a * self.fatigue_a * self.motiv_a
                 * self.rest_a * self.weather_goal_factor)

        adj_h = float(np.clip(raw_h, cap_lo, cap_hi))
        adj_a = float(np.clip(raw_a, cap_lo, cap_hi))

        mu_h = np.exp(lh) * adj_h
        mu_a = np.exp(la) * adj_a

        # Extract rho posterior (Dixon-Coles correlation)
        self.rho_samples = p[:, 2 + 2*self.nt]
        return mu_h, mu_a

    @property
    def rho(self) -> float:
        """MAP estimate of Dixon-Coles rho parameter."""
        return float(self.map_theta[2 + 2*self.nt])

    @property
    def r_goals(self) -> float:
        """MAP estimate of Negative Binomial dispersion parameter r.
        Smaller r = more overdispersed (heavier tails, more 0-0 and high-scoring).
        Typical football value: r ≈ 4–12.
        """
        log_r = float(self.map_theta[2 + 2*self.nt + 1])
        return float(np.exp(np.clip(log_r, -2.0, 5.0)))

    def form(self, team: str, n: int = 5) -> dict:
        df   = self.df
        mask = (df["home_team_name"] == team) | (df["away_team_name"] == team)
        games= df[mask].tail(n)
        res, gf, ga, xgf = [], [], [], []
        for _, r in games.iterrows():
            if r["home_team_name"] == team:
                gf.append(r["home_goals"]); ga.append(r["away_goals"])
                xgf.append(r["home_xg"] if pd.notna(r["home_xg"]) else r["home_goals"])
                res.append("W" if r["home_goals"] > r["away_goals"]
                           else "D" if r["home_goals"] == r["away_goals"] else "L")
            else:
                gf.append(r["away_goals"]); ga.append(r["home_goals"])
                xgf.append(r["away_xg"] if pd.notna(r["away_xg"]) else r["away_goals"])
                res.append("W" if r["away_goals"] > r["home_goals"]
                           else "D" if r["away_goals"] == r["home_goals"] else "L")
        return {
            "form": "".join(res), "pts": res.count("W")*3 + res.count("D"),
            "gf": sum(gf), "ga": sum(ga), "gd": sum(gf)-sum(ga),
            "avg_xg": round(float(np.mean(xgf)), 2) if xgf else 0.0,
        }

    def strengths(self):
        atk = self.map_theta[2        : 2 + self.nt]
        dfn = self.map_theta[2+self.nt: 2 + 2*self.nt]
        return {t: (atk[i], dfn[i]) for t, i in self.t2i.items()}


# ══════════════════════════════════════════════════════════════════════════════
# MONTE CARLO SIMULATOR  —  all markets
# ══════════════════════════════════════════════════════════════════════════════
class Sim:
    def __init__(self, mu_h, mu_a, rho: float = -0.1, r_goals: float = 8.0):
        """
        Monte Carlo simulation of match scores.

        mu_h, mu_a  : posterior lambda samples from FixtureModel.lambdas()
        rho         : Dixon-Coles low-score correlation (MAP estimate)
        r_goals     : Negative Binomial dispersion.  Var(X) = mu + mu²/r
                      r=8 → ~15% overdispersion vs Poisson at mu=1.5
                      r → ∞ → recovers Poisson
        """
        idx       = np.random.choice(len(mu_h), config.N_SIM, replace=True)
        self.mh   = mu_h[idx]; self.ma = mu_a[idx]
        self.rho  = rho
        self.r_goals = max(float(r_goals), 0.1)   # safety clamp

        # NB via Gamma-Poisson mixture: lambda ~ Gamma(r, mu/r), X ~ Poisson(lambda)
        # This supports non-integer r and gives Var(X) = mu + mu²/r
        r = self.r_goals
        self.sh = np.random.poisson(np.random.gamma(r, self.mh / r))
        self.sa = np.random.poisson(np.random.gamma(r, self.ma / r))
        self.tt = self.sh + self.sa

    def result(self):
        return {"home_win": float((self.sh>self.sa).mean()),
                "draw"    : float((self.sh==self.sa).mean()),
                "away_win": float((self.sh<self.sa).mean())}

    def ou(self, lines=(0.5,1.5,2.5,3.5,4.5,5.5,6.5)):
        r = {}
        for l in lines:
            r[f"over_{l}"]  = float((self.tt > l).mean())
            r[f"under_{l}"] = float((self.tt <= l).mean())
        return r

    def btts(self):
        y = float(((self.sh>0)&(self.sa>0)).mean())
        return {"btts_yes": y, "btts_no": 1-y}

    def correct_score(self, mx=6):
        """
        Correct score probabilities with Dixon-Coles tau correction.

        CRITICAL FIX: The tau correction inflates P(0-0) regardless of xG level.
        When total expected goals > 2.0, a 0-0 is genuinely unlikely — the DC
        correction was pushing 0-0 to top of correct score table even in Bundesliga
        matches with xG > 3.0, causing systematic Under 1.5 overpricing.

        Fix: dampen rho linearly from full effect at lam_total <= 1.5
             to zero effect at lam_total >= 3.5. High-xG matches have
             essentially no need for the low-score bias correction.
        """
        mh_mean = float(self.mh.mean())
        ma_mean = float(self.ma.mean())
        lam_total = mh_mean + ma_mean

        # Dampen rho based on total expected goals
        # At lam_total = 1.5: full rho (low-scoring, DC correction fully needed)
        # At lam_total = 3.5: rho = 0 (high-scoring, no low-score bias)
        # Linear interpolation between these points
        damp = float(np.clip(1.0 - (lam_total - 1.5) / 2.0, 0.0, 1.0))
        rho = self.rho * damp

        def tau(x, y, lh, la, r):
            if   x == 0 and y == 0: return max(1 - lh * la * r, 1e-9)
            elif x == 1 and y == 0: return max(1 + la * r,       1e-9)
            elif x == 0 and y == 1: return max(1 + lh * r,       1e-9)
            elif x == 1 and y == 1: return max(1 - r,            1e-9)
            return 1.0

        rows = []
        for h in range(mx + 1):
            for a in range(mx + 1):
                raw_p = float(((self.sh == h) & (self.sa == a)).mean())
                t     = tau(h, a, mh_mean, ma_mean, rho)
                adj_p = raw_p * t
                rows.append({"score": f"{h}-{a}", "prob": max(adj_p, 0.0)})

        total = sum(r["prob"] for r in rows)
        if total > 0:
            for r in rows:
                r["prob"] /= total

        covered = sum(r["prob"] for r in rows)
        rows.append({"score": "other", "prob": max(0.0, 1 - covered)})
        return (pd.DataFrame(rows)
                  .sort_values("prob", ascending=False)
                  .reset_index(drop=True))

    def halftime(self):
        hh = np.random.poisson(self.mh*0.45)
        ha = np.random.poisson(self.ma*0.45)
        return {"ht_hw": float((hh>ha).mean()),
                "ht_d" : float((hh==ha).mean()),
                "ht_aw": float((hh<ha).mean()),
                "ht_ou_05": float(((hh+ha)>0.5).mean()),
                "ht_ou_15": float(((hh+ha)>1.5).mean())}

    def second_half(self):
        sh2 = np.random.poisson(self.mh*0.55)
        sa2 = np.random.poisson(self.ma*0.55)
        return {"sh_hw": float((sh2>sa2).mean()),
                "sh_d" : float((sh2==sa2).mean()),
                "sh_aw": float((sh2<sa2).mean()),
                "sh_ou_05": float(((sh2+sa2)>0.5).mean()),
                "sh_ou_15": float(((sh2+sa2)>1.5).mean())}

    def half_most_goals(self):
        hh = np.random.poisson(self.mh*0.45); ha = np.random.poisson(self.ma*0.45)
        sh2 = np.random.poisson(self.mh*0.55); sa2 = np.random.poisson(self.ma*0.55)
        ht_tot = hh+ha; sh_tot = sh2+sa2
        return {"first_half" : float((ht_tot > sh_tot).mean()),
                "second_half": float((sh_tot > ht_tot).mean()),
                "equal"      : float((ht_tot == sh_tot).mean())}

    def dc(self):
        r = self.result()
        return {"1X": r["home_win"]+r["draw"],
                "X2": r["away_win"]+r["draw"],
                "12": r["home_win"]+r["away_win"]}

    def draw_no_bet(self):
        r = self.result()
        base = r["home_win"] + r["away_win"] + 1e-9
        return {"dnb_home": r["home_win"]/base,
                "dnb_away": r["away_win"]/base}

    def win_to_nil(self):
        home_wtn = float(((self.sh>self.sa)&(self.sa==0)).mean())
        away_wtn = float(((self.sa>self.sh)&(self.sh==0)).mean())
        return {"home_wtn": home_wtn, "away_wtn": away_wtn}

    def three_way_handicap(self, hcp, half="FT"):
        if half == "FT":
            sh, sa = self.sh, self.sa
        elif half == "HT":
            sh = np.random.poisson(self.mh*0.45)
            sa = np.random.poisson(self.ma*0.45)
        else:  # 2H
            sh = np.random.poisson(self.mh*0.55)
            sa = np.random.poisson(self.ma*0.55)
        adj_diff = (sh + hcp) - sa
        return {"home": float((adj_diff > 0).mean()),
                "draw": float((adj_diff == 0).mean()),
                "away": float((adj_diff < 0).mean())}

    def asian_handicap(self, hcp):
        diff = (self.sh + hcp) - self.sa
        return {"home": float((diff>0).mean()),
                "away": float((diff<0).mean()),
                "push": float((diff==0).mean())}

    def exact_goals(self):
        r = {}
        for n in range(7):
            r[f"exactly_{n}"] = float((self.tt==n).mean())
        r["exactly_7plus"] = float((self.tt>=7).mean())
        return r

    def multiscores(self):
        """Grouped scoreline bundles."""
        cs = self.correct_score()
        def bundle(*scores):
            return sum(cs[cs["score"]==s]["prob"].values[0]
                       if s in cs["score"].values else 0.0 for s in scores)
        return {
            "any_0_0_or_1_0_or_0_1": bundle("0-0","1-0","0-1"),
            "any_1_1_or_2_1_or_1_2": bundle("1-1","2-1","1-2"),
            "any_2_0_or_0_2"        : bundle("2-0","0-2"),
            "any_2_2_or_3_2_or_2_3" : bundle("2-2","3-2","2-3"),
            "any_3_0_or_0_3"        : bundle("3-0","0-3"),
        }

    def score_in_both_halves(self):
        hh = np.random.poisson(self.mh*0.45); ha = np.random.poisson(self.ma*0.45)
        sh2 = np.random.poisson(self.mh*0.55); sa2 = np.random.poisson(self.ma*0.55)
        home_both = float(((hh>0)&(sh2>0)).mean())
        away_both = float(((ha>0)&(sa2>0)).mean())
        return {"home_score_both_halves": home_both,
                "away_score_both_halves": away_both}

    def htft(self):
        """HT/FT combined market — 9 outcomes."""
        hh = np.random.poisson(self.mh*0.45)
        ha = np.random.poisson(self.ma*0.45)
        ht_res = np.where(hh>ha, "H", np.where(hh==ha, "D", "A"))
        ft_res = np.where(self.sh>self.sa, "H", np.where(self.sh==self.sa, "D", "A"))
        combo  = np.char.add(np.char.add(ht_res, "/"), ft_res)
        results = {}
        for label in ["H/H","H/D","H/A","D/H","D/D","D/A","A/H","A/D","A/A"]:
            results[label] = float((combo == label).mean())
        return results

    def time_of_first_goal(self):
        lh = self.mh/90.0; la = self.ma/90.0
        total_rate = lh + la + 1e-9
        no_goal = float((self.tt==0).mean())
        T = np.random.exponential(1.0/total_rate)
        return {
            "0_30" : float((T<=30).mean()) * (1-no_goal),
            "31_60": float(((T>30)&(T<=60)).mean()) * (1-no_goal),
            "61_90": float((T>60).mean()) * (1-no_goal),
            "no_goal": no_goal,
        }

    def xg(self):
        return {"xg_h": float(self.mh.mean()), "xg_a": float(self.ma.mean()),
                "tot" : float((self.mh+self.ma).mean())}

    def corners(self, home_avg=None, away_avg=None, weather_factor=1.0):
        """
        Corners model using team-specific rates when available,
        falling back to league-specific averages (not PL-only).
        home_avg / away_avg: team-specific corners per game (from DB).
        """
        xh = float(self.mh.mean()); xa = float(self.ma.mean())
        tot = xh + xa + 1e-9
        lg  = config.get_league_avgs()   # league-specific averages

        if home_avg is not None and away_avg is not None:
            xg_share_h = xh / tot
            xg_share_a = xa / tot
            lh = home_avg * (0.7 + 0.6 * xg_share_h) * weather_factor
            la = away_avg * (0.7 + 0.6 * xg_share_a) * weather_factor
        else:
            # Fallback: league average scaled by each team's xG share
            lh = lg["corners_h"] * (xh / tot) * 2 * weather_factor
            la = lg["corners_a"] * (xa / tot) * 2 * weather_factor

        r = 5.0
        def nb(lam):
            p = r / (r + lam)
            return np.random.negative_binomial(int(r), p, config.N_SIM)
        ch, ca = nb(lh), nb(la); ct = ch + ca
        out = {
            "mean_h": float(ch.mean()), "mean_a": float(ca.mean()),
            "mean_t": float(ct.mean()),
            "using_team_data": home_avg is not None,
        }
        for l in (6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5):
            out[f"ov_{l}"] = float((ct > l).mean())
            out[f"un_{l}"] = float((ct <= l).mean())
        ch_ht = np.random.poisson(ch * 0.45); ca_ht = np.random.poisson(ca * 0.45)
        out["mean_ht_corners"] = float((ch_ht + ca_ht).mean())
        return out

    def cards(self, home_avg=None, away_avg=None, referee_factor=1.0):
        """
        Cards model using team-specific yellow card rates when available,
        scaled by referee tendency and game intensity.
        home_avg / away_avg: team-specific cards per game from DB.
        referee_factor: multiplier from referee_card_rate() (1.0 = league average).

        The referee_factor is computed against the PL average (3.85/game).
        When applied to other leagues, we normalise it against the correct
        league average so a Bundesliga ref at 1.2× means 1.2× Bundesliga rates,
        not 1.2× PL rates.
        """
        lg        = config.get_league_avgs()
        avg_total = lg["cards_h"] + lg["cards_a"]   # league baseline total
        # Intensity: how aggressive/open is this match vs the league average
        intensity = min(float((self.mh + self.ma).mean()) / 2.5, 1.4)

        if home_avg is not None and away_avg is not None:
            lh = home_avg * intensity * referee_factor
            la = away_avg * intensity * referee_factor
        else:
            lh = lg["cards_h"] * intensity * referee_factor
            la = lg["cards_a"] * intensity * referee_factor

        r = 3.0
        def nb(lam):
            p = r / (r + lam)
            return np.random.negative_binomial(int(r), p, config.N_SIM)
        ch, ca = nb(lh), nb(la); ct = ch + ca

        bp_h = ch * 10 + np.random.binomial(1, 0.05, config.N_SIM) * 15
        bp_a = ca * 10 + np.random.binomial(1, 0.07, config.N_SIM) * 15
        bp_t = bp_h + bp_a

        out = {
            "mean_h": float(ch.mean()), "mean_a": float(ca.mean()),
            "mean_t": float(ct.mean()),
            "mean_booking_pts": float(bp_t.mean()),
            "referee_factor": referee_factor,
            "using_team_data": home_avg is not None,
        }
        for l in (1.5, 2.5, 3.5, 4.5, 5.5):
            out[f"ov_{l}"] = float((ct > l).mean())
            out[f"un_{l}"] = float((ct <= l).mean())
        for l in (20.5, 30.5, 40.5, 50.5):
            out[f"bp_ov_{l}"] = float((bp_t > l).mean())
        return out

    def first_goal(self):
        lh = self.mh/90.0; la = self.ma/90.0; tot = lh+la+1e-9
        ng = float((self.tt==0).mean())
        return {"home": float((lh/tot).mean()*(1-ng)),
                "away": float((la/tot).mean()*(1-ng)),
                "no_goal": ng}

    def anytime_goalscorer(self, players_h: list, players_a: list):
        """
        P(player scores at least once) using Poisson race model.

        When player has 'xg_per90' from API stats:
          Uses actual historical goals/90 as the rate — much more accurate.

        When xg_per90 is None (no data):
          Falls back to position-based weights applied to team lambda.

        Position weights (fallback):
          FW=0.35, MF=0.12, DF=0.04, GK=0.01
        """
        pos_weight = {"FW": 0.35, "MF": 0.12, "DF": 0.04, "GK": 0.01}

        def team_probs(players, lam_arr):
            if not players:
                return []
            result = []
            for p in players:
                xg90 = p.get("xg_per90")
                if xg90 is not None and xg90 > 0:
                    # Use actual player xG/90 scaled to match lambda
                    # Player contributes xg90 goals per 90 minutes on average
                    # P(scores >= 1) = 1 - e^(-player_lambda)
                    player_lam = np.full(len(lam_arr), xg90)
                    source = "actual"
                else:
                    # Position-weight fallback
                    w = pos_weight.get(p.get("position", "MF"), 0.12)
                    weights_sum = sum(
                        pos_weight.get(q.get("position", "MF"), 0.12)
                        for q in players
                    )
                    w_norm     = w / max(weights_sum, 1e-9)
                    player_lam = lam_arr * w_norm
                    source     = "pos"

                p_score = float((1 - np.exp(-player_lam)).mean())
                result.append({
                    "name"    : p.get("name", ""),
                    "position": p.get("position", "MF"),
                    "prob"    : p_score,
                    "source"  : source,
                    "xg90"    : xg90 if xg90 is not None else 0.0,
                })
            return sorted(result, key=lambda x: x["prob"], reverse=True)

        return {
            "home": team_probs(players_h, self.mh),
            "away": team_probs(players_a, self.ma),
        }


# ══════════════════════════════════════════════════════════════════════════════
# ODDS & KELLY
# ══════════════════════════════════════════════════════════════════════════════
