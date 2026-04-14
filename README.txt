============================================================
  BAYESIAN FOOTBALL PREDICTION ENGINE  v6.0
  Setup Guide for Anaconda Prompt (Windows)
============================================================

STEP 1 — Install Python dependencies
─────────────────────────────────────
Open Anaconda Prompt and run:

  pip install numpy pandas scipy requests openpyxl mysql-connector-python python-dotenv

STEP 2 — Set up the MySQL database
────────────────────────────────────
Open Anaconda Prompt and run:

  mysql -u root -p < db_setup.sql

Enter your MySQL password when prompted.
This creates the football_predictor database and all tables.

STEP 3 — Run the script for the first time
───────────────────────────────────────────
In Anaconda Prompt, navigate to the script folder:

  cd C:\path\to\your\folder

Then run:

  python football_predictor.py

On first run you will be asked to enter:
  - API-Football key    (from dashboard.api-football.com)
  - football-data.org key (from football-data.org, free)
  - MySQL host          (press Enter for localhost)
  - MySQL port          (press Enter for 3306)
  - MySQL username      (press Enter for root)
  - MySQL password      (your password)
  - MySQL database      (press Enter for football_predictor)
  - Starting bankroll   (press Enter for 20000 RWF)

These are saved to a .env file and never asked again.

STEP 4 — What happens on startup every day
───────────────────────────────────────────
1. Connects to MySQL
2. Checks for new finished matches and adds them
3. Shows backfill progress (how many matches have rich stats)
4. Uses remaining API budget to enrich matches with:
   shots, xG, corners, cards, lineups, events
5. Fetches upcoming Premier League fixtures
6. Waits for your fixture input

STEP 5 — Entering a prediction
────────────────────────────────
Type the fixture when prompted:

  Arsenal vs Man City

Then:
  - Enter your current bankroll in RWF
  - Choose whether to enter lineups (y/n)
  - If yes, enter each player as:  Name,POS
    Examples:  Saka,FW   or   Odegaard,MF   or   White,DF
  - Enter bookmaker odds (decimal) for any market
    Press Enter to skip markets you don't want to analyze

The script will print the full betting ticket and
automatically add a row to predictions_tracker.xlsx

STEP 6 — Using the Excel tracker
──────────────────────────────────
Open predictions_tracker.xlsx after each match.

In the MANUAL columns (orange headers) fill in:
  - ACTUAL Score         e.g. 2-1
  - ACTUAL 1X2           H (home win), D (draw), or A (away win)
  - ACTUAL Goals         total goals e.g. 3
  - ACTUAL BTTS          Y or N
  - ACTUAL HT Score      e.g. 1-0
  - ACTUAL Corners       total corners
  - ACTUAL Cards         total cards
  - Bet Placed?          Y or N
  - Market Bet On        e.g. over_2.5
  - Stake Placed (RWF)   exact amount
  - Odds Taken           decimal odds
  - Result (W/L/P)       W, L, or P (push/void)
  - Profit/Loss (RWF)    positive if won, negative if lost

The green formula columns calculate automatically:
  - Whether 1X2, O/U, BTTS predictions were correct
  - Running bankroll
  - Running ROI%

The Performance sheet shows your overall statistics.

DAILY API BUDGET (100 requests)
─────────────────────────────────
The script manages this automatically.
It reserves 10 requests for live prediction data
and uses the rest for background enrichment.

Each prediction uses up to 4 requests:
  - Injuries (home team)    1
  - Injuries (away team)    1
  - Head-to-head history    1
  - Upcoming fixtures       1

Full historical backfill takes approximately 15 days
(the script does this silently in the background).

FILES IN THIS FOLDER
─────────────────────
  football_predictor.py       Main script
  db_setup.sql                MySQL schema (run once)
  requirements.txt            Python dependencies
  .env                        Your credentials (auto-created, keep private)
  predictions_tracker.xlsx    Excel tracker (auto-created)
  README.txt                  This file

COMMANDS INSIDE THE SCRIPT
───────────────────────────
  teams         Show all known team names
  quit          Exit the program

TROUBLESHOOTING
───────────────
Cannot connect to MySQL:
  Make sure MySQL is running.
  Check credentials in .env file.

Team not found:
  Type 'teams' to see exact team names.
  Common aliases work: arsenal, man city, spurs, etc.

API limit reached:
  Wait until tomorrow. The script will tell you.
  Predictions still work — they use cached data.

.env file issues:
  Delete .env and restart the script to re-enter credentials.

============================================================
