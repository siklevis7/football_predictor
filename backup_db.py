#!/usr/bin/env python3
"""
backup_db.py  —  Database Backup & Restore Tool
─────────────────────────────────────────────────
Backs up all five league databases to compressed .sql.gz files.
Run manually or schedule weekly via Task Scheduler / cron.

SCHEDULING ON WINDOWS (Task Scheduler):
  Program : C:/Users/semig/anaconda3/python.exe
  Arguments: C:/path/to/scripts/backup_db.py
  Trigger  : Weekly, Sunday at 22:00

SCHEDULING ON LINUX/MAC (cron):
  0 22 * * 0 /path/to/python /path/to/backup_db.py

HOW TO RESTORE AFTER DATA LOSS:
  python backup_db.py  → choose option 2 (Restore)
  Pick the backup file → all data restored in minutes

WHERE ARE BACKUPS SAVED?
  By default: same folder as this script, in a 'db_backups' subfolder.
  Change BACKUP_DIR below to save to OneDrive, Google Drive, etc.
  Example: BACKUP_DIR = r"C:/Users/semig/OneDrive/FootballPredictor/Backups"
"""

import os, sys, gzip, subprocess, shutil, time
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

# ── WHERE TO SAVE BACKUPS ─────────────────────────────────────────────────────
# Change this path to save backups to OneDrive / Google Drive / USB drive.
# Default: 'db_backups' folder next to your scripts.
BACKUP_DIR = SCRIPT_DIR / "db_backups"

# How many weekly backups to keep (older ones are deleted automatically)
BACKUPS_TO_KEEP = 4

try:
    import config
    from database import setup_env, get_env
except ImportError as e:
    print(f"\n❌  Cannot import modules: {e}")
    print(f"    Make sure backup_db.py is in the same folder as config.py")
    sys.exit(1)

LEAGUES = config.LEAGUES


def fmt_size(path: Path) -> str:
    """Human-readable file size."""
    b = path.stat().st_size
    if b < 1024:       return f"{b}B"
    if b < 1024**2:    return f"{b/1024:.1f}KB"
    if b < 1024**3:    return f"{b/1024**2:.1f}MB"
    return f"{b/1024**3:.1f}GB"


def fmt_time(seconds: float) -> str:
    s = int(seconds)
    if s < 60:   return f"{s}s"
    if s < 3600: return f"{s//60}m {s%60}s"
    return f"{s//3600}h {(s%3600)//60}m"


def find_mysqldump() -> str | None:
    """Locate mysqldump executable."""
    for candidate in [
        "mysqldump",
        r"C:/Program Files/MySQL/MySQL Server 8.0/bin/mysqldump.exe",
        r"C:/Program Files/MySQL/MySQL Server 8.4/bin/mysqldump.exe",
        r"C:/xampp/mysql/bin/mysqldump.exe",
        "/usr/bin/mysqldump",
        "/usr/local/bin/mysqldump",
    ]:
        if shutil.which(candidate):
            return candidate
        if Path(candidate).exists():
            return candidate
    return None


def find_mysql() -> str | None:
    """Locate mysql client executable."""
    for candidate in [
        "mysql",
        r"C:/Program Files/MySQL/MySQL Server 8.0/bin/mysql.exe",
        r"C:/Program Files/MySQL/MySQL Server 8.4/bin/mysql.exe",
        r"C:/xampp/mysql/bin/mysql.exe",
        "/usr/bin/mysql",
        "/usr/local/bin/mysql",
    ]:
        if shutil.which(candidate):
            return candidate
        if Path(candidate).exists():
            return candidate
    return None


def run_backup():
    """Back up all league databases to compressed .sql.gz files."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    setup_env()
    host = get_env("DB_HOST", "localhost")
    port = get_env("DB_PORT", "3306")
    user = get_env("DB_USER", "root")
    pwd  = get_env("DB_PASSWORD", "")

    mysqldump = find_mysqldump()
    if not mysqldump:
        print("❌  mysqldump not found.")
        print("   Make sure MySQL is installed and in your PATH.")
        print("   Or add the full path to find_mysqldump() in this script.")
        return False

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    overall_start = time.time()
    backed_up = []

    print(f"\n  Backup folder: {BACKUP_DIR}")
    print(f"  Timestamp    : {timestamp}")
    print()

    for key, league in LEAGUES.items():
        db_name = league["db"]
        filename = f"{db_name}_{timestamp}.sql.gz"
        out_path = BACKUP_DIR / filename

        print(f"  [{league['code']}] Backing up {db_name} …", end=" ", flush=True)
        start = time.time()

        cmd = [
            mysqldump,
            f"--host={host}",
            f"--port={port}",
            f"--user={user}",
            f"--password={pwd}",
            "--single-transaction",
            "--routines",
            "--triggers",
            "--set-gtid-purged=OFF",
            db_name,
        ]

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=300,
            )
            if result.returncode != 0:
                err = result.stderr.decode("utf-8", errors="replace")
                # Suppress the "password on command line" warning — not an error
                real_errors = [l for l in err.splitlines()
                               if l and "password" not in l.lower()]
                if real_errors:
                    print(f"FAILED")
                    print(f"    Error: {chr(10).join(real_errors[:3])}")
                    continue

            with gzip.open(out_path, "wb") as gz:
                gz.write(result.stdout)

            size = fmt_size(out_path)
            elapsed = fmt_time(time.time() - start)
            print(f"✅  {size}  ({elapsed})")
            backed_up.append(out_path)

        except subprocess.TimeoutExpired:
            print(f"TIMEOUT — skipped")
        except Exception as ex:
            print(f"ERROR — {ex}")

    # Prune old backups (keep only BACKUPS_TO_KEEP most recent per database)
    print()
    print("  Pruning old backups …")
    for league in LEAGUES.values():
        db_name = league["db"]
        old_files = sorted(BACKUP_DIR.glob(f"{db_name}_*.sql.gz"),
                           key=lambda p: p.stat().st_mtime, reverse=True)
        for old in old_files[BACKUPS_TO_KEEP:]:
            old.unlink()
            print(f"    Deleted: {old.name}")

    total_elapsed = fmt_time(time.time() - overall_start)
    total_size    = sum(p.stat().st_size for p in BACKUP_DIR.glob("*.sql.gz"))
    total_size_mb = total_size / 1024**2

    print()
    print(f"  ✅  Backup complete in {total_elapsed}")
    print(f"  📦  {len(backed_up)} databases backed up")
    print(f"  💾  Total backup size on disk: {total_size_mb:.1f} MB")
    print(f"  📁  Location: {BACKUP_DIR}")
    return True


def run_restore():
    """Restore a database from a backup file."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    setup_env()
    host = get_env("DB_HOST", "localhost")
    port = get_env("DB_PORT", "3306")
    user = get_env("DB_USER", "root")
    pwd  = get_env("DB_PASSWORD", "")

    mysql = find_mysql()
    if not mysql:
        print("❌  mysql client not found.")
        print("   Make sure MySQL is installed and in your PATH.")
        return False

    # List available backups
    backups = sorted(BACKUP_DIR.glob("*.sql.gz"),
                     key=lambda p: p.stat().st_mtime, reverse=True)
    if not backups:
        print(f"  ❌  No backups found in {BACKUP_DIR}")
        print(f"     Run option 1 (Backup) first.")
        return False

    print(f"\n  Available backups ({len(backups)} files):")
    print(f"  {'#':<4} {'File':<50} {'Size':<10} {'Date'}")
    print("  " + "─" * 80)
    for i, p in enumerate(backups, 1):
        mtime = datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        print(f"  {i:<4} {p.name:<50} {fmt_size(p):<10} {mtime}")

    print()
    choice = input("  Enter backup number to restore (or Enter to cancel): ").strip()
    if not choice:
        print("  Cancelled.")
        return False

    try:
        idx = int(choice) - 1
        backup_file = backups[idx]
    except (ValueError, IndexError):
        print("  Invalid choice.")
        return False

    # Infer DB name from filename (format: dbname_YYYYMMDD_HHMM.sql.gz)
    db_name = backup_file.name.rsplit("_", 2)[0]

    print(f"\n  File    : {backup_file.name}")
    print(f"  Database: {db_name}")
    print(f"  Size    : {fmt_size(backup_file)}")
    print()
    print(f"  ⚠  This will OVERWRITE the existing '{db_name}' database.")
    confirm = input("  Are you sure? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("  Cancelled.")
        return False

    print(f"\n  Restoring {backup_file.name} …")
    start = time.time()

    # Read and decompress the backup
    try:
        with gzip.open(backup_file, "rb") as gz:
            sql_data = gz.read()
    except Exception as e:
        print(f"  ❌  Could not read backup file: {e}")
        return False

    # Create the database if it doesn't exist, then restore
    cmd_create = [
        mysql,
        f"--host={host}", f"--port={port}",
        f"--user={user}", f"--password={pwd}",
        "-e", f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
              f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
    ]
    subprocess.run(cmd_create, capture_output=True)

    cmd_restore = [
        mysql,
        f"--host={host}", f"--port={port}",
        f"--user={user}", f"--password={pwd}",
        db_name,
    ]

    try:
        result = subprocess.run(
            cmd_restore,
            input=sql_data,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=600,
        )
        if result.returncode != 0:
            err = result.stderr.decode("utf-8", errors="replace")
            real_errors = [l for l in err.splitlines()
                           if l and "password" not in l.lower()]
            if real_errors:
                print(f"  ❌  Restore failed:")
                for line in real_errors[:5]:
                    print(f"    {line}")
                return False
    except subprocess.TimeoutExpired:
        print("  ❌  Restore timed out after 10 minutes.")
        return False
    except Exception as ex:
        print(f"  ❌  {ex}")
        return False

    print(f"  ✅  Restore complete in {fmt_time(time.time() - start)}")
    print(f"  Database '{db_name}' is ready to use.")
    return True


def show_status():
    """Show existing backups without making any changes."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backups = sorted(BACKUP_DIR.glob("*.sql.gz"),
                     key=lambda p: p.stat().st_mtime, reverse=True)

    if not backups:
        print(f"\n  No backups found in {BACKUP_DIR}")
        print(f"  Run option 1 (Backup all databases) to create your first backup.")
        return

    total = sum(p.stat().st_size for p in backups)
    print(f"\n  Backup folder: {BACKUP_DIR}")
    print(f"  Total size   : {total / 1024**2:.1f} MB  ({len(backups)} files)")
    print()
    print(f"  {'File':<52} {'Size':<10} {'Date'}")
    print("  " + "─" * 80)

    by_db: dict = {}
    for p in backups:
        db = p.name.rsplit("_", 2)[0]
        by_db.setdefault(db, []).append(p)

    for db, files in by_db.items():
        print(f"  {db}:")
        for p in files:
            mtime = datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            print(f"    {p.name:<50} {fmt_size(p):<10} {mtime}")
        print()


def main():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║         FOOTBALL PREDICTOR — DATABASE BACKUP TOOL           ║")
    print("║  Protects your enriched match data from loss                 ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    while True:
        print()
        print("  ┌─────────────────────────────────────────────────────────┐")
        print("  │  MENU                                                   │")
        print("  ├─────────────────────────────────────────────────────────┤")
        print("  │  1.  Back up all databases                              │")
        print("  │      Saves compressed .sql.gz files — safe to repeat   │")
        print("  │                                                         │")
        print("  │  2.  Restore a database from backup                    │")
        print("  │      Pick a backup file to restore all enriched data    │")
        print("  │                                                         │")
        print("  │  3.  Show existing backups                              │")
        print("  │      No changes made — just lists what's saved          │")
        print("  │                                                         │")
        print("  │  0.  Exit                                               │")
        print("  └─────────────────────────────────────────────────────────┘")
        print()
        print(f"  Backups folder: {BACKUP_DIR}")
        print()

        choice = input("  Choice (0-3): ").strip()
        if   choice == "0": print("\n  Goodbye!\n"); break
        elif choice == "1": run_backup()
        elif choice == "2": run_restore()
        elif choice == "3": show_status()
        else: print("  Invalid choice. Enter 0, 1, 2, or 3.")


if __name__ == "__main__":
    main()
