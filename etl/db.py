"""
etl/db.py
─────────────────────────────────────────────────────────────────────────────
Single source of truth for all database connections.

Reads DB_TARGET from .env:
  "local"    → local PostgreSQL  (DB_URL)
  "supabase" → Supabase cloud    (SUPABASE_DB_URL)
  "both"     → returns both engines (used by sync_db.py)

Usage:
    from etl.db import get_engine
    engine = get_engine()          # respects DB_TARGET in .env
    engine = get_engine("local")   # override target explicitly
─────────────────────────────────────────────────────────────────────────────
"""

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)
load_dotenv()

TARGET       = os.getenv("DB_TARGET", "local").strip().lower()
LOCAL_URL    = os.getenv("DB_URL")
SUPABASE_URL = os.getenv("SUPABASE_DB_URL")


def get_engine(target: str = None):
    """
    Returns a SQLAlchemy engine for the requested target.
    Falls back to DB_TARGET env var when target is not specified.
    """
    t = (target or TARGET).lower()

    if t == "local":
        if not LOCAL_URL:
            raise ValueError(
                "DB_URL is not set in .env — cannot connect to local PostgreSQL."
            )
        print(f"  {Fore.CYAN}[db]{Style.RESET_ALL} → local PostgreSQL")
        return create_engine(
            LOCAL_URL,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )

    elif t == "supabase":
        if not SUPABASE_URL:
            raise ValueError(
                "SUPABASE_DB_URL is not set in .env — cannot connect to Supabase."
            )
        print(f"  {Fore.MAGENTA}[db]{Style.RESET_ALL} → Supabase (cloud)")
        return create_engine(
            SUPABASE_URL,
            connect_args={
                "sslmode":          "require",
                "connect_timeout":  30,
                "application_name": "uae-talent-etl",
            },
            pool_pre_ping=True,
            pool_size=3,
            max_overflow=5,
        )

    else:
        raise ValueError(
            f"DB_TARGET='{t}' is invalid. "
            f"Use 'local' or 'supabase' in .env, or call get_both_engines()."
        )


def get_both_engines():
    """Returns (local_engine, supabase_engine) — used by sync_db.py."""
    return get_engine("local"), get_engine("supabase")


def test_connection(target: str = None) -> bool:
    """Quick connectivity check — prints DB name and PostgreSQL version."""
    try:
        engine = get_engine(target)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT current_database(), version()")
            ).fetchone()
            print(f"  {Fore.GREEN}✅ Connected:{Style.RESET_ALL} {row[0]}")
            print(f"     Version : {row[1][:70]}...")
        return True
    except Exception as exc:
        print(f"  {Fore.RED}✗ Connection failed:{Style.RESET_ALL} {exc}")
        return False


if __name__ == "__main__":
    print("\n── Testing connections ─────────────────────────────────")
    for t in ["local", "supabase"]:
        print(f"\n  Target: {t.upper()}")
        test_connection(t)