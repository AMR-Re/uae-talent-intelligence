"""
etl/db.py
Single source of truth for database connections.
Reads DB_TARGET from .env:
  - "local"    → uses DB_URL (your local PostgreSQL)
  - "supabase" → uses SUPABASE_DB_URL (cloud)
  - "both"     → returns both engines (used by sync commands)

Usage anywhere in the project:
    from etl.db import get_engine
    engine = get_engine()
"""

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

TARGET         = os.getenv("DB_TARGET", "local").strip().lower()
LOCAL_URL      = os.getenv("DB_URL")
SUPABASE_URL   = os.getenv("SUPABASE_DB_URL")


def get_engine(target: str = None):
    """
    Returns a SQLAlchemy engine.
    target overrides DB_TARGET env var if provided.
    """
    t = (target or TARGET).lower()

    if t == "local":
        if not LOCAL_URL:
            raise ValueError("DB_URL is not set in .env")
        print(f"  [db] → local PostgreSQL")
        return create_engine(LOCAL_URL)

    elif t == "supabase":
        if not SUPABASE_URL:
            raise ValueError("SUPABASE_DB_URL is not set in .env")
        print(f"  [db] → Supabase (cloud)")
        return create_engine(
            SUPABASE_URL,
            connect_args={
                "sslmode":        "require",      # Supabase requires SSL
                "connect_timeout": 30,
                "application_name": "uae-talent-etl",
            },
            pool_pre_ping=True,     # test connection before using from pool
            pool_size=3,
            max_overflow=5,
        )

    else:
        raise ValueError(
            f"DB_TARGET='{t}' is invalid. Use 'local', 'supabase', or run sync.py for 'both'."
        )


def get_both_engines():
    """Returns (local_engine, supabase_engine) for sync operations."""
    local    = get_engine("local")
    supabase = get_engine("supabase")
    return local, supabase


def test_connection(target: str = None):
    """Quick connectivity check — prints current database info."""
    engine = get_engine(target)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT current_database(), version()"))
        row = result.fetchone()
        print(f"  ✅ Connected to: {row[0]}")
        print(f"     PostgreSQL:   {row[1][:60]}...")
    return True


if __name__ == "__main__":
    print("\n── Testing connections ─────────────────────────────")
    for t in ["local", "supabase"]:
        try:
            test_connection(t)
        except Exception as e:
            print(f"  ⚠  {t} failed: {e}")