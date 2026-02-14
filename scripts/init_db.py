#!/usr/bin/env python
"""
Database Initialization Script

This script creates all database tables defined in SQLAlchemy models.
Run this before starting the application for the first time.

Usage:
    python scripts/init_db.py
"""

import sys
import os
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import engine, Base, DATABASE_URL
from app.models import Trade  # Import to register models


def init_db():
    """Initialize database by creating all tables"""
    print("Database Initialization")
    print(f"\nDatabase URL: {DATABASE_URL}")
    print(f"Engine: {engine}")

    try:
        # Create all tables
        print("\nCreating tables...")
        Base.metadata.create_all(bind=engine)

        # Verify tables were created
        print("\n Tables created successfully:")
        for table in Base.metadata.sorted_tables:
            print(f"  - {table.name}")
            print(f"    Columns: {', '.join([col.name for col in table.columns])}")

        print("\n" + "=" * 60)
        print(" Database initialization complete!")

    except Exception as e:
        print(f"\n✗ Error creating database tables: {e}")
        print("\nPlease ensure:")
        print("  1. PostgreSQL is running")
        print("  2. Database exists (tradedb)")
        print("  3. User credentials are correct in .env file")
        print("  4. User has CREATE TABLE permissions")
        sys.exit(1)


if __name__ == "__main__":
    init_db()
