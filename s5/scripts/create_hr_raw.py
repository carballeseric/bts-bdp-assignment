"""Create HR database using raw SQL connectors.

Usage:
    python create_hr_raw.py              # SQLite (default)
    python create_hr_raw.py postgresql   # PostgreSQL via docker-compose
"""

import sqlite3
import sys
from pathlib import Path

SCHEMA_FILE = Path(__file__).parent.parent / "hr_schema.sql"
SEED_FILE = Path(__file__).parent.parent / "hr_seed_data.sql"

SETTINGS = {
    "sqlite": {
        "db_path": str(Path(__file__).parent.parent / "hr_database.db"),
    },
    "postgresql": {
        "url": "postgresql://postgres:ericevaericeva9@bdi-hr-database.cfowxstntumk.us-east-1.rds.amazonaws.com:5432/hr_database",
    },
}


def adapt_sql_for_sqlite(sql: str) -> str:
    import re

    replacements = {
        "SERIAL": "INTEGER",
        "DECIMAL(10, 2)": "REAL",
        "DECIMAL(12, 2)": "REAL",
        "VARCHAR(100)": "TEXT",
        "VARCHAR(50)": "TEXT",
        "VARCHAR(200)": "TEXT",
    }
    for old, new in replacements.items():
        sql = sql.replace(old, new)
    sql = re.sub(r"DROP TABLE IF EXISTS \w+ CASCADE;", lambda m: m.group(0).replace(" CASCADE", ""), sql)
    sql = re.sub(r"ON DELETE (CASCADE|SET NULL)", "", sql)
    return sql


def run_sqlite() -> None:
    db_path = SETTINGS["sqlite"]["db_path"]
    Path(db_path).unlink(missing_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        schema_sql = adapt_sql_for_sqlite(SCHEMA_FILE.read_text())
        conn.executescript(schema_sql)
        print(f"Schema created: {db_path}")

        conn.executescript(SEED_FILE.read_text())
        print("Seed data inserted")

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM employee")
        print(f"Employees: {cursor.fetchone()[0]}")

        print("\nEmployees with department:")
        cursor.execute("""
            SELECT e.first_name, e.last_name, d.name AS department
            FROM employee e
            INNER JOIN department d ON e.department_id = d.id
            ORDER BY d.name, e.last_name
        """)
        for row in cursor.fetchall():
            print(f"  {row[0]} {row[1]} - {row[2]}")
    finally:
        conn.close()


def run_postgresql() -> None:
    import psycopg2

    conn = psycopg2.connect(SETTINGS["postgresql"]["url"])
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_FILE.read_text())
        conn.commit()
        print("Schema created on PostgreSQL")

        with conn.cursor() as cur:
            cur.execute(SEED_FILE.read_text())
        conn.commit()
        print("Seed data inserted")

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM employee")
            print(f"Employees: {cur.fetchone()[0]}")

            print("\nEmployees with department:")
            cur.execute("""
                SELECT e.first_name, e.last_name, d.name AS department
                FROM employee e
                INNER JOIN department d ON e.department_id = d.id
                ORDER BY d.name, e.last_name
            """)
            for row in cur.fetchall():
                print(f"  {row[0]} {row[1]} - {row[2]}")
    finally:
        conn.close()


if __name__ == "__main__":
    backend = sys.argv[1] if len(sys.argv) > 1 else "sqlite"
    print(f"Using backend: {backend}\n")
    if backend == "postgresql":
        run_postgresql()
    else:
        run_sqlite()
