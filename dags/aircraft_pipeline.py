"""
DAG: aircraft_data_pipeline
Descarga datos de tracking de aviones, los enriquece y los almacena en SQLite.
Sigue el patrón bronze/silver/gold.
"""
import gzip
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Configuración ──────────────────────────────────────────────────────────────
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"
AIRCRAFT_DB_URL = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
FUEL_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"

PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze" / "day=20231101"
SILVER_DIR = DATA_DIR / "silver"
DB_PATH = PROJECT_DIR / "s8_aircraft.db"
DAY = "2023-11-01"
NUM_FILES = 10

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="aircraft_data_pipeline",
    default_args=default_args,
    description="Downloads, enriches and stores aircraft tracking data",
    schedule_interval=None,  # manual trigger
    start_date=datetime(2023, 11, 1),
    catchup=False,
    tags=["s8", "aircraft"],
)


# ── Task 1: Download raw tracking files → bronze layer ────────────────────────
def download_to_bronze(**context):
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    filenames = [f"{str(h).zfill(2)}0000Z.json.gz" for h in range(NUM_FILES)]
    downloaded = 0
    for fname in filenames:
        fpath = BRONZE_DIR / fname
        if fpath.exists():
            print(f"Already exists: {fname}")
            downloaded += 1
            continue
        url = BASE_URL + fname
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            fpath.write_bytes(r.content)
            print(f"Downloaded: {fname}")
            downloaded += 1
        else:
            print(f"Failed: {fname} (status {r.status_code})")
    print(f"Bronze layer: {downloaded}/{len(filenames)} files")


# ── Task 2: Parse and clean bronze → silver (Parquet) ─────────────────────────
def process_to_silver(**context):
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    records = []
    for fpath in sorted(BRONZE_DIR.glob("*.json.gz")):
        raw = fpath.read_bytes()
        try:
            text = gzip.decompress(raw).decode("utf-8")
        except Exception:
            text = raw.decode("utf-8")
        data = json.loads(text)
        now_val = data.get("now", 0)
        for ac in data.get("aircraft", []):
            icao = ac.get("hex", "").upper().strip()
            if not icao:
                continue
            records.append({
                "icao": icao,
                "registration": ac.get("r"),
                "type": ac.get("t"),
                "day": DAY,
                "timestamp": now_val,
            })

    df = pd.DataFrame(records)
    parquet_path = SILVER_DIR / "tracking.parquet"
    df.to_parquet(parquet_path, index=False)
    print(f"Silver layer: {len(df)} observations saved to {parquet_path}")


# ── Task 3: Enrich with aircraft metadata ─────────────────────────────────────
def enrich_data(**context):
    # Descargar aircraft database
    aircraft_csv = SILVER_DIR / "aircraftDatabase.csv"
    if not aircraft_csv.exists():
        r = requests.get(AIRCRAFT_DB_URL, timeout=60)
        aircraft_csv.write_bytes(r.content)
    aircraft_df = pd.read_csv(aircraft_csv, dtype=str, low_memory=False)
    aircraft_df.columns = [c.strip().lower() for c in aircraft_df.columns]
    aircraft_df["icao24"] = aircraft_df["icao24"].str.upper().str.strip()

    # Descargar fuel rates
    fuel_path = SILVER_DIR / "fuel_rates.json"
    if not fuel_path.exists():
        r = requests.get(FUEL_URL, timeout=30)
        fuel_path.write_bytes(r.content)

    # Leer tracking del silver layer
    tracking_df = pd.read_parquet(SILVER_DIR / "tracking.parquet")

    # Enriquecer
    merged = tracking_df.merge(
        aircraft_df[["icao24", "manufacturername", "model", "typecode", "owner"]],
        left_on="icao",
        right_on="icao24",
        how="left"
    )
    merged = merged.rename(columns={
        "typecode": "aircraft_type",
        "manufacturername": "manufacturer",
    })
    merged["aircraft_type"] = merged["aircraft_type"].fillna(merged.get("type", ""))

    # Guardar enriquecido en silver
    merged.to_parquet(SILVER_DIR / "aircraft_enriched.parquet", index=False)
    print(f"Enriched: {len(merged)} records saved")


# ── Task 4: Load into SQLite database ─────────────────────────────────────────
def load_to_database(**context):
    enriched = pd.read_parquet(SILVER_DIR / "aircraft_enriched.parquet")

    fuel_path = SILVER_DIR / "fuel_rates.json"
    with open(fuel_path) as f:
        fuel_rates = json.load(f)

    conn = sqlite3.connect(DB_PATH)

    # Tabla aircraft (silver)
    aircraft_silver = enriched[["icao", "registration", "aircraft_type", "owner", "manufacturer", "model"]].drop_duplicates(subset=["icao"])
    aircraft_silver.to_sql("aircraft", conn, if_exists="replace", index=False)

    # Tabla tracking
    enriched[["icao", "day", "timestamp"]].to_sql("tracking", conn, if_exists="replace", index=False)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tracking_icao_day ON tracking(icao, day)")

    # Tabla fuel_rates
    fuel_rows = [{"aircraft_type": k, "galph": v.get("galph")} for k, v in fuel_rates.items() if "galph" in v]
    pd.DataFrame(fuel_rows).to_sql("fuel_rates", conn, if_exists="replace", index=False)

    conn.commit()
    n_aircraft = conn.execute("SELECT COUNT(*) FROM aircraft").fetchone()[0]
    n_tracking = conn.execute("SELECT COUNT(*) FROM tracking").fetchone()[0]
    conn.close()

    print(f"Database loaded: {n_aircraft} aircraft, {n_tracking} tracking records")


# ── Definir tareas y dependencias ─────────────────────────────────────────────
t1_download = PythonOperator(
    task_id="download_to_bronze",
    python_callable=download_to_bronze,
    dag=dag,
)

t2_process = PythonOperator(
    task_id="process_to_silver",
    python_callable=process_to_silver,
    dag=dag,
)

t3_enrich = PythonOperator(
    task_id="enrich_data",
    python_callable=enrich_data,
    dag=dag,
)

t4_load = PythonOperator(
    task_id="load_to_database",
    python_callable=load_to_database,
    dag=dag,
)

# Orden de ejecución: download → process → enrich → load
t1_download >> t2_process >> t3_enrich >> t4_load
