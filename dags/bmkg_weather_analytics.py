"""DAG 2: bmkg_weather_analytics - Analitik cuaca BMKG (cuaca ekstrem & ringkasan harian)."""
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

# ---------- Konfigurasi ----------
OUTPUT_DIR = Path(
    os.environ.get("BMKG_OUTPUT_DIR", "/opt/airflow/dags/output_project_bmkg")
)
DB_FILE = OUTPUT_DIR / "cuaca_surabaya.db"

# Batas ambang cuaca ekstrem
EXTREME_THRESHOLDS = {
    "precip": 20.0,            # mm (hujan lebat)
    "wind_speed_kmh": 30.0,    # km/jam (angin kencang)
    "temperature_c": 35.0,     # °C (suhu panas ekstrem)
}


# ---------- Fungsi bantu ----------
def _pastikan_tabel(conn, nama_tabel):
    """Gagal eksplisit jika database atau tabel prasyarat belum ada."""
    if not DB_FILE.exists():
        raise AirflowFailException(
            f"Database {DB_FILE} belum ada. Jalankan bmkg_weather_etl dulu."
        )
    ada = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (nama_tabel,)
    ).fetchone()
    if not ada:
        raise AirflowFailException(f"Tabel {nama_tabel} belum ada di {DB_FILE}.")


# ---------- Task ----------
@task
def detect_extreme_weather():
    """Pindai data prakiraan terbaru dan simpan peringatan cuaca ekstrem ke weather_alerts."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        _pastikan_tabel(conn, "weather_forecast")
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_alerts (
                location_code TEXT,
                forecast_utc TEXT,
                alert_type TEXT,
                forecast_local TEXT,
                province TEXT,
                city TEXT,
                district TEXT,
                village TEXT,
                metric_name TEXT,
                metric_value REAL,
                threshold REAL,
                severity TEXT,
                description TEXT,
                analysis_utc TEXT,
                created_at TEXT,
                PRIMARY KEY (location_code, forecast_utc, alert_type)
            )
        """)

        # Data prakiraan dari analysis_utc terkini per lokasi yang memenuhi ambang
        query = """
            SELECT
                wf.location_code, wf.forecast_utc, wf.forecast_local,
                wf.province, wf.city, wf.district, wf.village,
                wf.temperature_c, wf.precip, wf.wind_speed_kmh, wf.analysis_utc
            FROM weather_forecast wf
            INNER JOIN (
                SELECT location_code, MAX(analysis_utc) AS max_analysis
                FROM weather_forecast
                GROUP BY location_code
            ) latest ON wf.location_code = latest.location_code
                    AND wf.analysis_utc = latest.max_analysis
            WHERE wf.precip > ?
               OR wf.wind_speed_kmh > ?
               OR wf.temperature_c >= ?
        """
        rows = cursor.execute(
            query,
            (
                EXTREME_THRESHOLDS["precip"],
                EXTREME_THRESHOLDS["wind_speed_kmh"],
                EXTREME_THRESHOLDS["temperature_c"],
            ),
        ).fetchall()

        if not rows:
            conn.commit()
            print("detect_extreme_weather: tidak ada kondisi cuaca ekstrem pada data terbaru")
            return 0

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        alerts = []
        for r in rows:
            meta = (r["location_code"], r["forecast_utc"])
            info = (r["forecast_local"], r["province"], r["city"],
                    r["district"], r["village"])
            ana = r["analysis_utc"]

            if r["precip"] is not None and r["precip"] > EXTREME_THRESHOLDS["precip"]:
                alerts.append(meta + ("HEAVY_RAIN",) + info + (
                    "precip", r["precip"], EXTREME_THRESHOLDS["precip"], "WARNING",
                    f"Peringatan hujan lebat: {r['precip']} mm (> {EXTREME_THRESHOLDS['precip']} mm)",
                    ana, now_str))

            if r["wind_speed_kmh"] is not None and r["wind_speed_kmh"] > EXTREME_THRESHOLDS["wind_speed_kmh"]:
                alerts.append(meta + ("STRONG_WIND",) + info + (
                    "wind_speed_kmh", r["wind_speed_kmh"], EXTREME_THRESHOLDS["wind_speed_kmh"], "WARNING",
                    f"Peringatan angin kencang: {r['wind_speed_kmh']} km/jam (> {EXTREME_THRESHOLDS['wind_speed_kmh']} km/jam)",
                    ana, now_str))

            if r["temperature_c"] is not None and r["temperature_c"] >= EXTREME_THRESHOLDS["temperature_c"]:
                alerts.append(meta + ("EXTREME_HEAT",) + info + (
                    "temperature_c", r["temperature_c"], EXTREME_THRESHOLDS["temperature_c"], "WARNING",
                    f"Peringatan suhu ekstrem: {r['temperature_c']} °C (>= {EXTREME_THRESHOLDS['temperature_c']} °C)",
                    ana, now_str))

        cursor.executemany("""
            INSERT OR REPLACE INTO weather_alerts (
                location_code, forecast_utc, alert_type, forecast_local,
                province, city, district, village, metric_name,
                metric_value, threshold, severity, description,
                analysis_utc, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, alerts)
        conn.commit()
        print(f"detect_extreme_weather: {len(alerts)} peringatan disimpan ke weather_alerts")
        return len(alerts)
    finally:
        conn.close()


@task
def build_daily_summary():
    """Hitung ringkasan statistik harian per lokasi dan simpan ke weather_daily_summary."""
    conn = sqlite3.connect(DB_FILE)
    try:
        _pastikan_tabel(conn, "weather_forecast")
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_daily_summary (
                location_code TEXT,
                forecast_date_local TEXT,
                province TEXT,
                city TEXT,
                district TEXT,
                village TEXT,
                avg_temp_c REAL,
                max_temp_c REAL,
                min_temp_c REAL,
                avg_humidity_pct REAL,
                total_precip_mm REAL,
                avg_wind_speed REAL,
                updated_at TEXT,
                PRIMARY KEY (location_code, forecast_date_local)
            )
        """)

        query = """
            SELECT
                wf.location_code,
                wf.forecast_date_local,
                MAX(wf.province) AS province,
                MAX(wf.city) AS city,
                MAX(wf.district) AS district,
                MAX(wf.village) AS village,
                ROUND(AVG(wf.temperature_c), 2) AS avg_temp_c,
                ROUND(MAX(wf.temperature_c), 2) AS max_temp_c,
                ROUND(MIN(wf.temperature_c), 2) AS min_temp_c,
                ROUND(AVG(wf.humidity_pct), 2) AS avg_humidity_pct,
                ROUND(SUM(wf.precip), 2) AS total_precip_mm,
                ROUND(AVG(wf.wind_speed_kmh), 2) AS avg_wind_speed
            FROM weather_forecast wf
            INNER JOIN (
                SELECT location_code, MAX(analysis_utc) AS max_analysis
                FROM weather_forecast
                GROUP BY location_code
            ) latest ON wf.location_code = latest.location_code
                    AND wf.analysis_utc = latest.max_analysis
            GROUP BY wf.location_code, wf.forecast_date_local
        """
        rows = cursor.execute(query).fetchall()
        if not rows:
            conn.commit()
            print("build_daily_summary: tidak ada data prakiraan untuk diringkas")
            return 0

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        records = [tuple(r) + (now_str,) for r in rows]
        cursor.executemany("""
            INSERT OR REPLACE INTO weather_daily_summary (
                location_code, forecast_date_local, province, city, district, village,
                avg_temp_c, max_temp_c, min_temp_c, avg_humidity_pct,
                total_precip_mm, avg_wind_speed, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records)
        conn.commit()
        print(f"build_daily_summary: {len(records)} baris ringkasan disimpan")
        return len(records)
    finally:
        conn.close()


# ---------- DAG ----------
default_args = {
    "owner": "kelompok1",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="bmkg_weather_analytics",
    description="Analitik cuaca BMKG: deteksi cuaca ekstrem dan ringkasan harian",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Jakarta"),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["uas", "bmkg", "analytics"],
)
def bmkg_weather_analytics():
    detect_extreme_weather()
    build_daily_summary()


bmkg_weather_analytics()