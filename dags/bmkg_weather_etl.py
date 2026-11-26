"""DAG 1: bmkg_weather_etl - ETL prakiraan cuaca BMKG (31 Kecamatan Kota Surabaya)."""
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ---------- Konfigurasi ----------
PROJECT_DIR = Path(__file__).resolve().parents[1]   # di Docker: /opt/airflow
OUTPUT_DIR = Path(
    os.environ.get("BMKG_OUTPUT_DIR", "/opt/airflow/dags/output_project_bmkg")
)
RAW_DIR = OUTPUT_DIR / "raw"
PROCESSED_DIR = OUTPUT_DIR / "processed"
DB_FILE = OUTPUT_DIR / "cuaca_surabaya.db"
CONFIG_FILE = PROJECT_DIR / "config" / "locations.json"
LOCATIONS = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))

API_URL = "https://api.bmkg.go.id/publik/prakiraan-cuaca"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "application/json",
}
REQUIRED_SLOT_KEYS = {"utc_datetime", "local_datetime", "t", "hu"}
LOCAL_TZ = "Asia/Jakarta"   # benar untuk lokasi WIB saja

# Kolom hasil (sesuai data dictionary B3) dan tipe di SQLite
COLUMNS = {
    "location_code": "TEXT", "province": "TEXT", "city": "TEXT",
    "district": "TEXT", "village": "TEXT",
    "latitude": "REAL", "longitude": "REAL",
    "forecast_utc": "TEXT", "forecast_local": "TEXT",
    "forecast_date_local": "TEXT",
    "temperature_c": "REAL", "humidity_pct": "REAL",
    "cloud_cover_pct": "REAL", "precip": "REAL", "weather_code": "INTEGER",
    "weather_desc": "TEXT", "weather_desc_en": "TEXT",
    "wind_speed_kmh": "REAL", "wind_dir_from": "TEXT",
    "wind_dir_to": "TEXT", "wind_dir_deg": "INTEGER",
    "visibility_text": "TEXT", "analysis_utc": "TEXT",
    "ingested_at": "TEXT", "source": "TEXT",
}
NUMERIC_COLS = [
    "latitude", "longitude", "temperature_c", "humidity_pct",
    "cloud_cover_pct", "precip", "weather_code", "wind_speed_kmh",
    "wind_dir_deg",
]


# ---------- Fungsi bantu ----------
def fetch_forecast(adm4, max_attempts=3):
    """Ambil prakiraan satu lokasi dengan jeda aman dan retry ringan untuk error sementara."""
    for attempt in range(1, max_attempts + 1):
        time.sleep(0.4)  # Jeda ringan untuk menghindari rate limit BMKG
        try:
            r = requests.get(
                API_URL, params={"adm4": adm4}, headers=HEADERS, timeout=30
            )
            if r.status_code == 200:
                return r
            print(
                f"adm4={adm4} percobaan={attempt}/{max_attempts} "
                f"status={r.status_code} isi={r.text[:200]}"
            )
            if r.status_code in (400, 404):
                raise AirflowFailException(
                    f"Kode wilayah {adm4} ditolak (status {r.status_code})"
                )
            if attempt == max_attempts:
                r.raise_for_status()
        except requests.RequestException as exc:
            print(f"adm4={adm4} percobaan={attempt}/{max_attempts} error={exc}")
            if attempt == max_attempts:
                raise
        time.sleep(2 * attempt)


def get_analysis(payload):
    """analysis_date prakiraan, dinormalkan ke 'YYYY-MM-DD HH:MM:SS'. None jika tidak ada."""
    try:
        raw = payload["data"][0]["cuaca"][0][0]["analysis_date"]
    except (KeyError, IndexError, TypeError):
        return None
    return str(raw).replace("T", " ")


def last_analysis(adm4):
    """analysis_utc terakhir yang sudah tersimpan di database untuk lokasi ini."""
    if not DB_FILE.exists():
        return None
    conn = sqlite3.connect(DB_FILE)
    try:
        row = conn.execute(
            "SELECT MAX(analysis_utc) FROM weather_forecast WHERE location_code = ?",
            (adm4,),
        ).fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:   # tabel belum ada
        return None
    finally:
        conn.close()


def flatten(payload, adm4):
    """Ratakan JSON bersarang (hari -> slot) menjadi satu baris per slot."""
    lok = payload["lokasi"]
    rows = []
    for hari in payload["data"][0]["cuaca"]:
        for s in hari:
            rows.append({
                "location_code": lok.get("adm4", adm4),
                "province": lok.get("provinsi"),
                "city": lok.get("kotkab"),
                "district": lok.get("kecamatan"),
                "village": lok.get("desa"),
                "latitude": lok.get("lat"),
                "longitude": lok.get("lon"),
                "forecast_utc": s.get("utc_datetime"),
                "forecast_local": s.get("local_datetime"),
                "temperature_c": s.get("t"),
                "humidity_pct": s.get("hu"),
                "cloud_cover_pct": s.get("tcc"),
                "precip": s.get("tp"),
                "weather_code": s.get("weather"),
                "weather_desc": s.get("weather_desc"),
                "weather_desc_en": s.get("weather_desc_en"),
                "wind_speed_kmh": s.get("ws"),
                "wind_dir_from": s.get("wd"),
                "wind_dir_to": s.get("wd_to"),
                "wind_dir_deg": s.get("wd_deg"),
                "visibility_text": s.get("vs_text"),
                "analysis_utc": s.get("analysis_date"),
            })
    return rows


# ---------- Task ----------
@task
def check_api():
    first = LOCATIONS[0]
    r = fetch_forecast(first["adm4"])
    print(
        f"check_api: status={r.status_code}, total_lokasi={len(LOCATIONS)}, "
        f"lokasi uji={first['nama']} ({first['kecamatan']})"
    )
    return r.status_code


@task(max_active_tis_per_dag=3)
def extract(location, ts_nodash=None):
    adm4 = location["adm4"]
    payload = fetch_forecast(adm4).json()
    analysis = get_analysis(payload)
    previous = last_analysis(adm4)
    is_new = analysis is None or analysis != previous

    out_dir = RAW_DIR / ts_nodash
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{adm4}.json"
    out_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    status = "data baru" if is_new else "belum diperbarui"
    print(
        f"extract: adm4={adm4} ({location['kecamatan']} - {location['nama']}) "
        f"analysis_utc={analysis} terakhir_di_db={previous} -> {status}"
    )
    return {
        "adm4": adm4,
        "nama": location["nama"],
        "kecamatan": location.get("kecamatan", ""),
        "raw_file": str(out_file),
        "analysis_utc": analysis,
        "is_new": is_new,
    }


@task(trigger_rule="all_done")   # tetap jalan walau ada lokasi yang gagal
def validate_raw(extracted):
    files = [f for f in extracted if f]
    berhasil = {f["adm4"] for f in files}
    gagal = [
        f"{loc['nama']} ({loc['kecamatan']})"
        for loc in LOCATIONS
        if loc["adm4"] not in berhasil
    ]
    if gagal:
        print(f"validate_raw PERINGATAN: extract gagal untuk {gagal}")
    if not files:
        raise AirflowFailException("Tidak ada satu pun lokasi yang berhasil diambil")

    errors = []
    for item in files:
        n_before = len(errors)
        try:
            payload = json.loads(Path(item["raw_file"]).read_text(encoding="utf-8"))
            days = payload["data"][0]["cuaca"]
            slots = [s for day in days for s in day]
            if not slots:
                errors.append(f"{item['nama']}: tidak ada slot prakiraan")
                continue
            for s in slots:
                missing = REQUIRED_SLOT_KEYS - s.keys()
                if missing:
                    errors.append(f"{item['nama']}: field hilang {sorted(missing)}")
                    break
            if len(errors) == n_before:
                print(
                    f"validate_raw: adm4={item['adm4']} {item['nama']} OK, "
                    f"{len(days)} hari, {len(slots)} slot"
                )
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
            errors.append(f"{item['nama']}: struktur JSON tidak valid ({exc!r})")
    if errors:
        raise AirflowFailException("Validasi gagal:\n" + "\n".join(errors))
    return files


@task
def transform(files, ts_nodash=None):
    baru = [f for f in files if f["is_new"]]
    if not baru:
        print("transform: semua lokasi belum diperbarui, tidak ada data baru")
        return None

    rows = []
    for item in baru:
        payload = json.loads(Path(item["raw_file"]).read_text(encoding="utf-8"))
        rows.extend(flatten(payload, item["adm4"]))
    df = pd.DataFrame(rows)
    if df.empty:
        raise AirflowFailException("Hasil transform kosong")

    # Tanggal lokal dihitung dari UTC, bukan dari field timezone BMKG
    utc = pd.to_datetime(df["forecast_utc"])
    df["forecast_date_local"] = (
        utc.dt.tz_localize("UTC").dt.tz_convert(LOCAL_TZ).dt.strftime("%Y-%m-%d")
    )
    for col in ("forecast_utc", "forecast_local", "analysis_utc"):
        df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df["source"] = "BMKG"
    df = df[list(COLUMNS)]

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out_file = PROCESSED_DIR / f"weather_{ts_nodash}.csv"
    df.to_csv(out_file, index=False)
    for (kode, analisis), n in df.groupby(["location_code", "analysis_utc"]).size().items():
        print(f"transform: adm4={kode} analysis_utc={analisis} baris={n}")
    print(f"transform: total {len(df)} baris dari {len(baru)} lokasi -> {out_file}")
    return str(out_file)


@task
def load(csv_path):
    if csv_path is None:
        print("load: tidak ada data baru, tidak ada yang ditulis")
        return 0
    df = pd.read_csv(csv_path)
    df = df.astype(object).where(df.notna(), None)
    cols = list(COLUMNS)
    ddl = ("CREATE TABLE IF NOT EXISTS weather_forecast ("
           + ", ".join(f"{c} {t}" for c, t in COLUMNS.items())
           + ", PRIMARY KEY (location_code, forecast_utc, analysis_utc))")
    sql = (f"INSERT OR IGNORE INTO weather_forecast ({', '.join(cols)}) "
           f"VALUES ({', '.join('?' * len(cols))})")
    records = [tuple(r) for r in df[cols].itertuples(index=False, name=None)]

    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_FILE, timeout=30)
    try:
        with conn:
            conn.execute(ddl)
            before = conn.total_changes
            conn.executemany(sql, records)
            inserted = conn.total_changes - before
    finally:
        conn.close()
    print(
        f"load: {len(records)} baris diproses, {inserted} baris baru, "
        f"{len(records) - inserted} dilewati (kunci sudah ada) -> {DB_FILE}"
    )
    return inserted


# ---------- DAG ----------
default_args = {
    "owner": "kelompok2",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="bmkg_weather_etl",
    description="ETL prakiraan cuaca BMKG (31 Kecamatan Kota Surabaya)",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Jakarta"),
    schedule=None,          # Fase D: "0 10,22 * * *" (WIB)
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["uas", "bmkg", "etl"],
)
def bmkg_weather_etl():
    ok = check_api()
    extracted = extract.expand(location=LOCATIONS)   # satu task per lokasi (maks 3 paralel)
    ok >> extracted
    validated = validate_raw(extracted)
    csv_path = transform(validated)
    loaded = load(csv_path)

    trigger_analytics = TriggerDagRunOperator(
        task_id="trigger_analytics",
        trigger_dag_id="bmkg_weather_analytics",
        wait_for_completion=False,
        reset_dag_run=True,
    )
    loaded >> trigger_analytics


bmkg_weather_etl()