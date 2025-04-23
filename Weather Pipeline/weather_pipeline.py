from concurrent.futures import ThreadPoolExecutor
import sqlite3
import threading
from datetime import datetime

# Lock to make DB thread-safe
db_lock = threading.Lock()

def process_chunk(chunk, db_path, min_temp, max_temp):
    cleaned = 0
    skipped = 0

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for record in chunk:
        city = record.get("city", "").strip()
        temp_raw = record.get("temperature", "").strip()
        condition = record.get("condition", "").strip()

        try:
            temp = float(temp_raw)
        except ValueError:
            skipped += 1
            continue

        if not city or not condition or temp < min_temp or temp > max_temp:
            skipped += 1
            continue

        with db_lock:
            cursor.execute('''
                INSERT INTO cleaned_weather (city, temperature, condition)
                VALUES (?, ?, ?)
            ''', (city, temp, condition))
            cleaned += 1

    conn.commit()
    conn.close()
    return cleaned, skipped


def clean_and_store(data, db_path, min_temp=-50, max_temp=60, num_threads=4):
    # Create table if not exists
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cleaned_weather (
            city TEXT,
            temperature REAL,
            condition TEXT
        )
    ''')
    conn.commit()
    conn.close()

    # Split data into chunks
    chunk_size = (len(data) + num_threads - 1) // num_threads
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    total_cleaned = 0
    total_skipped = 0

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(process_chunk, chunk, db_path, min_temp, max_temp)
            for chunk in chunks
        ]
        for future in futures:
            cleaned, skipped = future.result()
            total_cleaned += cleaned
            total_skipped += skipped

    return len(data), total_cleaned, total_skipped

def log_cleaning_run(db_path, total, cleaned, skipped):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cleaning_log (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                total_records INTEGER,
                cleaned INTEGER,
                skipped INTEGER
            )
        ''')
        cursor.execute('''
            INSERT INTO cleaning_log (timestamp, total_records, cleaned, skipped)
            VALUES (?, ?, ?, ?)
        ''', (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), total, cleaned, skipped))
        conn.commit()
