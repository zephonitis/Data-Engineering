import threading
import sqlite3
import os
from datetime import datetime

def simulate_messy_data():
    return [
        {"city": "  Tokyo ", "temperature": " 29.5 ", "condition": " Sunny "},
        {"city": "Chennai", "temperature": "N/A", "condition": "  Cloudy"},
        {"city": "London", "temperature": "15C", "condition": "Rainy "},
        {"city": "", "temperature": "25.3", "condition": "Clear"},
        {"city": "Kolkata", "temperature": " 42 ", "condition": ""},
        {"city": "Delhi", "temperature": "38.2", "condition": "Hazy"},
        {"city": "Mumbai", "temperature": "", "condition": "Humid"},
        {"city": "Auckland", "temperature": "17", "condition": "Windy"},
    ]

def clean_and_store_parallel(data, db_path, min_temp=-50, max_temp=60, num_threads=4):
    os.makedirs("logs/threads", exist_ok=True)
    chunk_size = len(data) // num_threads
    threads = []
    results = [None] * num_threads

    def worker(chunk, thread_id):
        skipped = 0
        cleaned = 0
        log_lines = [f"[Thread-{thread_id}] Started\n"]

        with sqlite3.connect(db_path) as conn:
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

                cursor.execute('''
                    INSERT INTO cleaned_weather (city, temperature, condition)
                    VALUES (?, ?, ?)
                ''', (city, temp, condition))
                cleaned += 1
            conn.commit()

        log_lines.append(f"[Thread-{thread_id}] Cleaned: {cleaned}, Skipped: {skipped}\n")
        log_lines.append(f"[Thread-{thread_id}] Finished\n")

        with open(f"logs/threads/thread_{thread_id}.log", "w", encoding="utf-8") as f:
            f.writelines(log_lines)

        results[thread_id] = (cleaned, skipped)

    for i in range(num_threads):
        start = i * chunk_size
        end = None if i == num_threads - 1 else (i + 1) * chunk_size
        chunk = data[start:end]
        t = threading.Thread(target=worker, args=(chunk, i))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    total = len(data)
    cleaned_total = sum(r[0] for r in results if r)
    skipped_total = sum(r[1] for r in results if r)

    return total, cleaned_total, skipped_total

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

def generate_weather_report(thresholds, output_folder, db_path):
    os.makedirs(output_folder, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM cleaned_weather ORDER BY temperature DESC')
        data = cursor.fetchall()

    if not data:
        print("Weather Summary Report\n-------------------------\nNo data available to generate report.")
        return

    hot, warm, cold = 0, 0, 0
    for _, temp, *_ in data:
        if temp >= thresholds["hot"]:
            hot += 1
        elif temp >= thresholds["warm"]:
            warm += 1
        else:
            cold += 1

    hottest_city, hottest_temp = data[0][0], data[0][1]
    coldest_city, coldest_temp = data[-1][0], data[-1][1]

    report = [
        "Weather Summary Report",
        "-------------------------",
        f"Hot cities : {hot}",
        f"Warm cities : {warm}",
        f"Cold cities : {cold}",
        f"Hottest City : {hottest_city} ({hottest_temp}°C)",
        f"Coldest City : {coldest_city} ({coldest_temp}°C)"
    ]

    report_text = "\n".join(report)
    print(report_text)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_path = os.path.join(output_folder, f"weather_report_{timestamp}.txt")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)

    print(f"\nReport saved to {report_path}")