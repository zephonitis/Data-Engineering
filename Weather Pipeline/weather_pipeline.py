import sqlite3
from datetime import datetime

def clean_and_store(data, db_path, min_temp=-50, max_temp=60):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cleaned_weather (
                city TEXT,
                temperature REAL,
                condition TEXT
            )
        ''')
        
        skipped = 0
        cleaned = 0
        
        for record in data:
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
    
    return len(data), cleaned, skipped

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
