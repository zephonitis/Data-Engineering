import sqlite3
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
    
def clean_and_store(data):
    conn = sqlite3.connect('weather.db')
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
        
        if not city or not condition or temp < -50 or temp > 60:
            skipped += 1
            continue
        
        cursor.execute('''
            INSERT INTO cleaned_weather (city, temperature, condition) VALUES (?,?,?)
        ''', (city, temp, condition))
        cleaned += 1
        
    conn.commit()
    conn.close()
    
    print(f"Skipped records : {skipped}")
    print(f"Cleaned records : {cleaned}")
    
    return len(data), cleaned, skipped

def log_cleaning_run(total, cleaned, skipped):
    conn = sqlite3.connect('weather.db')
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
        VALUES (?,?,?,?)
        ''', (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), total, cleaned, skipped))
    
    conn.commit()
    conn.close()
    
def generate_weather_report():
    with sqlite3.connect('weather.db') as conn:
        cursor = conn.cursor()
    
        cursor.execute('SELECT * FROM cleaned_weather ORDER BY temperature DESC')
        data = cursor.fetchall()
        
        if not data:
            print("📊 Weather Summary Report")
            print("-------------------------")
            print("No data available to generate report.")
            return
        
        hot_cities = 0
        warm_cities = 0
        cold_cities = 0
        
        for record in data:
            temp = record[1]
            
            if temp >= 30:
                hot_cities += 1
            
            elif temp >= 20:
                warm_cities += 1
            
            else:
                cold_cities += 1
        
        hottest_city = data[0][0]
        hottest_temp = data[0][1]
        
        coldest_city = data[-1][0]
        coldest_temp = data[-1][1]
        
        # Generate report text
        report = []
        report.append("📊 Weather Summary Report")
        report.append("-------------------------")
        report.append(f"Hot cities : {hot_cities}")
        report.append(f"Warm cities : {warm_cities}")
        report.append(f"Cold cities : {cold_cities}")
        report.append(f"Hottest City : {hottest_city} ({hottest_temp}°C)")
        report.append(f"Coldest City : {coldest_city} ({coldest_temp}°C)")
        
        report_text = "\n".join(report)
        
        # Print to console
        print(report_text)
        
        # Write to file with timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"weather_report_{timestamp}.txt"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(report_text)
        
        print(f"\nReport saved to {filename}")
        
    
raw_data = simulate_messy_data()
total, cleaned, skipped = clean_and_store(raw_data)
log_cleaning_run(total, cleaned, skipped)
generate_weather_report()