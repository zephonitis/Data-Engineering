import os
import sqlite3
from datetime import datetime

def generate_weather_report(thresholds, output_folder="reports", db_path='weather.db'):
    os.makedirs(output_folder, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM cleaned_weather ORDER BY temperature DESC')
        data = cursor.fetchall()
        
        if not data:
            print("No data available to generate report.")
            return
        
        hot, warm = thresholds["hot"], thresholds["warm"]
        hot_cities = warm_cities = cold_cities = 0
        
        for record in data:
            temp = record[1]
            if temp >= hot:
                hot_cities += 1
            elif temp >= warm:
                warm_cities += 1
            else:
                cold_cities += 1
        
        report = [
            "📊 Weather Summary Report",
            "-------------------------",
            f"Hot cities : {hot_cities}",
            f"Warm cities : {warm_cities}",
            f"Cold cities : {cold_cities}",
            f"Hottest City : {data[0][0]} ({data[0][1]}°C)",
            f"Coldest City : {data[-1][0]} ({data[-1][1]}°C)"
        ]
        
        report_text = "\n".join(report)
        print(report_text)
        
        filename = f"weather_report_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"
        path = os.path.join(output_folder, filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write(report_text)
        
        print(f"Report saved to {path}")
