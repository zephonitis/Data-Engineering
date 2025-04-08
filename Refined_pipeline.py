import schedule
import sqlite3
import requests
from datetime import datetime
import time
import csv
import logging
from logging.handlers import RotatingFileHandler

# === Set up rotating log ===
log_handler = RotatingFileHandler(
    "warm_weather_pipeline.log",
    maxBytes = 1024 * 1024,  # 1 MB
    backupCount = 3          # Keep 3 old logs
)

logging.basicConfig(
    level = logging.INFO,
    handlers = [log_handler],
    format = '%(asctime)s - %(levelname)s - %(message)s'
)

# === Export function ===
def export_weather_to_csv():
    try:
        conn = sqlite3.connect('weather.db')
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM weather')
        rows = cursor.fetchall()

        filename = f"warm_cities_{datetime.now().strftime('%Y-%m-%d')}.csv"
        with open(filename, mode="w", newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['City', 'Date', 'Condition', 'Temperature', 'Category'])
            for row in rows:
                city, date, condition, temp = row
                category = "Hot" if temp >= 30 else "Warm"
                writer.writerow([city, date, condition, temp, category])

        logging.info(f"Exported weather data to {filename}")
        print(f"Exported to {filename}")
        conn.close()
    except Exception as e:
        logging.error(f"Export failed: {e}")
        print("Export failed. Check logs.")

# === Fetching + Storing function ===
def job():
    cities = ["London", "Hyderabad", "Kolkata", "Chennai", "Auckland", "Tokyo"]
    conn = sqlite3.connect('weather.db')
    cursor = conn.cursor()

    # Create weather table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            city TEXT,
            date TEXT,
            state TEXT,
            temp REAL
        )
    ''')

    # Create last_updated table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS last_updated (
            city TEXT PRIMARY KEY,
            last_fetch TEXT
        )
    ''')

    for city in cities:
        try:
            url = f"https://wttr.in/{city}?format=j1"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            current = data['current_condition'][0]
            condition = current['weatherDesc'][0]['value']
            temp = float(current['temp_C'])
            date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if temp > 20:
                # Insert weather
                cursor.execute('''
                    INSERT INTO weather (city, date, state, temp)
                    VALUES (?, ?, ?, ?)
                ''', (city, date, condition, temp))

                # Update last_updated
                cursor.execute('''
                    INSERT INTO last_updated (city, last_fetch)
                    VALUES (?, ?)
                    ON CONFLICT(city) DO UPDATE SET last_fetch = excluded.last_fetch
                ''', (city, date))

                logging.info(f"Weather fetched and saved for {city}")
                print(f"Weather in {city}")
                print(f"Condition: {condition}")
                print(f"Temperature: {temp}°C")
                print(f"Humidity: {current['humidity']}%")

        except requests.exceptions.RequestException as e:
            logging.warning(f"API error for {city}: {e}")
            print(f"API error for {city}")

        except sqlite3.Error as e:
            logging.error(f"DB error for {city}: {e}")
            print(f"DB error for {city}")

        except Exception as e:
            logging.critical(f"Unexpected error for {city}: {e}")
            print(f"Unexpected error for {city}")

    conn.commit()
    conn.close()

# === Scheduling ===
schedule.every(1).minutes.do(job)
schedule.every().day.at("23:29").do(export_weather_to_csv)

# === Run loop ===
while True:
    schedule.run_pending()
    time.sleep(1)
