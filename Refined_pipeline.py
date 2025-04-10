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
        
    except Exception as e:
        logging.error(f"Export failed: {e}")
        print("Export failed. Check logs.")
        
    finally:
        if conn:
            conn.close()

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

    skipped_count = 0
    
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
            
            if not city.strip() or not condition.strip() or temp < -50 or temp > 60:
                logging.warning(f"Invalid data skipped for {city}: temp={temp}, condition={condition}")
                skipped_count += 1 
                continue

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
            
        logging.info(f"Skipped {skipped_count} records in this run.")
        print(f"Total records skipped: {skipped_count}")

    conn.commit()
    conn.close()

def update_daily_summary():
    try:
        conn = sqlite3.connect('weather.db')
        cursor = conn.cursor()
        
        # Create the daily summary table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_summary_new (
                city TEXT,
                date TEXT,
                avg_temp REAL,
                min_temp REAL,
                max_temp REAL,
                category TEXT,
                PRIMARY KEY (city, date)
            )
        ''')
        
        # Aggregate data from weather table
        cursor.execute('''
            SELECT city, DATE(date), AVG(temp), MIN(temp), MAX(temp)
            FROM weather
            GROUP BY city, DATE(date)
        ''')
        
        rows = cursor.fetchall()
        
        for row in rows:
            city, date, avg_temp, min_temp, max_temp = row
            
            if avg_temp >= 30:
                category = "Hot"
            elif avg_temp >= 20:
                category = "Warm"
            else:
                category = "Cool"
            
            # Insert or update summary table
            cursor.execute('''
                INSERT INTO daily_summary_new (city, date, avg_temp, min_temp, max_temp, category)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(city, date) DO UPDATE SET
                    avg_temp = excluded.avg_temp,
                    min_temp = excluded.min_temp,
                    max_temp = excluded.max_temp,
                    category = excluded.category
            ''', (city, date, avg_temp, min_temp, max_temp, category))
        
        conn.commit()
        print("Daily summary updated successfully.")
        
    
    except Exception as e:
        print(f"Error updating daily summary: {e}")
    
    finally:
        conn.close()
     

# === Scheduling ===
schedule.every(1).minutes.do(job)
schedule.every().day.at("15:54").do(update_daily_summary)
schedule.every().day.at("15:55").do(export_weather_to_csv)

# === Run loop ===
while True:
    schedule.run_pending()
    time.sleep(1)
