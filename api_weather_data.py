import requests
import sqlite3
from datetime import datetime

# === Fetch weather data from wttr.in ===
city = "London"
url = f"https://wttr.in/{city}?format=j1"

response = requests.get(url)
data = response.json()

# Extract current weather data
current = data['current_condition'][0]
condition = current['weatherDesc'][0]['value']
temp = float(current['temp_C'])
date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Print to console
print(f"Weather in {city}")
print(f"Condition: {condition}")
print(f"Temperature: {temp}°C")
print(f"Humidity: {current['humidity']}%")

# === Save to SQLite ===
conn = sqlite3.connect('weather.db')
cursor = conn.cursor()

# Create the table if not exists
cursor.execute('''
CREATE TABLE IF NOT EXISTS weather (
    city TEXT,
    date TEXT,
    state TEXT,
    temp REAL
)
''')

# Insert current weather data
cursor.execute('''
INSERT INTO weather (city, date, state, temp)
VALUES (?, ?, ?, ?)
''', (city, date, condition, temp))

conn.commit()

# Fetch and print rows
cursor.execute('SELECT * FROM weather')
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()
