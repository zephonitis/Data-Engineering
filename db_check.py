import sqlite3

conn = sqlite3.connect('weather.db')
cursor = conn.cursor()

cursor.execute('SELECT * FROM last_updated')
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()