import sqlite3

conn = sqlite3.connect('weather.db')
cursor = conn.cursor()

# cursor.execute('DELETE FROM weather')
# cursor.execute('DELETE FROM daily_summary')
cursor.execute('SELECT * FROM daily_summary_new')
rows = cursor.fetchall()
for row in rows:
    print(row)
conn.commit()
conn.close()


