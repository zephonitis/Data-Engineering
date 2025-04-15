import sqlite3

conn = sqlite3.connect('people.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS people (
    name TEXT,
    age INTEGER,
    role TEXT
)
''')

data = [
    ('Alice', 29, 'Engineer'),
    ('Bob', 35, 'Analyst'),
    ('Charlie', 40, 'Manager')
]

cursor.executemany('INSERT INTO people VALUES(?,?,?)', data)
conn.commit()

cursor.execute('SELECT * FROM people')
rows = cursor.fetchall()
for row in rows: 
    print(row)
    
conn.close()