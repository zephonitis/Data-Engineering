# import sqlite3
# print(sqlite3.sqlite_version)


import sqlite3

# Connect to the database
conn = sqlite3.connect(r"C:\Users\ashwi\OneDrive\Desktop\DE\my_database.db")
cursor = conn.cursor()

# Query the data
cursor.execute("SELECT * FROM students")
rows = cursor.fetchall()

# Print the data
for row in rows:
    print(row)

# Close the connection
conn.close()