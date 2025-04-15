import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname="learning_db",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Query the data
cursor.execute("SELECT * FROM students")
rows = cursor.fetchall()

# Print the data
for row in rows:
    print(row)

# Close the connection
conn.close()
