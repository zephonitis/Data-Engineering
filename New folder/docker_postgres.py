import psycopg2

conn = psycopg2.connect(
    dbname="learning_db",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)
