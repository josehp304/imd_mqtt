import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="postgres",  # default DB
    user="postgres",      # default user
    password="123.",
    port=5432
)

cur = conn.cursor()
cur.execute("SELECT 1;")
print(cur.fetchone())
cur.close()
conn.close()