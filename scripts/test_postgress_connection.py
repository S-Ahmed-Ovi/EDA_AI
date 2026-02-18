import psycopg2

DB_HOST = "localhost"
DB_PORT = "5432"    
DB_NAME = "demodb"
DB_USER = "postgres"
DB_PASSWORD = "    "

try:
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database= DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = connection.cursor()
    print("Connection to PostgreSQL database successful!")

    #execute a simple query to test the connection
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    tables = cursor.fetchall()
    print("Tables in the database:")
    for table in tables:
        print(table[0])

    #close the cursor and connection
    cursor.close()
    connection.close()
    print("PostgreSQL connection closed.")
    
        
except Exception as e:
    print(f"Error connecting to PostgreSQL database: {e}")
