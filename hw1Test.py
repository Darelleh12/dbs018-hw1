# Import the psycopg2 (Python driver that connects to psql)
import psycopg2
# Import your psql credentials from the python file db_config.py
from db_config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

# Initialize cursor and connection objects
# The "cursor" allows you to execute querys in psql and store its results.
# The "connection" authenticates the imported credentials from db_config.py to establish a connection with the "cursor" to psql.
cursor = None
connection = None

try:
    # Establish a connection to the PostgreSQL database
    # Edit db_config.py to change the values that get imported
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    
    # Create a new database session and return a cursor object
    cursor = connection.cursor()

    # Execute an SQL query to fetch data from table T0
    cursor.execute("SELECT * FROM T0;")
    
    # Fetch all rows from the cursor into a list
    rows = cursor.fetchall()
    
    # Display fetched rows
    print("Fetched rows:")
    for row in rows:
        col1, col2, col3 = row 
        print(f"col1: {col1}, col2: {col2}, col3: {col3}")
    

# Print any errors that occured trying to establish connection or execute a query
except Exception as e:
    print(f"An error occurred: {e}")

# After the code succesfully executes make sure to close connections
# Connections can remain open if your program unexpectedly closes
finally:
    # Close the cursor to avoid memory leak
    if cursor:
        cursor.close()
    
    # Close the connection to free up resources
    if connection:
        connection.close()
