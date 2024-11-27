import mysql.connector

# Establishing the connection
connection = mysql.connector.connect(
    host="localhost",          # Host (e.g., 'localhost')
    user="root",      # Your MySQL username
    password="admin",  # Your MySQL password
    database="test"   # Your database name
)

# Create a cursor object using the connection
cursor = connection.cursor()

# Define your SQL query
query = "SELECT * FROM Departments"

# Execute the query
cursor.execute(query)

# Fetch all rows from the executed query
rows = cursor.fetchall()

# Print each row
for row in rows:
    print(row)

# Close the cursor and connection
cursor.close()
connection.close()