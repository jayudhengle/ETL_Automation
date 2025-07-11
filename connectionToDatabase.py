import pyodbc
import pandas as pd

# Define connection parameters
server = 'localhost\\SQLEXPRESS'  # e.g., 'localhost' or 'MY-PC\SQLEXPRESS'
database = 'jayudb'
username = "myuser"
password = "myuser"

conn_str = (
    f"DRIVER={{SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
)

# Establish a connection to the database
conn = pyodbc.connect(conn_str)

# Create a cursor to execute queries
cursor = conn.cursor()



