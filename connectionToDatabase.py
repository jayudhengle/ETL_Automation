import pyodbc
import pandas as pd
from sqlalchemy import create_engine

server = 'localhost\\SQLEXPRESS'
database = 'jayudb'
username = 'myuser'
password = 'myuser'

# Note the double slashes in connection string
conn_url = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(conn_url)

