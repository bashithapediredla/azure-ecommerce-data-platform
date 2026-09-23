import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD") 
driver = "{ODBC Driver 18 for SQL Server}"
print("user:", username)
print("password length:", len(password) if password else None)
conn_str = (
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    f"Encrypt=yes;"
)

conn = pyodbc.connect(conn_str)
print("Connected successfully!")
conn.close()


from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd

password_encoded = quote_plus(password)

connection_string = f"mssql+pyodbc://{username}:{password_encoded}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server"

engine = create_engine(connection_string)

df_customers = pd.read_csv(r"C:\Users\bashi\OneDrive\Desktop\e-commerce project\source_data\customers.csv")
df_orders= pd.read_csv(r"C:\Users\bashi\OneDrive\Desktop\e-commerce project\source_data\orders.csv")

df_customers.to_sql("customers", engine, if_exists="replace", index=False)
df_orders.to_sql("orders", engine, if_exists="replace", index=False)

print("customers table loaded successfully!")
print ("orders table loaded successfully")