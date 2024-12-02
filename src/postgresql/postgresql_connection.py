from sqlalchemy import create_engine
import pandas as pd
# Using flask to make an api 
# import necessary libraries and functions 
from flask import Flask, jsonify, request 

# creating a Flask app 
app = Flask(__name__) 

filePath = "D:\\SampleData\\uk_cities.csv"
myTableName = "UKCityMaster"
username = "consultants"
password = "WelcomeItc%402022"
host = "18.132.73.146"
databaseName = "testdb"
port = 5432

def connect_to_postgres():
    try:
        # Create an engine to connect to PostgreSQL
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{databaseName}')
        print("Connection to PostgreSQL established successfully.")
        return engine
    except Exception as error:
        print(f"Error while connecting to PostgreSQL: {error}")
        return None

def read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
        print(f"CSV file read successfully from {file_path}")
        return df
    except Exception as error:
        print(f"Error while reading CSV file: {error}")
        return None

def write_to_postgres(engine, df, table_name, if_exists='replace'):
    try:
        # Write the DataFrame to the PostgreSQL table
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        print(f"Data written to {table_name} successfully.")
    except Exception as error:
        print(f"Error while writing data to PostgreSQL: {error}")

def close_connection(engine):
    if engine:
        engine.dispose()
        print("PostgreSQL connection is closed.")


@app.route('/', methods = ['GET', 'POST']) 
def home(df): 
	if(request.method == 'GET'): 
		return jsonify({'data': df}) 

def main():
    engine = connect_to_postgres()
    if engine is not None:
        df = read_csv_file(filePath)
        if df is not None:
            table_name = myTableName
            write_to_postgres(engine, df, table_name)
        close_connection(engine)

if __name__ == "__main__":
    main()
