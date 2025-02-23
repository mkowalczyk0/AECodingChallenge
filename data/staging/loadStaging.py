import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()


DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
CSV_PATH = os.getenv("CSV_PATH")
CLEANED_JSON_PATH = os.getenv("CLEANED_JSON_PATH")


def load_dataframes():
    return {
        'stg_brands': pd.read_json(f'{CLEANED_JSON_PATH}brands.json'),
        'stg_items': pd.read_json(f'{CLEANED_JSON_PATH}items.json'),
        'stg_receipts': pd.read_json(f'{CLEANED_JSON_PATH}receipts.json'),
        'stg_users': pd.read_json(f'{CLEANED_JSON_PATH}users.json')
    }

def create_database_connection():
    connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def load_to_mysql(dfs, engine):
    for table_name, df in dfs.items():
        try:
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',  # going to be treating this as an SCD-Type 1 data model, so replacing will give us the 'latest' data
                index=False,
                chunksize=1000  # batch size
            )
            print(f"Successfully loaded {table_name} table")
        except Exception as e:
            print(f"Error loading {table_name}: {str(e)}")

def main():
    try:
        dfs = load_dataframes()
        engine = create_database_connection()
        
        print("loading to MySQL...")
        load_to_mysql(dfs, engine)
        
        print("--------------------DONE--------------------")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
    