import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# ---------------- LOGGING CONFIG ----------------
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ---------------- DB ENGINE ----------------
engine = create_engine("sqlite:///inventory.db")

# ---------------- CHUNKED INGEST FUNCTION ----------------
def ingest_db_chunked(csv_path, table_name, engine, chunksize=100_000):
    for chunk in pd.read_csv(csv_path, chunksize=chunksize):
        chunk.to_sql(
            table_name,
            con=engine,
            if_exists="append",
            index=False
        )

# ---------------- LOAD ALL CSVs ----------------
def load_raw_data():
    start = time.time()
    logging.info("Starting data ingestion")

    for file in os.listdir("data"):
        if file.endswith(".csv"):
            logging.info(f"Ingesting {file} into database")
            print(f"Ingesting {file}...")
            
            ingest_db_chunked(
                csv_path=f"data/{file}",
                table_name=file[:-4],
                engine=engine
            )

    end = time.time()
    total_time = (end - start) / 60

    logging.info("---------------- ingestion complete ----------------")
    logging.info(f"Total time taken: {total_time:.2f} minutes")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    load_raw_data()
