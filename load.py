import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def generate_file_urls():
    yellow_files, green_files = [], []
    for year in range(2015, 2025):
        for month in range(1, 13):
            yellow_files.append(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet")
            green_files.append(f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet")
    logger.info("Generated file URLs for yellow and green taxi trips (2015–2024, all months)")
    return yellow_files, green_files

def load_parquet_files(batch_sleep=60):
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        yellow_files, green_files = generate_file_urls()

        # Drop and recreate base tables
        con.execute("DROP TABLE IF EXISTS yellow_trips")
        con.execute("DROP TABLE IF EXISTS green_trips")
        con.execute("DROP TABLE IF EXISTS vehicle_emissions")
        logger.info("Dropped existing tables if they existed")

        # Load YELLOW trips in batches
        logger.info("Loading yellow taxi trips...")
        for i, f in enumerate(yellow_files):
            if i == 0:
                con.execute(f"""
                    CREATE TABLE yellow_trips AS
                    SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, passenger_count
                    FROM read_parquet('{f}', union_by_name=True)
                """)
            else:
                con.execute(f"""
                    INSERT INTO yellow_trips
                    SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, passenger_count
                    FROM read_parquet('{f}', union_by_name=True)
                """)
            logger.info(f"Inserted {f} into yellow_trips ({i+1}/{len(yellow_files)})")
            time.sleep(batch_sleep)

        # Load GREEN trips in batches
        logger.info("Loading green taxi trips...")
        for i, f in enumerate(green_files):
            if i == 0:
                con.execute(f"""
                    CREATE TABLE green_trips AS
                    SELECT lpep_pickup_datetime, lpep_dropoff_datetime, trip_distance, passenger_count
                    FROM read_parquet('{f}', union_by_name=True)
                """)
            else:
                con.execute(f"""
                    INSERT INTO green_trips
                    SELECT lpep_pickup_datetime, lpep_dropoff_datetime, trip_distance, passenger_count
                    FROM read_parquet('{f}', union_by_name=True)
                """)
            logger.info(f"Inserted {f} into green_trips ({i+1}/{len(green_files)})")
            time.sleep(batch_sleep)

        # Load vehicle emissions
        con.execute("CREATE TABLE vehicle_emissions AS SELECT * FROM read_csv_auto('vehicle_emissions.csv', HEADER=TRUE)")
        logger.info("Loaded vehicle emissions")

        yellow_count = con.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
        green_count = con.execute("SELECT COUNT(*) FROM green_trips").fetchone()[0]
        print(f"Yellow trips: {yellow_count}, Green trips: {green_count}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()
