import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

def clean_data():
    try:
        con = duckdb.connect("emissions.duckdb")

        logger.info("Starting cleaning process...")

        # Clean yellow trips -> new table
        con.execute("""
            CREATE OR REPLACE TABLE yellow_trips_clean AS
            SELECT DISTINCT
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                trip_distance,
                passenger_count
            FROM yellow_trips
            WHERE passenger_count > 0
              AND trip_distance > 0
              AND trip_distance <= 100
              AND tpep_pickup_datetime IS NOT NULL
              AND tpep_dropoff_datetime IS NOT NULL
              AND tpep_pickup_datetime < tpep_dropoff_datetime
              AND date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 0
              AND date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) <= 86400
        """)
        logger.info("Yellow trips cleaned into yellow_trips_clean")

        # Clean green trips -> new table
        con.execute("""
            CREATE OR REPLACE TABLE green_trips_clean AS
            SELECT DISTINCT
                lpep_pickup_datetime,
                lpep_dropoff_datetime,
                trip_distance,
                passenger_count
            FROM green_trips
            WHERE passenger_count > 0
              AND trip_distance > 0
              AND trip_distance <= 100
              AND lpep_pickup_datetime IS NOT NULL
              AND lpep_dropoff_datetime IS NOT NULL
              AND lpep_pickup_datetime < lpep_dropoff_datetime
              AND date_diff('second', lpep_pickup_datetime, lpep_dropoff_datetime) > 0
              AND date_diff('second', lpep_pickup_datetime, lpep_dropoff_datetime) <= 86400
        """)
        logger.info("Green trips cleaned into green_trips_clean")

        # === Verification Checks ===
        def verify_table(table, pickup, dropoff):
            issues = con.execute(f"""
                SELECT
                    SUM(CASE WHEN passenger_count <= 0 THEN 1 ELSE 0 END) AS zero_passengers,
                    SUM(CASE WHEN trip_distance <= 0 THEN 1 ELSE 0 END) AS zero_distance,
                    SUM(CASE WHEN trip_distance > 100 THEN 1 ELSE 0 END) AS over_100_miles,
                    SUM(CASE WHEN date_diff('second', {pickup}, {dropoff}) > 86400 THEN 1 ELSE 0 END) AS over_24_hours,
                    SUM(CASE WHEN date_diff('second', {pickup}, {dropoff}) <= 0 THEN 1 ELSE 0 END) AS invalid_duration,
                    SUM(CASE WHEN {pickup} IS NULL OR {dropoff} IS NULL THEN 1 ELSE 0 END) AS null_timestamps,
                    COUNT(*) AS total_trips
                FROM {table};
            """).fetchone()

            logger.info(f"Verification for {table}:")
            logger.info(f"   Zero passengers: {issues[0]}")
            logger.info(f"   Zero distance:   {issues[1]}")
            logger.info(f"   Over 100 miles:  {issues[2]}")
            logger.info(f"   Over 24 hours:   {issues[3]}")
            logger.info(f"   Invalid duration:{issues[4]}")
            logger.info(f"   Null timestamps: {issues[5]}")
            logger.info(f"   Total trips:     {issues[6]}")

            if sum(issues[:6]) == 0:
                print(f"{table} passed verification ({issues[6]} trips remain).")
            else:
                print(f"{table} still has issues, check logs.")

        verify_table("yellow_trips_clean", "tpep_pickup_datetime", "tpep_dropoff_datetime")
        verify_table("green_trips_clean", "lpep_pickup_datetime", "lpep_dropoff_datetime")

        con.close()

    except Exception as e:
        logger.error(f"Error during cleaning: {e}", exc_info=True)
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_data()
