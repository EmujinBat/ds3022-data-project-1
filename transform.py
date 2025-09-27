import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log'
)
logger = logging.getLogger(__name__)

def transform_data():
    try: 
        # connecting to duckdb
        con = duckdb.connect(database="emissions.duckdb", read_only=False)
        logger.info("Connected to Duckdb")

        # yellow trips
        # joining cleaned yellow_trip table with emission data
        # then calculating total co2 per trip and other extractions
        # new columns are created in the new transformed table
        con.execute("""
            CREATE or REPLACE TABLE yellow_trips_transformed AS 
            SELECT yt.*, 
                    (yt.trip_distance * ve.co2_grams_per_mile / 1000) AS trip_co2_kgs, 
                    yt.trip_distance / NULLIF(datediff('second', yt.tpep_pickup_datetime, yt.tpep_dropoff_datetime) / 3600.0, 0) AS avg_mph,
                    EXTRACT(HOUR FROM yt.tpep_pickup_datetime) AS hour_of_day, 
                    EXTRACT(DAYOFWEEK FROM yt.tpep_pickup_datetime) AS day_of_week,
                    EXTRACT(WEEK FROM yt.tpep_pickup_datetime) AS week_of_year,
                    EXTRACT(MONTH FROM yt.tpep_pickup_datetime) AS month_of_year
                FROM yellow_trips yt
                JOIN vehicle_emissions ve 
                    ON ve.vehicle_type = 'yellow_taxi'
            """)
        logger.info("yellow trips transformed")

        # green trips
        # same calculation and column insertion
        con.execute("""
            CREATE or REPLACE TABLE green_trips_transformed AS 
            SELECT gt.*, 
                    (gt.trip_distance * ve.co2_grams_per_mile / 1000) AS trip_co2_kgs, 
                    gt.trip_distance / NULLIF(datediff('second', gt.lpep_pickup_datetime, gt.lpep_dropoff_datetime) / 3600.0, 0) AS avg_mph,
                    EXTRACT(HOUR FROM gt.lpep_pickup_datetime) AS hour_of_day, 
                    EXTRACT(DAYOFWEEK FROM gt.lpep_pickup_datetime) AS day_of_week,
                    EXTRACT(WEEK FROM gt.lpep_pickup_datetime) AS week_of_year,
                    EXTRACT(MONTH FROM gt.lpep_pickup_datetime) AS month_of_year
                FROM green_trips gt
                JOIN vehicle_emissions ve 
                    ON ve.vehicle_type = 'green_taxi'
            """)
        logger.info("green trips transformed")

        # peek at new columns
        peek_y = con.execute("""
           SELECT trip_co2_kgs, 
                             avg_mph, 
                             hour_of_day, 
                             day_of_week, 
                             week_of_year,
                             month_of_year
            FROM yellow_trips_transformed
            LIMIT 5              
        """).fetchdf()
        print(peek_y)

    except Exception as e:
        print(f"Error during transform: {e}")
        logger.error(f"Error during transform: {e}")

if __name__ == "__main__":
    transform_data()