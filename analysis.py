import duckdb
import matplotlib.pyplot as plt
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="analysis.log"
)
logger = logging.getLogger(__name__)

def analyze_data():
    """NYC Taxi CO2 Emissions Analysis (Yellow & Green)"""
    con = None
    try:
        con = duckdb.connect(database="emissions.duckdb", read_only=False)
        logger.info("Connected to DuckDB for analysis")

        logger.info("=" * 60)
        logger.info("NYC TAXI CO2 EMISSIONS ANALYSIS")
        logger.info("=" * 60)

        # === 1. LARGEST TRIP ===
        logger.info("1. LARGEST CO2 TRIP")
        for taxi in ["yellow", "green"]:
            table = f"{taxi}_trips_transformed"
            query = f"""
                SELECT *
                FROM {table}
                ORDER BY trip_co2_kgs DESC
                LIMIT 1
            """
            trip = con.execute(query).fetchdf()
            logger.info(f"   {taxi.title()} taxi: {trip.iloc[0].to_dict()}")
            print(f"\nLargest CO2 trip ({taxi.upper()}):")
            print(trip)

        # === 2. HOURLY PATTERNS ===
        logger.info("2. HOURLY CO2 PATTERNS")
        for taxi in ["yellow", "green"]:
            table = f"{taxi}_trips_transformed"
            query = f"""
                SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_co2
                FROM {table}
                GROUP BY hour_of_day
                ORDER BY avg_co2 DESC
            """
            stats = con.execute(query).fetchdf()
            logger.info(f"   {taxi.title()} max hour: {stats.iloc[0].to_dict()}")
            logger.info(f"   {taxi.title()} min hour: {stats.iloc[-1].to_dict()}")

        # === 3. DAY OF WEEK PATTERNS ===
        logger.info("3. WEEKDAY CO2 PATTERNS")
        days = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]
        for taxi in ["yellow", "green"]:
            table = f"{taxi}_trips_transformed"
            query = f"""
                SELECT day_of_week, AVG(trip_co2_kgs) AS avg_co2
                FROM {table}
                GROUP BY day_of_week
                ORDER BY avg_co2 DESC
            """
            stats = con.execute(query).fetchdf()
            logger.info(f"   {taxi.title()} max day: {days[int(stats.iloc[0]['day_of_week'])]} "
                        f"({stats.iloc[0]['avg_co2']:.2f} kg)")
            logger.info(f"   {taxi.title()} min day: {days[int(stats.iloc[-1]['day_of_week'])]} "
                        f"({stats.iloc[-1]['avg_co2']:.2f} kg)")

        # === 4. WEEK & MONTH PATTERNS ===
        logger.info("4. WEEK & MONTH CO2 PATTERNS")
        for period, col in [("week", "week_of_year"), ("month", "month_of_year")]:
            for taxi in ["yellow", "green"]:
                table = f"{taxi}_trips_transformed"
                query = f"""
                    SELECT {col}, AVG(trip_co2_kgs) AS avg_co2
                    FROM {table}
                    GROUP BY {col}
                    ORDER BY avg_co2 DESC
                """
                stats = con.execute(query).fetchdf()
                logger.info(f"   {taxi.title()} max {period}: {stats.iloc[0].to_dict()}")
                logger.info(f"   {taxi.title()} min {period}: {stats.iloc[-1].to_dict()}")

        # === 5. MONTHLY TOTALS PLOT ===
        logger.info("5. MONTHLY TOTALS PLOT")
        yellow_monthly = con.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2
            FROM yellow_trips_transformed
            GROUP BY month_of_year
            ORDER BY month_of_year
        """).fetchdf()

        green_monthly = con.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2
            FROM green_trips_transformed
            GROUP BY month_of_year
            ORDER BY month_of_year
        """).fetchdf()

        plt.figure(figsize=(10,6))
        plt.plot(yellow_monthly["month_of_year"], yellow_monthly["total_co2"],
                 label="Yellow Taxi", marker="o")
        plt.plot(green_monthly["month_of_year"], green_monthly["total_co2"],
                 label="Green Taxi", marker="o")
        plt.xlabel("Month")
        plt.ylabel("Total CO2 (kg)")
        plt.title("Monthly CO2 Totals for NYC Taxis")
        plt.legend()
        plt.xticks(range(1,13))
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig("monthly_co2_totals.png", dpi=300, bbox_inches="tight")
        logger.info("Saved monthly_co2_totals.png")
        print("\n Monthly CO2 totals plot saved as 'monthly_co2_totals.png'")

        return True

    except Exception as e:
        logger.error(f"Error during analysis: {e}", exc_info=True)
        print(f"Error during analysis: {e}")
        return False
    finally:
        if con:
            con.close()
            logger.info("Connection closed")

if __name__ == "__main__":
    start = datetime.now()
    success = analyze_data()
    duration = datetime.now() - start
    if success:
        logger.info(f"Analysis completed in {duration}")
        print(f"\nAnalysis completed in {duration}")
    else:
        logger.error("Analysis failed")
