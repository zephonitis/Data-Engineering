import time
from config_loader import load_config
from cleaned_weather_pipeline import (
    simulate_messy_data,
    clean_and_store_parallel,
    log_cleaning_run,
    generate_weather_report
)

if __name__ == "__main__":
    overall_start = time.perf_counter()

    config = load_config()
    raw_data = simulate_messy_data()

    db_path = config.get("db_path", "weather.db")
    thresholds = config["temperature_thresholds"]
    output_folder = config["output_folder"]

    print("Cleaning and storing data...")
    clean_start = time.perf_counter()
    total, cleaned, skipped = clean_and_store_parallel(raw_data, db_path, min_temp=-50, max_temp=60)
    clean_end = time.perf_counter()
    print(f"Cleaning done in {clean_end - clean_start:.4f} seconds\n")

    log_cleaning_run(db_path, total, cleaned, skipped)

    print("Generating weather report...")
    report_start = time.perf_counter()
    generate_weather_report(thresholds=thresholds, output_folder=output_folder, db_path=db_path)
    report_end = time.perf_counter()
    print(f"Report generated in {report_end - report_start:.4f} seconds\n")

    overall_end = time.perf_counter()
    print(f"Total pipeline execution time: {overall_end - overall_start:.4f} seconds")