# Commercial Booking Analysis

This project analyzes flight bookings to produce insights such as passenger counts per country, day of week, and season, using PySpark.

## Features

- Cleans and transforms booking and airport data
- Joins flights, passengers, and airport information
- Counts unique confirmed passengers per flight leg
- Outputs aggregated tables by country, day of week, and season

## How to Run

1. **Install dependencies:**
   ```
   pip install -r requirements.txt
   ```

2. **Configure environment variables:**
   - Copy `.env.example` to `.env` and fill in paths for data files.

3. **Run the main application:**
   ```
   python src/main_application.py --input <input_json> --output <output_dir> --start_date YYYY-MM-DD --end_date YYYY-MM-DD
   ```

## Project Structure

- `src/main_application.py` — Main PySpark logic
- `src/utils/` — Utility functions
- `requirements.txt` — Python dependencies
- `.env.example` — Example environment configuration

## Output

Results are saved as parquet files in the specified output directory.

## Notes

- Requires Python 3.12 and Java (for Spark)
- Data files must be in the format specified in configs