# Commercial Booking Analysis

This project analyzes flight bookings using PySpark to produce insights such as passenger counts per country, day of week, and season. It supports scalable processing of large datasets from local directories or HDFS.

## Features

- Cleans and transforms booking and airport data
- Joins flights, passengers, and airport information
- Counts unique confirmed passengers per flight leg
- Aggregates statistics by country, including number of adults, children, and average age
- Outputs tables by country, day of week, and season

## How to Run (Standalone)

1. **Install dependencies:**
   ```
   pip install -r requirements.txt
   ```

2. **Configure environment variables:**
   - Copy `.env.example` to `.env` and fill in paths for data files, configs, and output directories.

3. **Run the main application:**
   ```
   python src/main_application.py --input <input_dir_or_hdfs> --output <output_dir_or_hdfs> --is_local True --start-date YYYY-MM-DD --end-date YYYY-MM-DD
   ```
   - Use `--is_local False` for HDFS mode.

## How to Run (Docker)

1. **Build the Docker image:**
   ```
   docker-compose build
   ```

2. **Start the container:**
   ```
   docker-compose up
   ```

   - Input, output, and config directories are mounted as persistent volumes.
   - Adjust the `command:` section in `docker-compose.yaml` to set your input/output paths and arguments.

3. **Example docker-compose command:**
   ```yaml
   command: >
     --input /app/data/bookings/
     --output /app/artifacts
     --is_local True
     --start-date 2019-01-01
     --end-date 2019-12-31
   ```

## Project Structure

- `src/main_application.py` — Main PySpark logic
- `src/utils/` — Utility functions
- `src/configs/` — Configuration files for schema and transformations
- `requirements.txt` — Python dependencies
- `.env.example` — Example environment configuration
- `Dockerfile` — Docker build instructions
- `docker-compose.yaml` — Docker Compose configuration

## Output

Results are saved as parquet files in the specified output directory or HDFS path.

## Notes

- Requires Python 3.12 and Java (for Spark)
- Data files must match the format specified in configs
- Supports scalable processing of many files in a directory or on HDFS

## Example (Standalone)

```
python src/main_application.py --input data/bookings/ --output artifacts --is_local True --start-date 2019-01-01 --end-date 2019-12-31
```

## Example (Docker Compose)

```
docker-compose up
```

For HDFS:
```
python src/main_application.py --input hdfs:///data/bookings/ --output hdfs:///artifacts --is_local False --start-date 2019-01-01 --end-date 2019-12-31
```