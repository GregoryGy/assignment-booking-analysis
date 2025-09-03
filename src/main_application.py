import os
import datetime
import json
from dotenv import load_dotenv
from pyspark.sql import DataFrame
from utils.args_utils import ArgsBuilder
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,
    explode,
    from_utc_timestamp,
    dayofweek,
    when, 
    row_number)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)
from pyspark.sql.window import Window
from pyspark.sql.functions import coalesce, regexp_replace, try_to_timestamp


class MainApplication:
    def __init__(self):
        self.args = ArgsBuilder.build_args()
        self.spark = self.__init_spark(self.args.is_local)

    def __init_spark(self, is_local: bool) -> SparkSession:
        if is_local:
            return (
                SparkSession.builder.appName("LocalBookingApp")
                .master("local[2]")
                .getOrCreate()
            )
        else:
            return SparkSession.builder.appName("ClusterBookingApp").getOrCreate()

    def _load_schema_from_json(self, json_path) -> StructType:
        """Load schema from JSON file."""
        type_map = {
            "string": StringType(),
            "double": DoubleType(),
            "integer": IntegerType(),
        }
        with open(json_path, "r") as f:
            fields = json.load(f)
        return StructType(
            [
                StructField(field["name"], type_map[field["type"]], True)
                for field in fields
            ]
        )

    def read_airports_data(self) -> DataFrame:
        """
        Read airports data from DAT file.
        """
        schema = self._load_schema_from_json(os.getenv("AIRPORTS_CONFIG"))
        airports_df = self.spark.read.csv(
            os.getenv("AIRPORTS_TABLE_PATH"),
            schema=schema,
            header=False,
            inferSchema=False,
        )
        airports_df.show()
        return airports_df

    def load_transform_config(self, path):
        with open(path, "r") as f:
            return json.load(f)

    def apply_transform(self, df: DataFrame, config) -> DataFrame:
        """
        Apply transformations to the DataFrame based on the config.
        """
        for exp in config.get("explodes", []):
            df = df.select(
                explode(exp["column"]).alias(exp["alias"]),
                *[col for col in df.columns if col != exp["column"]],
            )

        select_exprs = []
        for sel in config["select"]:
            if "alias" in sel:
                select_exprs.append(f"{sel['name']} as {sel['alias']}")
            else:
                select_exprs.append(sel["name"])
        df = df.selectExpr(*select_exprs)
        return df

    def clean_silver_layer(self) -> None:
        """
        Clean the silver layer by removing duplicates.
        """
        if self.args.is_local:
            silver_dir = os.getenv("SILVER_PATH")
            subdirs = [
                os.path.join(silver_dir, name)
                for name in os.listdir(silver_dir)
                if os.path.isdir(os.path.join(silver_dir, name))
            ]
        else:
            silver_dir = f"hdfs:///{os.getenv("SILVER_PATH")}"
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jsc.hadoopConfiguration()
            )
            path = self.spark._jvm.org.apache.hadoop.fs.Path(silver_dir)
            status = fs.listStatus(path)
            subdirs = [
                str(file.getPath().toString()) for file in status if file.isDirectory()
            ]
        for path in subdirs:
            df = self.spark.read.parquet(path)
            df = df.dropDuplicates()
            df.write.parquet(path, mode="overwrite")

    def load_data(self, input_path: str) -> None:
        """
        Load data from the specified input path.
        """

        if self.args.is_local:
            bronze_path = os.getenv("BRONZE_PATH")
            silver_base = os.getenv("SILVER_PATH")
        else:
            bronze_path = f"hdfs:///{os.getenv("BRONZE_PATH")}"
            silver_base = f"hdfs:///{os.getenv("SILVER_PATH")}"

        df_bronze = self.spark.read.json(input_path)
        df_bronze.printSchema()
        df_bronze.createOrReplaceTempView("bronze")
        df_bronze = self.spark.sql(
            """
            SELECT *,
            COALESCE(
                TRY_TO_TIMESTAMP(timestamp, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                TRY_TO_TIMESTAMP(timestamp, "yyyy-MM-dd'T'HH:mm"),
                TRY_TO_TIMESTAMP(timestamp, "yyyy-MM-dd HH:mm")
            ) AS formatted_timestamp
            FROM bronze
            """
        )
        df_bronze.write.parquet(bronze_path, mode="overwrite")

        travelrecord_df = df_bronze.select(
            "event.DataElement.travelrecord.*", "formatted_timestamp"
        )
        travelrecord_df.write.parquet(silver_base + "travelrecord/", mode="overwrite")

        passengers_config = self.load_transform_config(os.getenv("PASSENGERS_CONFIG"))
        passengers_df = self.apply_transform(df_bronze, passengers_config)
        flights_config = self.load_transform_config(os.getenv("FLIGHTS_CONFIG"))
        flights_df = self.apply_transform(df_bronze, flights_config)
        passengers_df.write.parquet(silver_base + "passengers/", mode="append")

        flights_df.write.parquet(silver_base + "flights/", mode="append")

    def join_airports(self) -> DataFrame:
        silver_base = (
            os.getenv("SILVER_PATH")
            if self.args.is_local
            else f"hdfs:///{os.getenv('SILVER_PATH')}"
        )
        flights_df = self.spark.read.parquet(silver_base + "flights/")
        airports_df = self.read_airports_data()

        airports_origin = airports_df.alias("origin")
        airports_dest = airports_df.alias("dest")
        airports_origin = airports_df.select(
            "IATA", "Country", "Timezone", "City", "Altitude", "Airport ID"
        ).alias("origin")
        airports_dest = airports_df.select(
            "IATA", "Country", "Timezone", "City", "Altitude", "Airport ID"
        ).alias("dest")

        flights_with_origin = flights_df.join(
            airports_origin, flights_df["origin_airport"] == col("origin.IATA"), "left"
        )
        flights_with_origin = flights_with_origin.withColumnRenamed("Country", "origin_country")
        flights_with_origin = flights_with_origin.withColumnRenamed("Timezone", "origin_timezone")
        flights_with_origin = flights_with_origin.withColumnRenamed("Airport ID", "origin_airport_id")
        flights_with_origin = flights_with_origin.withColumnRenamed("Altitude", "origin_altitude")
        flights_with_origin = flights_with_origin.withColumnRenamed("City", "origin_city")

        flights_with_countries = flights_with_origin.join(
            airports_dest,
            flights_with_origin["destination_airport"] == col("dest.IATA"),
            "left",
        )
        flights_with_countries = flights_with_countries.withColumnRenamed("Country", "destination_country")
        flights_with_countries = flights_with_countries.withColumnRenamed("Timezone", "destination_timezone")
        flights_with_countries = flights_with_countries.withColumnRenamed("Airport ID", "destination_airport_id")
        flights_with_countries = flights_with_countries.withColumnRenamed("Altitude", "destination_altitude")
        flights_with_countries = flights_with_countries.withColumnRenamed("City", "destination_city")

        return flights_with_countries
    
    def join_flights_passengers(self, flights_df) -> DataFrame:
        """
        Join flights DataFrame with passengers DataFrame.
        """
        silver_base = (
            os.getenv("SILVER_PATH")
            if self.args.is_local
            else f"hdfs:///{os.getenv('SILVER_PATH')}"
        )
        flights_df = flights_df.alias("flights")
        passengers_df = self.spark.read.parquet(silver_base + "passengers/").alias(
            "passengers"
        )
        joined_df = flights_df.join(
            passengers_df,
            (col("flights.envelop_number") == col("passengers.envelop_number"))
            & (
                col("flights.formatted_timestamp")
                == col("passengers.formatted_timestamp")
            ),
            "inner",
        )
        return joined_df

    def run(self):
        input_path = self.args.input
        output_path = self.args.output
        start_date = datetime.datetime.strptime(self.args.start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(self.args.end_date, "%Y-%m-%d")

        self.load_data(input_path=input_path)
        self.clean_silver_layer()
        flights_df = self.join_airports()
        joined_df = self.join_flights_passengers(flights_df)

        filtered_df = joined_df.filter(
            (col("flights.formatted_timestamp") >= start_date)
            & (col("flights.formatted_timestamp") <= end_date)
        )

        filtered_df = filtered_df.filter(
            (col("flights.operating_airline") == "KL")
            & (col("origin_country") == "Netherlands")
        )

        filtered_df = filtered_df.dropDuplicates([
            "crid", "departure_date", "origin_airport", "destination_airport"
        ])
        
        filtered_df = filtered_df.withColumn(
            "origin_timezone",
            when(
                (col("origin_timezone").isNull()) | (col("origin_timezone").rlike("^[0-9.]+$")),
                "Europe/Amsterdam"
            ).otherwise(col("origin_timezone").cast("string"))
        )

        filtered_df = filtered_df.withColumn(
            "local_datetime",
            from_utc_timestamp(col("flights.formatted_timestamp"), col("origin_timezone"))
        )

        filtered_df = filtered_df.withColumn(
            "day_of_week",
            dayofweek(col("local_datetime"))
        )

        window_spec = Window.partitionBy(
            "crid", "departure_date", "origin_airport", "destination_airport"
        ).orderBy(col("flights.formatted_timestamp").desc())

        latest_status_df = filtered_df.withColumn(
            "row_num", row_number().over(window_spec)
        ).filter(
            (col("row_num") == 1) & (col("flights.booking_status") == "CONFIRMED")
        )
        output_columns = [
            "crid", "departure_date", "origin_airport", "destination_airport",
            "origin_country", "destination_country", "day_of_week", "local_datetime",
            col("flights.envelop_number").alias("envelop_number"),
            col("flights.formatted_timestamp").alias("formatted_timestamp")
        ]
        latest_status_df = latest_status_df.select(*output_columns)

        if self.args.is_local:
            gold_dir = os.getenv("GOLD_PATH")
        else:
            gold_dir = f"hdfs:///{os.getenv('GOLD_PATH')}"

        latest_status_df.write.mode("overwrite").parquet(gold_dir)

        breakpoint()


if __name__ == "__main__":
    load_dotenv()
    main = MainApplication()
    main.run()
