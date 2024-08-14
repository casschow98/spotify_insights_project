from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse


def ms_reformat(milliseconds):
    total_seconds = milliseconds / 1000
    minutes = int(total_seconds // 60)
    seconds = int(total_seconds % 60)

    return f"{minutes}:{seconds:02}"


def main(project_id, dataset, table, gcs_path):
    # Initialize Spark session

    spark = SparkSession.builder \
        .appName("Spotify Pipeline") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.2") \
        .getOrCreate()

    # Load and transform data
    df = spark.read.csv(gcs_path, header=True, inferSchema=True)

    df_transformed = df.withColumn("track_duration", ms_reformat("duration_ms"))  # Example transformation

    # Write directly to BigQuery
    df_transformed.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{dataset}.{table}") \
        .save()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True, type=str)
    parser.add_argument("--dataset", required=True, type=str)
    parser.add_argument("--table", required=True, type=str)
    parser.add_argument("--gcs_path", required=True, type=str)
    args = parser.parse_args()

    main(args.gcs_path, args.bq_table_input, args.project_id, args.dataset, args.table)