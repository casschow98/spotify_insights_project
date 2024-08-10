from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse


def main(project_id, bq_table_input, bucket):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Spotify Pipeline") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.2") \
        .getOrCreate()

    # Load and transform data
    df = spark.read.csv(f"gs://{bucket}/path/to/data.csv", header=True, inferSchema=True)
    df_transformed = df.withColumn("new_column", col("existing_column") * 2)  # Example transformation

    # Write directly to BigQuery
    df_transformed.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{bq_table_input}") \
        .save()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True, type=str)
    parser.add_argument("--bq_table_input", required=True, type=str)
    parser.add_argument("--bucket", required=True, type=str)
    args = parser.parse_args()

    main(args.project_id, args.bq_table_input, args.bucket)