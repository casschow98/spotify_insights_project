from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse



def main(project_id, dataset, table):
    # Initialize Spark session

    spark = SparkSession.builder \
        .appName("Spotify Pipeline") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.2") \
        .getOrCreate()

    table = f"{args.project_id}.{args.dataset}.{args.table}"

    # Load and transform data
    df = spark.read \
        .format("bigquery") \
        .option("table", table) \
        .load()

    # Write directly to BigQuery
    df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{dataset}.{table}") \
        .save()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True, type=str)
    parser.add_argument("--dataset", required=True, type=str)
    parser.add_argument("--table", required=True, type=str)
    args = parser.parse_args()

    main(args.project_id, args.dataset, args.table)