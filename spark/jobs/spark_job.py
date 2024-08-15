from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, first
import argparse
import os



def main(project_id, dataset, table):

    GCP_CREDS= os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Spotify Pipeline") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.2") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_CREDS) \
        .getOrCreate()

    table = f"{args.project_id}.{args.dataset}.{args.table}"

    # Load and transform data
    df = spark.read \
        .format("bigquery") \
        .option("table", table) \
        .load()

    df_summary = df.groupby("track_id").agg(
        count("*").alias("times_played"),
        first("track_name").alias("track_name"),
        first("artist").alias("artist")
    )

    top_tracks = df_summary.orderBy(col("times_played").desc()).limit(10)

    # Write directly to BigQuery
    top_tracks.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{dataset}.spotify_summary") \
        .save()
    
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True, type=str)
    parser.add_argument("--dataset", required=True, type=str)
    parser.add_argument("--table", required=True, type=str)
    args = parser.parse_args()

    main(args.project_id, args.dataset, args.table)