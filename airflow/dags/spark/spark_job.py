from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col, count, first, row_number
from pyspark.sql.window import Window
import argparse
import os
import logging

logging.basicConfig(level=logging.INFO)



def main(project_id, dataset, table, bucket):

    GCP_CREDS= os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    
    conf = SparkConf() \
        .setMaster('spark://spark-master:7077') \
        .setAppName('Spotify Pipeline') \
        .set("spark.jars.packages","com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.2,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.10,com.google.guava:guava:30.1-jre") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_CREDS) \
        .set("spark.executor.userClassPathFirst", "true") \
        .set("spark.driver.userClassPathFirst", "true") \
        .set("spark.executor.memory", "2G") \
        .set("spark.executor.cores", "1") \
        .set("spark.driver.memory", "2G") \
        .set("spark.driver.cores", "1") \
        .set("spark.eventLog.enabled", "true") \
        .set("spark.eventLog.dir", f"gs://{bucket}/spark-logs") \
        .set("spark.sql.adaptive.enabled", "true") \
        .set("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .set("spark.sql.shuffle.partitions", 2)



    sc = SparkContext(conf=conf)

    hadoop_conf = sc._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", GCP_CREDS)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
    hadoop_conf.set("fs.gs.temp.dir", f"gs://{bucket}")

    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()
    
    table = f"{project_id}.{dataset}.{table}"

    logging.info("Loading data from BigQuery")

    df = spark.read \
        .format("bigquery") \
        .option("table", table) \
        .load()
    
    logging.info("Aggregating data")

    df.createOrReplaceTempView("spotify_data")


    query = f"""
        SELECT 
            track_id,
            COUNT(*) AS times_played,
            FIRST(track_name) AS track_name,
            FIRST(artists) AS artists,
            FIRST(spotify_url) AS spotify_url,
            FIRST(danceability) AS danceability,
            FIRST(energy) AS energy,
            FIRST(valence) AS valence
        FROM 
            spotify_data
        GROUP BY 
            track_id
        ORDER BY 
            times_played DESC
        LIMIT 10
    """
    
    top_tracks = spark.sql(query)

    # df_summary = df.groupby("track_id") \
    #     .agg(
    #         count("*").alias("times_played"),
    #         first("track_name").alias("track_name"),
    #         first("artists").alias("artists"),
    #         first("spotify_url").alias("spotify_url"),
    #         first("danceability").alias("danceability"),
    #         first("energy").alias("energy"),
    #         first("valence").alias("valence")
    #     )
    

    # top_tracks = df_summary.orderBy(col("times_played").desc()).limit(10)
    # window_spec = Window.orderBy(col("times_played").desc())
    # top_tracks = top_tracks.withColumn("rank", row_number().over(window_spec))
    # top_tracks = top_tracks.coalesce(2)

    output_table = f"{project_id}.{dataset}.spotify_summary"


    logging.info(f"Writing top tracks to BigQuery: {output_table}")

    # Write directly to BigQuery
    top_tracks.write \
        .format("bigquery") \
        .option("table", output_table) \
        .option("writeMethod", "direct") \
        .option("temporaryGcsBucket", bucket) \
        .mode("overwrite") \
        .save()
    
    logging.info("Job completed successfully")


    
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True, type=str)
    parser.add_argument("--dataset", required=True, type=str)
    parser.add_argument("--table", required=True, type=str)
    parser.add_argument("--bucket", required=True, type=str)
    args = parser.parse_args()

    main(args.project_id, args.dataset, args.table, args.bucket)