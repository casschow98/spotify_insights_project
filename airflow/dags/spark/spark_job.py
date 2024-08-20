from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col, count, first
import argparse
import os



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
        .set("spark.eventLog.dir", f"gs://{bucket}/spark-logs")


        # .set("spark.jars","gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar,gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.10.jar") \


    sc = SparkContext(conf=conf)

    hadoop_conf = sc._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", GCP_CREDS)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()
    
    table = f"{project_id}.{dataset}.{table}"

    # Load and transform data
    df = spark.read \
        .format("bigquery") \
        .option("table", table) \
        .load() \
        .repartition(10)

    df_summary = df.groupby("track_id").agg(
        count("*").alias("times_played"),
        first("track_name").alias("track_name"),
        first("artists").alias("artists")
    )

    top_tracks = df_summary.orderBy(col("times_played").desc()).limit(10)

    output_table = f"{project_id}.{dataset}.spotify_summary"

    # Write directly to BigQuery
    top_tracks.write \
        .format("bigquery") \
        .option("table", output_table) \
        .option("temporaryGcsBucket", bucket) \
        .option("writeMethod", "direct") \
        .mode("overwrite") \
        .save()
    
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True, type=str)
    parser.add_argument("--dataset", required=True, type=str)
    parser.add_argument("--table", required=True, type=str)
    parser.add_argument("--bucket", required=True, type=str)
    args = parser.parse_args()

    main(args.project_id, args.dataset, args.table, args.bucket)