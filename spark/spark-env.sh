#!/bin/bash

# Set Spark home directory
export SPARK_HOME=/opt/spark

# Set Java home directory
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Set the location of the Hadoop configuration directory if using Spark with Hadoop
# export HADOOP_CONF_DIR=/etc/hadoop/conf

# Set the location of the log directory
export SPARK_LOG_DIR=/opt/spark/logs

# Set the amount of memory for the Spark driver
export SPARK_DRIVER_MEMORY=2g

# Set the amount of memory for Spark executors
export SPARK_EXECUTOR_MEMORY=4g

# Set the number of cores to use for Spark executors
export SPARK_EXECUTOR_CORES=2

# Set additional JVM options if needed
export SPARK_JAVA_OPTS="-Djava.security.egd=file:/dev/./urandom"
