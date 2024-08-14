# Use the official Spark base image
FROM apache/spark:3.5.1

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

USER root
# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install pyspark && \
    apt-get clean

# Install PySpark
RUN pip3 install pyspark

COPY ./jobs /opt/spark/jobs

# Expose the ports for Spark
EXPOSE 8080 7077

USER spark

# Set the default command to start Spark
CMD ["/opt/spark/bin/spark-submit", "org.apache.spark.deploy.master.Master"]

