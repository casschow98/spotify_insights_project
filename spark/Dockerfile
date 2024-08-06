# Dockerfile for Spark

FROM bitnami/spark:3.5.1


# Custom logging
# COPY log4j2.properties /opt/bitnami/spark/conf/log4j2.properties

# any files/libraries you need on the cluster, install here ie:
RUN pip install --no-cache-dir pandas

# FROM apache/spark:latest

# USER root

# # Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# RUN mkdir -p /var/lib/apt/lists/partial

# # Copy configuration files if you have custom configurations
# # COPY spark-defaults.conf $SPARK_HOME/conf/
# # COPY spark-env.sh $SPARK_HOME/conf/

# # Install additional dependencies if needed
# RUN apt-get update && apt-get install -y curl procps && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/*

# # Default command
# CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
CMD ["sh", "-c", "if [ \"$SPARK_MODE\" = \"master\" ]; then \
        /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master; \
    elif [ \"$SPARK_MODE\" = \"worker\" ]; then \
        /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker --master spark://spark-master:7077; \
    else \
        echo \"Invalid SPARK_MODE value: $SPARK_MODE\"; \
        exit 1; \
    fi"]
