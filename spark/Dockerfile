# Dockerfile for Spark
FROM bitnami/spark:latest

USER root

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN mkdir -p /var/lib/apt/lists/partial

# Copy configuration files if you have custom configurations
# COPY spark-defaults.conf $SPARK_HOME/conf/
# COPY spark-env.sh $SPARK_HOME/conf/

# Install additional dependencies if needed
RUN apt-get update && apt-get install -y curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Expose necessary ports
EXPOSE 8080 7077 6066

# Default command
CMD ["/opt/bitnami/scripts/spark/entrypoint.sh"]
