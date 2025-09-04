FROM ubuntu:20.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install Python, OpenJDK 11, and essential tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    openjdk-11-jdk \
    bash \
    curl \
    wget \
    procps \
    net-tools \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create python symlink
RUN ln -s /usr/bin/python3 /usr/bin/python

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Install Spark 3.5.0
RUN curl -L -o spark-3.5.0-bin-hadoop3.tgz https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz && \
    mv spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm spark-3.5.0-bin-hadoop3.tgz

# Set PYTHONPATH after Spark is installed
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

# Install Python requirements
RUN pip3 install --no-cache-dir numpy pandas requests pyspark==3.5.0

# Add Hadoop-AWS, AWS SDK, and PostgreSQL JDBC driver
RUN curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -o /opt/spark/jars/postgresql-42.3.5.jar https://jdbc.postgresql.org/download/postgresql-42.3.5.jar

# Create Spark history folder
RUN mkdir -p /opt/spark/history

# Copy configuration files
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY spark-master.sh /spark-master.sh
COPY spark-worker.sh /spark-worker.sh
COPY delta-core_2.12-2.4.0.jar /opt/spark/jars/

# Make scripts executable
RUN chmod +x /spark-master.sh /spark-worker.sh