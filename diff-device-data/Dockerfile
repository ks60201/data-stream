FROM apache/spark-py:3.3.0

# Install Python packages
RUN pip install pyspark kafka-python

WORKDIR /app

# Copy the Spark application
COPY spark_streaming.py /app/

# Keep the container running
CMD ["python", "spark_streaming.py"]