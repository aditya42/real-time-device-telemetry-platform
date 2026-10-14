FROM spark:3.5.3-python3

USER root

COPY requirements.txt /tmp/requirements.txt

RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt \
    && python3 -c "import pandas, pyarrow, numpy, boto3, psycopg2; print('Streaming Python dependencies OK')"

# Fix Spark Ivy cache directory and permissions
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark/.ivy2

COPY jobs /app/jobs
COPY telemetry_streaming /app/telemetry_streaming

ENV PYTHONPATH=/app
ENV SPARK_SUBMIT_OPTS="-Divy.home=/home/spark/.ivy2"

# Create writable directories for Spark
RUN mkdir -p /home/spark/.ivy2/cache \
             /home/spark/.ivy2/jars \
             /checkpoints/telemetry \
    && chown -R spark:spark /home/spark/.ivy2 /checkpoints

USER spark

CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3", "/app/jobs/telemetry_stream.py"]