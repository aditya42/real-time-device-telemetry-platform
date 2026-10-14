FROM python:3.11-slim
RUN pip install --no-cache-dir boto3==1.35.81
COPY init_bucket.py /app/init_bucket.py
CMD ["python", "/app/init_bucket.py"]
