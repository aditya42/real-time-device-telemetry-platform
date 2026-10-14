FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY telemetry_generator ./telemetry_generator
CMD ["python", "-m", "telemetry_generator.main"]
