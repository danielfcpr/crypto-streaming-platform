FROM python:3.11-slim

WORKDIR /app

# Install deps
COPY services/athena_apply/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy script
COPY services/athena_apply/athena_apply.py /app/athena_apply.py
COPY sql/athena /sql/athena

# Default command (overridden by docker-compose if you want)
CMD ["python", "/app/athena_apply.py"]
