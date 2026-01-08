FROM python:3.11-slim

WORKDIR /app

# Install deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy script
COPY athena_apply.py /app/athena_apply.py


# Default command (overridden by docker-compose if you want)
CMD ["python", "/app/athena_apply.py"]
