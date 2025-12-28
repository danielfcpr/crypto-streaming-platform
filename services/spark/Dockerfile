FROM apache/spark:3.5.0

USER root

# Add python3 + pip (needed to run your job.py)
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

# Make "python" available (Spark often calls python)
RUN ln -sf /usr/bin/python3 /usr/bin/python

USER 185
