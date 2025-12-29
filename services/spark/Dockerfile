FROM apache/spark:3.5.0

USER root

# Add python3 + pip
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

# Make "python" available
RUN ln -sf /usr/bin/python3 /usr/bin/python

USER 185
