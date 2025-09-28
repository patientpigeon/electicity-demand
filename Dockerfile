# Install Python
ARG PYTHON_VERSION=3.9.6
FROM python:${PYTHON_VERSION}-slim as base

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
WORKDIR /app
COPY . .
CMD ["bash"]





# Download dependencies as a separate step to take advantage of Docker's caching.
# Leverage a cache mount to /root/.cache/pip to speed up subsequent builds.
# Leverage a bind mount to requirements.txt to avoid having to copy them into
# into this layer.
RUN --mount=type=cache,target=/root/.cache/pip \
	--mount=type=bind,source=requirements.txt,target=requirements.txt \
	python -m pip install -r requirements.txt















