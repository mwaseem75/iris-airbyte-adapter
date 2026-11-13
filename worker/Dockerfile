FROM python:3.11-slim

# docker CLI only (static binary): PyAirbyte starts the connector image through the mounted Docker socket
ARG DOCKER_CLI_VERSION=28.1.1
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_CLI_VERSION}.tgz \
    | tar -xz --strip-components=1 -C /usr/local/bin docker/docker \
 && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY tests ./tests
# Connector containers are siblings started via the Docker socket. Every path PyAirbyte bind-mounts must
# exist at the same path in Docker Desktop's Linux VM: keep them all under the /tmp/airbyte bind mount.
ENV TMPDIR=/tmp/airbyte/tmp
ENV AIRBYTE_TEMP_DIR=/tmp/airbyte/tmp
ENV AIRBYTE_PROJECT_DIR=/tmp/airbyte/project
ENV READ_TIMEOUT_SECONDS=120
# no anonymous PyAirbyte telemetry
ENV DO_NOT_TRACK=true
# connector image is pinned: do not call the Airbyte connector registry at run time
ENV AIRBYTE_OFFLINE_MODE=1
VOLUME /data
EXPOSE 8000
# /tmp/airbyte is a bind mount at run time, so the sub-folders are created on start
CMD ["sh", "-c", "mkdir -p /tmp/airbyte/tmp /tmp/airbyte/project && exec uvicorn app.main:app --host 0.0.0.0 --port 8000"]
