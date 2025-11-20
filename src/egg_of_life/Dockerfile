# ** Download dependencies **
FROM python:3.12-slim AS builder

WORKDIR /app

COPY src/egg_of_life/ .

RUN pip install protobuf

# ** Run program **
FROM python:3.12-slim AS runner

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-29.0.2.tgz | tar xz --strip-components=1 -C /usr/local/bin docker/docker && \
    apt-get remove -y curl && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir protobuf pyyaml

COPY --from=builder /app ./

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-u", "egg_of_life.py"]

