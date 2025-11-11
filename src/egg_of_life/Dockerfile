# ** Download dependencies **
FROM python:3.12-slim AS builder

WORKDIR /app

COPY src/egg_of_life/ .

RUN pip install protobuf

# ** Run program **
FROM python:3.12-slim AS runner

WORKDIR /app

RUN pip install --no-cache-dir protobuf pyyaml

COPY --from=builder /app ./

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-u", "egg_of_life.py"]

