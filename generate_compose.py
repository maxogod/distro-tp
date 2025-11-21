#!/usr/bin/env python3
import sys
import yaml

if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} <config.yaml>")
    sys.exit(1)

config_file = sys.argv[1]
output_file = "docker-compose.yaml"

with open(config_file, "r") as f:
    config = yaml.safe_load(f)

# Extract service counts
service_counts = {}
for svc in config.get("services", []):
    service_counts.update(svc)

lines = []

# ==============================
# Header
# ==============================
lines.append("name: distro")
lines.append("services:")

# ==============================
# RabbitMQ
# ==============================
lines.append(
    """  rabbitmq:
    container_name: rabbitmq
    build:
      context: ./src/rabbitmq
      dockerfile: Dockerfile
    image: rabbitmq:latest
    command: sh -c "rabbitmq-server > /dev/null 2>&1"
    ports:
      - '5670:5672'
      - '15670:15672'
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s
      timeout: 10s
      start_period: 15s
      retries: 5
    networks:
      - tp_net
    """
)

# ==============================
# Egg of life
# ==============================
lines.append(
    """  egg_of_life:
    container_name: egg_of_life
    build:
      dockerfile: ./src/egg_of_life/Dockerfile
    image: egg_of_life:latest
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./config.yaml:/app/config.yaml
    networks:
      - tp_net
    environment:
      - NETWORK=distro_tp_net
    """
)

# ==============================
# Gateway
# ==============================
lines.append(
    """  gateway:
    container_name: gateway
    build:
      dockerfile: ./src/gateway/Dockerfile
    image: gateway:latest
    ports:
      - '8080:8080'
      - '8081:8081'
    volumes:
      - ./src/gateway/config.yaml:/app/config.yaml
    depends_on:
      rabbitmq:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/ping"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 10s
    networks:
      - tp_net
    """
)

# ==============================
# Controller
# ==============================
lines.append(
    """  controller:
    container_name: controller 
    build:
      dockerfile: ./src/controller/Dockerfile
    image: controller:latest
    volumes:
      - ./src/controller/config.yaml:/app/config.yaml
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - tp_net
    """
)

# ==============================
# Aggregator (always 1)
# ==============================
lines.append(
    """  aggregator:
    container_name: aggregator
    build:
      dockerfile: ./src/aggregator/Dockerfile
    image: aggregator:latest
    networks:
      - tp_net
    volumes:
      - ./src/aggregator/config.yaml:/app/config.yaml
    depends_on:
      rabbitmq:
        condition: service_healthy
    """
)


# ==============================
# Function to add replicated services
# ==============================
def add_services(name, count):
    for i in range(1, count + 1):
        cname = name if count == 1 else f"{name}{i}"
        lines.append(
            f"""  {cname}:
    container_name: {cname}
    build:
      dockerfile: ./src/{name}/Dockerfile
    image: {name}:latest
    networks:
      - tp_net
    volumes:
      - ./src/{name}/config.yaml:/app/config.yaml
    depends_on:
      rabbitmq:
        condition: service_healthy
        """
        )


# ==============================
# Add other services
# ==============================
for svc in ["filter", "joiner", "reducer", "group_by"]:
    count = service_counts.get(svc, 0)
    if count > 0:
        print(f"Adding {count} {svc}(s)")
        add_services(svc, count)

# ==============================
# Clients
# ==============================
gw_count = service_counts.get("client", 0)
for i in range(gw_count):
    lines.append(
        f"""  client{i+1}:
    container_name: client{i+1}
    entrypoint: ["/app/app", "t{(i % 4)+1}"]
    build:
      dockerfile: ./src/client/Dockerfile
    image: client:latest
    networks:
      - tp_net
    volumes:
      - ./.data:/app/.data
      - ./.output{i+1}:/app/.output
      - ./src/client/config.yaml:/app/config.yaml
    depends_on:
      gateway:
        condition: service_healthy
        """
    )
    print(f"Adding client{i+1}")

# ==============================
# Networks
# ==============================
lines.append(
    """networks:
  tp_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24"""
)

# Write to file
with open(output_file, "w") as f:
    f.write("\n".join(lines))

print(f"Generated {output_file} successfully!")
