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

# Extract service configuration
services_config = config.get("services", {})

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
    f"""  egg_of_life:
    container_name: egg_of_life
    build:
      dockerfile: ./src/egg_of_life/Dockerfile
    image: egg_of_life:latest
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./src/egg_of_life/config.yaml:/app/config.yaml
    networks:
      - tp_net
    environment:
      - NETWORK=distro_tp_net
      - HOST_PROJECT_PATH={"${PWD}"}
    """
)


# ==============================
# Function to add gateway
# ==============================
def add_gateway(count, tags=None):
    for i in range(1, count + 1):
        cname = f"gateway{i}"
        service_def = f"""  {cname}:
    container_name: {cname}
    build:
      dockerfile: ./src/gateway/Dockerfile"""

        if tags:
            service_def += f"""
      args:
        BUILD_TAGS: "{tags}" """

        service_def += f"""
    image: gateway:latest
    environment:
      - LEADER_ELECTION_ID={i}
      - LEADER_ELECTION_HOST=gateway{i}
      - LEADER_ELECTION_PORT=9090
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
        lines.append(service_def)


# ==============================
# Function to add controller
# ==============================
def add_controller(count, tags=None):
    for i in range(1, count + 1):
        cname = "controller" if count == 1 else f"controller{i}"
        service_def = f"""  {cname}:
    container_name: {cname}
    build:
      dockerfile: ./src/controller/Dockerfile"""

        if tags:
            service_def += f"""
      args:
        BUILD_TAGS: "{tags}" """

        service_def += f"""
    image: controller:latest
    volumes:
      - ./src/controller/config.yaml:/app/config.yaml
    environment:
      - ID={cname}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - tp_net
        """
        lines.append(service_def)


# ==============================
# Function to add aggregator
# ==============================
def add_aggregator(count, tags=None):
    for i in range(1, count + 1):
        cname = f"aggregator{i}"
        service_def = f"""  {cname}:
    container_name: {cname}
    build:
      dockerfile: ./src/aggregator/Dockerfile"""

        if tags:
            service_def += f"""
      args:
        BUILD_TAGS: "{tags}" """

        service_def += f"""
    image: aggregator:latest
    networks:
      - tp_net
    environment:
      - ID={cname}
      - LEADER_ELECTION_ID={i}
      - LEADER_ELECTION_HOST=aggregator{i}
      - LEADER_ELECTION_PORT=7070
    volumes:
      - ./src/aggregator/config.yaml:/app/config.yaml
      - ./.storage/aggregator{i}:/app/storage
    depends_on:
      rabbitmq:
        condition: service_healthy
        """
        lines.append(service_def)


# ==============================
# Function to add replicated services
# ==============================
def add_services(name, count, tags=None):
    for i in range(1, count + 1):
        cname = name if count == 1 else f"{name}{i}"
        service_def = f"""  {cname}:
    container_name: {cname}
    build:
      dockerfile: ./src/{name}/Dockerfile"""

        # Add build tags if present
        if tags:
            service_def += f"""
      args:
        BUILD_TAGS: "{tags}" """

        service_def += f"""
    image: {name}:latest
    networks:
      - tp_net
    volumes:
      - ./src/{name}/config.yaml:/app/config.yaml
    environment:
      - ID={cname}
    depends_on:
      rabbitmq:
        condition: service_healthy
        """
        lines.append(service_def)


# ==============================
# Function to add joiner
# ==============================
def add_joiner(count, tags=None):
    for i in range(1, count + 1):
        cname = f"joiner{i}"
        service_def = f"""  {cname}:
    container_name: {cname}
    build:
      dockerfile: ./src/joiner/Dockerfile"""

        if tags:
            service_def += f"""
      args:
        BUILD_TAGS: "{tags}" """

        service_def += f"""
    image: joiner:latest
    networks:
      - tp_net
    environment:
      - ID={cname}
    volumes:
      - ./src/joiner/config.yaml:/app/config.yaml
      - ./.storage/joiner{i}:/app/storage
    depends_on:
      rabbitmq:
        condition: service_healthy
        """
        lines.append(service_def)

# ==============================
# Add gateway
# ==============================
gateway_config = services_config.get("gateway", {})
if isinstance(gateway_config, dict):
    count = gateway_config.get("instances", 0)
    tags = gateway_config.get("tags", None)
else:
    count = gateway_config if isinstance(gateway_config, int) else 0
    tags = None

if count > 0:
    tag_info = f" with tags '{tags}'" if tags else ""
    print(f"Adding {count} gateway(s){tag_info}")
    add_gateway(count, tags)

# ==============================
# Add controller
# ==============================
controller_config = services_config.get("controller", {})
if isinstance(controller_config, dict):
    count = controller_config.get("instances", 0)
    tags = controller_config.get("tags", None)
else:
    count = controller_config if isinstance(controller_config, int) else 0
    tags = None

if count > 0:
    tag_info = f" with tags '{tags}'" if tags else ""
    print(f"Adding {count} controller(s){tag_info}")
    add_controller(count, tags)

# ==============================
# Add aggregator
# ==============================
aggregator_config = services_config.get("aggregator", {})
if isinstance(aggregator_config, dict):
    count = aggregator_config.get("instances", 0)
    tags = aggregator_config.get("tags", None)
else:
    count = aggregator_config if isinstance(aggregator_config, int) else 0
    tags = None

if count > 0:
    tag_info = f" with tags '{tags}'" if tags else ""
    print(f"Adding {count} aggregator(s){tag_info}")
    add_aggregator(count, tags)

# ==============================
# Add joiners
# ==============================
joiner_config = services_config.get("joiner", {})
if isinstance(joiner_config, dict):
    count = joiner_config.get("instances", 0)
    tags = joiner_config.get("tags", None)
else:
    count = joiner_config if isinstance(joiner_config, int) else 0
    tags = None

if count > 0:
    tag_info = f" with tags '{tags}'" if tags else ""
    print(f"Adding {count} joiner(s){tag_info}")
    add_joiner(count, tags)

# ==============================
# Add other services
# ==============================
for svc in ["filter", "reducer", "group_by"]:
    svc_config = services_config.get(svc, {})

    # Handle both dict and int formats for backwards compatibility
    if isinstance(svc_config, dict):
        count = svc_config.get("instances", 0)
        tags = svc_config.get("tags", None)
    else:
        count = svc_config if isinstance(svc_config, int) else 0
        tags = None

    if count > 0:
        tag_info = f" with tags '{tags}'" if tags else ""
        print(f"Adding {count} {svc}(s){tag_info}")
        add_services(svc, count, tags)

# ==============================
# Clients
# ==============================
client_config = services_config.get("client", {})
if isinstance(client_config, dict):
    gw_count = client_config.get("instances", 0)
else:
    gw_count = client_config if isinstance(client_config, int) else 0

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
      gateway1:
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
