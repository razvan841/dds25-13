#!/usr/bin/env python3
"""Generate docker-compose and NGINX configs for small/medium/large deployments.

Usage:
    python deploy/generate.py

Produces 6 files in the repo root:
    docker-compose-small.yml   / nginx-small.conf    (1 shard,  ~8 CPUs)
    docker-compose-medium.yml  / nginx-medium.conf   (5 shards, ~49 CPUs)
    docker-compose-large.yml   / nginx-large.conf    (10 shards, ~90 CPUs)
"""

import os
import textwrap

SIZES = {
    "small": {
        "shards": 1,
        "service_cpus": "1.0",
        "redis_cpus": "1.0",
        "saga_cpus": "1.0",
        "gw_cpus": "1.0",
    },
    "medium": {
        "shards": 5,
        "service_cpus": "2.0",
        "redis_cpus": "1.0",
        "saga_cpus": "0.5",
        "gw_cpus": "1.5",
    },
    "large": {
        "shards": 10,
        "service_cpus": "2.0",
        "redis_cpus": "0.8",
        "saga_cpus": "0.5",
        "gw_cpus": "1.0",
    },
}

GUNICORN_CMD = (
    "gunicorn -b 0.0.0.0:5000 -w 2 --worker-class gevent "
    "--worker-connections 1000 --timeout 30 --log-level=info app:app"
)


# ---------------------------------------------------------------------------
# Docker Compose generation
# ---------------------------------------------------------------------------

def generate_compose(size_name: str, cfg: dict) -> str:
    shards = cfg["shards"]
    sc = cfg["service_cpus"]
    rc = cfg["redis_cpus"]
    sac = cfg["saga_cpus"]
    gc = cfg["gw_cpus"]

    lines = []
    w = lines.append  # shorthand

    w('version: "3"')
    w("")

    # -- YAML anchors --
    for svc in ("stock", "payment", "order"):
        extra_env = ""
        if svc == "order":
            extra_env = ", env/checkout.env"
        w(f"x-{svc}-base: &{svc}-base")
        w(f"  image: {svc}:latest")
        w(f"  restart: always")
        w(f"  command: {GUNICORN_CMD}")
        w(f"  env_file: [ env/saga_redis.env{extra_env} ]")
        w(f"  deploy:")
        w(f"    resources:")
        w(f"      limits:")
        w(f"        cpus: \"{sc}\"")
        w("")

    w("x-redis: &redis-base")
    w("  image: redis:7.2-bookworm")
    w("  restart: always")
    w("  command: redis-server --requirepass redis --maxmemory 512mb")
    w("  deploy:")
    w("    resources:")
    w("      limits:")
    w(f"        cpus: \"{rc}\"")
    w("")
    w("services:")
    w("")

    # -- Gateway --
    all_services = []
    for svc in ("order", "stock", "payment"):
        for i in range(shards):
            all_services.append(f"{svc}-service-{i}")

    w("  gateway:")
    w("    image: nginx:1.25-bookworm")
    w("    restart: always")
    w("    volumes:")
    w(f"      - ./nginx-{size_name}.conf:/etc/nginx/nginx.conf:ro")
    w("    ports:")
    w('      - "8000:80"')
    w("    deploy:")
    w("      resources:")
    w("        limits:")
    w(f"          cpus: \"{gc}\"")
    w("    depends_on:")
    for s in all_services:
        w(f"      - {s}")
    w("")

    saga_deps = ", ".join(f"saga-redis-{i}" for i in range(shards))

    # -- Service shards --
    for svc, dockerfile_dir in [("order", "order"), ("stock", "stock"), ("payment", "payment")]:
        w(f"  # --- {svc.capitalize()} shards ---")
        w("")
        for i in range(shards):
            w(f"  {svc}-service-{i}:")
            w(f"    <<: *{svc}-base")
            if i == 0:
                w(f"    build: {{ context: ., dockerfile: {dockerfile_dir}/Dockerfile }}")
            w(f"    environment:")
            w(f"      SHARD_ID: \"{i}\"")
            w(f"      SHARD_COUNT: \"{shards}\"")
            w(f"      REDIS_HOST: {svc}-db-{i}")
            w(f"      REDIS_PORT: \"6379\"")
            w(f"      REDIS_PASSWORD: redis")
            w(f"      REDIS_DB: \"0\"")
            if svc == "order":
                w(f"      GATEWAY_URL: http://gateway:80")
            w(f"    depends_on: [{svc}-db-{i}, {saga_deps}]")
            w("")

        # Redis DBs for this service
        for i in range(shards):
            w(f"  {svc}-db-{i}:")
            w(f"    <<: *redis-base")
        w("")

    # -- Saga-redis instances --
    w("  # --- Sharded saga-redis (one per shard) ---")
    w("")
    for i in range(shards):
        w(f"  saga-redis-{i}:")
        w(f"    image: redis:7.2-bookworm")
        w(f"    restart: always")
        w(f"    command: redis-server --requirepass redis --maxmemory 256mb")
        w(f"    deploy:")
        w(f"      resources:")
        w(f"        limits:")
        w(f"          cpus: \"{sac}\"")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# NGINX config generation
# ---------------------------------------------------------------------------

def generate_nginx(size_name: str, cfg: dict) -> str:
    shards = cfg["shards"]
    lines = []
    w = lines.append

    w("events { worker_connections 2048; }")
    w("")
    w("http {")

    # -- Map blocks (identical across sizes) --
    w("    # --- ID extraction from URLs ---")
    w("    map $request_uri $stock_resource_id {")
    w("        ~^/stock/(?:find|add|subtract)/(?P<id>[^/]+)  $id;")
    w("        default  $request_id;")
    w("    }")
    w("    map $request_uri $payment_resource_id {")
    w("        ~^/payment/(?:find_user|add_funds|pay)/(?P<id>[^/]+)  $id;")
    w("        default  $request_id;")
    w("    }")
    w("    map $request_uri $order_resource_id {")
    w("        ~^/orders/(?:find|checkout)/(?P<id>[^/]+)  $id;")
    w("        ~^/orders/addItem/(?P<id>[^/]+)/           $id;")
    w("        default  $request_id;")
    w("    }")
    w("")
    w("    # --- Suffix extraction for mirror locations ---")
    w("    map $request_uri $stock_suffix  { ~^/stock/(.*)$   /$1; default /; }")
    w("    map $request_uri $payment_suffix { ~^/payment/(.*)$ /$1; default /; }")
    w("    map $request_uri $orders_suffix  { ~^/orders/(.*)$  /$1; default /; }")
    w("")

    # -- Upstream blocks --
    w("    # --- Hash-routed upstreams ---")
    for svc_name, upstream_name in [("stock", "stock-app"), ("payment", "payment-app"), ("order", "order-app")]:
        w(f"    upstream {upstream_name} {{")
        if shards > 1:
            var = {"stock": "$stock_resource_id", "payment": "$payment_resource_id", "order": "$order_resource_id"}[svc_name]
            w(f"        hash {var};")
        for i in range(shards):
            w(f"        server {svc_name}-service-{i}:5000;")
        w("    }")
    w("")

    # -- Server block --
    w("    server {")
    w("        listen 80;")
    w("        mirror_request_body on;")
    w("        resolver 127.0.0.11 valid=30s;  # Docker embedded DNS")
    w("")

    # -- batch_init locations --
    for svc_name, prefix, suffix_var in [
        ("stock", "/stock", "$stock_suffix"),
        ("payment", "/payment", "$payment_suffix"),
        ("order", "/orders", "$orders_suffix"),
    ]:
        mirror_name = svc_name if svc_name != "order" else "order"
        w(f"        # --- {svc_name} batch_init ---")
        w(f"        location ~ ^{prefix}/batch_init/ {{")
        for i in range(1, shards):
            w(f"            mirror /_mirror_{mirror_name}_{i};")
        w(f"            rewrite ^{prefix}/(.*) /$1 break;")
        w(f"            proxy_pass http://{svc_name}-service-0:5000;")
        w(f"        }}")
        for i in range(1, shards):
            w(f"        location /_mirror_{mirror_name}_{i} {{ internal; proxy_pass http://{svc_name}-service-{i}:5000{suffix_var}; }}")
        w("")

    # -- Hash-routed proxy locations --
    w("        # --- All other endpoints: hash-routed ---")
    w("        location /orders/ {")
    w("            rewrite ^/orders/(.*) /$1 break;")
    w("            proxy_pass http://order-app;")
    w("        }")
    w("        location /stock/ {")
    w("            rewrite ^/stock/(.*) /$1 break;")
    w("            proxy_pass http://stock-app;")
    w("        }")
    w("        location /payment/ {")
    w("            rewrite ^/payment/(.*) /$1 break;")
    w("            proxy_pass http://payment-app;")
    w("        }")
    w("")
    w("        access_log /var/log/nginx/server.access.log;")
    w("    }")
    w("    access_log /var/log/nginx/access.log;")
    w("}")
    w("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    for size_name, cfg in SIZES.items():
        compose_path = os.path.join(repo_root, f"docker-compose-{size_name}.yml")
        nginx_path = os.path.join(repo_root, f"nginx-{size_name}.conf")

        compose_content = generate_compose(size_name, cfg)
        nginx_content = generate_nginx(size_name, cfg)

        with open(compose_path, "w") as f:
            f.write(compose_content)

        with open(nginx_path, "w") as f:
            f.write(nginx_content)

        # Calculate total CPUs
        shards = cfg["shards"]
        total = (
            shards * 3 * float(cfg["service_cpus"])
            + shards * 3 * float(cfg["redis_cpus"])
            + shards * float(cfg["saga_cpus"])
            + float(cfg["gw_cpus"])
        )
        containers = shards * 7 + 1
        print(f"  {size_name:8s}: {shards:2d} shards, {containers:3d} containers, {total:5.1f} CPUs")
        print(f"           -> {compose_path}")
        print(f"           -> {nginx_path}")

    print("\nDone! Use: docker compose -f docker-compose-<size>.yml up --build")


if __name__ == "__main__":
    main()
