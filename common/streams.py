import binascii
import json
import os
import socket
import time
import uuid
import logging

import redis

logger = logging.getLogger(__name__)

# Stream and consumer group names (backward-compat defaults)
STOCK_COMMANDS = "stock-commands"
PAYMENT_COMMANDS = "payment-commands"
SAGA_REPLIES = "saga-replies"
STOCK_WORKERS = "stock-workers"
PAYMENT_WORKERS = "payment-workers"
ORCHESTRATOR_WORKERS = "orchestrator-workers"

# Shard configuration
SHARD_ID = int(os.environ.get("SHARD_ID", "0"))
SHARD_COUNT = int(os.environ.get("SHARD_COUNT", "1"))


def compute_shard(key: str, num_shards: int) -> int:
    """Hash routing compatible with NGINX's upstream hash directive.

    NGINX uses a Cache::Memcached-compatible hash:
    ((crc32(key) >> 16) & 0x7fff) % num_peers
    """
    crc = binascii.crc32(key.encode()) & 0xFFFFFFFF
    return ((crc >> 16) & 0x7FFF) % num_shards


def generate_shard_affine_uuid(shard_id: int, num_shards: int) -> str:
    """Generate a UUID that hashes to the given shard. Average ~num_shards iterations."""
    while True:
        candidate = str(uuid.uuid4())
        if compute_shard(candidate, num_shards) == shard_id:
            return candidate


# Shard-specific stream name helpers
def stock_commands_stream(shard_id: int) -> str:
    return f"stock-commands-{shard_id}"


def payment_commands_stream(shard_id: int) -> str:
    return f"payment-commands-{shard_id}"


def saga_replies_stream(shard_id: int) -> str:
    return f"saga-replies-{shard_id}"


def get_saga_redis() -> redis.Redis:
    return redis.Redis(
        host=os.environ["SAGA_REDIS_HOST"],
        port=int(os.environ["SAGA_REDIS_PORT"]),
        password=os.environ["SAGA_REDIS_PASSWORD"],
        db=int(os.environ["SAGA_REDIS_DB"]),
        decode_responses=True,
    )


def ensure_stream_group(r: redis.Redis, stream: str, group: str):
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def ensure_all_streams(r: redis.Redis, stock_shard_count: int = None, payment_shard_count: int = None, order_shard_count: int = None):
    sc = stock_shard_count or SHARD_COUNT
    pc = payment_shard_count or SHARD_COUNT
    oc = order_shard_count or SHARD_COUNT

    pairs = []
    for i in range(sc):
        pairs.append((stock_commands_stream(i), STOCK_WORKERS))
    for i in range(pc):
        pairs.append((payment_commands_stream(i), PAYMENT_WORKERS))
    for i in range(oc):
        pairs.append((saga_replies_stream(i), ORCHESTRATOR_WORKERS))

    for attempt in range(3):
        try:
            for stream, group in pairs:
                ensure_stream_group(r, stream, group)
            return
        except redis.exceptions.ConnectionError:
            if attempt < 2:
                logger.warning("saga-redis not ready, retrying in 1s...")
                time.sleep(1)
            else:
                raise


def publish_command(r: redis.Redis, stream: str, saga_id: str, idempotency_key: str, command: str, payload_json: str, reply_stream: str = None):
    fields = {
        "saga_id": saga_id,
        "idempotency_key": idempotency_key,
        "command": command,
        "payload": payload_json,
    }
    if reply_stream:
        fields["reply_stream"] = reply_stream
    r.xadd(stream, fields)


def publish_reply(r: redis.Redis, saga_id: str, idempotency_key: str, command: str, status: str, reason: str = "", reply_stream: str = None):
    stream = reply_stream or saga_replies_stream(SHARD_ID)
    r.xadd(stream, {
        "saga_id": saga_id,
        "idempotency_key": idempotency_key,
        "command": command,
        "status": status,
        "reason": reason,
    })


RELEASE_LOCK_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""


def acquire_lock(db: redis.Redis, resource_id: str, txn_id: str, ttl_ms: int = 30000) -> bool:
    return db.set(f"lock:{resource_id}", txn_id, nx=True, px=ttl_ms) is not None


def release_lock(db: redis.Redis, resource_id: str, txn_id: str) -> bool:
    return db.eval(RELEASE_LOCK_SCRIPT, 1, f"lock:{resource_id}", txn_id) == 1


# --- Idempotency with stored replies ---

def check_idempotency(business_db: redis.Redis, key: str) -> bool:
    """Returns True if this is a new key (safe to process). False if duplicate."""
    return business_db.set(f"idempotency:{key}", "processing", nx=True, ex=3600) is not None


def mark_idempotency_done(business_db: redis.Redis, key: str, reply_json: str):
    """Store the reply data so it can be re-sent on duplicate delivery."""
    business_db.set(f"idempotency:{key}", reply_json, ex=3600)


def get_idempotency_state(business_db: redis.Redis, key: str):
    """Returns (state, reply_json): 'new'/None, 'processing'/None, or 'done'/reply_json."""
    val = business_db.get(f"idempotency:{key}")
    if val is None:
        return ("new", None)
    if isinstance(val, bytes):
        val = val.decode()
    if val == "processing":
        return ("processing", None)
    return ("done", val)


def publish_reply_with_idempotency(business_db: redis.Redis, saga_redis: redis.Redis,
                                    idempotency_key: str, saga_id: str, command: str,
                                    status: str, reason: str = "", reply_stream: str = None):
    """Mark idempotency done with reply data, then publish the reply."""
    reply_data = json.dumps({"saga_id": saga_id, "command": command, "status": status, "reason": reason})
    mark_idempotency_done(business_db, idempotency_key, reply_data)
    publish_reply(saga_redis, saga_id, idempotency_key, command, status, reason, reply_stream=reply_stream)


# --- Pending message recovery ---

def _recover_pending(saga_redis, stream, group, consumer_name, handler_fn):
    """Claim and process stale messages from dead consumers using XAUTOCLAIM."""
    start_id = "0-0"
    while True:
        try:
            result = saga_redis.xautoclaim(stream, group, consumer_name,
                                            min_idle_time=5000, start_id=start_id, count=10)
            next_id, messages, _deleted = result
            if not messages:
                break
            for message_id, fields in messages:
                if fields is None:
                    saga_redis.xack(stream, group, message_id)
                    continue
                try:
                    handler_fn(message_id, fields)
                except Exception:
                    logger.exception(f"Error handling recovered message {message_id}")
                saga_redis.xack(stream, group, message_id)
            if next_id == "0-0" or next_id == b"0-0":
                break
            start_id = next_id
        except redis.exceptions.ConnectionError:
            logger.warning("Lost connection during recovery, will retry on next restart")
            break
        except redis.exceptions.ResponseError as e:
            logger.warning(f"XAUTOCLAIM error (possibly old Redis): {e}")
            break


def consume_loop(saga_redis: redis.Redis, stream: str, group: str, handler_fn):
    consumer_name = f"{socket.gethostname()}-{os.getpid()}"
    logger.info(f"Starting consumer {consumer_name} on {stream}/{group}")

    # Recover pending messages from dead consumers before processing new ones
    _recover_pending(saga_redis, stream, group, consumer_name, handler_fn)

    while True:
        try:
            results = saga_redis.xreadgroup(group, consumer_name, {stream: ">"}, block=1000, count=10)
            if not results:
                continue
            for _stream_name, messages in results:
                for message_id, fields in messages:
                    try:
                        handler_fn(message_id, fields)
                    except Exception:
                        logger.exception(f"Error handling message {message_id}")
                    saga_redis.xack(stream, group, message_id)
        except redis.exceptions.ConnectionError:
            logger.warning("Lost connection to saga-redis, retrying in 1s...")
            time.sleep(1)
