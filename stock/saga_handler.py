import json

import redis
from msgspec import msgpack

from common.streams import publish_reply_with_idempotency
from models import StockValue


def handle(db, saga_redis, fields):
    saga_id = fields["saga_id"]
    idempotency_key = fields["idempotency_key"]
    command = fields["command"]
    payload = json.loads(fields["payload"])
    reply_stream = fields.get("reply_stream")

    if command == "stock_subtract":
        item_id = payload["item_id"]
        quantity = payload["quantity"]
        while True:
            try:
                with db.pipeline() as pipe:
                    pipe.watch(item_id)
                    entry_raw = pipe.get(item_id)
                    if not entry_raw:
                        pipe.unwatch()
                        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "failure", f"Item {item_id} not found", reply_stream=reply_stream)
                        return
                    item = msgpack.decode(entry_raw, type=StockValue)
                    item.stock -= quantity
                    if item.stock < 0:
                        pipe.unwatch()
                        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "failure", f"Insufficient stock for {item_id}", reply_stream=reply_stream)
                        return
                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item))
                    pipe.execute()
                    break
            except redis.WatchError:
                continue
            except redis.exceptions.RedisError:
                publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "failure", "DB error", reply_stream=reply_stream)
                return
        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "success", reply_stream=reply_stream)

    elif command == "stock_add":
        item_id = payload["item_id"]
        quantity = payload["quantity"]
        while True:
            try:
                with db.pipeline() as pipe:
                    pipe.watch(item_id)
                    entry_raw = pipe.get(item_id)
                    if not entry_raw:
                        pipe.unwatch()
                        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "success", reply_stream=reply_stream)
                        return
                    item = msgpack.decode(entry_raw, type=StockValue)
                    item.stock += quantity
                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item))
                    pipe.execute()
                    break
            except redis.WatchError:
                continue
            except redis.exceptions.RedisError:
                break
        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "success", reply_stream=reply_stream)
