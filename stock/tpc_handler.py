import json

import redis
from msgspec import msgpack

from common.streams import publish_reply_with_idempotency, acquire_lock, release_lock
from models import StockValue


def handle(db, saga_redis, fields):
    saga_id = fields["saga_id"]
    idempotency_key = fields["idempotency_key"]
    command = fields["command"]
    payload = json.loads(fields["payload"])
    reply_stream = fields.get("reply_stream")

    if command == "stock_prepare":
        items = payload["items"]
        txn_id = payload["txn_id"]
        acquired = []
        for item_id, qty in items:
            if not acquire_lock(db, item_id, txn_id):
                for locked_id in acquired:
                    release_lock(db, locked_id, txn_id)
                publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "VOTE-ABORT", "lock_contention", reply_stream=reply_stream)
                return
            acquired.append(item_id)
            entry_raw = db.get(item_id)
            if not entry_raw:
                for locked_id in acquired:
                    release_lock(db, locked_id, txn_id)
                publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "VOTE-ABORT", f"Item {item_id} not found", reply_stream=reply_stream)
                return
            item = msgpack.decode(entry_raw, type=StockValue)
            if item.stock < qty:
                for locked_id in acquired:
                    release_lock(db, locked_id, txn_id)
                publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "VOTE-ABORT", "insufficient_stock", reply_stream=reply_stream)
                return
        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "VOTE-COMMIT", reply_stream=reply_stream)

    elif command == "stock_commit":
        items = payload["items"]
        txn_id = payload["txn_id"]
        for item_id, qty in items:
            while True:
                try:
                    with db.pipeline() as pipe:
                        pipe.watch(item_id)
                        entry_raw = pipe.get(item_id)
                        if not entry_raw:
                            pipe.unwatch()
                            break
                        item = msgpack.decode(entry_raw, type=StockValue)
                        item.stock -= qty
                        pipe.multi()
                        pipe.set(item_id, msgpack.encode(item))
                        pipe.execute()
                        break
                except redis.WatchError:
                    continue
            release_lock(db, item_id, txn_id)
        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "ACK", reply_stream=reply_stream)

    elif command == "stock_abort":
        items = payload["items"]
        txn_id = payload["txn_id"]
        for item_id, _qty in items:
            release_lock(db, item_id, txn_id)
        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "ACK", reply_stream=reply_stream)
