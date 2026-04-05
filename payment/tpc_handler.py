import json

import redis
from msgspec import msgpack

from common.streams import publish_reply_with_idempotency, acquire_lock, release_lock
from models import UserValue


def handle(db, saga_redis, fields):
    saga_id = fields["saga_id"]
    idempotency_key = fields["idempotency_key"]
    command = fields["command"]
    payload = json.loads(fields["payload"])
    reply_stream = fields.get("reply_stream")

    if command == "payment_prepare":
        user_id = payload["user_id"]
        amount = payload["amount"]
        txn_id = payload["txn_id"]
        if not acquire_lock(db, user_id, txn_id):
            publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "VOTE-ABORT", "lock_contention", reply_stream=reply_stream)
            return
        entry_raw = db.get(user_id)
        if not entry_raw:
            release_lock(db, user_id, txn_id)
            publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "VOTE-ABORT", f"User {user_id} not found", reply_stream=reply_stream)
            return
        user = msgpack.decode(entry_raw, type=UserValue)
        if user.credit < amount:
            release_lock(db, user_id, txn_id)
            publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "VOTE-ABORT", "insufficient_credit", reply_stream=reply_stream)
            return
        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "VOTE-COMMIT", reply_stream=reply_stream)

    elif command == "payment_commit":
        user_id = payload["user_id"]
        amount = payload["amount"]
        txn_id = payload["txn_id"]
        while True:
            try:
                with db.pipeline() as pipe:
                    pipe.watch(user_id)
                    entry_raw = pipe.get(user_id)
                    if not entry_raw:
                        pipe.unwatch()
                        break
                    user = msgpack.decode(entry_raw, type=UserValue)
                    user.credit -= amount
                    pipe.multi()
                    pipe.set(user_id, msgpack.encode(user))
                    pipe.execute()
                    break
            except redis.WatchError:
                continue
        release_lock(db, user_id, txn_id)
        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "ACK", reply_stream=reply_stream)

    elif command == "payment_abort":
        user_id = payload["user_id"]
        txn_id = payload["txn_id"]
        release_lock(db, user_id, txn_id)
        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "ACK", reply_stream=reply_stream)
