import json

import redis
from msgspec import msgpack

from common.streams import publish_reply_with_idempotency
from models import UserValue


def handle(db, saga_redis, fields):
    saga_id = fields["saga_id"]
    idempotency_key = fields["idempotency_key"]
    command = fields["command"]
    payload = json.loads(fields["payload"])
    reply_stream = fields.get("reply_stream")

    if command == "payment_pay":
        user_id = payload["user_id"]
        amount = payload["amount"]
        while True:
            try:
                with db.pipeline() as pipe:
                    pipe.watch(user_id)
                    entry_raw = pipe.get(user_id)
                    if not entry_raw:
                        pipe.unwatch()
                        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "failure", f"User {user_id} not found", reply_stream=reply_stream)
                        return
                    user = msgpack.decode(entry_raw, type=UserValue)
                    user.credit -= amount
                    if user.credit < 0:
                        pipe.unwatch()
                        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "failure", f"Insufficient credit for {user_id}", reply_stream=reply_stream)
                        return
                    pipe.multi()
                    pipe.set(user_id, msgpack.encode(user))
                    pipe.execute()
                    break
            except redis.WatchError:
                continue
            except redis.exceptions.RedisError:
                publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "failure", "DB error", reply_stream=reply_stream)
                return
        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "success", reply_stream=reply_stream)

    elif command == "payment_refund":
        user_id = payload["user_id"]
        amount = payload["amount"]
        while True:
            try:
                with db.pipeline() as pipe:
                    pipe.watch(user_id)
                    entry_raw = pipe.get(user_id)
                    if not entry_raw:
                        pipe.unwatch()
                        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "success", reply_stream=reply_stream)
                        return
                    user = msgpack.decode(entry_raw, type=UserValue)
                    user.credit += amount
                    pipe.multi()
                    pipe.set(user_id, msgpack.encode(user))
                    pipe.execute()
                    break
            except redis.WatchError:
                continue
            except redis.exceptions.RedisError:
                break
        publish_reply_with_idempotency(db, saga_redis, idempotency_key, saga_id, command, "success", reply_stream=reply_stream)
