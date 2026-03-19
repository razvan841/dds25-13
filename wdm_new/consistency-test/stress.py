import asyncio
import json
import logging
import os
import random
import time
from tempfile import gettempdir

import aiohttp

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                    datefmt='%I:%M:%S')
logger = logging.getLogger(__name__)

tmp_folder_path: str = os.path.join(gettempdir(), 'wdm_consistency_test')

NUMBER_OF_ORDERS = 1000
NUM_SHARDS = int(os.environ.get("NUM_SHARDS", "3"))
WARMUP_RETRY_WINDOW_SECONDS = int(os.environ.get("WARMUP_RETRY_WINDOW_SECONDS", "90"))
WARMUP_STATUS_TIMEOUT_SECONDS = int(os.environ.get("WARMUP_STATUS_TIMEOUT_SECONDS", "20"))

with open(os.path.join('..', 'urls.json')) as f:
    urls = json.load(f)
    ORDER_URL = urls['ORDER_URL']
    PAYMENT_URL = urls['PAYMENT_URL']
    STOCK_URL = urls['STOCK_URL']


async def create_order(session, url):
    async with session.post(url) as resp:
        jsn = await resp.json()
        return jsn['order_id']


async def post_and_get_status(session, url, checkout=None):
    async with session.post(url) as resp:
        if checkout:
            if 400 <= resp.status < 500:
                log = f"CHECKOUT | ORDER: {checkout[0]} USER: {checkout[1]} FAIL __OUR_LOG__\n"
                checkout[2].write(log)
            else:
                log = f"CHECKOUT | ORDER: {checkout[0]} USER: {checkout[1]} SUCCESS __OUR_LOG__\n"
                checkout[2].write(log)
        return resp.status


def compute_shard(entity_id: str) -> int:
    hex_clean = entity_id.replace("-", "")
    return int(hex_clean[:16], 16) % NUM_SHARDS


async def _read_payload(resp):
    content_type = resp.headers.get("Content-Type", "")
    if "application/json" in content_type:
        return await resp.json()
    return await resp.text()


async def _post_expect_success(session, url):
    async with session.post(url) as resp:
        payload = await _read_payload(resp)
        if resp.status >= 400:
            raise RuntimeError(f"POST {url} returned {resp.status}: {payload}")
        return payload


async def _post_get_field(session, url, field):
    payload = await _post_expect_success(session, url)
    if not isinstance(payload, dict) or field not in payload:
        raise RuntimeError(f"POST {url} did not return field {field!r}: {payload}")
    return payload[field]


async def _get_expect_json(session, url):
    async with session.get(url) as resp:
        payload = await _read_payload(resp)
        if resp.status >= 400:
            raise RuntimeError(f"GET {url} returned {resp.status}: {payload}")
        if not isinstance(payload, dict):
            raise RuntimeError(f"GET {url} did not return JSON: {payload}")
        return payload


async def _post_checkout(session, order_id):
    url = f"{ORDER_URL}/orders/checkout/{order_id}"
    async with session.post(url) as resp:
        payload = await _read_payload(resp)
        if resp.status == 408:
            return payload
        if resp.status >= 400:
            raise RuntimeError(f"POST {url} returned {resp.status}: {payload}")
        if not isinstance(payload, dict):
            raise RuntimeError(f"POST {url} did not return JSON: {payload}")
        return payload


async def _wait_for_terminal_checkout_status(session, order_id):
    deadline = time.monotonic() + WARMUP_STATUS_TIMEOUT_SECONDS
    status_url = f"{ORDER_URL}/orders/checkout_status/{order_id}"
    while time.monotonic() < deadline:
        payload = await _get_expect_json(session, status_url)
        status = payload.get("status")
        if status in {"COMMITTED", "FAILED", "CANCELLED", "ABORTED"}:
            return status
        await asyncio.sleep(0.25)
    raise TimeoutError(f"Warmup checkout {order_id} did not reach a terminal state in time")


async def _run_warmup_checkout(session, payment_shard, stock_shard):
    user_id = await _post_get_field(
        session,
        f"{PAYMENT_URL}/payment/shard/{payment_shard}/create_user",
        "user_id",
    )
    await _post_expect_success(session, f"{PAYMENT_URL}/payment/add_funds/{user_id}/1")

    item_id = await _post_get_field(
        session,
        f"{STOCK_URL}/stock/shard/{stock_shard}/item/create/1",
        "item_id",
    )
    await _post_expect_success(session, f"{STOCK_URL}/stock/add/{item_id}/1")

    order_id = await _post_get_field(session, f"{ORDER_URL}/orders/create/{user_id}", "order_id")
    await _post_expect_success(session, f"{ORDER_URL}/orders/addItem/{order_id}/{item_id}/1")

    initial_payload = await _post_checkout(session, order_id)
    initial_status = initial_payload.get("status") if isinstance(initial_payload, dict) else None
    if initial_status == "COMMITTED":
        return initial_status

    return await _wait_for_terminal_checkout_status(session, order_id)


async def warmup(item_ids):
    if not item_ids:
        logger.info("Skipping warmup because there are no benchmark items")
        return

    stock_shards = sorted({compute_shard(item_id) for item_id in item_ids})

    async with aiohttp.ClientSession() as session:
        for stock_shard in stock_shards:
            for payment_shard in range(NUM_SHARDS):
                retry_deadline = time.monotonic() + WARMUP_RETRY_WINDOW_SECONDS
                attempt = 0
                while True:
                    attempt += 1
                    status = await _run_warmup_checkout(session, payment_shard, stock_shard)
                    if status == "COMMITTED":
                        logger.info(
                            "Warmup committed for order/payment shard %s against stock shard %s on attempt %s",
                            payment_shard,
                            stock_shard,
                            attempt,
                        )
                        break

                    if time.monotonic() >= retry_deadline:
                        raise RuntimeError(
                            "Warmup failed to commit for "
                            f"payment shard {payment_shard} and stock shard {stock_shard}; "
                            f"last terminal status was {status}"
                        )

                    logger.warning(
                        "Warmup for payment shard %s and stock shard %s finished with %s; retrying",
                        payment_shard,
                        stock_shard,
                        status,
                    )
                    await asyncio.sleep(1.0)


async def create_orders(session, item_ids, user_ids, number_of_orders):
    tasks = []
    # Create orders
    orders_user_id = []
    for _ in range(number_of_orders):
        user_id = random.choice(user_ids)
        orders_user_id.append(user_id)
        create_order_url = f"{ORDER_URL}/orders/create/{user_id}"
        tasks.append(asyncio.ensure_future(create_order(session, create_order_url)))
    order_ids = list(await asyncio.gather(*tasks))
    tasks = []
    # Add items
    for order_id in order_ids:
        item_id = random.choice(item_ids)
        create_item_url = f"{ORDER_URL}/orders/addItem/{order_id}/{item_id}/1"
        tasks.append(asyncio.ensure_future(post_and_get_status(session, create_item_url)))
    await asyncio.gather(*tasks)
    return order_ids, orders_user_id


async def perform_checkouts(session, order_ids, orders_user_id, log_file):
    tasks = []
    for i, order_id in enumerate(order_ids):
        url = f"{ORDER_URL}/orders/checkout/{order_id}"
        tasks.append(asyncio.ensure_future(post_and_get_status(session, url,
                                                               checkout=(order_id, orders_user_id[i], log_file))))
    order_responses = await asyncio.gather(*tasks)
    return order_responses


async def stress(item_ids, user_ids):
    async with aiohttp.ClientSession() as session:
        logger.info("Creating orders...")
        order_ids, orders_user_id = await create_orders(session, item_ids, user_ids, NUMBER_OF_ORDERS)
        logger.info("Orders created ...")
        logger.info("Running concurrent checkouts...")
        with open(f"{tmp_folder_path}/consistency-test.log", "w") as log_file:
            await perform_checkouts(session, order_ids, orders_user_id, log_file)
        logger.info("Concurrent checkouts finished...")
