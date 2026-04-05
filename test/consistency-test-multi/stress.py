import asyncio
import json
import logging
import os
import random
from tempfile import gettempdir
from typing import Dict, List, Tuple

import aiohttp

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                    datefmt='%I:%M:%S')
logger = logging.getLogger(__name__)

tmp_folder_path: str = os.path.join(gettempdir(), 'wdm_consistency_test_multi')

# 2000 orders (2× the original), each containing 1-4 distinct item types at qty 1-2.
NUMBER_OF_ORDERS = 2000
MAX_DISTINCT_ITEMS_PER_ORDER = 4
MAX_QTY_PER_ITEM = 2

with open(os.path.join('..', 'urls.json')) as f:
    urls = json.load(f)
    ORDER_URL = urls['ORDER_URL']
    PAYMENT_URL = urls['PAYMENT_URL']
    STOCK_URL = urls['STOCK_URL']


async def create_order(session, url: str) -> str:
    async with session.post(url) as resp:
        jsn = await resp.json()
        return jsn['order_id']


async def post_and_get_status(session, url: str) -> int:
    async with session.post(url) as resp:
        return resp.status


async def create_orders_with_items(
    session,
    item_configs: Dict[str, dict],
    user_ids: List[str],
    number_of_orders: int,
) -> Tuple[List[str], Dict[str, dict]]:
    """
    Creates orders and adds multiple distinct items to each one.

    Returns
    -------
    order_ids : list of created order IDs (same order as creation)
    order_metadata : order_id → {user_id, items: [(item_id, qty, price), ...], total_cost}
    """
    item_ids = list(item_configs.keys())

    # --- Phase 1: create all orders concurrently ---
    orders_user_id: List[str] = []
    create_tasks = []
    for _ in range(number_of_orders):
        user_id = random.choice(user_ids)
        orders_user_id.append(user_id)
        create_tasks.append(
            asyncio.ensure_future(create_order(session, f"{ORDER_URL}/orders/create/{user_id}"))
        )
    order_ids: List[str] = list(await asyncio.gather(*create_tasks))

    # --- Phase 2: decide item composition, then add items per order sequentially ---
    # Items within the same order must be added sequentially to avoid the lost-update
    # race on the order's read-modify-write in the order service.  Different orders
    # are still processed concurrently.
    order_metadata: Dict[str, dict] = {}

    async def add_items_to_order(order_id: str, items_in_order: List[Tuple[str, int, int]]):
        for item_id, qty, _ in items_in_order:
            await post_and_get_status(session, f"{ORDER_URL}/orders/addItem/{order_id}/{item_id}/{qty}")

    add_order_tasks = []
    for i, order_id in enumerate(order_ids):
        user_id = orders_user_id[i]
        n_items = random.randint(1, min(MAX_DISTINCT_ITEMS_PER_ORDER, len(item_ids)))
        chosen_item_ids = random.sample(item_ids, n_items)

        items_in_order: List[Tuple[str, int, int]] = []  # (item_id, qty, price)
        for item_id in chosen_item_ids:
            qty = random.randint(1, MAX_QTY_PER_ITEM)
            price = item_configs[item_id]["price"]
            items_in_order.append((item_id, qty, price))

        total_cost = sum(qty * price for _, qty, price in items_in_order)
        order_metadata[order_id] = {
            "user_id": user_id,
            "items": items_in_order,
            "total_cost": total_cost,
        }
        add_order_tasks.append(
            asyncio.ensure_future(add_items_to_order(order_id, items_in_order))
        )

    await asyncio.gather(*add_order_tasks)
    return order_ids, order_metadata


async def perform_checkouts(
    session,
    order_ids: List[str],
    order_metadata: Dict[str, dict],
    log_file,
) -> List[int]:
    """
    Fires all checkouts concurrently and writes one log line per response.

    Log format (same sentinel as the original test):
        CHECKOUT | ORDER: <oid> USER: <uid> COST: <int> ITEMS: <iid>:<qty>,... SUCCESS|FAIL __OUR_LOG__
    """
    async def checkout_one(order_id: str) -> int:
        meta = order_metadata[order_id]
        user_id = meta["user_id"]
        total_cost = meta["total_cost"]
        items_str = ",".join(f"{iid}:{qty}" for iid, qty, _ in meta["items"])
        url = f"{ORDER_URL}/orders/checkout/{order_id}"
        async with session.post(url) as resp:
            outcome = "SUCCESS" if 200 <= resp.status < 300 else "FAIL"
            log_line = (
                f"CHECKOUT | ORDER: {order_id} USER: {user_id} "
                f"COST: {total_cost} ITEMS: {items_str} {outcome} __OUR_LOG__\n"
            )
            log_file.write(log_line)
            return resp.status

    tasks = [asyncio.ensure_future(checkout_one(oid)) for oid in order_ids]
    return list(await asyncio.gather(*tasks))


async def stress(item_configs: Dict[str, dict], user_ids: List[str]) -> None:
    async with aiohttp.ClientSession() as session:
        logger.info(f"Creating {NUMBER_OF_ORDERS} orders with multiple item types...")
        order_ids, order_metadata = await create_orders_with_items(
            session, item_configs, user_ids, NUMBER_OF_ORDERS
        )
        logger.info(f"{len(order_ids)} orders created")

        logger.info("Running concurrent checkouts...")
        with open(f"{tmp_folder_path}/consistency-test.log", "w") as log_file:
            statuses = await perform_checkouts(session, order_ids, order_metadata, log_file)

        successes = sum(1 for s in statuses if 200 <= s < 300)
        logger.info(f"Concurrent checkouts finished — {successes}/{len(order_ids)} succeeded")
