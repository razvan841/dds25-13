"""
Crash recovery consistency test.
Runs the benchmark's consistency test with a service crash mid-checkouts.

Usage: python3 crash_consistency_test.py [saga|2pc] [service_to_kill]
  Default: saga, stock-service
"""
import asyncio
import json
import logging
import os
import random
import re
import shutil
import subprocess
import sys
import time
from tempfile import gettempdir

import aiohttp

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                    datefmt='%I:%M:%S')
logger = logging.getLogger("CrashTest")

ORDER_URL = STOCK_URL = PAYMENT_URL = "http://127.0.0.1:8000"

NUMBER_OF_ITEMS = 1
ITEM_STARTING_STOCK = 100
ITEM_PRICE = 1
NUMBER_OF_USERS = 1000
USER_STARTING_CREDIT = 1
NUMBER_OF_ORDERS = 1000

CORRECT_USER_STATE = (NUMBER_OF_USERS * USER_STARTING_CREDIT) - (NUMBER_OF_ITEMS * ITEM_STARTING_STOCK * ITEM_PRICE)


async def post_json_field(session, url, field):
    async with session.post(url) as resp:
        jsn = await resp.json()
        return jsn[field]


async def post_status(session, url):
    async with session.post(url) as resp:
        return resp.status


async def populate(session):
    logger.info("Creating items...")
    item_tasks = [post_json_field(session, f"{STOCK_URL}/stock/item/create/{ITEM_PRICE}", "item_id")
                  for _ in range(NUMBER_OF_ITEMS)]
    item_ids = list(await asyncio.gather(*item_tasks))
    stock_tasks = [post_status(session, f"{STOCK_URL}/stock/add/{iid}/{ITEM_STARTING_STOCK}") for iid in item_ids]
    await asyncio.gather(*stock_tasks)

    logger.info("Creating users...")
    user_tasks = [post_json_field(session, f"{PAYMENT_URL}/payment/create_user", "user_id")
                  for _ in range(NUMBER_OF_USERS)]
    user_ids = list(await asyncio.gather(*user_tasks))
    fund_tasks = [post_status(session, f"{PAYMENT_URL}/payment/add_funds/{uid}/{USER_STARTING_CREDIT}") for uid in user_ids]
    await asyncio.gather(*fund_tasks)
    return item_ids, user_ids


async def checkout_one(session, order_id, user_id, results):
    try:
        async with session.post(f"{ORDER_URL}/orders/checkout/{order_id}") as resp:
            status = resp.status
            results.append(("SUCCESS" if 200 <= status < 300 else "FAIL", order_id, user_id))
    except Exception as e:
        logger.warning(f"Checkout {order_id} error: {e}")
        results.append(("FAIL", order_id, user_id))


def kill_service_after_delay(container_name, delay):
    """Restart a docker container after a delay (simulates crash + recovery)."""
    time.sleep(delay)
    logger.info(f"=== RESTARTING {container_name} (simulating crash) ===")
    subprocess.run(["docker", "restart", "-t", "0", container_name], capture_output=True)
    logger.info(f"=== {container_name} restarted ===")
    # Wait for it to become healthy
    for _ in range(30):
        time.sleep(1)
        result = subprocess.run(["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Status}}"],
                                capture_output=True, text=True)
        status = result.stdout.strip()
        if "Up" in status:
            logger.info(f"=== {container_name} healthy: {status} ===")
            return
    logger.error(f"=== {container_name} did not come back! ===")


async def run_test(service_to_kill):
    # Long timeout to survive the crash period
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        item_ids, user_ids = await populate(session)

        logger.info("Creating orders...")
        order_user_pairs = []
        order_tasks = []
        for _ in range(NUMBER_OF_ORDERS):
            uid = random.choice(user_ids)
            order_user_pairs.append(uid)
            order_tasks.append(post_json_field(session, f"{ORDER_URL}/orders/create/{uid}", "order_id"))
        order_ids = list(await asyncio.gather(*order_tasks))

        add_item_tasks = []
        for oid in order_ids:
            iid = random.choice(item_ids)
            add_item_tasks.append(post_status(session, f"{ORDER_URL}/orders/addItem/{oid}/{iid}/1"))
        await asyncio.gather(*add_item_tasks)
        logger.info("Orders created")

        # Schedule the kill
        import threading
        container_name = f"dds-{service_to_kill}-1"
        kill_thread = threading.Thread(target=kill_service_after_delay, args=(container_name, 5))
        kill_thread.start()

        logger.info("Running concurrent checkouts...")
        results = []
        checkout_tasks = [checkout_one(session, order_ids[i], order_user_pairs[i], results)
                          for i in range(NUMBER_OF_ORDERS)]
        await asyncio.gather(*checkout_tasks)
        kill_thread.join()
        logger.info("Checkouts finished")

        # Count successes from log
        success_count = sum(1 for r in results if r[0] == "SUCCESS")
        logger.info(f"Successful checkouts: {success_count}")
        log_inconsistency = success_count - (NUMBER_OF_ITEMS * ITEM_STARTING_STOCK)
        logger.info(f"Stock service inconsistencies in the logs: {log_inconsistency}")

        # Compute expected user credit from log
        user_credit_expected = {uid: USER_STARTING_CREDIT for uid in user_ids}
        for status, oid, uid in results:
            if status == "SUCCESS":
                user_credit_expected[uid] -= ITEM_PRICE
        logged_user_credit = sum(user_credit_expected.values())
        logger.info(f"Payment service inconsistencies in the logs: {abs(CORRECT_USER_STATE - logged_user_credit)}")

        # Verify actual DB state
        logger.info("Verifying database state...")
        stock_tasks = []
        for iid in item_ids:
            stock_tasks.append(get_field(session, f"{STOCK_URL}/stock/find/{iid}", "stock", iid))
        stock_results = dict(await asyncio.gather(*stock_tasks))
        total_stock = sum(stock_results.values())
        items_bought = (NUMBER_OF_ITEMS * ITEM_STARTING_STOCK) - total_stock
        db_stock_inconsistency = items_bought - (NUMBER_OF_ITEMS * ITEM_STARTING_STOCK)
        logger.info(f"Stock service inconsistencies in the database: {db_stock_inconsistency}")

        credit_tasks = []
        for uid in user_ids:
            credit_tasks.append(get_field(session, f"{PAYMENT_URL}/payment/find_user/{uid}", "credit", uid))
        credit_results = dict(await asyncio.gather(*credit_tasks))
        total_credit = sum(credit_results.values())
        db_payment_inconsistency = abs(CORRECT_USER_STATE - total_credit)
        logger.info(f"Payment service inconsistencies in the database: {db_payment_inconsistency}")

        return log_inconsistency == 0 and db_stock_inconsistency == 0 and db_payment_inconsistency == 0


async def get_field(session, url, field, key):
    async with session.get(url) as resp:
        jsn = await resp.json()
        return key, jsn[field]


if __name__ == "__main__":
    service = sys.argv[1] if len(sys.argv) > 1 else "stock-service"
    ok = asyncio.run(run_test(service))
    sys.exit(0 if ok else 1)
