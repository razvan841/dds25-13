import asyncio
import os
import shutil
import logging
from tempfile import gettempdir
import aiohttp
import time
import json
import sys

from verify import verify_systems_consistency
from populate import populate_databases
from stress import stress

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                    datefmt='%I:%M:%S')
logger = logging.getLogger("Consistency test")


def load_service_urls():
    with open(os.path.join('..', 'urls.json')) as f:
        return json.load(f)


async def check_service_ready(session, url):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return False
            data = await resp.json()
            return data.get("status") == "sent"
    except Exception:
        return False


async def wait_for_services(urls, timeout=120, interval=2):
    logger.info("Waiting for services to be ready...")
    start = time.time()
    async with aiohttp.ClientSession() as session:
        while time.time() - start < timeout:
            results = await asyncio.gather(*[check_service_ready(session, url) for url in urls])
            if all(results):
                logger.info("All services are ready.")
                return True
            not_ready = [url for url, ok in zip(urls, results) if not ok]
            logger.info("Services not ready yet, retrying... (waiting on: %s)", not_ready)
            await asyncio.sleep(interval)
    logger.error("Timeout waiting for services.")
    return False


async def main(tmp_folder_path: str):
    service_urls = load_service_urls()
    kafka_ping_urls = [
        f"{service_urls['ORDER_URL']}/orders/kafka_ping",
        f"{service_urls['PAYMENT_URL']}/payment/kafka_ping",
        f"{service_urls['STOCK_URL']}/stock/kafka_ping",
    ]

    if not await wait_for_services(kafka_ping_urls):
        raise RuntimeError("Services not ready. Aborting test.")

    logger.info("Populating the databases...")
    item_ids, user_ids = await populate_databases()
    logger.info("Databases populated")

    logger.info("Starting the load test...")
    await stress(item_ids, user_ids)
    logger.info("Load test completed")

    logger.info("Starting the consistency evaluation...")
    await verify_systems_consistency(tmp_folder_path, item_ids, user_ids)
    logger.info("Consistency evaluation completed")


if __name__ == "__main__":
    logger.info("Creating tmp folder...")
    tmp_folder_path: str = os.path.join(gettempdir(), 'wdm_consistency_test')

    if os.path.isdir(tmp_folder_path):
        shutil.rmtree(tmp_folder_path)

    os.mkdir(tmp_folder_path)
    logger.info("tmp folder created")

    try:
        asyncio.run(main(tmp_folder_path))
    except RuntimeError as e:
        logger.error(str(e))
        sys.exit(1)
    finally:
        if os.path.isdir(tmp_folder_path):
            shutil.rmtree(tmp_folder_path)