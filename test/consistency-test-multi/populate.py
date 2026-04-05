import asyncio
import json
import logging
import os
from typing import Dict, List, Tuple

import aiohttp

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                    datefmt='%I:%M:%S')
logger = logging.getLogger(__name__)

# Five item types with distinct prices and stock levels.
# Total stock units: 1000+800+600+400+200 = 3000
# 2000 orders × avg 2.5 items × avg 1.5 qty ≈ 7500 units requested → heavy contention.
ITEM_CONFIGS = [
    {"price": 1,  "stock": 1000},
    {"price": 2,  "stock": 800},
    {"price": 5,  "stock": 600},
    {"price": 10, "stock": 400},
    {"price": 20, "stock": 200},
]

NUMBER_OF_USERS = 1000
USER_STARTING_CREDIT = 500

with open(os.path.join('..', 'urls.json')) as f:
    urls = json.load(f)
    ORDER_URL = urls['ORDER_URL']
    PAYMENT_URL = urls['PAYMENT_URL']
    STOCK_URL = urls['STOCK_URL']


async def post_and_get_status(session, url) -> int:
    async with session.post(url) as resp:
        return resp.status


async def post_and_get_field(session, url, field):
    async with session.post(url) as resp:
        jsn = await resp.json()
        return jsn[field]


async def create_item_with_stock(session, price: int, stock: int) -> str:
    item_id = await post_and_get_field(session, f"{STOCK_URL}/stock/item/create/{price}", 'item_id')
    await post_and_get_status(session, f"{STOCK_URL}/stock/add/{item_id}/{stock}")
    return item_id


async def create_items(session) -> Dict[str, dict]:
    """Create all item types concurrently. Returns item_id → {price, stock}."""
    tasks = [
        asyncio.ensure_future(create_item_with_stock(session, cfg["price"], cfg["stock"]))
        for cfg in ITEM_CONFIGS
    ]
    item_ids: List[str] = list(await asyncio.gather(*tasks))
    return {
        item_id: {"price": ITEM_CONFIGS[i]["price"], "stock": ITEM_CONFIGS[i]["stock"]}
        for i, item_id in enumerate(item_ids)
    }


async def create_users(session, number_of_users: int, credit: int) -> List[str]:
    tasks = [
        asyncio.ensure_future(post_and_get_field(session, f"{PAYMENT_URL}/payment/create_user", 'user_id'))
        for _ in range(number_of_users)
    ]
    user_ids: List[str] = list(await asyncio.gather(*tasks))
    fund_tasks = [
        asyncio.ensure_future(post_and_get_status(session, f"{PAYMENT_URL}/payment/add_funds/{uid}/{credit}"))
        for uid in user_ids
    ]
    await asyncio.gather(*fund_tasks)
    return user_ids


async def populate_databases() -> Tuple[Dict[str, dict], List[str]]:
    async with aiohttp.ClientSession() as session:
        logger.info("Creating items with different prices and stock levels...")
        item_configs = await create_items(session)
        for iid, cfg in item_configs.items():
            logger.info(f"  item {iid}  price={cfg['price']}  stock={cfg['stock']}")
        logger.info("Items created")

        logger.info("Creating users...")
        user_ids = await create_users(session, NUMBER_OF_USERS, USER_STARTING_CREDIT)
        logger.info("Users created")

    return item_configs, user_ids
