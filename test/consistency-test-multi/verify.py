import asyncio
import re
import os
import json
import logging
from typing import Dict, List, Tuple

import aiohttp

from populate import NUMBER_OF_USERS, USER_STARTING_CREDIT

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                    datefmt='%I:%M:%S')
logger = logging.getLogger(__name__)

with open(os.path.join('..', 'urls.json')) as f:
    urls = json.load(f)
    ORDER_URL = urls['ORDER_URL']
    PAYMENT_URL = urls['PAYMENT_URL']
    STOCK_URL = urls['STOCK_URL']

# Pre-compiled log pattern for the extended format written by stress.py:
#   CHECKOUT | ORDER: <oid> USER: <uid> COST: <int> ITEMS: <iid>:<qty>,... SUCCESS|FAIL __OUR_LOG__
_LOG_RE = re.compile(
    r"ORDER: (\S+) USER: (\S+) COST: (\d+) ITEMS: (\S+) (SUCCESS|FAIL) __OUR_LOG__"
)


async def _get_field(session, url: str, field: str, key: str) -> Tuple[str, int]:
    async with session.get(url) as resp:
        jsn = await resp.json()
        return key, jsn[field]


async def get_user_credit_dict(session, user_ids: List[str]) -> Dict[str, int]:
    tasks = [
        asyncio.ensure_future(_get_field(session, f"{PAYMENT_URL}/payment/find_user/{uid}", 'credit', uid))
        for uid in user_ids
    ]
    return dict(await asyncio.gather(*tasks))


async def get_item_stock_dict(session, item_ids: List[str]) -> Dict[str, int]:
    tasks = [
        asyncio.ensure_future(_get_field(session, f"{STOCK_URL}/stock/find/{iid}", 'stock', iid))
        for iid in item_ids
    ]
    return dict(await asyncio.gather(*tasks))


def parse_log(
    tmp_dir: str,
    item_configs: Dict[str, dict],
    user_ids: List[str],
) -> Tuple[Dict[str, int], Dict[str, int], int]:
    """
    Replay the checkout log to derive the expected state.

    For every SUCCESS line:
      - deduct COST from the user's expected credit
      - deduct each item's qty from that item's expected remaining stock

    Returns
    -------
    expected_user_credits  : user_id → expected credit after all successful checkouts
    expected_item_stocks   : item_id → expected remaining stock after all successful checkouts
    n_successes            : number of successful checkouts recorded in the log
    """
    expected_user_credits: Dict[str, int] = {uid: USER_STARTING_CREDIT for uid in user_ids}
    expected_item_stocks: Dict[str, int] = {iid: cfg["stock"] for iid, cfg in item_configs.items()}
    n_successes = 0

    log_path = os.path.join(tmp_dir, 'consistency-test.log')
    with open(log_path, 'r') as f:
        for line in f:
            if not line.endswith('__OUR_LOG__\n'):
                continue
            m = _LOG_RE.search(line)
            if not m:
                continue
            user_id = m.group(2)
            cost = int(m.group(3))
            items_str = m.group(4)
            status = m.group(5)

            if status != 'SUCCESS':
                continue

            n_successes += 1
            expected_user_credits[user_id] = expected_user_credits.get(user_id, USER_STARTING_CREDIT) - cost
            for part in items_str.split(','):
                iid, qty_str = part.split(':')
                expected_item_stocks[iid] = expected_item_stocks.get(iid, 0) - int(qty_str)

    return expected_user_credits, expected_item_stocks, n_successes


async def verify_systems_consistency(
    tmp_dir: str,
    item_configs: Dict[str, dict],
    user_ids: List[str],
) -> None:
    expected_user_credits, expected_item_stocks, n_successes = parse_log(tmp_dir, item_configs, user_ids)

    async with aiohttp.ClientSession() as session:
        actual_user_credits = await get_user_credit_dict(session, user_ids)
        actual_item_stocks = await get_item_stock_dict(session, list(item_configs.keys()))

    # ---- Per-user credit check ----
    user_errors: List[str] = []
    for uid in user_ids:
        exp = expected_user_credits[uid]
        act = actual_user_credits[uid]
        if exp != act:
            user_errors.append(f"  user {uid}: expected={exp}  actual={act}  diff={act - exp:+d}")

    # ---- Per-item stock check ----
    stock_errors: List[str] = []
    for iid, cfg in item_configs.items():
        exp = expected_item_stocks[iid]
        act = actual_item_stocks[iid]
        if exp != act:
            stock_errors.append(
                f"  item {iid} (price={cfg['price']}): "
                f"expected={exp}  actual={act}  diff={act - exp:+d}"
            )

    # ---- Aggregate totals ----
    total_credit_initial = NUMBER_OF_USERS * USER_STARTING_CREDIT
    total_credit_expected = sum(expected_user_credits.values())
    total_credit_actual = sum(actual_user_credits.values())

    total_stock_consumed_expected = sum(
        cfg["stock"] - expected_item_stocks[iid] for iid, cfg in item_configs.items()
    )
    total_stock_consumed_actual = sum(
        cfg["stock"] - actual_item_stocks[iid] for iid, cfg in item_configs.items()
    )

    # ---- Report ----
    logger.info("=" * 60)
    logger.info(f"Successful checkouts logged   : {n_successes}")
    logger.info("-" * 60)
    logger.info("PAYMENT consistency")
    logger.info(f"  Initial total credit        : {total_credit_initial}")
    logger.info(f"  Expected total credit       : {total_credit_expected}")
    logger.info(f"  Actual total credit         : {total_credit_actual}")
    logger.info(f"  Payment inconsistencies     : {abs(total_credit_actual - total_credit_expected)}")
    logger.info(f"  Users with wrong credit     : {len(user_errors)} / {len(user_ids)}")
    for line in user_errors[:20]:   # cap noisy output
        logger.warning(line)
    if len(user_errors) > 20:
        logger.warning(f"  ... and {len(user_errors) - 20} more")

    logger.info("-" * 60)
    logger.info("STOCK consistency")
    logger.info(f"  Total units consumed (log)  : {total_stock_consumed_expected}")
    logger.info(f"  Total units consumed (DB)   : {total_stock_consumed_actual}")
    logger.info(f"  Stock inconsistencies       : {abs(total_stock_consumed_actual - total_stock_consumed_expected)}")
    logger.info(f"  Items with wrong stock      : {len(stock_errors)} / {len(item_configs)}")
    for line in stock_errors:
        logger.warning(line)

    per_item_log = []
    for iid, cfg in item_configs.items():
        consumed = cfg["stock"] - actual_item_stocks[iid]
        per_item_log.append(
            f"  price={cfg['price']:>2}  initial={cfg['stock']}  "
            f"remaining={actual_item_stocks[iid]}  consumed={consumed}"
        )
    logger.info("-" * 60)
    logger.info("Per-item stock summary (actual DB state):")
    for line in per_item_log:
        logger.info(line)
    logger.info("=" * 60)

    all_ok = (len(user_errors) == 0 and len(stock_errors) == 0)
    if all_ok:
        logger.info("RESULT: CONSISTENT — no discrepancies found")
    else:
        logger.error("RESULT: INCONSISTENT — discrepancies detected (see above)")
