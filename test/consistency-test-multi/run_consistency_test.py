import asyncio
import os
import shutil
import logging
from tempfile import gettempdir

from verify import verify_systems_consistency
from populate import populate_databases
from stress import stress, tmp_folder_path

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                    datefmt='%I:%M:%S')
logger = logging.getLogger("Consistency test (multi-item)")

# ---- Setup tmp folder ----
logger.info("Creating tmp folder...")
if os.path.isdir(tmp_folder_path):
    shutil.rmtree(tmp_folder_path)
os.mkdir(tmp_folder_path)
logger.info("tmp folder created")

# ---- Populate ----
logger.info("Populating the databases...")
item_configs, user_ids = asyncio.run(populate_databases())
logger.info("Databases populated")

# ---- Stress ----
logger.info("Starting the load test...")
asyncio.run(stress(item_configs, user_ids))
logger.info("Load test completed")

# ---- Verify ----
logger.info("Starting the consistency evaluation...")
asyncio.run(verify_systems_consistency(tmp_folder_path, item_configs, user_ids))
logger.info("Consistency evaluation completed")

# ---- Cleanup ----
if os.path.isdir(tmp_folder_path):
    shutil.rmtree(tmp_folder_path)
