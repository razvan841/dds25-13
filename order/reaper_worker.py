import os
import time

# Ensure the app starts the background threads instead of the web server only.
os.environ.setdefault("RUN_SAGA_WORKER", "1")

# Import app to reuse the existing worker functions and Kafka/Redis config.
from app import start_background_workers  # noqa: E402


def main():
    start_background_workers()
    # Keep the process alive; threads are daemonized.
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
