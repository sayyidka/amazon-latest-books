import os
import time
import logging

if not os.path.exists("logs"):
    os.mkdir("logs")

logging.basicConfig(
    filename="logs/app.log",
    level=logging.INFO,
    format="%(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("app")


def timer(func):
    """Decorator that print how long function took to run

    Args:
        func (func): the function to be returned
    """

    def wrapper():
        start = time.time()
        results = func()
        end = time.time()
        logger.info("{} function took {}s".format(func.__name__, round(end - start, 2)))
        return results

    return wrapper
