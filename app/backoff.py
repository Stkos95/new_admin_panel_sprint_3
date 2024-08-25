import time
from functools import wraps

from elasticsearch.exceptions import ConnectionError
from main_logger import MainLogger
from psycopg import OperationalError

logger = MainLogger().get_logger("backoff")


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """Функция для повторного подключения к базе данных.
        Постоянно увеличивает время ожидания.

    Args:
        start_sleep_time (float, optional): Начальное время ожидания. Defaults to 0.1.
        factor (int, optional): Величина, от которой зависит увеличение ожидания. Defaults to 2.
        border_sleep_time (int, optional): Максимальное время ожидания. Defaults to 10.
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(instance, *args, **kwargs):
            time_to_sleep = 0
            counter = 0
            while True:
                try:
                    return func(instance, *args, **kwargs)
                except (ConnectionError, OperationalError):
                    time_to_sleep = start_sleep_time * (factor**counter)
                    if time_to_sleep > border_sleep_time:
                        time_to_sleep = border_sleep_time
                    logger.info(
                        f"Пробую подключиться к базе данных повторно повторно. Жду %s", time_to_sleep
                    )
                    counter += 1
                    time.sleep(time_to_sleep)
                except KeyboardInterrupt:
                    exit()

        return inner

    return func_wrapper
