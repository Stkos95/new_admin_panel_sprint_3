import json
import time
from typing import Any, Dict
from abc import ABC, abstractmethod
from functools import wraps
import psycopg


dsn = {}
class BaseStorage(ABC):

    @abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        pass

class JsonStorage(BaseStorage):

    def __init__(self, file_path: str):
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        with open(self.file_path, 'w') as file:
            json.dump(state, file)

    def retrieve_state(self) -> Dict[str, Any]:
        try:
            with open(self.file_path, 'r') as file:
                data = json.load(file)
            return data
        except FileNotFoundError:
            return {}
    
class State:

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def save_storage(self, key: str, value: Any) -> None:
        self.storage.save_state({key: value})
    
    def get_storage(self, key: str) -> Any:
        return self.storage.retrieve_state().get(key, None)

def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = 0
            counter = 0
            while True:
                try:
                    res = func()
                    return res
                except:
                    print(f'retry - {t}')
                    t = start_sleep_time * (factor ** counter) 
                    if t > border_sleep_time:
                        t = border_sleep_time
                    counter += 1
                    time.sleep(t)
        return inner
    return func_wrapper