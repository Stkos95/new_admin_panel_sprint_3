import json
import time
from typing import Any, Dict
from abc import ABC, abstractmethod
from functools import wraps

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
        with open(self.file_path, 'r') as file:
            json.dump(file, state)

    def retrieve_state(self) -> Dict[str, Any]:
        with open(self.file_path, 'w') as file:
            data = json.load(self.file_path)
        return data
    

class State:

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def save_storage(self, key: str, value: Any) -> None:
        self.storage.save_state({key: value})
    
    def get_storage(self, key: str) -> Any:
        return self.storage.retrieve_state().get(key, None)


def backoff(start_time=0.1, delta=2, max_time_value=15):
    def decor(func):
        @wraps
        def wrapper(*args, **kwargs):
            counter = 0
            while True:
                try:
                    conn = func()
                    return conn
                except:
                    counter += start_time * delta ** 2
                    if counter > max_time_value:
                        counter = max_time_value
                    time.sleep(counter)
                    


        return wrapper    


    return decor

