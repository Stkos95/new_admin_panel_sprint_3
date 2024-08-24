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
        data = {}
        try:
                with open(self.file_path, 'r') as file:
                     data = json.load(file)
        except:
             pass
        finally:         
                with open(self.file_path, 'w') as file:
                    data.update(state)    

                    # try:
                    #     data = json.load(file)

                    # except Exception as e:
                    #     print(e)
                    #     data = {}
                    # print(data)
                    # data.update(state)
                    json.dump(data, file)





    def retrieve_state(self) -> Dict[str, Any]:
        try:
            with open(self.file_path, 'r') as file:
                data = json.load(file)
            return data
        except:
            return {}
    
class State:

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def save_storage(self, key: str, value: Any) -> None:
        self.storage.save_state({key: value})
    
    def get_storage(self, key: str) -> Any:
        return self.storage.retrieve_state().get(key, None)