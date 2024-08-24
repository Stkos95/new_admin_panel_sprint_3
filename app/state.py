import json
from abc import ABC, abstractmethod
from typing import Any, Dict

from config import load_config


class BaseStorage(ABC):

    @abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        pass


class JsonStorage(BaseStorage):

    def __init__(self):
        self.file_path = load_config().state.file_name

    def save_state(self, state: Dict[str, Any]) -> None:
        """Функция сохраняет состояние.

        Args:
            state (Dict[str, Any]): Словарь с состоянием и его значением.
        """
        data = {}
        try:
            with open(self.file_path, "r") as file:
                data = json.load(file)
        except FileNotFoundError:
            pass
        finally:
            with open(self.file_path, "w") as file:
                data.update(state)
                json.dump(data, file)

    def retrieve_state(self) -> Dict[str, Any]:
        """Функция получает состояние из файла.

        Returns:
            Dict[str, Any]: Словарь с состояниями.
        """
        try:
            with open(self.file_path, "r") as file:
                data = json.load(file)
            return data
        except FileNotFoundError:
            return {}


class State:

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def save_storage(self, key: str, value: Any) -> None:
        """Функция принимает состояние и его значение и
            сохраняет их в хранилище.

        Args:
            key (str): Состояние
            value (Any): Значение состояния
        """
        self.storage.save_state({key: value})

    def get_storage(self, key: str) -> Any:
        """Функция получает значение указанного состояния.

        Args:
            key (str): Состояние.

        Returns:
            Any: Значение состояния.
        """
        return self.storage.retrieve_state().get(key, None)
