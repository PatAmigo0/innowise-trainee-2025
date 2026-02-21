from abc import ABC, abstractmethod
import pkgutil
import importlib

TASK_REGISTRY: list[type['BaseTask']] = []

class BaseTask(ABC):

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        TASK_REGISTRY.append(cls)

    @staticmethod
    @abstractmethod
    def name() -> str:
        pass

    @staticmethod
    @abstractmethod
    def run() -> None:
        pass

for loader, module_name, is_pkg in pkgutil.iter_modules(__path__):
    importlib.import_module(f".{module_name}", __name__)