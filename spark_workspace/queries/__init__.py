from abc import ABC, abstractmethod, abstractproperty
import pkgutil
import importlib

TASK_REGISTRY = []

class BaseTask(ABC):

    @staticmethod
    def name():
        pass

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        TASK_REGISTRY.append(cls)

    @staticmethod
    def run():
        pass

for loader, module_name, is_pkg in pkgutil.iter_modules(__path__):
    importlib.import_module(f".{module_name}", __name__)