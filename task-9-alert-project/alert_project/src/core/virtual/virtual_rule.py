from abc import ABC, abstractmethod

import pandas as pd


class BaseAlertRule(ABC):
    @abstractmethod
    def check(self, df: pd.DataFrame) -> list[str | object]:
        pass
