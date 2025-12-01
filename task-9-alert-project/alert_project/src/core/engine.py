from pandas import DataFrame

from src.core.virtual.virtual_rule import BaseAlertRule


class Engine:
    def __init__(self):
        self.rules: list[BaseAlertRule] = []

    def add_rule(self, rule: BaseAlertRule):
        self.rules.append(rule)

    def run(self, df: DataFrame):
        for rule in self.rules:
            alerts = rule.check(df)
            for alert in alerts:
                self._send_alert(alert)

    def _send_alert(self, msg: str):
        print(f'ALLERT <<< {msg}')
