from pandas import Timedelta

from src.core.virtual.virtual_rule import BaseAlertRule


class LocalBundleErrorRule(BaseAlertRule):
    def check(self, df) -> list[str]:
        alerts = []

        fatal_df = df[df['severity'] == 'Error']
        if fatal_df.empty:
            return alerts

        counts = fatal_df.groupby('bundle_id', observed=True).resample(Timedelta(hours=1)).size()
        spikes = counts[counts > 10]

        for (bundle_id, time_bucket), count in spikes.items():
            alerts.append(f'[{time_bucket}]: {bundle_id} ENCOUNTERED {count} ERRORS (FATAL)')

        return alerts
