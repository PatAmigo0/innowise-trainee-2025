from pathlib import Path

from pandas import DataFrame, Timedelta, concat

from src.config.errorlog_config import columns
from src.core.engine import Engine
from src.ingestors.csv_ingestor import CsvIngestor
from src.rules.global_rules import GlobalFatalErrorRule
from src.rules.specific_rules import LocalBundleErrorRule


def main():
    BASE_DIR = Path(__file__).resolve().parent
    DATA_PATH = BASE_DIR / 'data' / 'data.csv'

    loader = CsvIngestor(DATA_PATH, chunk_size=1000000)
    engine = Engine()

    engine.add_rule(GlobalFatalErrorRule())
    engine.add_rule(LocalBundleErrorRule())

    buffer = DataFrame()

    for chunk in loader.load_chunks(names=columns):
        window = concat([buffer, chunk])

        engine.run(window)

        if not chunk.empty:
            max_time = chunk.index.max()
            cutoff = max_time - Timedelta(hours=1)
            buffer = window[window.index > cutoff]


if __name__ == '__main__':
    main()
