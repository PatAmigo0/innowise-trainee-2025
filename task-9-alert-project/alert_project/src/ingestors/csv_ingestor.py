from typing import Iterator

from pandas import DataFrame, read_csv, to_datetime

from src.core.virtual.virtual_ingestor import BaseIngestor


class CsvIngestor(BaseIngestor):
    def load_chunks(self, **kwargs) -> Iterator[DataFrame]:
        dtypes = {
            'severity': 'category',
            'bundle_id': 'category',
            'os': 'category',
        }

        default_params = {
            'filepath_or_buffer': self.endpoint,
            'chunksize': self.chunk_size,
            'header': 0,
            'dtype': dtypes,
        }

        with read_csv(**default_params, **kwargs) as reader:
            for chunk in reader:
                chunk: DataFrame = chunk

                chunk['date'] = to_datetime(chunk['date'], unit='s')

                chunk.set_index('date', inplace=True)
                chunk.sort_index(inplace=True)

                yield chunk
