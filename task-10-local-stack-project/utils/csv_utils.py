from pathlib import Path

from pandas import read_csv, to_datetime


def split_by_month(path: Path):
    dtypes = {'departure_id': 'category', 'departure_name': 'category', 'return_name': 'category'}

    df = read_csv(path, low_memory=False, dtype=dtypes)
    df.columns = df.columns.str.strip()
    df['return'] = to_datetime(df['return'])

    parts = df.groupby(df['return'].dt.to_period('M'))

    for period, group_df in parts:
        print(f'=== Data for {period} ===')
        print(group_df.head())
        group_df.to_csv(path.parent / f'{period}.csv', index=False)


if __name__ == '__main__':
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_PATH = BASE_DIR / 'data' / 'database.csv'

    if DATA_PATH.exists():
        split_by_month(DATA_PATH)
    else:
        print(f'File not found: {DATA_PATH}')
