import pandas as pd
from pathlib import Path

for path in Path('.data/staging/github/events/push/').rglob('*.parquet'):
    pd.read_parquet(path).astype({'language': str, 'org_name': str}).to_parquet(path, index=False)