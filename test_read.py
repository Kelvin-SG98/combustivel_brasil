import pandas as pd

# Caminho do parquet
parquet_path = 'data/bronze/combustivel_raw.parquet'

# Lê o parquet e transforma em DataFrame pandas
df = pd.read_parquet(parquet_path)

print(df.head())