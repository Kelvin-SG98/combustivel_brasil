import pandas as pd

# Caminho do parquet
parquet_path = 'data/bronze/combustivel_raw.parquet'

# Lê o parquet e transforma em DataFrame pandas
df = pd.read_parquet(parquet_path)

# Filtrar por tipo de combustível
df_filtrado = df[df['PRODUTO'] == 'GASOLINA C COMUM']

# Agrupar e somar
df_grouped = df_filtrado.groupby('ESTADO')['PREÇO MÉDIO DE DISTRIBUIÇÃO'].sum().reset_index()

# Ordenar do maior para o menor
df_grouped = df_grouped.sort_values(by='PREÇO MÉDIO DE DISTRIBUIÇÃO', ascending=False)

df_grouped.to_csv("media_distribuicao_por_estado.csv", index=False)

# Exibir bonito
# print(df_grouped.to_string(index=False))

