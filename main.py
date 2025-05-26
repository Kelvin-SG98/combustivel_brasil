import pandas as pd
import os
import requests
from io import BytesIO
from pyspark.sql import SparkSession


def extract_data(spark: SparkSession, bronze_path: str):
    """
    Extrai os dados de combustíveis líquidos dos estados do Brasil e salva no formato Parquet.
    :param spark: SparkSession
    :param bronze_path: Caminho para o diretório de armazenamento dos dados brutos (bronze).
    """

    url = 'https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/pdc/mensal/combustiveis-liquidos-estados.xlsx'
    
    print("Iniciando extração de dados de combustíveis líquidos...")

    response = requests.get(url)

    df_pandas = pd.read_excel(
        BytesIO(response.content),
        skiprows=8,      # Pula as 8 primeiras linhas
        skipfooter=3,    # Ignora as 3 últimas linhas
        engine='openpyxl'
    )

    # Converte para Spark
    df_spark = spark.createDataFrame(df_pandas)

    os.makedirs(bronze_path, exist_ok=True)
    df_spark.write.mode("overwrite").parquet(os.path.join(bronze_path, "combustivel_raw.parquet"))

    print("Bronze: extração concluída.")


spark = SparkSession.builder \
        .appName("ANP Combustível Bronze Test") \
        .config("spark.hadoop.io.native.lib", "false") \
        .getOrCreate()

bronze_path = 'data/bronze'

extract_data(spark, bronze_path)

spark.stop()