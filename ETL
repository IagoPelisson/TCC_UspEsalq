
import pandas as pd
import pyodbc
import numpy as np
from decimal import Decimal

# Caminho do arquivo
caminho = r"C:\Users\iago_\tcc\Dados_Brasileirao_SerieA.csv"
df = pd.read_csv(caminho, encoding="utf-8")

# Converter coluna 'data' (se existir)
if "data" in df.columns:
    df["data"] = pd.to_datetime(df["data"], errors="coerce")

# Substituir NaN por None
df = df.replace({np.nan: None})

# Normalizar colunas de valor (exemplo: todas que começam com 'valor_')
for col in df.columns:
    if "valor" in col.lower():
        df[col] = df[col].apply(
            lambda x: Decimal(str(round(x, 2))) if x is not None else None
        )

# Função para mapear tipos com base em TODA a coluna
def map_dtype(series):
    if series.dropna().apply(lambda x: isinstance(x, str)).any():
        return "VARCHAR(255)"
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "DATE"
    elif pd.api.types.is_integer_dtype(series):
        return "INT"
    elif pd.api.types.is_float_dtype(series) or series.dropna().apply(lambda x: isinstance(x, Decimal)).any():
        return "DECIMAL(18,2)"   # precisão exata
    else:
        return "VARCHAR(255)"

# Gerar CREATE TABLE dinamicamente
table_name = "Brasileirao_SerieA"
schema_name = "masculino"

columns_sql = []
for col in df.columns:
    sql_type = map_dtype(df[col])
    columns_sql.append(f"[{col}] {sql_type}")

create_table_sql = f"""
IF OBJECT_ID('{schema_name}.{table_name}', 'U') IS NOT NULL
    DROP TABLE [{schema_name}].[{table_name}];

CREATE TABLE [{schema_name}].[{table_name}] (
    {',\n    '.join(columns_sql)}
);
"""

# Conectar ao SQL Server
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=futebol;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

# Criar tabela
cursor.execute(create_table_sql)
conn.commit()
print("Tabela criada com sucesso!")

# Alimentar a tabela
cols = ", ".join([f"[{c}]" for c in df.columns])
placeholders = ", ".join(["?" for _ in df.columns])
insert_sql = f"INSERT INTO {schema_name}.{table_name} ({cols}) VALUES ({placeholders})"

for row in df.itertuples(index=False, name=None):
    try:
        cursor.execute(insert_sql, row)
    except Exception as e:
        print("Erro ao inserir linha:", row)
        print("Detalhe:", e)

conn.commit()
print("Dados inseridos com sucesso!")
