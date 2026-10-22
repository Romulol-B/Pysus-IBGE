# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze — INMET de São Paulo
# MAGIC Lê o CSV da landing com `;`, seleciona SP e grava uma tabela Delta gerenciada.
# MAGIC Mantém todos os anos e campos como texto, sem agregação ou limpeza climática.
# MAGIC Reexecutar substitui o recorte da bronze. Tipagem e transformações ficam para a silver.

# COMMAND ----------

import csv

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

ORIGEM = "/Volumes/dengue_sus/landing/inmet/INMET_dataset_agregado.csv"
SCHEMA_BRONZE = "dengue_sus.bronze"
TABELA_BRONZE = f"{SCHEMA_BRONZE}.inmet_sp"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler o cabeçalho e preservar as duas colunas de radiação
# MAGIC `RADIACAO GLOBAL (Kj/m²)` recebe o nome `RADIACAO GLOBAL ALTERNATIVA (Kj/m²)`.
# MAGIC Os valores e a ordem das colunas são preservados. O schema textual evita inferência
# MAGIC sobre o CSV inteiro e mantém vírgulas decimais e sentinelas para a próxima etapa.

# COMMAND ----------

with open(ORIGEM, encoding="utf-8-sig", newline="") as arquivo:
    colunas = next(csv.reader(arquivo, delimiter=";"))

colunas = [
    "RADIACAO GLOBAL ALTERNATIVA (Kj/m²)" if c == "RADIACAO GLOBAL (Kj/m²)" else c
    for c in colunas
]
if not {"UF", "ANO_REFERENCIA"}.issubset(colunas):
    raise ValueError("Cabeçalho sem UF/ANO_REFERENCIA: confira arquivo e separador ';'.")
if len({c.casefold() for c in colunas}) != len(colunas):
    raise ValueError("O cabeçalho ainda contém nomes de colunas ambíguos.")

schema = StructType([StructField(c, StringType(), True) for c in colunas])
dados = (
    spark.read.schema(schema)
    .option("header", True)
    .option("enforceSchema", True)
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .option("mode", "FAILFAST")
    .csv(ORIGEM)
)
recorte = dados.filter(F.upper(F.trim(F.col("UF"))) == "SP")
if not recorte.head(1):
    raise ValueError("Nenhum registro de SP encontrado; a saída não foi substituída.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publicar a bronze
# MAGIC Column mapping em modo `name` preserva os cabeçalhos com espaços e caracteres especiais.
# MAGIC Um destino existente incompatível interrompe a execução antes da escrita.
# MAGIC A gravação substitui integralmente o recorte, sem acrescentar duplicatas entre execuções.

# COMMAND ----------

def validar_bronze():
    if spark.catalog.getTable(TABELA_BRONZE).tableType != "MANAGED":
        raise ValueError(f"{TABELA_BRONZE} deve ser uma tabela gerenciada.")
    detalhe = spark.sql(f"DESCRIBE DETAIL {TABELA_BRONZE}").first()
    if detalhe["format"] != "delta" or detalhe["properties"].get("delta.columnMapping.mode") != "name":
        raise ValueError(f"{TABELA_BRONZE} deve usar Delta com column mapping 'name'.")
    tabela = spark.table(TABELA_BRONZE)
    if tabela.dtypes != [(c, "string") for c in colunas]:
        raise ValueError(f"Schema incompatível em {TABELA_BRONZE}; confira os nomes, a ordem e os tipos.")
    return tabela


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_BRONZE}")
if spark.catalog.tableExists(TABELA_BRONZE):
    validar_bronze()

(
    recorte.write.format("delta")
    .mode("overwrite")
    .option("delta.columnMapping.mode", "name")
    .saveAsTable(TABELA_BRONZE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferir o resultado salvo
# MAGIC Confirme o formato, os tipos textuais e a distribuição por ano da tabela persistida.
# MAGIC Execute novamente com o mesmo CSV e compare as contagens antes de avançar para a silver.

# COMMAND ----------

salvo = validar_bronze()
uf = F.upper(F.trim(F.col("UF")))
if salvo.filter(uf.isNull() | (uf != "SP")).head(1):
    raise ValueError("A bronze contém registros fora de SP.")

por_ano = salvo.groupBy("ANO_REFERENCIA").count().orderBy("ANO_REFERENCIA")
total = sum(linha["count"] for linha in por_ano.collect())
if total == 0:
    raise ValueError("A tabela bronze está vazia.")

display(por_ano)
display(salvo.limit(20))
print(f"Bronze salva: {total:,} registros de SP em {TABELA_BRONZE}")