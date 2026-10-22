# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SINAN — Bronze de residentes em SP
# MAGIC Execute depois de `03_validar_landing_sinan`, com `DENGBR10.parquet` e
# MAGIC `DENGBR22.parquet` na Landing. Cada ano é publicado separadamente para
# MAGIC preservar o esquema original, inclusive diferenças entre arquivos.

# COMMAND ----------

from pyspark.sql import functions as F

ANOS = (2010, 2022)
PASTA = "/Volumes/dengue_sus/landing/sinan_s3"
SCHEMA = "dengue_sus.bronze"

# COMMAND ----------

fontes = {}
for ano in ANOS:
    caminho = f"{PASTA}/DENGBR{ano % 100:02d}.parquet"
    if not any(item.name.rstrip("/") == caminho.rsplit("/", 1)[-1] for item in dbutils.fs.ls(PASTA)):
        raise FileNotFoundError(caminho)
    dados = spark.read.parquet(caminho)
    if "ID_MN_RESI" not in dados.columns or "DT_NOTIFIC" not in dados.columns:
        raise ValueError(f"{caminho}: faltam ID_MN_RESI ou DT_NOTIFIC")
    residente = F.trim(F.col("ID_MN_RESI").cast("string"))
    recorte = dados.filter(residente.startswith("35"))
    if not recorte.head(1):
        raise ValueError(f"{caminho}: sem residentes de SP")
    fontes[ano] = recorte

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
for ano, recorte in fontes.items():
    destino = f"{SCHEMA}.sinan_sp_{ano}"
    if spark.catalog.tableExists(destino):
        detalhe = spark.sql(f"DESCRIBE DETAIL {destino}").first()
        if spark.catalog.getTable(destino).tableType != "MANAGED" or detalhe["format"] != "delta":
            raise ValueError(f"{destino}: destino existente não é Delta gerenciado")
        if spark.table(destino).dtypes != recorte.dtypes:
            raise ValueError(f"{destino}: esquema incompatível")
    recorte.write.format("delta").mode("overwrite").saveAsTable(destino)
    print(f"{destino}: {spark.table(destino).count():,} notificações")
