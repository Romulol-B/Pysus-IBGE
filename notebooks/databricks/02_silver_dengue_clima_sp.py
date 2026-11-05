# Databricks notebook source
# MAGIC %md
# MAGIC # Dengue × clima — painel mensal de SP
# MAGIC O clima representa a UF, não cada município. A taxa usa a população
# MAGIC anual dos 645 municípios do SIDRA como denominador em cada mês.

# COMMAND ----------

from pyspark.sql import functions as F

SILVER = "dengue_sus.silver"
clima = spark.table(f"{SILVER}.inmet_sp_mes")
sinan = spark.table(f"{SILVER}.sinan_sp_mes").filter(
    F.col("ano_calendario_notificacao") == F.col("ano")
).drop("ano_calendario_notificacao")
populacao = (
    spark.table(f"{SILVER}.sidra_populacao_sp")
    .filter(F.col("ano").isin(2010, 2022))
    .groupBy("ano").agg(F.count("*").alias("municipios"), F.sum("populacao").alias("populacao"))
)
if populacao.filter((F.col("municipios") != 645) | (F.col("populacao") <= 0)).head(1):
    raise ValueError("População anual de SP incompleta")
for nome, quadro in (("INMET", clima), ("SINAN", sinan)):
    if quadro.groupBy("ano", "mes").count().filter("count > 1").head(1):
        raise ValueError(f"{nome}: mês duplicado")
if clima.count() != 24 or clima.groupBy("ano").count().filter("count != 12").head(1):
    raise ValueError("INMET: esperados 12 meses em cada ano")
if sinan.join(clima.select("ano", "mes"), ["ano", "mes"], "left_anti").head(1):
    raise ValueError("SINAN: mês fora da grade climática")

DESFECHOS = ("notificacoes", "casos_confirmados", "casos_graves", "obitos_dengue")
painel = (
    clima.join(sinan, ["ano", "mes"], "left")
    .join(populacao.select("ano", "populacao"), "ano")
)
for desfecho in DESFECHOS:
    painel = (painel.withColumn(desfecho, F.coalesce(desfecho, F.lit(0)))
              .withColumn(f"taxa_{desfecho}_100k", F.col(desfecho) * 100000.0 / F.col("populacao")))
painel = (
    painel.select("ano", "mes", "populacao",
            *[coluna for desfecho in DESFECHOS for coluna in (desfecho, f"taxa_{desfecho}_100k")],
            "precip_media_estacoes_mm", "temp_media_c", "umidade_media_pct",
            "dias_precip", "dias_temp", "dias_umidade", "dias_mes", "estacoes_medias_presentes")
)
if painel.count() != 24:
    raise ValueError("O join climático alterou os 24 meses esperados")
display(painel.orderBy("ano", "mes"))

# COMMAND ----------

destino = f"{SILVER}.dengue_clima_sp_mes"
if spark.catalog.tableExists(destino):
    detalhe = spark.sql(f"DESCRIBE DETAIL {destino}").first()
    if spark.catalog.getTable(destino).tableType != "MANAGED" or detalhe["format"] != "delta":
        raise ValueError(f"{destino}: destino existente não é Delta gerenciado")
painel.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino)
print(f"{destino}: {spark.table(destino).count():,} meses")

