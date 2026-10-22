# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Dengue × SIDRA — painel municipal de SP
# MAGIC Água, esgoto e lixo são percentuais domiciliares. As categorias dos
# MAGIC Censos de 2010 e 2022 são diferentes: compare municípios dentro de cada
# MAGIC ano, não interprete a diferença entre coeficientes como mudança causal.

# COMMAND ----------

from pyspark.sql import functions as F

SILVER = "dengue_sus.silver"
sidra = spark.table(f"{SILVER}.sidra_municipio_ano_sp").filter(F.col("ano").isin(2010, 2022))
sinan = spark.table(f"{SILVER}.sinan_municipio_ano_sp")
chave = ["ano", "codigo_municipio"]
if sidra.count() != 1290 or sidra.select(*chave).distinct().count() != 1290:
    raise ValueError("SIDRA deve conter 645 municípios únicos em 2010 e 2022")
if sinan.select(*chave).distinct().count() != sinan.count():
    raise ValueError("SINAN municipal contém chave duplicada")
if sinan.join(sidra.select(*chave), chave, "left_anti").head(1):
    raise ValueError("SINAN contém municípios fora da grade SIDRA")

# COMMAND ----------

campos = {
    "c2010_pct_agua_rede_geral", "c2010_pct_banheiro_exclusivo_rede_ou_fossa_septica",
    "c2010_pct_sanitario_rede_ou_fossa_septica", "c2010_pct_lixo_servico_limpeza",
    "c2010_pct_lixo_cacamba_servico_limpeza", "c2022_pct_agua_rede_geral_principal",
    "c2022_pct_esgoto_rede_geral_ou_pluvial", "c2022_pct_esgoto_fossa_ligada_rede",
    "c2022_pct_esgoto_fossa_nao_ligada_rede", "c2022_pct_lixo_coletado_domicilio",
    "c2022_pct_lixo_cacamba",
}
if campos - set(sidra.columns):
    raise ValueError(f"Indicadores SIDRA ausentes: {sorted(campos - set(sidra.columns))}")

painel = (
    sidra.join(sinan.select(*chave, "notificacoes"), chave, "left")
    .withColumn("notificacoes", F.coalesce("notificacoes", F.lit(0)))
    .withColumn("taxa_notificacoes_100k", F.col("notificacoes") * 100000.0 / F.col("populacao"))
    .withColumn("agua_pct", F.when(F.col("ano") == 2010, F.col("c2010_pct_agua_rede_geral"))
                .otherwise(F.col("c2022_pct_agua_rede_geral_principal")))
    .withColumn("esgoto_pct", F.when(F.col("ano") == 2010,
                F.col("c2010_pct_banheiro_exclusivo_rede_ou_fossa_septica")
                + F.col("c2010_pct_sanitario_rede_ou_fossa_septica"))
                .otherwise(F.col("c2022_pct_esgoto_rede_geral_ou_pluvial")
                + F.col("c2022_pct_esgoto_fossa_ligada_rede")
                + F.col("c2022_pct_esgoto_fossa_nao_ligada_rede")))
    .withColumn("lixo_pct", F.when(F.col("ano") == 2010,
                F.col("c2010_pct_lixo_servico_limpeza")
                + F.col("c2010_pct_lixo_cacamba_servico_limpeza"))
                .otherwise(F.col("c2022_pct_lixo_coletado_domicilio")
                + F.col("c2022_pct_lixo_cacamba")))
    .select(*chave, "municipio", "populacao", "notificacoes", "taxa_notificacoes_100k",
            "agua_pct", "esgoto_pct", "lixo_pct")
)
if painel.filter(F.col("populacao") <= 0).head(1):
    raise ValueError("População não positiva")
for indicador in ("agua_pct", "esgoto_pct", "lixo_pct"):
    if painel.filter(F.col(indicador).isNull() | ~F.col(indicador).between(0, 100.1)).head(1):
        raise ValueError(f"{indicador}: percentual ausente ou inválido")
if painel.count() != 1290:
    raise ValueError("O join alterou a grade municipal")
display(painel.groupBy("ano").agg(F.count("*").alias("municipios"),
                                    F.sum("notificacoes").alias("notificacoes")))

# COMMAND ----------

destino = f"{SILVER}.dengue_sidra_municipio_ano_sp"
if spark.catalog.tableExists(destino):
    detalhe = spark.sql(f"DESCRIBE DETAIL {destino}").first()
    if spark.catalog.getTable(destino).tableType != "MANAGED" or detalhe["format"] != "delta":
        raise ValueError(f"{destino}: destino existente não é Delta gerenciado")
    if spark.table(destino).dtypes != painel.dtypes:
        raise ValueError(f"{destino}: esquema incompatível")
painel.write.format("delta").mode("overwrite").saveAsTable(destino)
print(f"{destino}: {spark.table(destino).count():,} linhas")
