# Databricks notebook source
# MAGIC %md
# MAGIC # Dengue × clima — painel mensal de SP
# MAGIC O clima representa a UF, não cada município. A taxa usa a população
# MAGIC anual dos 645 municípios do SIDRA como denominador em cada mês.

# COMMAND ----------

# MAGIC %run ./setup_pipeline

# COMMAND ----------

for parametro in ("execucao_id", "versao_codigo"):
    dbutils.widgets.text(parametro, "")
EXECUCAO, REVISAO = iniciar_execucao(dbutils.widgets.get("execucao_id"), dbutils.widgets.get("versao_codigo"))

from pyspark.sql import functions as F

SILVER = "dengue_sus.silver"
clima = ler_tabela(f"{SILVER}.inmet_sp_mes")
sinan = ler_tabela(f"{SILVER}.sinan_sp_mes").filter(
    F.col("ano_calendario_notificacao") == F.col("ano")
).drop("ano_calendario_notificacao")
populacao = ler_tabela(f"{SILVER}.sidra_populacao_sp").filter(F.col("ano").isin(*ANOS))
exigir_chave(populacao, ["ano", "codigo_municipio"], "População")
exigir_anos(populacao, "População", 645)
if populacao.filter(F.col("populacao").isNull() | (F.col("populacao") <= 0)).head(1):
    raise ValueError("População anual de SP inválida")
populacao = populacao.groupBy("ano").agg(F.sum("populacao").alias("populacao"))
for nome, quadro in (("INMET", clima), ("SINAN", sinan)):
    exigir_chave(quadro, ["ano", "mes"], nome)
    exigir_anos(quadro, nome, 12)
    if quadro.filter(~F.col("mes").between(1, 12)).head(1):
        raise ValueError(f"{nome}: mês fora do calendário")
validar_contagens(sinan)

painel = (
    clima.join(sinan, ["ano", "mes"])
    .join(populacao.select("ano", "populacao"), "ano")
)
for desfecho in DESFECHOS:
    painel = painel.withColumn(f"taxa_{desfecho}_100k", F.col(desfecho) * 100000.0 / F.col("populacao"))
painel = (
    painel.select("ano", "mes", "populacao", *DESCONHECIDOS,
            *[coluna for desfecho in DESFECHOS for coluna in (desfecho, f"taxa_{desfecho}_100k")],
            "precip_media_estacoes_mm", "temp_media_c", "umidade_media_pct",
            "dias_precip", "dias_temp", "dias_umidade", "dias_mes", "estacoes_medias_presentes")
)
if painel.count() != 24:
    raise ValueError("O join climático alterou os 24 meses esperados")
display(painel.orderBy("ano", "mes"))

# COMMAND ----------

destino = f"{SILVER}.dengue_clima_sp_mes"
publicar({destino: painel}, overwriteSchema="true")
