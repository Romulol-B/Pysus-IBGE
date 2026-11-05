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

# MAGIC %run ./setup_pipeline

# COMMAND ----------

for parametro in ("execucao_id", "versao_codigo"):
    dbutils.widgets.text(parametro, "")
EXECUCAO, REVISAO = iniciar_execucao(dbutils.widgets.get("execucao_id"), dbutils.widgets.get("versao_codigo"))

from pyspark.sql import functions as F

SILVER = "dengue_sus.silver"
sidra = ler_tabela(f"{SILVER}.sidra_municipio_ano_sp").filter(F.col("ano").isin(2010, 2022))
sinan = ler_tabela(f"{SILVER}.sinan_municipio_ano_sp")
chave = ["ano", "codigo_municipio"]
exigir_chave(sidra, chave, "SIDRA")
exigir_anos(sidra, "SIDRA", 645)
exigir_chave(sinan, chave, "SINAN")
exigir_anos(sinan, "SINAN")
validar_contagens(sinan)
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

painel = sidra.join(
    sinan.select(*chave, *CONTAGENS).withColumn("tem_notificacao_registrada", F.lit(True)), chave, "left"
).withColumn("tem_notificacao_registrada", F.coalesce("tem_notificacao_registrada", F.lit(False)))
for nome in CONTAGENS:
    painel = painel.withColumn(nome, F.when(~F.col("tem_notificacao_registrada"), F.lit(0)).otherwise(F.col(nome)))
painel = painel.withColumn("situacao_registro_sinan", F.when(
    F.col("tem_notificacao_registrada"), "com_notificacao_registrada").otherwise("sem_notificacao_registrada"))
for nome in DESFECHOS:
    painel = painel.withColumn(f"taxa_{nome}_100k", F.col(nome) * 100000.0 / F.col("populacao"))
painel = (
    painel
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
    .select(*chave, "municipio", "populacao", *DESCONHECIDOS,
            "tem_notificacao_registrada", "situacao_registro_sinan",
            *[coluna for desfecho in DESFECHOS for coluna in (desfecho, f"taxa_{desfecho}_100k")],
            "agua_pct", "esgoto_pct", "lixo_pct")
)
if painel.filter(F.col("populacao").isNull() | (F.col("populacao") <= 0)).head(1):
    raise ValueError("População não positiva")
for indicador in ("agua_pct", "esgoto_pct", "lixo_pct"):
    if painel.filter(F.col(indicador).isNull() | ~F.col(indicador).between(0, 100.1)).head(1):
        raise ValueError(f"{indicador}: percentual ausente ou inválido")
if painel.count() != 1290:
    raise ValueError("O join alterou a grade municipal")
display(painel.groupBy("ano").agg(F.count("*").alias("municipios"),
                                    *[F.sum(c).alias(c) for c in DESFECHOS]))

# COMMAND ----------

destino = f"{SILVER}.dengue_sidra_municipio_ano_sp"
publicar({destino: painel}, overwriteSchema="true")
