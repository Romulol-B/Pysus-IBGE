# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SINAN — Silver municipal e mensal de SP
# MAGIC Município significa residência (`ID_MN_RESI`). Uma linha é uma
# MAGIC notificação, independentemente da classificação final. Códigos fora da
# MAGIC grade oficial e datas inválidas ficam auditáveis. A contagem anual usa
# MAGIC o ano do arquivo; a série mensal preserva o ano civil da notificação,
# MAGIC inclusive datas de anos adjacentes e mês desconhecido.
# MAGIC Casos graves harmonizam as classificações de cada época e exigem critério
# MAGIC laboratorial ou clínico-epidemiológico. Óbitos exigem dengue confirmada e
# MAGIC evolução `2` (óbito pelo agravo); não representam pessoas deduplicadas.
# MAGIC Flags indeterminadas são contadas separadamente em `*_desconhecidos`;
# MAGIC zero elegíveis não reclassifica esses registros como negativos.

# COMMAND ----------

# MAGIC %run ./setup_pipeline

# COMMAND ----------

for parametro in ("execucao_id", "versao_codigo"):
    dbutils.widgets.text(parametro, "")
EXECUCAO, REVISAO = iniciar_execucao(dbutils.widgets.get("execucao_id"), dbutils.widgets.get("versao_codigo"))

from pyspark.sql import functions as F

BRONZE = "dengue_sus.bronze"
SILVER = "dengue_sus.silver"
ANOS = (2010, 2022)
populacao = ler_tabela(f"{SILVER}.sidra_populacao_sp").filter(F.col("ano").isin(*ANOS))
grade = populacao.select("ano", "codigo_municipio")
exigir_chave(grade, ["ano", "codigo_municipio"], "População SP")
exigir_anos(grade, "População SP", 645)

def codigo(nome):
    valor = F.trim(F.col(nome).cast("string"))
    return F.when(F.length(valor) > 0, valor)

# COMMAND ----------

quadros = []
for ano in ANOS:
    origem = f"{BRONZE}.sinan_sp_{ano}"
    if not spark.catalog.tableExists(origem):
        raise ValueError(f"Execute primeiro a Bronze: {origem}")
    bruto = ler_tabela(origem)
    if not bruto.head(1):
        raise ValueError(f"{origem}: fonte vazia; não inferir ausência de notificações")
    obrigatorias = {"ID_MN_RESI", "DT_NOTIFIC", "CLASSI_FIN", "CRITERIO", "EVOLUCAO"}
    if obrigatorias - set(bruto.columns):
        raise ValueError(f"{origem}: campos SINAN ausentes: {sorted(obrigatorias - set(bruto.columns))}")
    municipio = codigo("ID_MN_RESI")
    classificacao = codigo("CLASSI_FIN")
    criterio = codigo("CRITERIO")
    evolucao = codigo("EVOLUCAO")
    confirmacoes = ["1", "2"]
    classes_dengue = ["1", "2", "3", "4"] if ano == 2010 else ["10", "11", "12"]
    classes_graves = ["2", "3", "4"] if ano == 2010 else ["11", "12"]
    quadro = bruto.select(
        F.lit(ano).alias("ano"), municipio.alias("codigo_original"),
        F.col("DT_NOTIFIC").cast("string").alias("data_original"),
        classificacao.alias("classificacao_original"),
        criterio.alias("criterio_original"), evolucao.alias("evolucao_original"),
        F.when(municipio.rlike(r"^\d{7}$"), F.substring(municipio, 1, 6))
        .when(municipio.rlike(r"^\d{6}$"), municipio).alias("codigo_municipio"),
        F.coalesce(
            F.to_date(F.expr("try_to_timestamp(trim(cast(`DT_NOTIFIC` as string)), 'yyyy-MM-dd')")),
            F.to_date(F.expr("try_to_timestamp(trim(cast(`DT_NOTIFIC` as string)), 'yyyyMMdd')")),
        ).alias("data_notificacao"),
        (classificacao.isin(classes_dengue) & criterio.isin(confirmacoes)).alias("caso_confirmado"),
        (classificacao.isin(classes_graves) & criterio.isin(confirmacoes)).alias("caso_grave"),
        (classificacao.isin(classes_dengue) & criterio.isin(confirmacoes)
         & (evolucao == "2")).alias("obito_dengue"),
    )
    quadros.append(quadro)

registros = quadros[0].unionByName(quadros[1])
com_grade = registros.join(
    grade.withColumn("codigo_valido", F.lit(True)), ["ano", "codigo_municipio"], "left"
)
auditoria = (
    com_grade.withColumn(
        "motivo",
        F.when(F.col("codigo_valido").isNull(), "codigo_fora_da_grade")
        .when(F.col("data_notificacao").isNull(), "data_invalida")
        .when(F.year("data_notificacao") != F.col("ano"), "data_fora_do_ano"),
    )
    .filter(F.col("motivo").isNotNull())
    .groupBy("ano", "motivo", "codigo_original", "data_original")
    .count().withColumnRenamed("count", "registros")
)
validos = com_grade.filter(F.col("codigo_valido").isNotNull())
flags = {"casos_confirmados": "caso_confirmado", "casos_graves": "caso_grave", "obitos_dengue": "obito_dengue"}
metricas = [F.count("*").alias("notificacoes")]
for nome, flag in flags.items():
    metricas.extend((F.count(F.when(F.col(flag), 1)).alias(nome),
                     F.count(F.when(F.col(flag).isNull(), 1)).alias(f"{nome}_desconhecidos")))
auditoria_desfechos = validos.groupBy(
    "ano", "classificacao_original", "criterio_original", "evolucao_original"
).agg(*metricas)
municipal = validos.groupBy("ano", "codigo_municipio").agg(*metricas)
mensal = (
    validos.withColumn("ano_calendario_notificacao", F.year("data_notificacao"))
    .withColumn("mes", F.month("data_notificacao"))
    .groupBy("ano", "ano_calendario_notificacao", "mes").agg(*metricas)
)
exigir_anos(municipal, "SINAN municipal")
mesmo_ano = mensal.filter(F.col("ano_calendario_notificacao") == F.col("ano"))
if {linha["ano"]: linha["meses"] for linha in mesmo_ano.groupBy("ano").agg(
    F.countDistinct("mes").alias("meses")
).collect()} != {2010: 12, 2022: 12}:
    raise ValueError("SINAN: esperados 12 meses com notificações em cada ano")
for metrica in CONTAGENS:
    anuais = {linha["ano"]: linha["total"] for linha in municipal.groupBy("ano").agg(
        F.sum(metrica).alias("total")).collect()}
    mensais = {linha["ano"]: linha["total"] for linha in mensal.groupBy("ano").agg(
        F.sum(metrica).alias("total")).collect()}
    if anuais != mensais:
        raise ValueError(f"SINAN: {metrica} não reconcilia entre ano-fonte e mês: {anuais} != {mensais}")
if municipal.filter((F.col("casos_graves") > F.col("casos_confirmados"))
                    | (F.col("obitos_dengue") > F.col("casos_confirmados"))).head(1):
    raise ValueError("SINAN: grave ou óbito excede casos confirmados")

display(com_grade.groupBy("ano").agg(
    F.count("*").alias("bronze"),
    F.sum(F.col("codigo_valido").isNotNull().cast("long")).alias("municipios_validos"),
))
display(auditoria.orderBy("ano", "motivo", "codigo_original"))
display(auditoria_desfechos.orderBy(
    "ano", "classificacao_original", "criterio_original", "evolucao_original"
))

# COMMAND ----------

saidas = {
    "sinan_municipio_ano_sp": municipal,
    "sinan_sp_mes": mensal,
    "sinan_auditoria_sp": auditoria,
    "sinan_desfechos_auditoria_sp": auditoria_desfechos,
}
publicar({f"{SILVER}.{nome}": quadro for nome, quadro in saidas.items()}, overwriteSchema="true")
