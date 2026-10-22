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

# COMMAND ----------

from pyspark.sql import functions as F

BRONZE = "dengue_sus.bronze"
SILVER = "dengue_sus.silver"
ANOS = (2010, 2022)
populacao = spark.table(f"{SILVER}.sidra_populacao_sp").filter(F.col("ano").isin(*ANOS))
grade = populacao.select("ano", "codigo_municipio")
if grade.count() != 645 * len(ANOS) or grade.distinct().count() != grade.count():
    raise ValueError("Grade SIDRA de SP deve ter 645 códigos únicos em cada ano")

# COMMAND ----------

quadros = []
for ano in ANOS:
    origem = f"{BRONZE}.sinan_sp_{ano}"
    if not spark.catalog.tableExists(origem):
        raise ValueError(f"Execute primeiro a Bronze: {origem}")
    bruto = spark.table(origem)
    codigo = F.trim(F.col("ID_MN_RESI").cast("string"))
    quadro = bruto.select(
        F.lit(ano).alias("ano"), codigo.alias("codigo_original"),
        F.col("DT_NOTIFIC").cast("string").alias("data_original"),
        F.when(codigo.rlike(r"^\d{7}$"), F.substring(codigo, 1, 6))
        .when(codigo.rlike(r"^\d{6}$"), codigo).alias("codigo_municipio"),
        F.coalesce(
            F.to_date(F.expr("try_to_timestamp(trim(cast(`DT_NOTIFIC` as string)), 'yyyy-MM-dd')")),
            F.to_date(F.expr("try_to_timestamp(trim(cast(`DT_NOTIFIC` as string)), 'yyyyMMdd')")),
        ).alias("data_notificacao"),
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
municipal = validos.groupBy("ano", "codigo_municipio").count().withColumnRenamed("count", "notificacoes")
mensal = (
    validos.withColumn("ano_calendario_notificacao", F.year("data_notificacao"))
    .withColumn("mes", F.month("data_notificacao"))
    .groupBy("ano", "ano_calendario_notificacao", "mes")
    .count().withColumnRenamed("count", "notificacoes")
)
if municipal.groupBy("ano", "codigo_municipio").count().filter("count > 1").head(1):
    raise ValueError("SINAN municipal contém chave duplicada")
if mensal.groupBy("ano", "ano_calendario_notificacao", "mes").count().filter("count > 1").head(1):
    raise ValueError("SINAN mensal contém chave duplicada")
mesmo_ano = mensal.filter(F.col("ano_calendario_notificacao") == F.col("ano"))
if {linha["ano"]: linha["meses"] for linha in mesmo_ano.groupBy("ano").agg(
    F.countDistinct("mes").alias("meses")
).collect()} != {2010: 12, 2022: 12}:
    raise ValueError("SINAN: esperados 12 meses com notificações em cada ano")
anuais = {linha["ano"]: linha["total"] for linha in municipal.groupBy("ano").agg(
    F.sum("notificacoes").alias("total")
).collect()}
mensais = {linha["ano"]: linha["total"] for linha in mensal.groupBy("ano").agg(
    F.sum("notificacoes").alias("total")
).collect()}
if anuais != mensais:
    raise ValueError(f"SINAN: mês civil/desconhecido não reconcilia com ano-fonte: {anuais} != {mensais}")

display(com_grade.groupBy("ano").agg(
    F.count("*").alias("bronze"),
    F.sum(F.col("codigo_valido").isNotNull().cast("long")).alias("municipios_validos"),
))
display(auditoria.orderBy("ano", "motivo", "codigo_original"))

# COMMAND ----------

saidas = {
    "sinan_municipio_ano_sp": municipal,
    "sinan_sp_mes": mensal,
    "sinan_auditoria_sp": auditoria,
}
for nome, quadro in saidas.items():
    destino = f"{SILVER}.{nome}"
    if spark.catalog.tableExists(destino):
        detalhe = spark.sql(f"DESCRIBE DETAIL {destino}").first()
        if spark.catalog.getTable(destino).tableType != "MANAGED" or detalhe["format"] != "delta":
            raise ValueError(f"{destino}: destino existente não é Delta gerenciado")
        if spark.table(destino).dtypes != quadro.dtypes:
            raise ValueError(f"{destino}: esquema incompatível")
for nome, quadro in saidas.items():
    destino = f"{SILVER}.{nome}"
    quadro.write.format("delta").mode("overwrite").saveAsTable(destino)
    print(f"{destino}: {spark.table(destino).count():,} linhas")
