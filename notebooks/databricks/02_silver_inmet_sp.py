# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # INMET — Silver diária e mensal de SP
# MAGIC Dia meteorológico em UTC; estações disponíveis podem variar. Chuva é
# MAGIC atribuída ao início do intervalo horário, como no pipeline local.
# MAGIC As colunas de data/hora antigas e novas são complementares entre anos;
# MAGIC os valores válidos são combinados por linha antes da agregação.

# COMMAND ----------

# MAGIC %run ./setup_pipeline

# COMMAND ----------

for parametro in ("execucao_id", "versao_codigo"):
    dbutils.widgets.text(parametro, "")
EXECUCAO, REVISAO = iniciar_execucao(dbutils.widgets.get("execucao_id"), dbutils.widgets.get("versao_codigo"))

import re
import unicodedata

from pyspark.sql import functions as F

SILVER = "dengue_sus.silver"
bruto = ler_tabela("dengue_sus.bronze.inmet_sp")

def chave(nome):
    ascii_nome = "".join(c for c in unicodedata.normalize("NFKD", nome) if not unicodedata.combining(c))
    return re.sub(r"[^A-Z0-9]+", "_", ascii_nome.upper()).strip("_")

colunas = {chave(nome): nome for nome in bruto.columns}

def coluna(prefixo):
    candidatas = [nome for nome in colunas if nome.startswith(prefixo)]
    if len(candidatas) != 1:
        raise ValueError(f"INMET: esperada uma coluna {prefixo}; encontradas {candidatas}")
    return colunas[candidatas[0]]

# Preservar as duas grafias: HORA (UTC) e Hora UTC têm a mesma chave normalizada.
datas = [nome for nome in bruto.columns if chave(nome) in ("DATA_YYYY_MM_DD", "DATA")]
horas = [nome for nome in bruto.columns if chave(nome) in ("HORA_UTC", "HORA")]
if not {"UF", "CODIGO_WMO", "ANO_REFERENCIA"}.issubset(colunas) or not datas or not horas:
    raise ValueError("INMET: faltam UF, WMO, ano, data ou hora")
horas_brutas = [F.trim(F.col(f"`{nome}`")) for nome in horas]

def numero(nome, minimo, maximo):
    valor = F.expr(f"try_cast(replace(trim(`{nome}`), ',', '.') as double)")
    return F.when(valor.between(minimo, maximo), valor)

# COMMAND ----------

# A seleção de anos é feita antes de tipar a Bronze completa.
fonte = bruto.filter(F.trim(F.col("ANO_REFERENCIA")).isin("2010", "2011", "2022", "2023"))
display(fonte.select("ANO_REFERENCIA", *datas, *horas).limit(20))
dados = fonte.select(
    F.trim(F.col("ANO_REFERENCIA")).alias("ano_fonte"),
    F.upper(F.trim(F.col("CODIGO_WMO"))).alias("estacao"),
    F.coalesce(*[
        F.to_date(F.try_to_timestamp(F.trim(F.col(f"`{nome}`")), F.lit(formato)))
        for nome in datas for formato in ("yyyy-MM-dd", "yyyy/MM/dd", "dd/MM/yyyy")
    ]).alias("data"),
    F.coalesce(*[
        F.when(hora.rlike(r"^(\d{2})(?::?00)(?:\s+UTC)?$"),
               F.substring(hora, 1, 2).cast("int"))
        for hora in horas_brutas
    ]).alias("hora"),
    numero(coluna("TEMPERATURA_DO_AR_BULBO_SECO"), -20, 55).alias("temp_c"),
    numero(coluna("UMIDADE_RELATIVA_DO_AR"), 0, 100).alias("umidade_pct"),
    numero(coluna("PRECIPITACAO_TOTAL"), 0, 300).alias("precip_mm"),
)
# Manter anos-alvo inteiros e somente a leitura de fronteira dos anos seguintes.
dados = dados.filter(
    F.col("ano_fonte").isin("2010", "2022")
    | (F.col("data").isin("2011-01-01", "2023-01-01") & (F.col("hora") == 0))
).drop("ano_fonte")
if dados.filter(F.col("data").isNull() | F.col("hora").isNull()
                | ~F.col("hora").between(0, 23)
                | F.col("estacao").isNull() | ~F.col("estacao").rlike(r"^(?:[A-Z][0-9]{3}|[0-9]{5})$")).head(1):
    raise ValueError("INMET: data, hora ou estação inválida")
if dados.groupBy("estacao", "data", "hora").count().filter("count > 1").head(1):
    raise ValueError("INMET: observação WMO/data/hora duplicada")
display(dados.select("estacao", "data", "hora").limit(20))

temperatura = dados.groupBy("estacao", "data").agg(
    F.when(F.count("temp_c") >= 18, F.avg("temp_c")).alias("temp_c"),
    F.when(F.count("umidade_pct") >= 18, F.avg("umidade_pct")).alias("umidade_pct"),
)
chuva = (
    dados.withColumn("data", F.when(F.col("hora") == 0, F.date_sub("data", 1)).otherwise(F.col("data")))
    .groupBy("estacao", "data")
    .agg(F.when(F.count("precip_mm") == 24, F.sum("precip_mm")).alias("precip_mm"))
)
estacao_dia = temperatura.join(chuva, ["estacao", "data"], "full")
diario = (
    estacao_dia.filter(F.year("data").isin(2010, 2022))
    .groupBy("data")
    .agg(F.countDistinct("estacao").alias("n_estacoes_presentes"),
         F.count("temp_c").alias("n_estacoes_temp"), F.avg("temp_c").alias("temp_media_c"),
         F.count("umidade_pct").alias("n_estacoes_umidade"), F.avg("umidade_pct").alias("umidade_media_pct"),
         F.count("precip_mm").alias("n_estacoes_precip"), F.avg("precip_mm").alias("precip_media_estacoes_mm"),
         *[F.sort_array(F.collect_set(F.when(F.col(c).isNotNull(), F.col("estacao"))))
           .alias(f"estacoes_{c}") for c in ("temp_c", "umidade_pct", "precip_mm")])
    .withColumn("ano", F.year("data"))
    .select("ano", "data", "n_estacoes_presentes", "n_estacoes_temp", "temp_media_c",
            "n_estacoes_umidade", "umidade_media_pct", "n_estacoes_precip", "precip_media_estacoes_mm",
            "estacoes_temp_c", "estacoes_umidade_pct", "estacoes_precip_mm")
)

# COMMAND ----------

contagens = diario.groupBy("ano").count().collect()
if {linha["ano"]: linha["count"] for linha in contagens} != {2010: 365, 2022: 365}:
    raise ValueError(f"INMET: cobertura de calendário incompleta: {contagens}")
mensal = (
    diario.withColumn("mes", F.month("data"))
    .groupBy("ano", "mes")
    .agg(F.count("temp_media_c").alias("dias_temp"),
         F.count("umidade_media_pct").alias("dias_umidade"),
         F.count("precip_media_estacoes_mm").alias("dias_precip"),
         F.avg("n_estacoes_presentes").alias("estacoes_medias_presentes"),
         F.avg("temp_media_c").alias("temp_bruta_c"),
         F.avg("umidade_media_pct").alias("umidade_bruta_pct"),
         F.sum("precip_media_estacoes_mm").alias("precip_bruta_mm"))
    .withColumn("dias_mes", F.dayofmonth(F.last_day(F.make_date("ano", "mes", F.lit(1)))))
    .withColumn("temp_media_c", F.when(F.col("dias_temp") >= 0.8 * F.col("dias_mes"), F.col("temp_bruta_c")))
    .withColumn("umidade_media_pct", F.when(F.col("dias_umidade") >= 0.8 * F.col("dias_mes"), F.col("umidade_bruta_pct")))
    .withColumn("precip_media_estacoes_mm", F.when(F.col("dias_precip") == F.col("dias_mes"), F.col("precip_bruta_mm")))
    .drop("temp_bruta_c", "umidade_bruta_pct", "precip_bruta_mm")
)
if mensal.count() != 24:
    raise ValueError("INMET mensal deve ter 12 meses por ano")
display(mensal.orderBy("ano", "mes"))

# COMMAND ----------

saidas = {"inmet_sp_diario": diario, "inmet_sp_mes": mensal}
publicar({f"{SILVER}.{nome}": quadro for nome, quadro in saidas.items()}, overwriteSchema="true")
