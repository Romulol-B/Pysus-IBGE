# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SINAN — validar arquivos de dengue no S3
# MAGIC Piloto de 2010: lê o Parquet enviado para a Landing e confere residentes em SP.
# MAGIC A publicação da Bronze histórica será a próxima entrega.

# COMMAND ----------

from pyspark.sql import functions as F

ANO = 2010
ORIGEM = "s3://pcd2-dengue/pysus-ibge/landing/sinan/"
VOLUME = "dengue_sus.landing.sinan_s3"
ARQUIVO = f"/Volumes/dengue_sus/landing/sinan_s3/DENGBR{ANO % 100:02d}.parquet"

if not 2000 <= ANO <= 2023:
    raise ValueError("Escolha um ano entre 2000 e 2023; comece pelo piloto de 2010.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registrar a Landing externa
# MAGIC Usa a localização externa `dengue_landing_s3` já configurada para
# MAGIC `s3://pcd2-dengue/pysus-ibge/`. Um volume existente deve apontar para
# MAGIC este mesmo prefixo e ser externo.

# COMMAND ----------

spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {VOLUME} LOCATION '{ORIGEM}'")
volume = spark.sql(f"DESCRIBE VOLUME {VOLUME}").first()
if volume["volume_type"] != "EXTERNAL" or volume["storage_location"].rstrip("/") != ORIGEM.rstrip("/"):
    raise ValueError(f"{VOLUME} já existe com outro tipo ou origem; confira antes de prosseguir.")

arquivos = {item.name.rstrip("/"): item for item in dbutils.fs.ls(f"/Volumes/dengue_sus/landing/sinan_s3/")}
nome = ARQUIVO.rsplit("/", 1)[-1]
if nome not in arquivos or arquivos[nome].size == 0:
    raise FileNotFoundError(f"Envie {nome} para {ORIGEM} e confira o nome e tamanho do objeto.")
print(f"Arquivo encontrado: {ARQUIVO} ({arquivos[nome].size:,} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferir esquema e recorte de SP
# MAGIC `ID_MN_RESI` é o município de residência. O prefixo `35` seleciona SP
# MAGIC somente para conferência; o Parquet nacional permanece intacto no S3.
# MAGIC As contagens são de notificações, não de casos confirmados ou pessoas únicas.

# COMMAND ----------

dados = spark.read.parquet(ARQUIVO)
dados.printSchema()
if "ID_MN_RESI" not in dados.columns:
    raise ValueError("ID_MN_RESI ausente; não é possível selecionar SP por residência.")

municipio = F.trim(F.col("ID_MN_RESI"))
residente_sp = municipio.startswith("35")
resumo = dados.agg(
    F.count("*").alias("registros_brasil"),
    F.count(F.when(residente_sp, 1)).alias("registros_sp"),
    F.count(F.when(municipio.isNull() | (municipio == ""), 1)).alias("sem_municipio_residencia"),
)
contagens = resumo.first()
display(resumo)
if not contagens["registros_brasil"] or not contagens["registros_sp"]:
    raise ValueError("O arquivo precisa conter registros nacionais e residentes em SP.")

print(f"Ano: {ANO}; colunas: {len(dados.columns)}")
print(f"Brasil: {contagens['registros_brasil']:,}; residentes em SP: {contagens['registros_sp']:,}.")
print("Landing validada. A tabela Bronze ainda não foi publicada.")

# COMMAND ----------

display(dados.limit(15))