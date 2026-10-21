# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Validar a landing INMET no S3
# MAGIC Execute após configurar IAM, credencial e localização externa conforme
# MAGIC `docs/databricks/01_conectar_s3.md` e enviar o CSV sintético de duas linhas.
# MAGIC Use o compute serverless do Databricks com Unity Catalog. Sem dependências extras.
# MAGIC Esta etapa registra schema/volume e lê apenas o arquivo de validação.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

CATALOGO = "dengue_sus"
SCHEMA = f"{CATALOGO}.landing"  
VOLUME = f"{SCHEMA}.inmet"
ORIGEM = "s3://pcd2-dengue/pysus-ibge/landing/inmet/"
ARQUIVO = f"/Volumes/{CATALOGO}/landing/inmet/_validacao/conexao.csv"
INMET_FILE_NAME = f"INMET_dataset_agregado.csv"
ARQUIVO_INMET_BUCKET = f"/Volumes/{CATALOGO}/landing/inmet/{INMET_FILE_NAME}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Registrar o volume externo
# MAGIC A localização externa `dengue_landing_s3` deve cobrir a origem configurada.
# MAGIC Objetos existentes são preservados; uma origem diferente interrompe a execução.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {VOLUME} LOCATION '{ORIGEM}'")

volume = spark.sql(f"DESCRIBE VOLUME {VOLUME}").first()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Conferir a leitura
# MAGIC São esperadas exatamente as linhas `1;INMET` e `2;INMET`.
# MAGIC A leitura é limitada a três linhas para detectar registros extras sem coletar um arquivo grande.

# COMMAND ----------

dados = (
    spark.read.option("header", True)
    .option("sep", ";")
    .option("mode", "FAILFAST")
    .csv(ARQUIVO)
)
linhas = sorted((tuple(linha) for linha in dados.limit(3).collect()), key=repr)
if dados.columns != ["id", "fonte"] or linhas != [("1", "INMET"), ("2", "INMET")]:
    raise ValueError("CSV divergente: envie o conexao.csv exatamente como descrito no guia.")

display(dados.limit(2).orderBy("id"))
print("Conexão validada: 2 linhas lidas do S3 pelo volume externo.")

# COMMAND ----------

dados_reais = (
    spark.read.option('header',True)
               .option('sep', ';') 
               .option('mode','FAILFAST')
               .csv(ARQUIVO_INMET_BUCKET)
)
display(dados_reais.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reexecutar e informar o resultado
# MAGIC Execute **Run all** novamente. As mesmas duas linhas devem aparecer.
# MAGIC Informe o resultado ou a célula e mensagem de erro antes da carga do CSV completo.