# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SINAN — Bronze de residentes em SP
# MAGIC Execute depois de `00_landing_validar_sinan`, com `DENGBR10.parquet` e
# MAGIC `DENGBR22.parquet` na Landing. Cada ano é publicado separadamente para
# MAGIC preservar o esquema original, inclusive diferenças entre arquivos.

# COMMAND ----------

# MAGIC %run ./setup_pipeline

# COMMAND ----------

for parametro in ("execucao_id", "versao_codigo"):
    dbutils.widgets.text(parametro, "")
EXECUCAO, REVISAO = iniciar_execucao(dbutils.widgets.get("execucao_id"), dbutils.widgets.get("versao_codigo"))

from pyspark.sql import functions as F

ANOS = (2010, 2022)
PASTA = "/Volumes/dengue_sus/landing/sinan_s3"
SCHEMA = "dengue_sus.bronze"

# COMMAND ----------

manifesto = Path(PASTA) / "manifesto_download.json"
registros = {r["ano"]: r for r in json.loads(manifesto.read_text())} if manifesto.exists() else {}
fontes = {}
for ano in ANOS:
    caminho = f"{PASTA}/DENGBR{ano % 100:02d}.parquet"
    if not any(item.name.rstrip("/") == caminho.rsplit("/", 1)[-1] for item in dbutils.fs.ls(PASTA)):
        raise FileNotFoundError(caminho)
    if manifesto.exists() and ano not in registros:
        raise ValueError(f"Manifesto SINAN sem o ano {ano}")
    registrar_arquivo(caminho, registros.get(ano))
    dados = spark.read.parquet(caminho)
    if ano in registros:
        registro = registros[ano]
        if (registro.get("origem") not in ("DADOSGOV", "FTP")
                or registro.get("arquivo") != Path(caminho).name
                or registro.get("registros") != dados.count()):
            raise ValueError(f"{caminho}: origem, arquivo ou contagem divergem do manifesto")
    if "ID_MN_RESI" not in dados.columns or "DT_NOTIFIC" not in dados.columns:
        raise ValueError(f"{caminho}: faltam ID_MN_RESI ou DT_NOTIFIC")
    residente = F.trim(F.col("ID_MN_RESI").cast("string"))
    recorte = dados.filter(residente.startswith("35"))
    if not recorte.head(1):
        raise ValueError(f"{caminho}: sem residentes de SP")
    fontes[ano] = recorte
if len({r["origem"] for ano, r in registros.items() if ano in ANOS}) > 1:
    raise ValueError("Os anos SINAN pertencem a coortes distintas")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
publicar({f"{SCHEMA}.sinan_sp_{ano}": quadro for ano, quadro in fontes.items()})
