# Databricks notebook source
# MAGIC %md
# MAGIC # Funções comuns da carga de SP
# MAGIC Carregue com `%run ./setup_pipeline` e chame `iniciar_execucao` na tarefa.
# MAGIC Use o mesmo `execucao_id` e
# MAGIC `versao_codigo` em todas as tarefas. A revisão do código é declarada pelo
# MAGIC operador; as versões Delta e os checksums das entradas são verificados.

# COMMAND ----------

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F

ANOS = (2010, 2022)
DESFECHOS = ("notificacoes", "casos_confirmados", "casos_graves", "obitos_dengue")
DESCONHECIDOS = tuple(f"{nome}_desconhecidos" for nome in DESFECHOS[1:])
CONTAGENS = (*DESFECHOS, *DESCONHECIDOS)
VERSOES, FONTES_EXECUCAO = {}, {}


def iniciar_execucao(execucao_id, versao_codigo):
    global EXECUCAO, REVISAO
    EXECUCAO, REVISAO = execucao_id.strip(), versao_codigo.strip()
    if not EXECUCAO or not REVISAO or "{{" in EXECUCAO + REVISAO:
        raise ValueError("Preencha execucao_id e versao_codigo; use os mesmos valores na carga inteira.")
    VERSOES.clear()
    FONTES_EXECUCAO.clear()
    return EXECUCAO, REVISAO


def historico(nome):
    return spark.sql(f"DESCRIBE HISTORY {nome} LIMIT 1").first()


def validar_leituras():
    for nome, versao in VERSOES.items():
        if historico(nome)["version"] != versao:
            raise ValueError(f"{nome} mudou durante a carga; reexecute os consumidores.")


def ler_tabela(nome):
    nome = nome.replace("`", "")
    registro = historico(nome)
    meta = json.loads(registro["userMetadata"] or "{}")
    if meta.get("execucao_id") != EXECUCAO or meta.get("versao_codigo") != REVISAO:
        raise ValueError(f"{nome}: origem de outra execução/revisão ou sem metadados.")
    lote = {}
    for saida in meta["saidas"]:
        atual = historico(saida)
        outro = json.loads(atual["userMetadata"] or "{}")
        if outro.get("lote") != meta["lote"]:
            raise ValueError(f"Publicação incompleta: {saida}. Reexecute o produtor e seus consumidores.")
        lote[saida] = atual["version"]
    for origem, versao in {**meta["entradas"], **lote, nome: registro["version"]}.items():
        if origem in VERSOES and VERSOES[origem] != versao:
            raise ValueError(f"Versões incompatíveis de {origem} nos painéis.")
        VERSOES[origem] = versao
    FONTES_EXECUCAO.update(meta["fontes"])
    validar_leituras()
    return spark.read.option("versionAsOf", registro["version"]).table(nome)


def registrar_arquivo(caminho, registro=None):
    arquivo = Path(caminho)
    with arquivo.open("rb") as stream:
        sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
    if registro is not None and registro.get("sha256") != sha256:
        raise ValueError(f"Checksum divergente do manifesto: {arquivo}")
    if registro is None:
        print(f"{arquivo.name}: checksum registrado; origem sem manifesto verificado.")
    FONTES_EXECUCAO[caminho] = {
        **(registro or {}), "sha256": sha256, "bytes": arquivo.stat().st_size,
        "manifesto_verificado": registro is not None,
        "lido_em_utc": datetime.now(timezone.utc).isoformat(),
    }


def exigir_chave(quadro, chave, origem):
    if quadro.filter(F.expr(" OR ".join(f"`{c}` IS NULL" for c in chave))).head(1):
        raise ValueError(f"{origem}: chave nula")
    if quadro.groupBy(*chave).count().filter("count > 1").head(1):
        raise ValueError(f"{origem}: chave duplicada")


def exigir_anos(quadro, origem, linhas=None):
    contagens = {r["ano"]: r["count"] for r in quadro.groupBy("ano").count().collect()}
    if set(contagens) != set(ANOS) or (linhas is not None and any(n != linhas for n in contagens.values())):
        raise ValueError(f"{origem}: anos/contagens incompletos: {contagens}")


def validar_contagens(quadro):
    invalidas = [f"{c} IS NULL OR {c} < 0" for c in CONTAGENS]
    invalidas += [f"{c} + {c}_desconhecidos > notificacoes" for c in CONTAGENS[1:4]]
    if quadro.filter(F.expr(" OR ".join(invalidas))).head(1):
        raise ValueError("Contagens ausentes, negativas ou incompatíveis com as notificações")


def publicar(saidas, **opcoes):
    # Validar todos os destinos antes da primeira escrita; Delta é atômico por tabela.
    validar_leituras()
    for nome in saidas:
        if spark.catalog.tableExists(nome):
            if (spark.catalog.getTable(nome).tableType != "MANAGED"
                    or spark.sql(f"DESCRIBE DETAIL {nome}").first()["format"] != "delta"):
                raise ValueError(f"{nome}: destino deve ser Delta gerenciado")
    meta = {
        "execucao_id": EXECUCAO, "versao_codigo": REVISAO, "lote": uuid4().hex,
        "gerado_em_utc": datetime.now(timezone.utc).isoformat(),
        "saidas": sorted(saidas), "entradas": VERSOES, "fontes": FONTES_EXECUCAO,
    }
    for nome, quadro in saidas.items():
        (quadro.write.format("delta").mode("overwrite").options(**opcoes)
         .option("userMetadata", json.dumps(meta, ensure_ascii=False)).saveAsTable(nome))
    validar_leituras()
    print(f"Lote {meta['lote']} publicado: {', '.join(saidas)}")
