# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Correlações geográficas — SP, 2010 × 2022
# MAGIC Execute após `02_silver_dengue_sidra_sp`. Compara autocorrelação das taxas
# MAGIC municipais e associação com saneamento dos municípios vizinhos.
# MAGIC Usa a malha IBGE 2022 como referência fixa nos dois anos e KNN com oito
# MAGIC vizinhos, seguindo o método já existente no projeto. Não cria tabelas.
# MAGIC Os resultados serão calculados na execução; não há conclusões antecipadas.

# COMMAND ----------

# MAGIC %pip install geopandas==1.1.4 libpysal==4.15.0 esda==2.10.0 statsmodels==0.14.6 requests==2.34.2 matplotlib==3.11.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./setup_pipeline

# COMMAND ----------

for parametro in ("execucao_id", "versao_codigo"):
    dbutils.widgets.text(parametro, "")
EXECUCAO, REVISAO = iniciar_execucao(dbutils.widgets.get("execucao_id"), dbutils.widgets.get("versao_codigo"))

import hashlib
from importlib.metadata import version

import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np
import pandas as pd
import requests
from esda import Moran, Moran_BV, Moran_Local
from libpysal.weights import KNN
from statsmodels.stats.multitest import multipletests
from pyspark.sql import functions as F

ANOS = (2010, 2022)
K = 8
PERMUTACOES = 9999
SEMENTE = 42
ALFA = 0.05
TABELA = "dengue_sus.silver.dengue_sidra_municipio_ano_sp"
DESFECHOS = {
    "notificacoes": "Notificações", "casos_graves": "Casos graves",
    "obitos_dengue": "Óbitos por dengue",
}
SANEAMENTO = {"agua_pct": "Água", "esgoto_pct": "Esgoto", "lixo_pct": "Lixo"}
URL_MALHA = (
    "https://servicodados.ibge.gov.br/api/v3/malhas/estados/35"
    "?formato=application%2Fvnd.geo%2Bjson&qualidade=minima&intrarregiao=municipio&periodo=2022"
)
plt.rcParams.update({"figure.dpi": 120, "font.size": 10})
display(pd.DataFrame({"pacote": ["geopandas", "libpysal", "esda"],
                      "versao": [version(p) for p in ("geopandas", "libpysal", "esda")]}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fontes e alinhamento geográfico
# MAGIC As contagens seguem o ano-fonte SINAN e as definições da Silver, incluindo
# MAGIC a mudança de classificação de gravidade. As taxas usam o Censo do respectivo
# MAGIC ano. A mesma ordem de municípios e os mesmos pesos são usados nos dois anos.
# MAGIC A malha fixa não reconstrói os limites municipais históricos de 2010.

# COMMAND ----------

dados = ler_tabela(TABELA).filter(F.col("ano").isin(*ANOS)).limit(1291).toPandas()
dados["codigo_municipio"] = dados["codigo_municipio"].astype(str).str.strip()
if len(dados) != 1290 or dados.duplicated(["ano", "codigo_municipio"]).any():
    raise ValueError("Esperados 645 municípios únicos por ano.")
if dados.groupby("ano").size().to_dict() != {2010: 645, 2022: 645}:
    raise ValueError("Cobertura anual incompleta.")
codigos = sorted(dados.loc[dados.ano == 2010, "codigo_municipio"])
if codigos != sorted(dados.loc[dados.ano == 2022, "codigo_municipio"]):
    raise ValueError("As grades municipais dos anos diferem.")
colunas = ["populacao", *DESFECHOS, *SANEAMENTO]
if not np.isfinite(dados[colunas].to_numpy(dtype=float)).all() or (dados.populacao <= 0).any():
    raise ValueError("Valores ausentes, infinitos ou população inválida.")
for desfecho in DESFECHOS:
    if (dados[desfecho] < 0).any():
        raise ValueError(f"Contagem negativa: {desfecho}")
    dados[f"taxa_{desfecho}_100k"] = dados[desfecho] / dados.populacao * 100000

resposta = requests.get(URL_MALHA, timeout=180)
resposta.raise_for_status()
malha = gpd.GeoDataFrame.from_features(resposta.json()["features"], crs="EPSG:4674")
malha["codigo_municipio"] = malha["codarea"].astype(str).str[:6]
malha = malha[["codigo_municipio", "geometry"]]
if malha.codigo_municipio.duplicated().any() or set(malha.codigo_municipio) != set(codigos):
    raise ValueError("A malha IBGE não corresponde à grade dos 645 municípios.")
malha = malha.set_index("codigo_municipio").loc[codigos].reset_index()
if malha.geometry.is_empty.any() or not malha.geometry.is_valid.all():
    raise ValueError("Geometrias vazias ou inválidas na malha.")
centros = malha.to_crs("EPSG:31983").geometry.centroid
pesos = KNN.from_array(np.column_stack([centros.x, centros.y]), k=K, ids=codigos)
pesos.transform = "r"
print({"fonte": TABELA, "malha": URL_MALHA,
       "sha256_malha": hashlib.sha256(resposta.content).hexdigest(),
       "municipios": pesos.n, "componentes": pesos.n_components, "ilhas": pesos.islands,
       "k": K, "permutacoes": PERMUTACOES, "semente": SEMENTE})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Moran global, bivariado e LISA
# MAGIC Moran global mede semelhança entre taxas de municípios vizinhos.
# MAGIC Moran bivariado relaciona **taxa local (x)** com **saneamento dos vizinhos (Wy)**;
# MAGIC não equivale a Pearson nem é simétrico. KNN é direcionado e padronizado por linha.
# MAGIC Valores constantes não são testados. Os pseudo-p-valores de permutação
# MAGIC `p_sim` seguem a convenção do PySAL. No LISA, `alternative="directed"`
# MAGIC explicita a hipótese direcional usada por padrão na versão fixada. Aplicamos Benjamini–Hochberg aos testes
# MAGIC globais em conjunto e aos 645 testes locais de cada combinação ano/desfecho.
# MAGIC Referências: [Moran BV](https://pysal.org/esda/stable/generated/esda.Moran_BV.html)
# MAGIC e [Moran local](https://pysal.org/esda/stable/generated/esda.Moran_Local.html).

# COMMAND ----------

globais, locais = [], []
for ano in ANOS:
    painel = dados.loc[dados.ano == ano].set_index("codigo_municipio").loc[codigos]
    for desfecho in DESFECHOS:
        x = painel[f"taxa_{desfecho}_100k"].to_numpy(dtype=float)
        for variavel in [None, *SANEAMENTO]:
            y = painel[variavel].to_numpy(dtype=float) if variavel else x
            np.random.seed(SEMENTE)
            valido = np.ptp(x) > 0 and np.ptp(y) > 0
            teste = (Moran_BV(x, y, pesos, permutations=PERMUTACOES) if variavel else
                     Moran(x, pesos, permutations=PERMUTACOES)) if valido else None
            globais.append({"ano": ano, "desfecho": desfecho,
                            "vizinhanca": variavel or "mesmo_desfecho",
                            "I": teste.I if valido else np.nan,
                            "p_sim": teste.p_sim if valido else np.nan})
        local = painel[["municipio", "populacao", desfecho]].copy()
        local["ano"], local["desfecho"] = ano, desfecho
        local["taxa_100k"] = x
        local["I_local"], local["p_sim"], local["p_fdr"] = np.nan, np.nan, np.nan
        local["cluster"] = "Sem variância"
        if np.ptp(x) > 0:
            teste = Moran_Local(x, pesos, permutations=PERMUTACOES, seed=SEMENTE,
                                n_jobs=1, keep_simulations=False, alternative="directed")
            local["I_local"], local["p_sim"] = teste.Is, teste.p_sim
            local["p_fdr"] = multipletests(teste.p_sim, method="fdr_bh")[1]
            local["cluster"] = np.where(local.p_fdr <= ALFA,
                pd.Series(teste.q).map({1: "Alto–Alto", 2: "Baixo–Alto", 3: "Baixo–Baixo", 4: "Alto–Baixo"}),
                "Não significativo")
        locais.append(local.reset_index())
globais = pd.DataFrame(globais)
globais["p_fdr"] = np.nan
validos = globais.p_sim.notna()
globais.loc[validos, "p_fdr"] = multipletests(globais.loc[validos, "p_sim"], method="fdr_bh")[1]
locais = pd.concat(locais, ignore_index=True)
display(globais)
comparacao = globais.pivot(index=["desfecho", "vizinhanca"], columns="ano", values="I")
comparacao["diferenca_descritiva"] = comparacao[2022] - comparacao[2010]
display(comparacao.reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparação dos coeficientes
# MAGIC A diferença de I é descritiva: não constitui teste da mudança entre anos.
# MAGIC As definições de saneamento dos Censos diferem; diferenças no Moran bivariado
# MAGIC não isolam uma mudança epidemiológica ou sanitária.

# COMMAND ----------

fig, eixos = plt.subplots(1, 3, figsize=(15, 5), sharey=True)
rotulos = ["mesmo_desfecho", *SANEAMENTO]
for eixo, (desfecho, titulo) in zip(eixos, DESFECHOS.items()):
    for ano, marcador in zip(ANOS, ("o", "s")):
        fatia = globais[(globais.ano == ano) & (globais.desfecho == desfecho)].set_index("vizinhanca").reindex(rotulos)
        eixo.plot(range(4), fatia.I, marker=marcador, label=str(ano))
    eixo.set_xticks(range(4), ["Global", "Água", "Esgoto", "Lixo"], rotation=25)
    eixo.axhline(0, color="gray", linewidth=0.7)
    eixo.set_title(titulo)
    eixo.legend()
eixos[0].set_ylabel("I de Moran")
fig.suptitle("SP — associação espacial das taxas por 100 mil, 2010 × 2022")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mapas LISA e transições municipais
# MAGIC Alto/Baixo é relativo à média do respectivo ano. Os mapas mostram apenas
# MAGIC clusters com pseudo-p ajustado ≤ 0,05. Ausência de significância não prova
# MAGIC ausência de associação. As transições são descritivas, sem teste longitudinal.

# COMMAND ----------

cores = {"Não significativo": "#dedede", "Alto–Alto": "#b2182b", "Baixo–Baixo": "#2166ac",
         "Baixo–Alto": "#92c5de", "Alto–Baixo": "#ef8a62", "Sem variância": "#ffffff"}
for desfecho, titulo in DESFECHOS.items():
    fig, eixos = plt.subplots(1, 2, figsize=(13, 6))
    for eixo, ano in zip(eixos, ANOS):
        fatia = locais[(locais.ano == ano) & (locais.desfecho == desfecho)]
        mapa = malha.merge(fatia, on="codigo_municipio", validate="one_to_one")
        mapa.plot(color=mapa.cluster.map(cores), edgecolor="white", linewidth=0.15, ax=eixo)
        eixo.set_title(f"{titulo} — {ano}")
        eixo.set_axis_off()
    fig.legend(handles=[Patch(facecolor=c, label=r) for r, c in cores.items()],
               loc="lower center", ncol=3)
    fig.tight_layout(rect=[0, 0.1, 1, 1])
    plt.show()
    transicoes = locais[locais.desfecho == desfecho].pivot(index="codigo_municipio", columns="ano", values="cluster")
    display(pd.crosstab(transicoes[2010], transicoes[2022]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limites de interpretação
# MAGIC Taxas de óbitos e casos graves são instáveis em populações pequenas.
# MAGIC A análise usa taxas brutas; não aplica suavização nem ajuste por idade.
# MAGIC KNN com k=8 é uma escolha de vizinhança, não uma ligação epidemiológica observada.
# MAGIC As inferências locais são exploratórias e dependem dessa escolha e da malha.
# MAGIC As três tabelas `globais`, `comparacao` e `locais` ficam disponíveis na sessão.
# MAGIC Clima não entra na análise municipal porque a Silver climática tem apenas
# MAGIC um valor para toda a UF por mês. Correlação espacial não demonstra causalidade.

# COMMAND ----------

validar_leituras()
