# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # MVP — correlações descritivas de dengue em SP
# MAGIC Execute após os notebooks Bronze e Silver de SINAN, SIDRA e INMET. Correlação ecológica não demonstra
# MAGIC causalidade. Saneamento: municípios no mesmo Censo. Clima: meses da UF,
# MAGIC com 0 ou 1 mês de antecedência; os dois grãos não são misturados.

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

SILVER = "dengue_sus.silver"
municipal = spark.table(f"{SILVER}.dengue_sidra_municipio_ano_sp").toPandas()
mensal = spark.table(f"{SILVER}.dengue_clima_sp_mes").toPandas()
if len(municipal) != 1290 or len(mensal) != 24:
    raise ValueError("Painéis Silver incompletos: esperados 1.290 municípios/ano e 24 meses")
if municipal.duplicated(["ano", "codigo_municipio"]).any() or mensal.duplicated(["ano", "mes"]).any():
    raise ValueError("Chave duplicada em um painel")

DESFECHOS = ("notificacoes", "taxa_notificacoes_100k")

def correlacoes(quadro, variaveis, analise, defasagem=0):
    resultados = []
    for ano, grupo in quadro.groupby("ano"):
        if analise == "clima":
            grupo = grupo.sort_values("mes").copy()
            for variavel in variaveis:
                grupo[variavel] = grupo[variavel].shift(defasagem)
            # A mesma amostra de meses permite comparar os três coeficientes.
            grupo = grupo.dropna(subset=[*variaveis, *DESFECHOS])
        for variavel in variaveis:
            for desfecho in DESFECHOS:
                pares = grupo[[variavel, desfecho]].dropna()
                n = len(pares)
                suficiente = n >= 10 and pares[variavel].nunique() > 1 and pares[desfecho].nunique() > 1
                for metodo in ("pearson", "spearman"):
                    resultados.append({
                        "analise": analise, "ano": int(ano), "variavel": variavel,
                        "desfecho": desfecho, "defasagem_meses": defasagem,
                        "metodo": metodo, "n": n,
                        "coeficiente": pares[variavel].corr(pares[desfecho], method=metodo) if suficiente else None,
                    })
    return resultados

resultados = correlacoes(municipal, ("agua_pct", "esgoto_pct", "lixo_pct"), "municipal")
resultados += correlacoes(mensal, ("precip_media_estacoes_mm", "temp_media_c", "umidade_media_pct"), "clima")
resultados += correlacoes(mensal, ("precip_media_estacoes_mm", "temp_media_c", "umidade_media_pct"), "clima", 1)
tabela = pd.DataFrame(resultados).sort_values(
    ["analise", "ano", "variavel", "desfecho", "defasagem_meses", "metodo"]
)
display(tabela)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparação municipal
# MAGIC Os três percentuais são construídos com categorias específicas de cada
# MAGIC Censo. Cada painel abaixo representa apenas municípios daquele ano.

# COMMAND ----------

fig, eixos = plt.subplots(2, 3, figsize=(13, 7), sharey="row")
rotulos = {"agua_pct": "Água (%)", "esgoto_pct": "Esgoto (%)", "lixo_pct": "Lixo (%)"}
for linha, ano in enumerate((2010, 2022)):
    dados_ano = municipal[municipal["ano"] == ano]
    for coluna, (variavel, rotulo) in enumerate(rotulos.items()):
        eixo = eixos[linha, coluna]
        eixo.scatter(dados_ano[variavel], dados_ano["taxa_notificacoes_100k"], s=12, alpha=0.35)
        eixo.set_title(f"{ano}: {rotulo}")
        eixo.set_xlabel(rotulo)
        if coluna == 0:
            eixo.set_ylabel("Notificações por 100 mil")
fig.suptitle("Saneamento e taxa de notificações — municípios de SP")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparação mensal
# MAGIC As taxas mensais usam a mesma população anual em cada ano. Assim,
# MAGIC dentro de um ano, sua correlação com o clima é matematicamente igual à
# MAGIC correlação da contagem mensal com o clima. Cada combinação de ano e
# MAGIC defasagem usa os mesmos meses completos nas três variáveis climáticas.

# COMMAND ----------

fig, eixos = plt.subplots(2, 1, figsize=(11, 6), sharex=True)
for ano in (2010, 2022):
    dados_ano = mensal[mensal["ano"] == ano].sort_values("mes")
    eixos[0].plot(dados_ano["mes"], dados_ano["notificacoes"], marker="o", label=str(ano))
    eixos[1].plot(dados_ano["mes"], dados_ano["precip_media_estacoes_mm"], marker="o", label=str(ano))
eixos[0].set_ylabel("Notificações")
eixos[1].set_ylabel("Chuva mensal (mm)")
eixos[1].set_xlabel("Mês")
eixos[1].set_xticks(range(1, 13))
for eixo in eixos:
    eixo.legend(title="Ano")
    eixo.grid(alpha=0.25)
fig.suptitle("Notificações e chuva em SP — meses de 2010 e 2022")
fig.tight_layout()
plt.show()
