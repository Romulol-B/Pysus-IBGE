"""Células de visualização, preservadas ao regenerar os notebooks analíticos.

O desenho dos gráficos vem da versão anterior às correções; os dados e os
rótulos continuam usando os conceitos corrigidos. Cada gráfico tem sua célula.
"""


def sinan_visualizations(content, state=None):
    content = list(content)
    seasonal = content[5][1]
    seasonal = seasonal[:seasonal.index('px.line(monthly')]
    if 'undated_count =' not in seasonal:
        seasonal = '''undated_count = raw.filter(pl.col("data_notificacao").is_null()).select(pl.len()).collect().item()
print("Datas desconhecidas: mantidas no ano-fonte, excluídas dos gráficos de calendário:", undated_count)
''' + seasonal
    # Inclui também as datas adjacentes aos anos-fonte, sem perder notificações.
    seasonal = seasonal.replace('daily = calendar.join(daily,', 'daily = calendar.join(daily.filter(pl.col("Dia").is_not_null()),').replace('how="left", validate="1:1"', 'how="full", coalesce=True, validate="1:1"')
    seasonal = seasonal.replace('display(monthly)', 'monthly = monthly.filter(pl.col("ano").is_in(years))\ndisplay(monthly)')
    seasonal += '''
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
df_notificacoes = daily.rename({"notificacoes": "Notificações"}).to_pandas()
df_notificacoes["Dia"] = pd.to_datetime(df_notificacoes["Dia"])
df_sazonalidade = monthly.rename({"ano": "Ano", "mes": "Mês", "media_por_dia_calendario": "Notificações"}).to_pandas()
'''
    charts = [("code", seasonal), *SINAN_TIME_CHARTS]
    categories = []
    for column, label in [("status_classificacao", "Classificação final"), ("sexo", "Sexo"), ("raca", "Raça/cor"), ("evolucao", "Evolução")]:
        categories.append(("code", f'''
summary = raw.group_by("{column}").len(name="Contagem").sort("Contagem", descending=True).collect().to_pandas()
summary["{column}"] = summary["{column}"].fillna("Não informado")
fig = px.bar(summary, x="{column}", y="Contagem", text="Contagem", color="{column}",
             title="Notificações por {label.lower()} — anos-fonte disponíveis",
             labels={{"Contagem": "Número de notificações", "{column}": "{label}"}})
fig.update_traces(textposition="outside")
fig.show()
'''))
    # A tabela anual e as auditorias existentes são mantidas.
    annual = content[9][1]
    old_chart = next(line for line in annual.splitlines() if line.startswith('px.bar('))
    annual = annual.replace(old_chart, '')
    spatial = [("markdown", "## Distribuição espacial e rankings\n\nContagens acumuladas usam os anos-fonte presentes; taxas usam somente o ano selecionado e sua população. Os pontos são centroides da malha IBGE de 2022, apenas como localização de referência, sem harmonização de territórios históricos. Municípios sem correspondência são exibidos na auditoria e não entram nos mapas. Os rankings não dependem da cobertura da malha."),
               ("code", SINAN_MAP_DATA)]
    if state is None:
        spatial.append(("code", SINAN_STATE_CHART))
    spatial.extend(SINAN_MAP_CHARTS)
    seasonal_note = content[4][1] + "\n\nO gráfico diário conserva datas adjacentes aos anos-fonte. A comparação mensal se limita aos anos-fonte presentes; anos adjacentes apenas parcialmente observados não entram nessa comparação. Datas ilegíveis não entram em nenhum gráfico de calendário."
    return content[:4] + [("markdown", seasonal_note)] + charts + [content[6]] + categories + [content[8], ("code", annual)] + spatial + [content[10]]


def integration_visualizations(content):
    content = list(content)
    content[4] = ("code", content[4][1].replace('opacity=0.45)', 'opacity=0.45, title="Esgotamento e taxa de casos confirmados — municípios, 2022")'))
    return content[:3] + INTEGRATION_CHARTS + content[3:]


def inmet_visualizations(content):
    # O ciclo horário original é substituído pela versão com hora UTC correta.
    return content[:3] + INMET_CHARTS[:3] + content[3:6] + INMET_CHARTS[3:] + content[7:]


def climate_visualizations(content):
    # Mantém as correlações dentro de UF e o denominador residente corrigido.
    return content[:2] + CLIMATE_CHARTS[:3] + content[3:7] + CLIMATE_CHARTS[3:] + content[7:]


SINAN_MAP_DATA = '''
import geopandas as gpd
from utils.spatial_analysis import ensure_boundary_cache, _municipality_code_column, _normalize_municipality_codes

cumulative = panel.group_by("codigo_municipio").agg(pl.col("notificacoes").sum())
names = panel.filter(pl.col("municipio").is_not_null()).sort("ano").unique("codigo_municipio", keep="last").select("codigo_municipio", "municipio")
cumulative = cumulative.join(names, on="codigo_municipio", how="left", validate="1:1").with_columns(pl.col("municipio").fill_null(pl.col("codigo_municipio")))
period = f"{panel['ano'].min()}–{panel['ano'].max()} (anos-fonte disponíveis)"
boundaries = gpd.read_file(ensure_boundary_cache())
boundaries["codigo_municipio"] = _normalize_municipality_codes(boundaries[_municipality_code_column(boundaries)])
centroids = boundaries.to_crs(5880).geometry.centroid.to_crs(4326)
locations = pl.DataFrame({"codigo_municipio": boundaries["codigo_municipio"].tolist(), "latitude": centroids.y.tolist(), "longitude": centroids.x.tolist()})
display(cumulative.join(locations.select("codigo_municipio"), on="codigo_municipio", how="anti"))
display(municipal.join(locations.select("codigo_municipio"), on="codigo_municipio", how="anti"))
df_municipio = cumulative.join(locations, on="codigo_municipio", how="inner", validate="1:1").rename({"notificacoes": "Notificações", "municipio": "name"}).to_pandas()
df_municipio_relativo = municipal.join(locations, on="codigo_municipio", how="inner", validate="1:1").rename({"notificacoes": "Notificações", "populacao": "POPULACAO", "municipio": "name", "taxa_confirmados_100k": "taxa_100k"}).to_pandas()
'''

SINAN_STATE_CHART = '''
UF_CODES = {"11":"RO", "12":"AC", "13":"AM", "14":"RR", "15":"PA", "16":"AP", "17":"TO", "21":"MA", "22":"PI", "23":"CE", "24":"RN", "25":"PB", "26":"PE", "27":"AL", "28":"SE", "29":"BA", "31":"MG", "32":"ES", "33":"RJ", "35":"SP", "41":"PR", "42":"SC", "43":"RS", "50":"MS", "51":"MT", "52":"GO", "53":"DF"}
df_estado = cumulative.with_columns(pl.col("codigo_municipio").str.slice(0, 2).replace_strict(UF_CODES, default=None).alias("Estado")).group_by("Estado").agg(pl.col("notificacoes").sum().alias("Notificações")).to_pandas()
display(df_estado[df_estado["Estado"].isna()])
df_estado = df_estado.dropna(subset=["Estado"])
df_estado["Percentual"] = df_estado["Notificações"] / df_estado["Notificações"].sum() * 100
df_estado = df_estado.sort_values("Notificações", ascending=False)
fig = px.bar(df_estado, x="Estado", y="Notificações", color="Notificações", color_continuous_scale="Reds",
             text=df_estado["Percentual"].map(lambda x: f"{x:.1f}%"),
             title=f"Distribuição de notificações por UF de residência — {period}")
fig.update_traces(textposition="outside")
fig.update_layout(xaxis_title="Estado (UF)", yaxis_title="Número de notificações", showlegend=False)
fig.show()
'''

# As listas abaixo preservam cores, disposição, marcadores e dimensões dos
# gráficos originais. Não incluem saídas antigas como se fossem recalculadas.
SINAN_TIME_CHARTS = [
    ("code", r'''
media = df_notificacoes['Notificações'].mean()

fig = px.line(
    df_notificacoes,
    x='Dia',
    y='Notificações'
)

fig.add_hline(
    y=media,
    line_dash='dash',
    annotation_text=f"Média: {media:.0f}",
    annotation_position="top left"
)

fig.update_traces(line=dict(width=1))

fig.show()
'''),
    ("code", r'''
fig = px.line(
    df_sazonalidade,
    x="Mês",
    y="Notificações",
    color="Ano",
    markers=True,
    labels={
        "Mês": "Mês",
        "Notificações": "Notificações",
        "Ano": "Ano"
    },
    title="Sazonalidade por ano — média por dia de calendário"
)

fig.update_layout(
    xaxis=dict(
        tickmode="array",
        tickvals=list(range(1, 13)),
        ticktext=[
            "Jan", "Fev", "Mar", "Abr",
            "Mai", "Jun", "Jul", "Ago",
            "Set", "Out", "Nov", "Dez"
        ]
    )
)

fig.show()
'''),
    ("code", r'''
from plotly.subplots import make_subplots

# Criar ano e mês
df_notificacoes["Ano"] = df_notificacoes["Dia"].dt.year
df_notificacoes["Mês"] = df_notificacoes["Dia"].dt.month

# Soma das notificações de cada mês de cada ano
df_mensal = (
    df_notificacoes
    .groupby(["Ano", "Mês"])["Notificações"]
    .sum()
    .reset_index()
)
df_mensal = df_mensal[df_mensal["Ano"].isin(years)]

nomes_meses = [
    "Janeiro", "Fevereiro", "Março", "Abril",
    "Maio", "Junho", "Julho", "Agosto",
    "Setembro", "Outubro", "Novembro", "Dezembro"
]

# Criar 12 gráficos
fig = make_subplots(
    rows=4,
    cols=3,
    subplot_titles=nomes_meses
)

for mes in range(1, 13):

    dados = df_mensal[df_mensal["Mês"] == mes]

    # Média histórica daquele mês
    media = dados["Notificações"].mean()

    linha = go.Scatter(
        x=dados["Ano"],
        y=dados["Notificações"],
        mode="lines+markers",
        name="Notificações",
        showlegend=False
    )

    media_linha = go.Scatter(
        x=dados["Ano"],
        y=[media] * len(dados),
        mode="lines",
        name="Média",
        line=dict(dash="dash"),
        showlegend=False
    )

    linha_idx = (mes - 1) // 3 + 1
    coluna_idx = (mes - 1) % 3 + 1

    fig.add_trace(
        linha,
        row=linha_idx,
        col=coluna_idx
    )

    fig.add_trace(
        media_linha,
        row=linha_idx,
        col=coluna_idx
    )

fig.update_layout(
    title="Tendência das notificações por mês ao longo dos anos",
    height=900,
    width=1200
)

fig.show()
'''),
]
SINAN_MAP_CHARTS = [
    ("code", r'''
fig = px.scatter_map(
    df_municipio,
    lat='latitude',
    lon='longitude',
    size='Notificações',
    hover_name='name',
    size_max=30,
    zoom=3,
    map_style="carto-positron",
    title=f"Notificações acumuladas por município — {period}",
    height=800
)

fig.update_traces(marker=dict(color='red'))

fig.show()
'''),
    ("code", r'''
fig = px.scatter_map(
    df_municipio_relativo,
    lat='latitude',
    lon='longitude',
    size='taxa_100k',
    hover_name='name',
    hover_data={
        'Notificações': True,
        'POPULACAO': True,
        'taxa_100k': ':.2f'
    },
    size_max=30,
    zoom=3,
    map_style="carto-positron",
    title=f"Confirmados por 100 mil residentes — ano-fonte {YEAR}"
)

fig.update_traces(marker=dict(color='red'))

fig.update_layout(
    width=1200,
    height=800
)

fig.show()
'''),
    ("code", r'''
top_mun = cumulative.rename({'notificacoes':'Notificações', 'municipio':'name'}).to_pandas().sort_values(
    'Notificações',
    ascending=False
).head(10)

top_mun['Notificações'] = top_mun['Notificações'].astype(float)

fig = px.bar(
    top_mun.sort_values('Notificações'),
    x='Notificações',
    y='name',
    orientation='h',
    text='Notificações',
    title=f"Top 10 municípios por notificações — {period}",
    labels={
        'Notificações': 'Número de notificações',
        'name': 'Município'
    },
    color='Notificações'
)

fig.update_traces(textposition='outside')

fig.show()
'''),
    ("code", r'''
top_mun_relativo = municipal.rename({'taxa_confirmados_100k':'taxa_100k', 'municipio':'name'}).to_pandas().sort_values(
    'taxa_100k',
    ascending=False
).head(10)

fig = px.bar(
    top_mun_relativo.sort_values('taxa_100k'),
    x='taxa_100k',
    y='name',
    orientation='h',
    text=top_mun_relativo['taxa_100k'].round(2),
    title=f"Top 10 por confirmados por 100 mil residentes — ano-fonte {YEAR}",
    labels={
        'taxa_100k': 'Confirmados por 100 mil residentes',
        'name': 'Município'
    },
    color='taxa_100k'
)

fig.update_traces(textposition='outside')

fig.show()
'''),
]
INTEGRATION_CHARTS = [
    ("code", r'''
classificacao = (
    dados['classificacao']
    .group_by('status_classificacao')
    .agg(pl.col('registros').sum())
    .with_columns(
        (pl.col('registros') / pl.col('registros').sum() * 100).alias('percentual')
    )
    .sort('registros', descending=True)
)
display(classificacao)
fig = px.bar(
    classificacao.to_pandas(), x='status_classificacao', y='registros',
    text_auto='.3s', title='Classificação das notificações SINAN — anos-fonte disponíveis',
)
fig.show()
'''),
    ("code", r'''
painel = dados["populacao_integrada"]
serie_anual = (
    painel
    .group_by('ano')
    .agg(
        pl.col('notificacoes').sum(),
        pl.col('casos_confirmados').sum(),
        pl.col('codigo_municipio').n_unique().alias('municipios'),
    )
    .sort('ano')
)
display(serie_anual)
fig = px.line(
    serie_anual.to_pandas(), x='ano', y=['notificacoes', 'casos_confirmados'],
    markers=True, title='Notificações e casos confirmados por ano-fonte',
)
fig.show()
'''),
    ("code", r'''
serie_mensal = dados["mensal"].filter(pl.col("mes").is_not_null()).group_by("ano", "mes").agg(pl.col("notificacoes").sum()).sort("ano", "mes").to_pandas()
fig = px.line(
    serie_mensal, x='mes', y='notificacoes', color='ano',
    title='Sazonalidade mensal das notificações por ano-fonte (mês da notificação)',
).update_xaxes(dtick=1)
fig.show()
'''),
]
INMET_CHARTS = [
    ("code", r'''
completude = pl.DataFrame(quality)
import matplotlib.pyplot as plt
import matplotlib as mpl

# --- paleta (light) ---------------------------------------------------------
SURFACE   = "#fcfcfb"
INK       = "#0b0b0b"
INK_2     = "#52514e"
MUTED     = "#898781"
GRID      = "#e1e0d9"
BASELINE  = "#c3c2b7"
AZUL      = "#2a78d6"   # slot categórico 1 / hue sequencial
LARANJA   = "#eb6834"   # slot categórico 2
CRITICO   = "#d03b3b"   # status: crítico

mpl.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "font.family": "sans-serif", "font.size": 10,
    "text.color": INK, "axes.labelcolor": INK_2,
    "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.edgecolor": BASELINE, "axes.linewidth": 1.0,
    "axes.spines.top": False, "axes.spines.right": False,
    "grid.color": GRID, "grid.linewidth": 0.8,
    "figure.dpi": 120,
})

def limpar(ax, eixo="y"):
    """Grid recessivo em um eixo só, sem spines supérfluas."""
    ax.grid(True, axis=eixo, zorder=0)
    ax.set_axisbelow(True)
    for lado in ("top", "right"):
        ax.spines[lado].set_visible(False)

def rotulo_curto(nome, n=42):
    return nome if len(nome) <= n else nome[:n-1] + "…"

# --- gráfico ----------------------------------------------------------------
dados = completude.sort("completude_pct")          # menor embaixo -> maior no topo
nomes = [rotulo_curto(v) for v in dados["variavel"].to_list()]
vals  = dados["completude_pct"].to_list()

# emphasis: destaca em vermelho o que está abaixo de 50% (cobertura inferior a 50% (sinalização descritiva))
cores = [CRITICO if v < 50 else AZUL for v in vals]

fig, ax = plt.subplots(figsize=(9, 0.42*len(nomes) + 1.8))
ax.barh(nomes, vals, color=cores, height=0.62, zorder=3)
limpar(ax, eixo="x")
ax.set_xlim(0, 104)
ax.set_xlabel("% de registros válidos após limpeza")

titulo = "Completude por variável"
subtitulo = "vermelho: abaixo de 50% — cobertura inferior a 50% (sinalização descritiva)" if any(v < 50 for v in vals) else None
ax.set_title(titulo, loc="left", fontsize=13, fontweight="bold",
             pad=22 if subtitulo else 12)
if subtitulo:
    ax.text(0.0, 1.015, subtitulo, transform=ax.transAxes,
            fontsize=8.5, color=MUTED)

# rótulo direto em cada barra — identidade nunca depende só da cor
for y, v in enumerate(vals):
    ax.text(v + 1.5, y, f"{v:.1f}%", va="center", ha="left",
            fontsize=9, color=INK_2)

plt.tight_layout()
plt.show()
'''),
    ("code", r'''
por_ano = lf.group_by("ANO_REFERENCIA").agg(pl.len().alias("registros")).sort("ANO_REFERENCIA").collect()
anos = por_ano["ANO_REFERENCIA"].to_list()
regs = por_ano["registros"].to_list()

fig, ax = plt.subplots(figsize=(8, 3.6))
ax.bar(anos, regs, color=AZUL, width=0.62, zorder=3)
limpar(ax, eixo="y")
ax.set_ylabel("registros")
ax.set_title("Registros por ano", loc="left", fontsize=13, fontweight="bold", pad=12)
ax.tick_params(axis="x", rotation=90)

# rótulo direto só nas pontas (mínimo e máximo) — não em toda barra
i_min, i_max = regs.index(min(regs)), regs.index(max(regs))
for i in {i_min, i_max}:
    ax.text(anos[i], regs[i], f"{regs[i]:,}", ha="center", va="bottom",
            fontsize=9, color=INK_2)

ax.margins(y=0.15)
plt.tight_layout()
plt.show()
'''),
    ("code", r'''
por_uf = lf.group_by("UF").agg(pl.len().alias("registros")).collect()
# comparação de magnitude -> uma cor só (sequencial), sem paleta categórica
d = por_uf.sort("registros")
fig, ax = plt.subplots(figsize=(8, 0.28*d.height + 1.8))
ax.barh(d["UF"].to_list(), d["registros"].to_list(),
        color=AZUL, height=0.62, zorder=3)
limpar(ax, eixo="x")
ax.set_xlabel("registros")
ax.set_title("Registros por UF", loc="left", fontsize=13, fontweight="bold", pad=12)
ax.margins(x=0.12)
plt.tight_layout()
plt.show()
'''),
    ("code", r'''
col_temp = next((c for c in measurements if c.startswith("TEMPERATURA_DO_AR_BULBO_SECO")), None)
if col_temp and "HORA_UTC_NUM" in lf.collect_schema().names():
    ciclo = (
        lf.filter(pl.col("HORA_UTC_NUM").is_not_null() & pl.col(col_temp).is_not_null())
          .group_by("HORA_UTC_NUM")
          .agg(pl.col(col_temp).mean().alias("media"))
          .sort("HORA_UTC_NUM")
          .collect(engine="streaming")
    )

    fig, ax = plt.subplots(figsize=(8, 3.6))
    ax.plot(ciclo["HORA_UTC_NUM"], ciclo["media"], color=AZUL, linewidth=2,
            marker="o", markersize=4, zorder=3)
    limpar(ax, eixo="y")
    ax.set_xlabel("hora UTC")
    ax.set_ylabel("°C")
    ax.set_xticks(range(0, 24, 3))
    ax.set_title(f"Ciclo diurno médio\n{rotulo_curto(col_temp, 46)}",
                 loc="left", fontsize=13, fontweight="bold", pad=14)

    # rótulo direto no pico e no vale
    s = ciclo["media"].to_list(); h = ciclo["HORA_UTC_NUM"].to_list()
    for idx, txt in ((s.index(max(s)), "máx"), (s.index(min(s)), "mín")):
        ax.annotate(f"{txt} {s[idx]:.1f}°C", (h[idx], s[idx]),
                    textcoords="offset points", xytext=(0, 10),
                    ha="center", fontsize=9, color=INK_2)
    ax.margins(y=0.20)
    plt.tight_layout()
    plt.show()
else:
    print("Sem coluna de temperatura ou de hora — gráfico pulado.")
'''),
    ("code", r'''
lf = lf.with_columns(pl.col("DATA").dt.month().alias("MES"))
if col_temp and "MES" in lf.collect_schema().names():
    sazonal = (
        lf.filter(pl.col("MES").is_not_null() & pl.col(col_temp).is_not_null())
          .group_by("MES")
          .agg(
              pl.col(col_temp).mean().alias("media"),
              pl.col(col_temp).quantile(0.10).alias("p10"),
              pl.col(col_temp).quantile(0.90).alias("p90"),
          )
          .sort("MES")
          .collect(engine="streaming")
    )

    MESES = ["jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez"]
    x = sazonal["MES"].to_list()

    fig, ax = plt.subplots(figsize=(8, 3.6))
    # faixa p10–p90 como contexto recessivo; a média é a série que importa
    ax.fill_between(x, sazonal["p10"], sazonal["p90"],
                    color=AZUL, alpha=0.14, linewidth=0, zorder=2)
    ax.plot(x, sazonal["media"], color=AZUL, linewidth=2,
            marker="o", markersize=4, zorder=3)
    limpar(ax, eixo="y")
    ax.set_xticks(x, [MESES[m-1] for m in x])
    ax.set_ylabel("°C")
    ax.set_title(f"Sazonalidade — média mensal\n{rotulo_curto(col_temp, 46)}",
                 loc="left", fontsize=13, fontweight="bold", pad=22)
    ax.text(0.0, 1.015, "faixa sombreada: percentis 10–90",
            transform=ax.transAxes, fontsize=8.5, color=MUTED)
    plt.tight_layout()
    plt.show()
else:
    print("Sem coluna de mês — gráfico pulado.")
'''),
    ("code", r'''
if col_temp:
    LARGURA_BIN = 1.0
    hist = (
        lf.filter(pl.col(col_temp).is_not_null())
          .select((pl.col(col_temp) / LARGURA_BIN).floor() * LARGURA_BIN)
          .group_by(col_temp)
          .agg(pl.len().alias("n"))
          .sort(col_temp)
          .collect(engine="streaming")
    )

    fig, ax = plt.subplots(figsize=(8, 3.6))
    ax.bar(hist[col_temp], hist["n"], width=LARGURA_BIN*0.9,
           color=AZUL, zorder=3)
    limpar(ax, eixo="y")
    ax.set_xlabel("°C")
    ax.set_ylabel("registros")
    ax.set_title(f"Distribuição\n{rotulo_curto(col_temp, 46)}",
                 loc="left", fontsize=13, fontweight="bold", pad=14)

    media = lf.select(pl.col(col_temp).mean()).collect(engine="streaming").item()
    ax.axvline(media, color=LARANJA, linewidth=2, zorder=4)
    ax.annotate(f"média {media:.1f}°C", (media, ax.get_ylim()[1]),
                textcoords="offset points", xytext=(6, -12),
                fontsize=9, color=LARANJA, fontweight="bold")
    plt.tight_layout()
    plt.show()
'''),
]
CLIMATE_CHARTS = [
    ("code", r'''
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
plt.rcParams.update({"figure.figsize": (11, 5.2), "figure.dpi": 120, "font.size": 13, "axes.titlesize": 15, "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.alpha": 0.25})
CORES_REGIAO = {"N": "#4C9F70", "NE": "#E1A730", "CO": "#B5651D", "SE": "#3A6EA5", "S": "#7A4FA3"}
MESES = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
AZUL, AZUL_ESC, VERDE, TERRA = "#3A6EA5", "#1F3F66", "#4C9F70", "#B5651D"
nac = df.group_by("DATA").agg(pl.col("notificacoes_dengue").sum().alias("notif")).sort("DATA").with_columns(pl.col("notif").rolling_mean(7, center=True).alias("notif_7d"))
NAC_DATAS = nac["DATA"].to_list()
mensal = df.group_by(pl.col("DATA").dt.month().alias("mes")).agg(pl.col("notificacoes_dengue").sum().alias("notif")).sort("mes")
fig, ax = plt.subplots()
ax.fill_between(NAC_DATAS, nac["notif"].to_numpy(), color=AZUL, alpha=0.25, label="por dia")
ax.plot(NAC_DATAS, nac["notif_7d"].to_numpy(), color=AZUL_ESC, lw=2.2, label="média móvel 7 dias")
ax.set_title("Notificações de dengue no Brasil — 2010")
ax.set_ylabel("notificações / dia")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
ax.legend(loc="upper right")
plt.show()
'''),
    ("code", r'''
n = mensal["notif"].to_list()
fig, ax = plt.subplots(figsize=(11, 4.6))
ax.bar(MESES, n, color=AZUL)
ax.set_title("Sazonalidade — notificações por mês (Brasil, 2010)")
ax.set_ylabel("notificações")
for i, v in enumerate(n):
    ax.text(i, v, f"{v/1000:.0f} k", ha="center", va="bottom", fontsize=11)
ax.grid(axis="x")
plt.show()
'''),
    ("code", r'''
pares = [("temp_media_c", "Temperatura média (°C)"),
         ("precip_media_mm", "Chuva diária por estação (mm)"),
         ("umidade_media_pct", "Umidade relativa média (%)")]
common = df.filter(pl.col("UF") == "AC").drop_nulls(["temp_media_c", "precip_media_mm", "umidade_media_pct", "notificacoes_dengue"])
y = common["notificacoes_dengue"].to_numpy()
def corr(frame, a, b):
    return frame.select(pl.corr(a, b)).item() if frame.height >= 30 and frame[a].std() and frame[b].std() else None
fig, axs = plt.subplots(1, 3, figsize=(13.5, 4.6))
for ax, (col, lab) in zip(axs, pares):
    ax.scatter(common[col].to_numpy(), y, s=6, alpha=0.12, color=AZUL)
    r_value = corr(common, col, 'notificacoes_dengue')
    r_label = f"{r_value:+.2f}" if r_value is not None else "indisponível"
    ax.set_title(f"{lab}\nr = {r_label}", fontsize=12)
    ax.set_xlabel(lab)
axs[0].set_ylabel("notificações / dia (AC)")
plt.tight_layout()
plt.show()
'''),
    ("code", r'''
uf_codes = {"11":"RO", "12":"AC", "13":"AM", "14":"RR", "15":"PA", "16":"AP", "17":"TO", "21":"MA", "22":"PI", "23":"CE", "24":"RN", "25":"PB", "26":"PE", "27":"AL", "28":"SE", "29":"BA", "31":"MG", "32":"ES", "33":"RJ", "35":"SP", "41":"PR", "42":"SC", "43":"RS", "50":"MS", "51":"MT", "52":"GO", "53":"DF"}
region_codes = {**dict.fromkeys(["11","12","13","14","15","16","17"], "N"), **dict.fromkeys(["21","22","23","24","25","26","27","28","29"], "NE"), **dict.fromkeys(["31","32","33","35"], "SE"), **dict.fromkeys(["41","42","43"], "S"), **dict.fromkeys(["50","51","52","53"], "CO")}
inc = rates.with_columns(pl.col("UF_codigo").replace_strict(uf_codes).alias("UF"), pl.col("UF_codigo").replace_strict(region_codes).alias("REGIAO")).sort("notificacoes_por_100k_residentes")
ufs = inc["UF"].to_list()
vals = inc["notificacoes_por_100k_residentes"].to_list()
cores = [CORES_REGIAO.get(x, "#888") for x in inc["REGIAO"].to_list()]
fig, ax = plt.subplots(figsize=(10, 8))
ax.barh(ufs, vals, color=cores)
ax.set_title("Taxa de notificações por 100 mil residentes — por UF (2010)")
ax.set_xlabel("notificações / 100 mil residentes (2010)")
ax.grid(axis="y")
handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in CORES_REGIAO.values()]
ax.legend(handles, CORES_REGIAO.keys(), title="Região", ncol=5, loc="lower right")
plt.show()
'''),
    ("code", r'''
from utils.analysis_pipeline import _fetch_sidra, _normalize_sidra
sanitation = pl.read_parquet(PROCESSED_DIR / "ibge_saneamento_2010.parquet").with_columns(
    pl.sum_horizontal("pct_agua_poco_nascente_aldeia", "pct_agua_poco_nascente_fora_aldeia", "pct_agua_poco_nascente_fora_propriedade", "pct_agua_poco_nascente_propriedade", ignore_nulls=False).alias("pct_agua_poco_nascente")
)
households = _normalize_sidra(_fetch_sidra("https://apisidra.ibge.gov.br/values/t/3218/n6/all/v/96/p/2010/c61/0/c299/0/c67/0/c309/0")).select("codigo_municipio", pl.col("valor").alias("domicilios"))
water = sanitation.join(households, on="codigo_municipio", how="inner", validate="1:1").with_columns(pl.col("codigo_municipio").str.slice(0, 2).alias("UF_codigo"))
water = water.drop_nulls(["pct_agua_rede_geral", "pct_agua_poco_nascente", "domicilios"]).group_by("UF_codigo").agg(*[((pl.col(c) * pl.col("domicilios")).sum() / pl.col("domicilios").sum()).alias(c) for c in ["pct_agua_rede_geral", "pct_agua_poco_nascente"]])
inc = inc.join(water, on="UF_codigo", how="inner", validate="1:1")
pares = [("pct_agua_poco_nascente", "% domicílios — água de poço ou nascente"),
         ("pct_agua_rede_geral", "% domicílios — água de rede geral")]
uf_lab = inc["UF"].to_list()
yy = np.array(inc["notificacoes_por_100k_residentes"].to_list())
fig, axs = plt.subplots(1, 2, figsize=(13, 5.2))
for ax, (col, lab) in zip(axs, pares):
    xx = np.array(inc[col].to_list())
    ax.scatter(xx, yy, color=TERRA, zorder=3)
    for x_i, y_i, t in zip(xx, yy, uf_lab):
        ax.annotate(t, (x_i, y_i), fontsize=8, xytext=(3, 3), textcoords="offset points")
    m, b = np.polyfit(xx, yy, 1)
    xs = np.array([xx.min(), xx.max()])
    ax.plot(xs, m * xs + b, color="#333", lw=1, ls="--")
    ax.set_title(f"{lab}\nr = {np.corrcoef(xx, yy)[0, 1]:+.2f}", fontsize=12)
    ax.set_xlabel(lab)
axs[0].set_ylabel("notificações / 100 mil residentes")
plt.tight_layout()
plt.show()
'''),
    ("code", r'''
uf = "AC"
s = df.filter(pl.col("UF") == uf).sort("DATA")
d = s["DATA"].to_list()
notif_7d = s["notificacoes_dengue"].rolling_mean(7, center=True).to_numpy()

fig, axs = plt.subplots(3, 1, figsize=(11, 7), sharex=True)
axs[0].fill_between(d, s["notificacoes_dengue"].to_numpy(), color=AZUL, alpha=0.3)
axs[0].plot(d, notif_7d, color=AZUL_ESC, lw=2)
axs[0].set_ylabel("notif. / dia")
axs[0].set_title(f"{uf} — dengue, chuva e temperatura ao longo de 2010")
axs[1].bar(d, s["precip_media_mm"].to_numpy(), color=VERDE, width=1)
axs[1].set_ylabel("chuva (mm)")
axs[2].plot(d, s["temp_media_c"].to_numpy(), color=TERRA)
axs[2].set_ylabel("temp. (°C)")
axs[2].xaxis.set_major_formatter(mdates.DateFormatter("%b"))
for a in axs:
    a.grid(axis="x")
plt.tight_layout()
plt.show()
'''),
]
