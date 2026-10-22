"""Regenera leitores analíticos sem duplicar transformações nos notebooks.

Executar da raiz: python -m utils.create_analysis_notebooks.
Saídas antigas são descartadas durante a regeneração: precisam ser recalculadas.
"""

import json
import hashlib
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = '''
from pathlib import Path
import sys
PROJECT_ROOT = next(p for p in (Path.cwd(), *Path.cwd().parents)
                    if (p / "utils/analysis_pipeline.py").exists())
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
import polars as pl
from IPython.display import display
'''


def cell(kind, source):
    source = textwrap.dedent(source).strip()
    result = {"cell_type": kind, "id":hashlib.sha256((kind + source).encode()).hexdigest()[:12], "metadata": {}, "source": source.splitlines(keepends=True)}
    if kind == "code":
        result.update(execution_count=None, outputs=[])
    return result


def write(relative, content):
    notebook = {"cells": [cell(kind, source) for kind, source in content],
                "metadata": {"kernelspec": {"display_name":"Python 3", "language":"python", "name":"python3"},
                             "language_info":{"name":"python", "version":"3.13"}},
                "nbformat":4, "nbformat_minor":5}
    path = ROOT / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


def sinan_notebook(state=None):
    title = "Brasil" if state is None else "São Paulo — residência"
    return [
        ("markdown", f"# EDA SINAN — {title}\n\nDados de anos encerrados, agregados por residência. Execute primeiro `python -m utils.deng_download` e `python -m utils.analysis_pipeline`. As saídas anteriores foram invalidadas após correção da classificação histórica. `DENGUE` não é diagnóstico final: usamos `CON_CLASSI` (até 2006) e `CLASSI_FIN` (demais anos)."),
        ("code", BOOTSTRAP + '''
import plotly.express as px
from utils.analysis_pipeline import PROCESSED_DIR, build_sinan_outputs, scan_sinan
outputs = build_sinan_outputs(allow_undated=True)
panel = pl.read_parquet(PROCESSED_DIR / "sinan_populacao_municipio_ano.parquet")
raw = scan_sinan()
''' + ('''\npanel = panel.filter(pl.col("codigo_municipio").str.starts_with("35"))
raw = raw.filter(pl.col("codigo_municipio").str.starts_with("35"))
''' if state else "")),
        ("markdown", "## Qualidade e universo\n\nNotificações não são necessariamente casos confirmados. Sem fonte anual não se infere zero. Municípios sem notificação recebem zero somente em anos com fonte presente. Registros sem residência válida ficam na auditoria; não são distribuídos entre municípios."),
        ("code", '''
display(outputs["qualidade"])
annual = panel.group_by("ano").agg(pl.col("notificacoes").sum(), pl.col("casos_confirmados").sum()).sort("ano")
display(annual)
px.line(annual.to_pandas(), x="ano", y=["notificacoes", "casos_confirmados"], markers=True).show()
'''),
        ("markdown", "## Sazonalidade\n\nGrade completa de dias dos anos-fonte, incluindo dias sem notificação registrada. A média mensal abaixo é por dia de calendário; a soma mensal é publicada separadamente."),
        ("code", '''
from datetime import date
years = outputs["qualidade"]["ano_arquivo"].to_list()
calendar = pl.concat([pl.DataFrame({"Dia": pl.date_range(date(y, 1, 1), date(y, 12, 31), eager=True)}) for y in years])
daily = raw.filter(pl.col("codigo_municipio").is_not_null()).group_by("data_notificacao").agg(pl.len().alias("notificacoes")).rename({"data_notificacao":"Dia"}).collect()
daily = calendar.join(daily, on="Dia", how="left", validate="1:1").with_columns(pl.col("notificacoes").fill_null(0)).sort("Dia")
monthly = daily.with_columns(pl.col("Dia").dt.year().alias("ano"), pl.col("Dia").dt.month().alias("mes")).group_by("ano", "mes").agg(pl.col("notificacoes").sum().alias("notificacoes_mes"), pl.col("notificacoes").mean().alias("media_por_dia_calendario")).sort("ano", "mes")
display(monthly)
px.line(monthly.to_pandas(), x="mes", y="media_por_dia_calendario", color="ano").show()
'''),
        ("markdown", "## Classificação, raça/cor, sexo e evolução das notificações"),
        ("code", '''
for column in ["status_classificacao", "raca", "sexo", "evolucao"]:
    summary = raw.group_by(column).len(name="registros").sort("registros", descending=True).collect()
    display(summary)
    px.bar(summary.to_pandas(), x=column, y="registros", title=f"Notificações por {column}").show()
'''),
        ("markdown", "## Taxas municipais anuais\n\nPopulação residente do mesmo ano e chave IBGE. Nunca se divide a soma de vários anos por uma única população censitária. A seleção abaixo usa 2010; altere apenas para um ano com denominador disponível."),
        ("code", '''
YEAR = 2010
municipal = panel.filter((pl.col("ano") == YEAR) & pl.col("correspondencia_populacao"))
if not municipal.height:
    raise ValueError(f"Sem denominador válido para {YEAR}")
display(municipal.select("codigo_municipio", "municipio", "populacao", "notificacoes", "casos_confirmados", "taxa_confirmados_100k").sort("taxa_confirmados_100k", descending=True))
px.bar(municipal.sort("taxa_confirmados_100k", descending=True).head(10).to_pandas(), x="taxa_confirmados_100k", y="municipio", orientation="h", title=f"Taxa de confirmados por 100 mil residentes — {YEAR}").show()
states = municipal.with_columns(pl.col("codigo_municipio").str.slice(0, 2).alias("UF_codigo")).group_by("UF_codigo").agg(pl.col("notificacoes").sum(), pl.col("casos_confirmados").sum(), pl.col("populacao").sum()).with_columns((pl.col("casos_confirmados") / pl.col("populacao") * 100_000).alias("taxa_confirmados_100k"))
display(states)
'''),
        ("markdown", "## Conclusões\n\nA série corrigida usa classificação final em todas as eras. As contagens antigas baseadas no antecedente `DENGUE` não fundamentam conclusões sobre magnitude ou tendência em 2000–2006. O período anual é o ano-fonte, e a localização é a residência. As taxas são descritivas: refletem vigilância, diagnóstico e transmissão, sem demonstrar causalidade ou eliminar subnotificação."),
    ]


def integration_notebook():
    return [
        ("markdown", "# Integração SINAN–IBGE/SIDRA\n\nMunicípio de residência (`ID_MN_RESI`), população residente do mesmo ano e saneamento somente nos anos censitários compatíveis. Classificação final histórica: `CON_CLASSI`, nunca o antecedente `DENGUE`."),
        ("code", BOOTSTRAP + '''
import plotly.express as px
from utils.analysis_pipeline import build_integrated_outputs
dados = build_integrated_outputs(allow_undated=True)
display(dados["auditoria"])
display(dados["qualidade"])
'''),
        ("code", '''
resumo = []
for year, key in [(2010, "saneamento_integrado"), (2022, "saneamento_2022_integrado")]:
    frame = dados[key]
    resumo.append({"ano":year, "municipios_grade":frame.height,
                   "municipios_com_notificacao":int(frame["tem_notificacao_registrada"].sum()),
                   "municipios_com_sidra":int(frame["correspondencia_sidra"].sum()),
                   "notificacoes":int(frame["notificacoes"].sum()),
                   "casos_confirmados":int(frame["casos_confirmados"].sum())})
display(pl.DataFrame(resumo))
'''),
        ("markdown", "## Associação descritiva em 2022\n\nMesma amostra municipal completa para todos os indicadores. São correlações ecológicas sem interpretação causal, sujeitas a confundimento e dependência espacial. O notebook espacial fornece sensibilidade à população e inferência local com FDR-BY."),
        ("code", '''
indicators = ["pct_agua_rede_geral_principal", "pct_esgoto_rede_geral_ou_pluvial", "pct_sem_banheiro_ou_sanitario", "pct_lixo_queimado_propriedade", "pct_lixo_terreno_encosta_area_publica"]
complete = dados["saneamento_2022_integrado"].drop_nulls([*indicators, "taxa_confirmados_100k"])
display(pl.DataFrame({"indicador":indicators, "n_comum":[complete.height] * len(indicators),
                     "r_pearson":[complete.select(pl.corr(c, "taxa_confirmados_100k")).item() for c in indicators]}))
px.scatter(complete.to_pandas(), x="pct_esgoto_rede_geral_ou_pluvial", y="taxa_confirmados_100k", hover_name="municipio", opacity=0.45).show()
'''),
        ("markdown", "## Conclusões\n\nNa coorte restaurada, as cinco correlações de saneamento em 2022 permanecem fracas e com os mesmos sinais: água r = 0,1621 e esgoto r = 0,1222; os outros três indicadores têm correlações negativas de baixa magnitude. A amostra reúne 5.570 municípios e não demonstra efeito causal do saneamento. A análise espacial identifica autocorrelação global positiva, mas nenhum cluster local significativo após FDR-BY a 5%. A série histórica deve usar a classificação final corrigida, não os antigos resultados baseados em `DENGUE`."),
    ]


def inmet_eda():
    return [
        ("markdown", "# EDA INMET — cobertura e triagem\n\nO parquet bruto é preservado. A visão limpa trata sentinelas, formatos históricos e valores fora de limites de triagem. Presença de dados não comprova representatividade espacial ou qualidade física."),
        ("code", BOOTSTRAP + '''
import numpy as np
import matplotlib.pyplot as plt
from utils.inmet_pipeline import clean_inmet, daily_weather
PATH = PROJECT_ROOT / "data/INMET_dataset_agregado.parquet"
lf, measurements = clean_inmet(pl.scan_parquet(PATH))
display(lf.select(pl.len().alias("registros"), pl.col("TEMPO_INVALIDO").sum().alias("tempo_invalido"), pl.col("ANO_REFERENCIA_DIVERGENTE").sum().alias("ano_arquivo_divergente")).collect())
'''),
        ("code", '''
quality = []
for column in measurements:
    row = lf.select(pl.len().alias("total"), pl.col(column).count().alias("validos"), pl.col(f"INVALIDO_{column}").sum().alias("fora_triagem")).collect().row(0, named=True)
    quality.append({"variavel":column, **row, "completude_pct":100 * row["validos"] / row["total"] if row["total"] else None})
display(pl.DataFrame(quality))
display(lf.group_by("ANO_REFERENCIA", "UF").agg(pl.col("CODIGO_WMO").n_unique().alias("estacoes_presentes"), pl.len().alias("registros")).sort("ANO_REFERENCIA", "UF").collect())
'''),
        ("markdown", "## Distribuições\n\nDireção do vento usa média circular e concentração resultante (0 a 1). As demais variáveis recebem resumos lineares. Valores rejeitados continuam disponíveis em `BRUTO_*` e `INVALIDO_*`."),
        ("code", '''
for column in measurements:
    if "DIRECAO" in column:
        angle = pl.col(column) * np.pi / 180
        result = lf.select(angle.sin().mean().alias("s"), angle.cos().mean().alias("c")).collect()
        s, c = result.row(0)
        concentration = np.hypot(s, c) if s is not None and c is not None else np.nan
        print(column, {"media_circular_graus":np.degrees(np.arctan2(s, c)) % 360 if concentration > 1e-12 else None, "concentracao":concentration})
    else:
        display(lf.select(pl.lit(column).alias("variavel"), pl.col(column).mean().alias("media"), pl.col(column).std().alias("desvio"), pl.col(column).min().alias("min"), pl.col(column).max().alias("max")).collect())
'''),
        ("markdown", "## Ciclo horário\n\nHoras UTC; não se aplica UTC−3 a todo o Brasil. Compare estações e anos comuns antes de interpretar diferenças regionais ou temporais."),
        ("code", '''
temperature = next(c for c in measurements if c.startswith("TEMPERATURA_DO_AR_BULBO_SECO"))
cycle = lf.filter(~pl.col("TEMPO_INVALIDO")).group_by("HORA_UTC_NUM").agg(pl.col(temperature).mean().alias("media")).sort("HORA_UTC_NUM").collect()
plt.plot(cycle["HORA_UTC_NUM"], cycle["media"])
plt.xlabel("Hora UTC")
plt.ylabel("Temperatura (°C), média das observações disponíveis")
plt.show()
'''),
        ("markdown", "## Cobertura diária por variável\n\nCada estação precisa de 24 horas válidas para precipitação e ao menos 18 para médias de temperatura/umidade. Chuva ausente não vira zero. A média estadual é entre estações válidas e informa o número usado por variável; não representa automaticamente a população ou toda a área estadual."),
        ("code", '''
daily = daily_weather(lf)
display(daily.limit(20).collect())
daily.sink_parquet(PROJECT_ROOT / "data/INMET_diario_uf.parquet")
'''),
    ]


def climate_notebook():
    return [
        ("markdown", "# Dengue e clima — exploração UF × dia, 2010\n\nBase reproduzível gerada neste repositório. Desfecho: notificações por residência e data de notificação. Meteorologia: dia UTC, com cobertura por variável; não se assume alinhamento exato com o dia administrativo local. Não se compara a magnitude dessas correlações com correlações anuais de saneamento."),
        ("code", BOOTSTRAP + '''
import plotly.express as px
from utils.climate_integration import build_daily_uf, within_uf_correlations
df = build_daily_uf(PROJECT_ROOT / "data/INMET_dataset_agregado.parquet", year=2010)
display(df.head(20))
'''),
        ("code", '''
national = df.group_by("DATA").agg(pl.col("notificacoes_dengue").sum()).sort("DATA")
national = national.with_columns(pl.col("notificacoes_dengue").rolling_mean(7, center=True).alias("media_7_dias"))
px.line(national.to_pandas(), x="DATA", y=["notificacoes_dengue", "media_7_dias"]).show()
'''),
        ("markdown", "## Correlações dentro de UF\n\nPreditores usam o mesmo desfecho e os mesmos dias completos dentro de cada UF. A defasagem de 28 dias é pré-especificada como análise descritiva; não foi selecionada pelo maior coeficiente. Sazonalidade, autocorrelação, acesso ao diagnóstico, mudanças de cobertura e atrasos de notificação impedem interpretar `r` como efeito climático."),
        ("code", '''
display(within_uf_correlations(df, lag_days=0))
display(within_uf_correlations(df, lag_days=28))
display(df.select("UF", "DATA", "n_estacoes_presentes", "n_estacoes_validas_temp_media_c", "n_estacoes_validas_precip_media_mm", "n_estacoes_validas_umidade_media_pct").head(30))
'''),
        ("markdown", "## Comparação anual de notificações por residente\n\nPopulação do mesmo ano, somada por UF. Não se usa número de domicílios como denominador de incidência residente. A investigação municipal de saneamento permanece no notebook SINAN–SIDRA, com universo e escala próprios."),
        ("code", '''
from utils.analysis_pipeline import PROCESSED_DIR
annual = pl.read_parquet(PROCESSED_DIR / "sinan_populacao_municipio_ano.parquet").filter((pl.col("ano") == 2010) & pl.col("correspondencia_populacao"))
rates = annual.with_columns(pl.col("codigo_municipio").str.slice(0, 2).alias("UF_codigo")).group_by("UF_codigo").agg(pl.col("notificacoes").sum(), pl.col("casos_confirmados").sum(), pl.col("populacao").sum()).with_columns((pl.col("notificacoes") / pl.col("populacao") * 100_000).alias("notificacoes_por_100k_residentes"))
display(rates)
'''),
    ]


def build_notebooks():
    base = "notebooks/Analises exploratorias"
    write(f"{base}/SINAN/sinan.ipynb", sinan_notebook())
    write(f"{base}/SINAN/sinan_corrected.ipynb", sinan_notebook("35"))
    content = integration_notebook()
    write("notebooks/Agregacao/DENGUE+IBGE/integracao_sinan_saneamento.ipynb", content)
    write("NSEI/integracao_sinan_saneamento.ipynb", content)
    write(f"{base}/IMNET/aed_inmet_parquet.ipynb", inmet_eda())
    write(f"{base}/IMNET/apresentacao_dengue_clima_2010.ipynb", climate_notebook())
    write("notebooks/Agregacao/DENGUE+IBGE+IMNET/agregardados_polars.ipynb", [
        ("markdown", "# Ingestão INMET com auditoria\n\nTodos os CSVs devem estar organizados em pastas por ano. Uma falha interrompe a publicação; o parquet anterior é preservado. A auditoria registra checksum, registros e falhas por arquivo. O cache é identificado pelo conteúdo da fonte."),
        ("code", BOOTSTRAP + '''
from utils.inmet_pipeline import ingest_inmet
SOURCE = PROJECT_ROOT / "base-INM"
DESTINATION = PROJECT_ROOT / "data/INMET_dataset_agregado.parquet"
display(ingest_inmet(SOURCE, DESTINATION))
'''),
    ])
    write(f"{base}/IBGE/seneamento.ipynb", [
        ("markdown", "# Exploração SIDRA\n\nExtrações municipais com variáveis, unidades e classificações explícitas. Totais não são somados às suas categorias. Dados especiais são interpretados pelo pipeline; percentuais precisam fechar 100% em cada dimensão."),
        ("code", BOOTSTRAP + '''
from utils.analysis_pipeline import build_ibge_outputs
ibge = build_ibge_outputs()
for name, frame in ibge.items():
    print(name, frame.shape)
    display(frame.head(10))
'''),
    ])


if __name__ == "__main__":
    build_notebooks()
