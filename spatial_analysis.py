"""Análise espacial municipal de dengue e saneamento com o ecossistema PySAL.

O módulo prepara a malha municipal, constrói pesos espaciais KNN e calcula
Moran global, Moran bivariado e LISA. As funções de visualização são usadas no
notebook ``analise_espacial_pysal.ipynb`` e também podem ser executadas pela
linha de comando para regenerar figuras e tabelas de auditoria.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import httpx
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from esda import Moran, Moran_BV, Moran_Local
from libpysal.weights import KNN, W
from matplotlib.lines import Line2D
from splot.esda import lisa_cluster, plot_moran, plot_moran_bv


PROCESSED_DIR = Path("sinan/processados")
DEFAULT_DATA_PATH = PROCESSED_DIR / "sinan_saneamento_2022.parquet"
DEFAULT_BOUNDARY_PATH = PROCESSED_DIR / "ibge_municipios_min.geojson"
DEFAULT_FIGURE_DIR = Path("figuras/analise_espacial")
DEFAULT_RESULT_DIR = PROCESSED_DIR / "analise_espacial"
BOUNDARY_URL = (
    "https://servicodados.ibge.gov.br/api/v3/malhas/paises/BR"
    "?formato=application%2Fvnd.geo%2Bjson"
    "&qualidade=minima&intrarregiao=municipio"
)

ANALYSIS_VARIABLES = {
    "taxa_confirmados_100k": "Casos confirmados de dengue por 100 mil habitantes",
    "pct_agua_rede_geral_principal": "Domicílios com rede geral como principal abastecimento (%)",
    "pct_esgoto_rede_geral_ou_pluvial": "Domicílios com esgoto em rede geral ou pluvial (%)",
    "pct_lixo_coletado_domicilio": "Domicílios com lixo coletado no domicílio (%)",
}

SANITATION_VARIABLES = {
    key: label
    for key, label in ANALYSIS_VARIABLES.items()
    if key != "taxa_confirmados_100k"
}

LISA_LABELS = {
    0: "Não significativo",
    1: "Alto–Alto",
    2: "Baixo–Alto",
    3: "Baixo–Baixo",
    4: "Alto–Baixo",
}

BLUE = "#3973ac"
BLUE_DARK = "#214d74"
GOLD = "#c58a2b"
INK = "#1f2933"
NEUTRAL = "#d9dee5"
BACKGROUND = "#f7f8fa"


def ensure_boundary_cache(
    path: Path = DEFAULT_BOUNDARY_PATH,
    url: str = BOUNDARY_URL,
) -> Path:
    """Baixa a malha simplificada do IBGE somente quando o cache não existe."""
    if path.exists():
        return path

    path.parent.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=180, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
        path.write_bytes(response.content)
    return path


def _municipality_code_column(boundaries: gpd.GeoDataFrame) -> str:
    candidates = ["codarea", "CD_MUN", "CD_MUNICIP", "code_muni"]
    for column in candidates:
        if column in boundaries.columns:
            return column
    raise ValueError(
        "A malha não contém uma coluna municipal reconhecida. "
        f"Colunas disponíveis: {boundaries.columns.tolist()}"
    )


def load_spatial_data(
    data_path: Path = DEFAULT_DATA_PATH,
    boundary_path: Path = DEFAULT_BOUNDARY_PATH,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame]:
    """Carrega, normaliza e vincula a base analítica à malha municipal.

    Retorna a malha completa, o subconjunto com dados para todas as variáveis
    analisadas e uma pequena tabela de auditoria da integração.
    """
    boundary_path = ensure_boundary_cache(boundary_path)
    boundaries = gpd.read_file(boundary_path)
    code_column = _municipality_code_column(boundaries)
    boundaries = boundaries[[code_column, "geometry"]].copy()
    boundaries["codigo_municipio"] = (
        boundaries[code_column].astype("string").str.strip().str.slice(0, 6)
    )
    boundaries = boundaries.drop(columns=code_column)

    invalid_geometry = ~boundaries.geometry.is_valid
    if invalid_geometry.any():
        boundaries.loc[invalid_geometry, "geometry"] = boundaries.loc[
            invalid_geometry, "geometry"
        ].buffer(0)

    if boundaries["codigo_municipio"].duplicated().any():
        raise ValueError("A malha contém códigos municipais duplicados.")
    if boundaries.geometry.isna().any() or boundaries.geometry.is_empty.any():
        raise ValueError("A malha contém geometrias nulas ou vazias.")
    if not boundaries.geometry.is_valid.all():
        raise ValueError("A malha ainda contém geometrias inválidas após o reparo.")

    indicators = pd.read_parquet(data_path)
    indicators["codigo_municipio"] = (
        indicators["codigo_municipio"].astype("string").str.strip().str.zfill(6)
    )
    if indicators["codigo_municipio"].duplicated().any():
        raise ValueError("A base analítica contém códigos municipais duplicados.")

    full_map = boundaries.merge(
        indicators,
        on="codigo_municipio",
        how="left",
        validate="one_to_one",
        indicator="_origem_integracao",
    )
    full_map["situacao_dado"] = np.where(
        full_map["_origem_integracao"].eq("both"),
        "Com registro SINAN",
        "Sem registro SINAN",
    )
    full_map = full_map.drop(columns="_origem_integracao")

    missing_geometry = indicators.loc[
        ~indicators["codigo_municipio"].isin(boundaries["codigo_municipio"]),
        "codigo_municipio",
    ]
    analysis_map = full_map.dropna(subset=list(ANALYSIS_VARIABLES)).copy()
    analysis_map = analysis_map.sort_values("codigo_municipio").reset_index(drop=True)

    audit = pd.DataFrame(
        {
            "metrica": [
                "municipios_malha",
                "municipios_base_analitica",
                "municipios_completos_analise",
                "municipios_base_sem_geometria",
                "cobertura_percentual_malha",
            ],
            "valor": [
                len(boundaries),
                len(indicators),
                len(analysis_map),
                len(missing_geometry),
                len(analysis_map) / len(boundaries) * 100,
            ],
        }
    )
    return full_map, analysis_map, audit


def build_knn_weights(
    analysis_map: gpd.GeoDataFrame,
    k: int = 8,
    projected_crs: str = "EPSG:5880",
) -> W:
    """Constrói pesos KNN sobre centroides em projeção métrica para o Brasil."""
    if len(analysis_map) <= k:
        raise ValueError(f"São necessárias mais de {k} observações para KNN.")

    projected = analysis_map.to_crs(projected_crs)
    centroids = projected.geometry.centroid
    coordinates = np.column_stack([centroids.x.to_numpy(), centroids.y.to_numpy()])
    identifiers = analysis_map["codigo_municipio"].astype(str).tolist()
    weights = KNN.from_array(coordinates, k=k, ids=identifiers)
    weights.transform = "R"
    return weights


def _aligned_values(
    analysis_map: gpd.GeoDataFrame,
    weights: W,
    variable: str,
) -> np.ndarray:
    indexed = analysis_map.set_index("codigo_municipio")
    return indexed.loc[weights.id_order, variable].to_numpy(dtype=float)


def calculate_global_moran(
    analysis_map: gpd.GeoDataFrame,
    weights: W,
    permutations: int = 999,
    seed: int = 42,
) -> tuple[pd.DataFrame, dict[str, Moran]]:
    """Calcula Moran global para dengue e três indicadores de saneamento."""
    rows: list[dict[str, float | int | str]] = []
    models: dict[str, Moran] = {}
    for offset, (variable, label) in enumerate(ANALYSIS_VARIABLES.items()):
        np.random.seed(seed + offset)
        values = _aligned_values(analysis_map, weights, variable)
        model = Moran(values, weights, permutations=permutations)
        models[variable] = model
        rows.append(
            {
                "variavel": variable,
                "indicador": label,
                "n": len(values),
                "media": float(np.mean(values)),
                "desvio_padrao": float(np.std(values, ddof=1)),
                "moran_i": float(model.I),
                "moran_esperado": float(model.EI),
                "p_sim": float(model.p_sim),
                "z_sim": float(model.z_sim),
                "permutacoes": permutations,
            }
        )
    return pd.DataFrame(rows), models


def calculate_bivariate_moran(
    analysis_map: gpd.GeoDataFrame,
    weights: W,
    permutations: int = 999,
    seed: int = 142,
) -> tuple[pd.DataFrame, dict[str, Moran_BV]]:
    """Relaciona dengue local ao saneamento dos municípios vizinhos."""
    dengue = _aligned_values(analysis_map, weights, "taxa_confirmados_100k")
    rows: list[dict[str, float | int | str]] = []
    models: dict[str, Moran_BV] = {}
    for offset, (variable, label) in enumerate(SANITATION_VARIABLES.items()):
        np.random.seed(seed + offset)
        sanitation = _aligned_values(analysis_map, weights, variable)
        model = Moran_BV(dengue, sanitation, weights, permutations=permutations)
        models[variable] = model
        rows.append(
            {
                "variavel_vizinhanca": variable,
                "indicador_vizinhanca": label,
                "n": len(dengue),
                "moran_bivariado_i": float(model.I),
                "p_sim": float(model.p_sim),
                "z_sim": float(model.z_sim),
                "permutacoes": permutations,
            }
        )
    return pd.DataFrame(rows), models


def calculate_local_moran(
    analysis_map: gpd.GeoDataFrame,
    weights: W,
    variable: str = "taxa_confirmados_100k",
    permutations: int = 999,
    seed: int = 242,
    alpha: float = 0.05,
) -> tuple[gpd.GeoDataFrame, Moran_Local]:
    """Calcula LISA e adiciona quadrantes significativos ao GeoDataFrame."""
    values = _aligned_values(analysis_map, weights, variable)
    model = Moran_Local(
        values,
        weights,
        permutations=permutations,
        seed=seed,
        n_jobs=1,
    )
    ordered = analysis_map.set_index("codigo_municipio").loc[weights.id_order].copy()
    significant_quadrant = np.where(model.p_sim < alpha, model.q, 0)
    ordered["lisa_i"] = model.Is
    ordered["lisa_p_sim"] = model.p_sim
    ordered["lisa_quadrante"] = significant_quadrant
    ordered["lisa_cluster"] = pd.Series(
        significant_quadrant, index=ordered.index
    ).map(LISA_LABELS)
    ordered["lisa_significativo"] = model.p_sim < alpha
    return ordered.reset_index(), model


def calculate_knn_sensitivity(
    analysis_map: gpd.GeoDataFrame,
    neighbors: tuple[int, ...] = (4, 8, 12, 16),
    variable: str = "taxa_confirmados_100k",
    permutations: int = 999,
    seed: int = 342,
) -> pd.DataFrame:
    """Verifica se o Moran global é estável a diferentes números de vizinhos."""
    rows: list[dict[str, float | int]] = []
    for offset, k in enumerate(neighbors):
        weights = build_knn_weights(analysis_map, k=k)
        values = _aligned_values(analysis_map, weights, variable)
        np.random.seed(seed + offset)
        model = Moran(values, weights, permutations=permutations)
        rows.append(
            {
                "k_vizinhos": k,
                "moran_i": float(model.I),
                "p_sim": float(model.p_sim),
                "z_sim": float(model.z_sim),
                "permutacoes": permutations,
            }
        )
    return pd.DataFrame(rows)


def _map_axes(title: str, figsize: tuple[int, int] = (12, 10)):
    fig, ax = plt.subplots(figsize=figsize)
    fig.patch.set_facecolor(BACKGROUND)
    ax.set_facecolor(BACKGROUND)
    ax.set_title(title, loc="left", fontsize=15, color=INK, pad=14, weight="bold")
    ax.set_axis_off()
    return fig, ax


def plot_data_coverage(full_map: gpd.GeoDataFrame):
    """Mapeia municípios presentes e ausentes no recorte SINAN 2022."""
    fig, ax = _map_axes("Cobertura municipal da base integrada — 2022")
    full_map.loc[full_map["situacao_dado"].eq("Sem registro SINAN")].plot(
        ax=ax, color=NEUTRAL, edgecolor="white", linewidth=0.08
    )
    full_map.loc[full_map["situacao_dado"].eq("Com registro SINAN")].plot(
        ax=ax, color=BLUE, edgecolor="white", linewidth=0.08
    )
    handles = [
        Line2D([0], [0], marker="s", linestyle="", color=BLUE, markersize=9),
        Line2D([0], [0], marker="s", linestyle="", color=NEUTRAL, markersize=9),
    ]
    ax.legend(
        handles,
        ["Com registro SINAN", "Sem registro SINAN"],
        title="Situação",
        loc="lower left",
        frameon=False,
    )
    return fig, ax


def plot_rate_choropleth(full_map: gpd.GeoDataFrame):
    """Cria coroplético quantílico da taxa de casos confirmados."""
    fig, ax = _map_axes("Taxa de casos confirmados de dengue — 2022")
    full_map.plot(
        column="taxa_confirmados_100k",
        scheme="Quantiles",
        k=5,
        cmap="Blues",
        linewidth=0.08,
        edgecolor="white",
        legend=True,
        legend_kwds={"title": "Casos por 100 mil", "loc": "lower left", "frameon": False},
        missing_kwds={
            "color": NEUTRAL,
            "edgecolor": "white",
            "label": "Sem registro SINAN",
        },
        ax=ax,
    )
    return fig, ax


def plot_global_moran_result(model: Moran, title: str):
    """Usa splot para mostrar distribuição permutada e dispersão de Moran."""
    fig, axes = plot_moran(
        model,
        figsize=(13, 5),
        scatter_kwds={"color": BLUE, "alpha": 0.45, "s": 16, "edgecolor": "none"},
        fitline_kwds={"color": GOLD, "linewidth": 2.2},
    )
    fig.patch.set_facecolor(BACKGROUND)
    fig.suptitle(title, x=0.04, ha="left", fontsize=15, color=INK, weight="bold")
    for ax in np.ravel(axes):
        ax.set_facecolor(BACKGROUND)
    axes[0].set_title("Distribuição de referência")
    axes[0].set_xlabel(f"Moran I: {model.I:.2f}")
    axes[0].set_ylabel("Densidade")
    axes[1].set_title(f"Diagrama de dispersão de Moran ({model.I:.2f})")
    axes[1].set_xlabel("Taxa de dengue padronizada")
    axes[1].set_ylabel("Defasagem espacial padronizada")
    fig.tight_layout()
    return fig, axes


def plot_bivariate_moran_result(model: Moran_BV, title: str):
    """Usa splot para mostrar Moran bivariado e sua distribuição permutada."""
    fig, axes = plot_moran_bv(
        model,
        figsize=(13, 5),
        scatter_kwds={"color": BLUE_DARK, "alpha": 0.45, "s": 16, "edgecolor": "none"},
        fitline_kwds={"color": GOLD, "linewidth": 2.2},
    )
    fig.patch.set_facecolor(BACKGROUND)
    fig.suptitle(title, x=0.04, ha="left", fontsize=15, color=INK, weight="bold")
    for ax in np.ravel(axes):
        ax.set_facecolor(BACKGROUND)
    axes[0].set_title("Distribuição de referência")
    axes[0].set_xlabel(f"Moran bivariado I: {model.I:.2f}")
    axes[0].set_ylabel("Densidade")
    axes[1].set_title(f"Diagrama de Moran bivariado ({model.I:.2f})")
    axes[1].set_xlabel("Taxa de dengue padronizada")
    axes[1].set_ylabel("Defasagem espacial do esgotamento")
    fig.tight_layout()
    return fig, axes


def plot_lisa_clusters(
    full_map: gpd.GeoDataFrame,
    lisa_map: gpd.GeoDataFrame,
    model: Moran_Local,
    alpha: float = 0.05,
):
    """Usa splot para mapear agrupamentos LISA significativos."""
    fig, ax = _map_axes("Clusters LISA da taxa de dengue — 2022")
    full_map.plot(ax=ax, color="#eef0f3", edgecolor="white", linewidth=0.08)
    lisa_cluster(
        model,
        lisa_map.copy(),
        p=alpha,
        ax=ax,
        legend=True,
        legend_kwds={"loc": "lower left", "frameon": False},
    )
    ax.set_title(
        f"Clusters LISA da taxa de dengue — 2022 (p simulado < {alpha:.2f})",
        loc="left",
        fontsize=15,
        color=INK,
        pad=14,
        weight="bold",
    )
    legend = ax.get_legend()
    if legend is not None:
        translated_labels = {
            "HH": "Alto–Alto",
            "HL": "Alto–Baixo",
            "LH": "Baixo–Alto",
            "LL": "Baixo–Baixo",
            "ns": "Não significativo",
        }
        for text in legend.get_texts():
            text.set_text(translated_labels.get(text.get_text(), text.get_text()))
    return fig, ax


def save_figure(fig, path: Path, dpi: int = 180) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=dpi, bbox_inches="tight", facecolor=fig.get_facecolor())


def run_analysis(
    data_path: Path = DEFAULT_DATA_PATH,
    boundary_path: Path = DEFAULT_BOUNDARY_PATH,
    figure_dir: Path = DEFAULT_FIGURE_DIR,
    result_dir: Path = DEFAULT_RESULT_DIR,
    k: int = 8,
    permutations: int = 999,
    seed: int = 42,
) -> dict[str, object]:
    """Executa a EDA espacial, salva resultados tabulares e exporta figuras."""
    full_map, analysis_map, audit = load_spatial_data(data_path, boundary_path)
    weights = build_knn_weights(analysis_map, k=k)
    global_table, global_models = calculate_global_moran(
        analysis_map, weights, permutations=permutations, seed=seed
    )
    bivariate_table, bivariate_models = calculate_bivariate_moran(
        analysis_map, weights, permutations=permutations, seed=seed + 100
    )
    lisa_map, lisa_model = calculate_local_moran(
        analysis_map,
        weights,
        permutations=permutations,
        seed=seed + 200,
    )
    sensitivity = calculate_knn_sensitivity(
        analysis_map,
        permutations=permutations,
        seed=seed + 300,
    )

    result_dir.mkdir(parents=True, exist_ok=True)
    audit.to_csv(result_dir / "auditoria_geografica_2022.csv", index=False)
    global_table.to_csv(result_dir / "moran_global_2022.csv", index=False)
    bivariate_table.to_csv(result_dir / "moran_bivariado_2022.csv", index=False)
    sensitivity.to_csv(result_dir / "sensibilidade_knn_2022.csv", index=False)
    lisa_map.drop(columns="geometry").to_csv(
        result_dir / "lisa_taxa_confirmados_2022.csv", index=False
    )

    figures = []
    fig, _ = plot_data_coverage(full_map)
    figures.append(fig)
    save_figure(fig, figure_dir / "01_cobertura_sinan_2022.png")

    fig, _ = plot_rate_choropleth(full_map)
    figures.append(fig)
    save_figure(fig, figure_dir / "02_taxa_confirmados_2022.png")

    fig, _ = plot_global_moran_result(
        global_models["taxa_confirmados_100k"],
        "Autocorrelação espacial global da taxa de dengue — 2022",
    )
    figures.append(fig)
    save_figure(fig, figure_dir / "03_moran_global_taxa_2022.png")

    fig, _ = plot_lisa_clusters(full_map, lisa_map, lisa_model)
    figures.append(fig)
    save_figure(fig, figure_dir / "04_lisa_taxa_2022.png")

    fig, _ = plot_bivariate_moran_result(
        bivariate_models["pct_esgoto_rede_geral_ou_pluvial"],
        "Moran bivariado: dengue e esgotamento da vizinhança — 2022",
    )
    figures.append(fig)
    save_figure(fig, figure_dir / "05_moran_bivariado_esgoto_2022.png")

    for figure in figures:
        plt.close(figure)

    return {
        "malha_completa": full_map,
        "base_analise": analysis_map,
        "auditoria": audit,
        "pesos": weights,
        "moran_global": global_table,
        "modelos_moran_global": global_models,
        "moran_bivariado": bivariate_table,
        "modelos_moran_bivariado": bivariate_models,
        "lisa": lisa_map,
        "modelo_lisa": lisa_model,
        "sensibilidade_knn": sensitivity,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA_PATH)
    parser.add_argument("--boundaries", type=Path, default=DEFAULT_BOUNDARY_PATH)
    parser.add_argument("--figures", type=Path, default=DEFAULT_FIGURE_DIR)
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULT_DIR)
    parser.add_argument("--neighbors", type=int, default=8)
    parser.add_argument("--permutations", type=int, default=999)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    outputs = run_analysis(
        data_path=args.data,
        boundary_path=args.boundaries,
        figure_dir=args.figures,
        result_dir=args.results,
        k=args.neighbors,
        permutations=args.permutations,
        seed=args.seed,
    )
    print(outputs["auditoria"].to_string(index=False))
    print(outputs["moran_global"].to_string(index=False))
    print(outputs["moran_bivariado"].to_string(index=False))
    print(outputs["sensibilidade_knn"].to_string(index=False))


if __name__ == "__main__":
    main()
