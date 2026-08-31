"""Pipeline reproduzível para analisar e integrar SINAN e IBGE/SIDRA.

O módulo concentra a lógica pesada usada pelos notebooks. As notificações são
processadas com Polars LazyFrame; apenas agregações municipais são coletadas.
"""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path
from typing import Iterable

import httpx
import polars as pl


SINAN_DIR = Path("sinan")
PROCESSED_DIR = SINAN_DIR / "processados"
SIDRA_API = "https://apisidra.ibge.gov.br/values"

POPULATION_YEARS = [
    2001,
    2002,
    2003,
    2004,
    2005,
    2006,
    2008,
    2009,
    2011,
    2012,
    2013,
    2014,
    2015,
    2016,
    2017,
    2018,
    2019,
    2020,
    2021,
]

WATER_CATEGORIES = {
    "92853": "pct_agua_rede_geral",
    "10971": "pct_agua_poco_nascente_propriedade",
    "121290": "pct_agua_poco_nascente_fora_propriedade",
    "121294": "pct_agua_rio_acude_lago_igarape",
    "121296": "pct_agua_poco_nascente_aldeia",
    "121297": "pct_agua_poco_nascente_fora_aldeia",
    "121295": "pct_agua_outra_forma",
}

SANITATION_CATEGORIES = {
    "9678": "pct_banheiro_exclusivo_rede_ou_fossa_septica",
    "2950": "pct_banheiro_exclusivo_outro_escoadouro",
    "9679": "pct_sanitario_rede_ou_fossa_septica",
    "2964": "pct_sanitario_outro_escoadouro",
    "10006": "pct_sem_banheiro_ou_sanitario",
}

WASTE_CATEGORIES = {
    "92863": "pct_lixo_servico_limpeza",
    "92864": "pct_lixo_cacamba_servico_limpeza",
    "1091": "pct_lixo_outro_destino",
}

ENERGY_CATEGORIES = {
    "3011": "pct_com_energia_eletrica",
    "3018": "pct_sem_energia_eletrica",
}

WATER_2022_CATEGORIES = {
    "72144": "pct_agua_rede_geral_principal",
    "72146": "pct_agua_ligada_rede_poco_profundo_principal",
    "72147": "pct_agua_ligada_rede_poco_raso_principal",
    "72148": "pct_agua_ligada_rede_nascente_principal",
    "72149": "pct_agua_ligada_rede_carro_pipa_principal",
    "72150": "pct_agua_ligada_rede_chuva_principal",
    "72151": "pct_agua_ligada_rede_superficial_principal",
    "72152": "pct_agua_ligada_rede_outra_principal",
    "72154": "pct_agua_sem_rede_poco_profundo",
    "72155": "pct_agua_sem_rede_poco_raso",
    "72156": "pct_agua_sem_rede_nascente",
    "72157": "pct_agua_sem_rede_carro_pipa",
    "72158": "pct_agua_sem_rede_chuva",
    "72159": "pct_agua_sem_rede_superficial",
    "72160": "pct_agua_sem_rede_outra",
}

SANITATION_2022_CATEGORIES = {
    "72110": "pct_esgoto_rede_geral_ou_pluvial",
    "72111": "pct_esgoto_fossa_ligada_rede",
    "72112": "pct_esgoto_fossa_nao_ligada_rede",
    "72113": "pct_esgoto_fossa_rudimentar",
    "92858": "pct_esgoto_vala",
    "72114": "pct_esgoto_rio_lago_corrego_mar",
    "72115": "pct_esgoto_outra_forma",
    "92861": "pct_sem_banheiro_ou_sanitario",
}

WASTE_2022_CATEGORIES = {
    "72120": "pct_lixo_coletado_domicilio",
    "72121": "pct_lixo_cacamba",
    "72122": "pct_lixo_queimado_propriedade",
    "72123": "pct_lixo_enterrado_propriedade",
    "72124": "pct_lixo_terreno_encosta_area_publica",
    "1091": "pct_lixo_outro_destino",
}


def _year_from_filename(path: Path) -> int:
    match = re.search(r"DENGBR(\d{2})\.parquet$", path.name)
    if not match:
        raise ValueError(f"Nome de arquivo SINAN inesperado: {path.name}")
    return 2000 + int(match.group(1))


def _raw_column(schema: dict[str, pl.DataType], name: str) -> pl.Expr:
    if name in schema:
        return pl.col(name).cast(pl.String).str.strip_chars()
    return pl.lit(None, dtype=pl.String)


def _classification_status(raw: pl.Expr, source: str) -> pl.Expr:
    confirmed = ["1", "2", "3", "4", "10", "11", "12"] if source == "CLASSI_FIN" else ["1"]
    return (
        pl.when(raw.is_null() | (raw == ""))
        .then(pl.lit("nao_preenchido"))
        .when(raw.is_in(confirmed))
        .then(pl.lit("confirmado"))
        .when(raw == "5" if source == "CLASSI_FIN" else raw == "2")
        .then(pl.lit("descartado"))
        .when(raw == "8")
        .then(pl.lit("inconclusivo"))
        .when(raw == "9")
        .then(pl.lit("ignorado"))
        .otherwise(pl.lit("outro_codigo"))
    )


def _race_status(raw: pl.Expr) -> pl.Expr:
    return (
        pl.when(raw.is_null() | raw.is_in(["", "@", "]"]))
        .then(pl.lit("nao_preenchido"))
        .when(raw == "1")
        .then(pl.lit("branca"))
        .when(raw == "2")
        .then(pl.lit("preta"))
        .when(raw == "3")
        .then(pl.lit("amarela"))
        .when(raw == "4")
        .then(pl.lit("parda"))
        .when(raw == "5")
        .then(pl.lit("indigena"))
        .when(raw == "9")
        .then(pl.lit("ignorado"))
        .otherwise(pl.lit("outro_codigo"))
    )


def _evolution_status(raw: pl.Expr) -> pl.Expr:
    return (
        pl.when(raw.is_null() | raw.is_in(["", "]", "@"]))
        .then(pl.lit("nao_preenchido"))
        .when(raw == "0")
        .then(pl.lit("nao_se_aplica"))
        .when(raw == "1")
        .then(pl.lit("cura"))
        .when(raw == "2")
        .then(pl.lit("obito_pelo_agravo"))
        .when(raw == "3")
        .then(pl.lit("obito_outras_causas"))
        .when(raw == "4")
        .then(pl.lit("obito_em_investigacao"))
        .when(raw == "9")
        .then(pl.lit("ignorado"))
        .otherwise(pl.lit("outro_codigo"))
    )


def scan_sinan(source_dir: Path = SINAN_DIR) -> pl.LazyFrame:
    """Harmoniza todos os arquivos SINAN sem descartar colunas por era."""
    paths = sorted(source_dir.glob("DENGBR*.parquet"))
    if not paths:
        raise FileNotFoundError(f"Nenhum DENGBR*.parquet encontrado em {source_dir}")

    frames: list[pl.LazyFrame] = []
    for path in paths:
        schema = dict(pl.read_parquet_schema(path))
        year = _year_from_filename(path)
        class_source = "CLASSI_FIN" if "CLASSI_FIN" in schema else "DENGUE"
        evolution_source = "EVOLUCAO" if "EVOLUCAO" in schema else "CON_EVOLUC"
        date_raw = _raw_column(schema, "DT_NOTIFIC")
        code_raw = _raw_column(schema, "ID_MUNICIP")
        class_raw = _raw_column(schema, class_source)
        race_raw = _raw_column(schema, "CS_RACA")
        evolution_raw = _raw_column(schema, evolution_source)
        reported_year = _raw_column(schema, "NU_ANO").cast(pl.Int32, strict=False)

        date_parsed = pl.coalesce(
            date_raw.str.to_date("%Y-%m-%d", strict=False),
            date_raw.str.to_date("%Y%m%d", strict=False),
        )
        normalized_code = (
            pl.when(code_raw.str.contains(r"^\d{7}$"))
            .then(code_raw.str.slice(0, 6))
            .when(code_raw.str.contains(r"^\d{6}$"))
            .then(code_raw)
            .otherwise(pl.lit(None, dtype=pl.String))
        )

        frame = (
            pl.scan_parquet(path)
            .select(
                pl.lit(path.name).alias("arquivo"),
                pl.lit(year, dtype=pl.Int32).alias("ano_arquivo"),
                reported_year.alias("ano_informado"),
                date_raw.alias("data_notificacao_original"),
                date_parsed.alias("data_notificacao"),
                code_raw.alias("codigo_municipio_original"),
                normalized_code.alias("codigo_municipio"),
                pl.lit(class_source).alias("fonte_classificacao"),
                class_raw.alias("classificacao_original"),
                _classification_status(class_raw, class_source).alias("status_classificacao"),
                race_raw.alias("raca_original"),
                _race_status(race_raw).alias("raca"),
                evolution_raw.alias("evolucao_original"),
                _evolution_status(evolution_raw).alias("evolucao"),
            )
            .with_columns(
                (pl.col("status_classificacao") == "confirmado").alias("caso_confirmado"),
                pl.col("data_notificacao").dt.month().alias("mes_notificacao"),
            )
        )
        frames.append(frame)
    return pl.concat(frames, how="vertical_relaxed")


def build_sinan_outputs(
    source_dir: Path = SINAN_DIR,
    output_dir: Path = PROCESSED_DIR,
) -> dict[str, pl.DataFrame]:
    """Gera painel municipal e tabelas compactas para a EDA."""
    output_dir.mkdir(parents=True, exist_ok=True)
    sinan = scan_sinan(source_dir)

    quality = (
        sinan.group_by("arquivo", "ano_arquivo")
        .agg(
            pl.len().alias("registros"),
            pl.col("codigo_municipio").is_null().sum().alias("codigo_municipio_invalido"),
            pl.col("data_notificacao").is_null().sum().alias("data_invalida"),
            (pl.col("ano_informado") != pl.col("ano_arquivo")).sum().alias("ano_informado_divergente"),
            (
                pl.col("data_notificacao").is_not_null()
                & (pl.col("data_notificacao").dt.year() != pl.col("ano_arquivo"))
            )
            .sum()
            .alias("ano_data_divergente"),
        )
        .sort("ano_arquivo")
        .collect(engine="streaming")
    )

    panel = (
        sinan.filter(pl.col("codigo_municipio").is_not_null())
        .group_by("ano_arquivo", "codigo_municipio")
        .agg(
            pl.len().alias("notificacoes"),
            pl.col("caso_confirmado").sum().cast(pl.UInt32).alias("casos_confirmados"),
            pl.col("data_notificacao").is_null().sum().cast(pl.UInt32).alias("notificacoes_data_invalida"),
        )
        .rename({"ano_arquivo": "ano"})
        .sort("ano", "codigo_municipio")
        .collect(engine="streaming")
    )

    yearly_classification = (
        sinan.group_by("ano_arquivo", "status_classificacao")
        .len(name="registros")
        .rename({"ano_arquivo": "ano"})
        .sort("ano", "status_classificacao")
        .collect(engine="streaming")
    )
    yearly_race = (
        sinan.group_by("ano_arquivo", "raca")
        .len(name="registros")
        .rename({"ano_arquivo": "ano"})
        .sort("ano", "raca")
        .collect(engine="streaming")
    )
    yearly_evolution = (
        sinan.group_by("ano_arquivo", "evolucao")
        .len(name="registros")
        .rename({"ano_arquivo": "ano"})
        .sort("ano", "evolucao")
        .collect(engine="streaming")
    )
    monthly = (
        sinan.filter(
            pl.col("data_notificacao").is_not_null()
            & (pl.col("data_notificacao").dt.year() == pl.col("ano_arquivo"))
        )
        .group_by("ano_arquivo", "mes_notificacao")
        .agg(
            pl.len().alias("notificacoes"),
            pl.col("caso_confirmado").sum().cast(pl.UInt32).alias("casos_confirmados"),
        )
        .rename({"ano_arquivo": "ano", "mes_notificacao": "mes"})
        .sort("ano", "mes")
        .collect(engine="streaming")
    )

    panel.write_parquet(output_dir / "sinan_municipio_ano.parquet")
    quality.write_csv(output_dir / "qualidade_sinan_por_arquivo.csv")
    yearly_classification.write_csv(output_dir / "eda_sinan_classificacao_ano.csv")
    yearly_race.write_csv(output_dir / "eda_sinan_raca_ano.csv")
    yearly_evolution.write_csv(output_dir / "eda_sinan_evolucao_ano.csv")
    monthly.write_csv(output_dir / "eda_sinan_mensal.csv")
    return {
        "painel": panel,
        "qualidade": quality,
        "classificacao": yearly_classification,
        "raca": yearly_race,
        "evolucao": yearly_evolution,
        "mensal": monthly,
    }


def _fetch_sidra(url: str, attempts: int = 4) -> pl.DataFrame:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            with httpx.Client(timeout=180, follow_redirects=True) as client:
                response = client.get(url)
                response.raise_for_status()
                rows = response.json()
            if not isinstance(rows, list) or len(rows) < 2:
                raise ValueError(f"Resposta SIDRA vazia ou inesperada: {url}")
            return pl.DataFrame(rows[1:]).rename(rows[0])
        except (httpx.HTTPError, ValueError) as error:
            last_error = error
            if attempt + 1 < attempts:
                time.sleep(2**attempt)
    raise RuntimeError(f"Falha ao consultar SIDRA depois de {attempts} tentativas: {url}") from last_error


def _sidra_value(column: str = "Valor") -> pl.Expr:
    return (
        pl.when(pl.col(column).str.strip_chars() == "-")
        .then(pl.lit("0"))
        .otherwise(pl.col(column).str.strip_chars())
        .cast(pl.Float64, strict=False)
    )


def _normalize_sidra(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        pl.col("Município (Código)").str.slice(0, 6).alias("codigo_municipio"),
        pl.col("Município").str.replace(r"\s+\([A-Z]{2}\)$", "").alias("municipio"),
        pl.col("Ano").cast(pl.Int32, strict=False).alias("ano"),
        _sidra_value().alias("valor"),
    )


def _chunks(values: list[int], size: int) -> Iterable[list[int]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def fetch_population() -> pl.DataFrame:
    frames = []
    for years in _chunks(POPULATION_YEARS, 7):
        periods = ",".join(map(str, years))
        url = f"{SIDRA_API}/t/6579/n6/all/v/9324/p/{periods}"
        frames.append(_normalize_sidra(_fetch_sidra(url)))
    estimates = (
        pl.concat(frames, how="diagonal_relaxed")
        .select(
            "ano",
            "codigo_municipio",
            "municipio",
            pl.col("valor").cast(pl.UInt64, strict=False).alias("populacao"),
            pl.lit("SIDRA 6579 - estimativa populacional").alias("fonte_populacao"),
        )
        .filter(pl.col("ano").is_in(POPULATION_YEARS))
        .unique(["ano", "codigo_municipio"], keep="first")
        .sort("ano", "codigo_municipio")
    )
    census_url = f"{SIDRA_API}/t/4709/n6/all/v/93/p/2022"
    census = (
        _normalize_sidra(_fetch_sidra(census_url))
        .select(
            "ano",
            "codigo_municipio",
            "municipio",
            pl.col("valor").cast(pl.UInt64, strict=False).alias("populacao"),
            pl.lit("SIDRA 4709 - Censo 2022").alias("fonte_populacao"),
        )
        .unique(["ano", "codigo_municipio"], keep="first")
    )
    return pl.concat([estimates, census], how="vertical_relaxed").sort("ano", "codigo_municipio")


def _fetch_3218_dimension(name: str, classifications: str) -> pl.DataFrame:
    url = f"{SIDRA_API}/t/3218/n6/all/v/1000096/p/2010/{classifications}"
    return _normalize_sidra(_fetch_sidra(url)).with_columns(pl.lit(name).alias("dimensao"))


def fetch_household_sanitation_2010() -> tuple[pl.DataFrame, pl.DataFrame]:
    configurations = {
        "agua": "c61/all/c299/0/c67/0/c309/0",
        "esgotamento": "c61/0/c299/all/c67/0/c309/0",
        "lixo": "c61/0/c299/0/c67/all/c309/0",
        "energia": "c61/0/c299/0/c67/0/c309/all",
    }
    code_columns = {
        "agua": "Forma de abastecimento de água (Código)",
        "esgotamento": "Existência de banheiro ou sanitário e esgotamento sanitário (Código)",
        "lixo": "Destino do lixo (Código)",
        "energia": "Existência de energia elétrica (Código)",
    }
    mappings = {
        "agua": WATER_CATEGORIES,
        "esgotamento": SANITATION_CATEGORIES,
        "lixo": WASTE_CATEGORIES,
        "energia": ENERGY_CATEGORIES,
    }

    long_frames = []
    for dimension, classifications in configurations.items():
        frame = _fetch_3218_dimension(dimension, classifications)
        code_column = code_columns[dimension]
        mapping = mappings[dimension]
        long_frames.append(
            frame.filter(pl.col(code_column).is_in(mapping))
            .with_columns(pl.col(code_column).replace_strict(mapping).alias("indicador"))
            .select("ano", "codigo_municipio", "municipio", "dimensao", "indicador", "valor")
        )
    long = pl.concat(long_frames, how="vertical_relaxed").sort("codigo_municipio", "dimensao", "indicador")
    wide = (
        long.pivot(
            on="indicador",
            index=["ano", "codigo_municipio", "municipio"],
            values="valor",
            aggregate_function="first",
        )
        .sort("codigo_municipio")
    )
    return long, wide


def fetch_household_sanitation_2022() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Combina água (6803), esgoto (6805) e lixo (6892) do Censo 2022."""
    configurations = [
        (6803, 1821, "Existência de ligação à rede geral de distribuição de água e principal forma de abastecimento de água (Código)", WATER_2022_CATEGORIES, "agua"),
        (6805, 11558, "Tipo de esgotamento sanitário (Código)", SANITATION_2022_CATEGORIES, "esgotamento"),
        (6892, 67, "Destino do lixo (Código)", WASTE_2022_CATEGORIES, "lixo"),
    ]
    frames = []
    for table, classification, code_column, mapping, dimension in configurations:
        category_codes = list(mapping)
        for category_chunk in _chunks([int(code) for code in category_codes], 7):
            categories = ",".join(map(str, category_chunk))
            url = f"{SIDRA_API}/t/{table}/n6/all/v/1000381/p/2022/c{classification}/{categories}"
            frame = _normalize_sidra(_fetch_sidra(url))
            frames.append(
                frame.filter(pl.col(code_column).is_in(mapping))
                .with_columns(
                    pl.col(code_column).replace_strict(mapping).alias("indicador"),
                    pl.lit(dimension).alias("dimensao"),
                    pl.lit(table, dtype=pl.Int32).alias("tabela_sidra"),
                )
                .select(
                    "ano",
                    "codigo_municipio",
                    "municipio",
                    "tabela_sidra",
                    "dimensao",
                    "indicador",
                    "valor",
                )
            )
    long = pl.concat(frames, how="vertical_relaxed").sort("codigo_municipio", "dimensao", "indicador")
    wide = (
        long.pivot(
            on="indicador",
            index=["ano", "codigo_municipio", "municipio"],
            values="valor",
            aggregate_function="first",
        )
        .sort("codigo_municipio")
    )
    return long, wide


def build_ibge_outputs(
    output_dir: Path = PROCESSED_DIR,
    refresh: bool = False,
) -> dict[str, pl.DataFrame]:
    """Obtém ou reutiliza as três bases SIDRA municipais."""
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "populacao": output_dir / "ibge_populacao_municipio_ano.parquet",
        "saneamento_2010": output_dir / "ibge_saneamento_2010.parquet",
        "saneamento_2010_longo": output_dir / "ibge_saneamento_2010_longo.parquet",
        "saneamento_2022": output_dir / "ibge_saneamento_2022.parquet",
        "saneamento_2022_longo": output_dir / "ibge_saneamento_2022_longo.parquet",
    }
    if not refresh and all(path.exists() for path in paths.values()):
        return {name: pl.read_parquet(path) for name, path in paths.items()}

    population = fetch_population()
    sanitation_long, sanitation = fetch_household_sanitation_2010()
    sanitation_2022_long, sanitation_2022 = fetch_household_sanitation_2022()
    frames = {
        "populacao": population,
        "saneamento_2010": sanitation,
        "saneamento_2010_longo": sanitation_long,
        "saneamento_2022": sanitation_2022,
        "saneamento_2022_longo": sanitation_2022_long,
    }
    for name, frame in frames.items():
        frame.write_parquet(paths[name])
    return frames


def _municipality_catalog(ibge: dict[str, pl.DataFrame]) -> pl.DataFrame:
    return (
        pl.concat(
            [
                ibge["populacao"].select("codigo_municipio", "municipio"),
                ibge["saneamento_2010"].select("codigo_municipio", "municipio"),
                ibge["saneamento_2022"].select("codigo_municipio", "municipio"),
            ],
            how="vertical_relaxed",
        )
        .drop_nulls("codigo_municipio")
        .unique("codigo_municipio", keep="first")
    )


def build_integrated_outputs(
    source_dir: Path = SINAN_DIR,
    output_dir: Path = PROCESSED_DIR,
    refresh_ibge: bool = False,
) -> dict[str, pl.DataFrame]:
    """Agrega notificações e cria os três contratos analíticos finais."""
    sinan = build_sinan_outputs(source_dir, output_dir)
    ibge = build_ibge_outputs(output_dir, refresh=refresh_ibge)
    panel = sinan["painel"]
    catalog = _municipality_catalog(ibge)

    population_panel = (
        panel.join(catalog, on="codigo_municipio", how="left")
        .join(
            ibge["populacao"].drop("municipio"),
            on=["ano", "codigo_municipio"],
            how="left",
        )
        .with_columns(
            pl.when(pl.col("populacao") > 0)
            .then(pl.col("notificacoes") / pl.col("populacao") * 100_000)
            .otherwise(None)
            .alias("taxa_notificacoes_100k"),
            pl.when(pl.col("populacao") > 0)
            .then(pl.col("casos_confirmados") / pl.col("populacao") * 100_000)
            .otherwise(None)
            .alias("taxa_confirmados_100k"),
            pl.col("populacao").is_not_null().alias("correspondencia_populacao"),
        )
        .sort("ano", "codigo_municipio")
    )

    sanitation = (
        population_panel.filter(pl.col("ano") == 2010)
        .join(
            ibge["saneamento_2010"].drop("ano", "municipio"),
            on="codigo_municipio",
            how="left",
        )
        .with_columns(pl.col("pct_agua_rede_geral").is_not_null().alias("correspondencia_sidra"))
        .sort("codigo_municipio")
    )
    sanitation_2022 = (
        population_panel.filter(pl.col("ano") == 2022)
        .join(
            ibge["saneamento_2022"].drop("ano", "municipio"),
            on="codigo_municipio",
            how="left",
        )
        .with_columns(pl.col("pct_agua_rede_geral_principal").is_not_null().alias("correspondencia_sidra"))
        .sort("codigo_municipio")
    )

    population_panel.write_parquet(output_dir / "sinan_populacao_municipio_ano.parquet")
    sanitation.write_parquet(output_dir / "sinan_saneamento_2010.parquet")
    sanitation_2022.write_parquet(output_dir / "sinan_saneamento_2022.parquet")

    population_eligible = population_panel.filter(
        pl.col("ano").is_in([*POPULATION_YEARS, 2022])
    )
    audit = pl.DataFrame(
        {
            "base": ["populacao", "saneamento_2010", "saneamento_2022"],
            "linhas_totais": [population_panel.height, sanitation.height, sanitation_2022.height],
            "linhas_elegiveis": [population_eligible.height, sanitation.height, sanitation_2022.height],
            "correspondencias": [
                population_eligible["correspondencia_populacao"].sum(),
                sanitation["correspondencia_sidra"].sum(),
                sanitation_2022["correspondencia_sidra"].sum(),
            ],
        }
    ).with_columns(
        (pl.col("correspondencias") / pl.col("linhas_elegiveis") * 100)
        .alias("percentual_correspondencia")
    )
    audit.write_csv(output_dir / "auditoria_integracao.csv")
    return {
        **sinan,
        **ibge,
        "populacao_integrada": population_panel,
        "saneamento_integrado": sanitation,
        "saneamento_2022_integrado": sanitation_2022,
        "auditoria": audit,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh-ibge", action="store_true", help="refaz as consultas à API SIDRA")
    args = parser.parse_args()
    outputs = build_integrated_outputs(refresh_ibge=args.refresh_ibge)
    print(outputs["auditoria"])
    print(outputs["qualidade"].select(pl.exclude("arquivo").sum()))


if __name__ == "__main__":
    main()
