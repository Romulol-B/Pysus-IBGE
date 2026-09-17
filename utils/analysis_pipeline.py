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

CENSUS_MUNICIPALITY_COUNTS = {2010: 5_565, 2022: 5_570}

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

SANITATION_PERCENTAGE_GROUPS_2010 = {
    "agua": tuple(WATER_CATEGORIES.values()),
    "esgotamento": tuple(SANITATION_CATEGORIES.values()),
    "lixo": tuple(WASTE_CATEGORIES.values()),
    "energia": tuple(ENERGY_CATEGORIES.values()),
}

SANITATION_PERCENTAGE_GROUPS_2022 = {
    "agua": tuple(WATER_2022_CATEGORIES.values()),
    "esgotamento": tuple(SANITATION_2022_CATEGORIES.values()),
    "lixo": tuple(WASTE_2022_CATEGORIES.values()),
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


def _normalize_municipality_code(raw: pl.Expr) -> pl.Expr:
    """Converte códigos IBGE de 6/7 dígitos para a chave municipal de 6 dígitos.

    O SINAN armazena, em geral, seis dígitos e o SIDRA devolve sete (incluindo
    o dígito verificador). Valores com outro formato permanecem nulos para que
    sejam excluídos da agregação, sem desaparecer das tabelas de auditoria.
    """
    cleaned = raw.cast(pl.String).str.strip_chars()
    return (
        pl.when(cleaned.str.contains(r"^\d{7}$"))
        .then(cleaned.str.slice(0, 6))
        .when(cleaned.str.contains(r"^\d{6}$"))
        .then(cleaned)
        .otherwise(pl.lit(None, dtype=pl.String))
    )


def _assert_unique(frame: pl.DataFrame, keys: list[str], source: str) -> None:
    """Falha cedo quando uma dimensão poderia multiplicar linhas em um join."""
    missing = [key for key in keys if key not in frame.columns]
    if missing:
        raise ValueError(f"{source}: colunas-chave ausentes: {missing}")
    null_keys = frame.filter(pl.any_horizontal([pl.col(key).is_null() for key in keys]))
    if null_keys.height:
        raise ValueError(f"{source}: {null_keys.height} linha(s) com chave nula em {keys}")
    duplicates = frame.group_by(keys).len().filter(pl.col("len") > 1)
    if duplicates.height:
        sample = duplicates.head(5).to_dicts()
        raise ValueError(
            f"{source}: {duplicates.height} chave(s) duplicada(s) em {keys}; exemplo: {sample}"
        )


def _left_join_preserving_rows(
    left: pl.DataFrame,
    right: pl.DataFrame,
    on: list[str],
    source: str,
) -> pl.DataFrame:
    """Executa left join somente com dimensão única e confirma a cardinalidade."""
    _assert_unique(right, on, source)
    joined = left.join(right, on=on, how="left")
    if joined.height != left.height:
        raise RuntimeError(
            f"{source}: join alterou a grade de {left.height} para {joined.height} linhas"
        )
    return joined


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
        notification_code_raw = _raw_column(schema, "ID_MUNICIP")
        residence_code_raw = _raw_column(schema, "ID_MN_RESI")
        class_raw = _raw_column(schema, class_source)
        race_raw = _raw_column(schema, "CS_RACA")
        evolution_raw = _raw_column(schema, evolution_source)
        reported_year = _raw_column(schema, "NU_ANO").cast(pl.Int32, strict=False)

        date_parsed = pl.coalesce(
            date_raw.str.to_date("%Y-%m-%d", strict=False),
            date_raw.str.to_date("%Y%m%d", strict=False),
        )
        notification_code = _normalize_municipality_code(notification_code_raw)
        residence_code = _normalize_municipality_code(residence_code_raw)

        frame = (
            pl.scan_parquet(path)
            .select(
                pl.lit(path.name).alias("arquivo"),
                pl.lit(year, dtype=pl.Int32).alias("ano_arquivo"),
                reported_year.alias("ano_informado"),
                date_raw.alias("data_notificacao_original"),
                date_parsed.alias("data_notificacao"),
                # A chave analítica é sempre o município de residência. Os
                # aliases sem sufixo são mantidos para compatibilidade com os
                # notebooks existentes.
                residence_code_raw.alias("codigo_municipio_original"),
                residence_code.alias("codigo_municipio"),
                residence_code_raw.alias("codigo_municipio_residencia_original"),
                residence_code.alias("codigo_municipio_residencia"),
                notification_code_raw.alias("codigo_municipio_notificacao_original"),
                notification_code.alias("codigo_municipio_notificacao"),
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
                pl.col("codigo_municipio_residencia")
                .is_null()
                .alias("codigo_municipio_residencia_invalido"),
                pl.col("codigo_municipio_notificacao")
                .is_null()
                .alias("codigo_municipio_notificacao_invalido"),
                (
                    pl.col("codigo_municipio_residencia").is_not_null()
                    & pl.col("codigo_municipio_notificacao").is_not_null()
                    & (
                        pl.col("codigo_municipio_residencia")
                        != pl.col("codigo_municipio_notificacao")
                    )
                ).alias("municipio_residencia_notificacao_divergente"),
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
            pl.col("codigo_municipio_residencia_invalido")
            .sum()
            .alias("codigo_municipio_residencia_invalido"),
            pl.col("codigo_municipio_notificacao_invalido")
            .sum()
            .alias("codigo_municipio_notificacao_invalido"),
            # Compatibilidade: desde esta versão, "codigo_municipio" significa
            # explicitamente município de residência.
            pl.col("codigo_municipio_residencia_invalido")
            .sum()
            .alias("codigo_municipio_invalido"),
            pl.col("municipio_residencia_notificacao_divergente")
            .sum()
            .alias("municipio_residencia_notificacao_divergente"),
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
        sinan.filter(pl.col("codigo_municipio_residencia").is_not_null())
        .group_by("ano_arquivo", "codigo_municipio_residencia")
        .agg(
            pl.len().alias("notificacoes"),
            pl.col("caso_confirmado").sum().cast(pl.UInt32).alias("casos_confirmados"),
            pl.col("data_notificacao").is_null().sum().cast(pl.UInt32).alias("notificacoes_data_invalida"),
            pl.col("codigo_municipio_notificacao_invalido")
            .sum()
            .cast(pl.UInt32)
            .alias("notificacoes_codigo_notificacao_invalido"),
            pl.col("municipio_residencia_notificacao_divergente")
            .sum()
            .cast(pl.UInt32)
            .alias("notificacoes_municipio_notificacao_divergente"),
            pl.col("codigo_municipio_notificacao")
            .drop_nulls()
            .n_unique()
            .cast(pl.UInt32)
            .alias("municipios_notificacao_distintos"),
        )
        .rename(
            {
                "ano_arquivo": "ano",
                "codigo_municipio_residencia": "codigo_municipio",
            }
        )
        .sort("ano", "codigo_municipio")
        .collect(engine="streaming")
    )

    invalid_codes = (
        sinan.filter(
            pl.col("codigo_municipio_residencia_invalido")
            | pl.col("codigo_municipio_notificacao_invalido")
        )
        .select(
            "arquivo",
            "ano_arquivo",
            "codigo_municipio_residencia_original",
            "codigo_municipio_residencia",
            "codigo_municipio_notificacao_original",
            "codigo_municipio_notificacao",
            "codigo_municipio_residencia_invalido",
            "codigo_municipio_notificacao_invalido",
        )
        .sort("ano_arquivo", "arquivo")
        .collect(engine="streaming")
    )

    residence_notification_flow = (
        sinan.filter(
            pl.col("codigo_municipio_residencia").is_not_null()
            & pl.col("codigo_municipio_notificacao").is_not_null()
        )
        .group_by(
            "ano_arquivo",
            "codigo_municipio_residencia",
            "codigo_municipio_notificacao",
        )
        .agg(
            pl.len().alias("notificacoes"),
            pl.col("caso_confirmado").sum().cast(pl.UInt32).alias("casos_confirmados"),
        )
        .rename({"ano_arquivo": "ano"})
        .sort(
            "ano",
            "codigo_municipio_residencia",
            "codigo_municipio_notificacao",
        )
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
    invalid_codes.write_csv(output_dir / "auditoria_sinan_codigos_municipio_invalidos.csv")
    residence_notification_flow.write_parquet(
        output_dir / "auditoria_sinan_fluxo_residencia_notificacao.parquet"
    )
    yearly_classification.write_csv(output_dir / "eda_sinan_classificacao_ano.csv")
    yearly_race.write_csv(output_dir / "eda_sinan_raca_ano.csv")
    yearly_evolution.write_csv(output_dir / "eda_sinan_evolucao_ano.csv")
    monthly.write_csv(output_dir / "eda_sinan_mensal.csv")
    return {
        "painel": panel,
        "qualidade": quality,
        "codigos_invalidos": invalid_codes,
        "fluxo_residencia_notificacao": residence_notification_flow,
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
        _normalize_municipality_code(pl.col("Município (Código)")).alias("codigo_municipio"),
        pl.col("Município").str.replace(r"\s+\([A-Z]{2}\)$", "").alias("municipio"),
        pl.col("Ano").cast(pl.Int32, strict=False).alias("ano"),
        _sidra_value().alias("valor"),
    )


def _chunks(values: list[int], size: int) -> Iterable[list[int]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _validate_census_population(population: pl.DataFrame) -> None:
    """Valida completude, unicidade e denominadores dos dois Censos."""
    _assert_unique(population, ["ano", "codigo_municipio"], "população SIDRA")
    for year, expected in CENSUS_MUNICIPALITY_COUNTS.items():
        census = population.filter(pl.col("ano") == year)
        if census.height != expected:
            raise ValueError(
                f"população SIDRA {year}: esperados {expected} municípios, "
                f"recebidos {census.height}"
            )
        invalid_population = census.filter(
            pl.col("populacao").is_null() | (pl.col("populacao") <= 0)
        )
        if invalid_population.height:
            raise ValueError(
                f"população SIDRA {year}: {invalid_population.height} denominador(es) "
                "nulo(s) ou não positivo(s)"
            )


def _has_complete_census_population(population: pl.DataFrame) -> bool:
    try:
        _validate_census_population(population)
    except (ValueError, pl.exceptions.PolarsError):
        return False
    return True


def _validate_percentage_groups(
    frame: pl.DataFrame,
    groups: dict[str, tuple[str, ...]],
    source: str,
    tolerance: float = 0.1,
) -> None:
    """Valida domínio [0, 100] e fechamento das categorias em 100%."""
    expected_columns = [column for columns in groups.values() for column in columns]
    missing = [column for column in expected_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"{source}: indicadores percentuais ausentes: {missing}")

    for column in expected_columns:
        invalid = frame.filter(
            pl.col(column).is_null()
            | (pl.col(column) < 0)
            | (pl.col(column) > 100)
        )
        if invalid.height:
            raise ValueError(
                f"{source}: {invalid.height} valor(es) ausente(s) ou fora de [0, 100] "
                f"em {column}"
            )

    for dimension, columns in groups.items():
        outside_tolerance = frame.filter(
            (pl.sum_horizontal(list(columns)) - 100).abs() > tolerance
        )
        if outside_tolerance.height:
            raise ValueError(
                f"{source}: {outside_tolerance.height} município(s) não fecham 100% "
                f"na dimensão {dimension} (tolerância {tolerance})"
            )


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
        .sort("ano", "codigo_municipio")
    )
    _assert_unique(estimates, ["ano", "codigo_municipio"], "SIDRA 6579")

    # Tabela 136: População residente, por cor ou raça; variável 93
    # (População residente, Pessoas); classificação 86, categoria 0 (Total).
    census_2010_url = f"{SIDRA_API}/t/136/n6/all/v/93/p/2010/c86/0"
    census_2010_raw = _fetch_sidra(census_2010_url)
    race_code_column = "Cor ou raça (Código)"
    if race_code_column not in census_2010_raw.columns:
        raise ValueError("SIDRA 136: classificação 'Cor ou raça (Código)' ausente")
    unexpected_categories = census_2010_raw.filter(
        pl.col(race_code_column).cast(pl.String).str.strip_chars() != "0"
    )
    if unexpected_categories.height:
        raise ValueError("SIDRA 136: a consulta retornou categorias diferentes de Total (c86=0)")
    census_2010 = (
        _normalize_sidra(census_2010_raw)
        .select(
            "ano",
            "codigo_municipio",
            "municipio",
            pl.col("valor").cast(pl.UInt64, strict=False).alias("populacao"),
            pl.lit("SIDRA 136 v93 c86=0 - Censo 2010").alias("fonte_populacao"),
        )
        .sort("codigo_municipio")
    )

    census_2022_url = f"{SIDRA_API}/t/4709/n6/all/v/93/p/2022"
    census_2022 = (
        _normalize_sidra(_fetch_sidra(census_2022_url))
        .select(
            "ano",
            "codigo_municipio",
            "municipio",
            pl.col("valor").cast(pl.UInt64, strict=False).alias("populacao"),
            pl.lit("SIDRA 4709 v93 - Censo 2022").alias("fonte_populacao"),
        )
        .sort("codigo_municipio")
    )
    population = pl.concat(
        [estimates, census_2010, census_2022], how="vertical_relaxed"
    ).sort("ano", "codigo_municipio")
    _validate_census_population(population)
    return population


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
    _assert_unique(
        long,
        ["ano", "codigo_municipio", "indicador"],
        "SIDRA 3218 - saneamento 2010",
    )
    wide = (
        long.pivot(
            on="indicador",
            index=["ano", "codigo_municipio", "municipio"],
            values="valor",
            aggregate_function="first",
        )
        .sort("codigo_municipio")
    )
    _assert_unique(wide, ["ano", "codigo_municipio"], "SIDRA 3218 - saneamento 2010")
    _validate_percentage_groups(
        wide,
        SANITATION_PERCENTAGE_GROUPS_2010,
        "SIDRA 3218 - saneamento 2010",
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
    _assert_unique(
        long,
        ["ano", "codigo_municipio", "indicador"],
        "SIDRA 6803/6805/6892 - saneamento 2022",
    )
    wide = (
        long.pivot(
            on="indicador",
            index=["ano", "codigo_municipio", "municipio"],
            values="valor",
            aggregate_function="first",
        )
        .sort("codigo_municipio")
    )
    _assert_unique(
        wide,
        ["ano", "codigo_municipio"],
        "SIDRA 6803/6805/6892 - saneamento 2022",
    )
    _validate_percentage_groups(
        wide,
        SANITATION_PERCENTAGE_GROUPS_2022,
        "SIDRA 6803/6805/6892 - saneamento 2022",
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
        cached = {name: pl.read_parquet(path) for name, path in paths.items()}
        if _has_complete_census_population(cached["populacao"]):
            _validate_percentage_groups(
                cached["saneamento_2010"],
                SANITATION_PERCENTAGE_GROUPS_2010,
                "cache SIDRA - saneamento 2010",
            )
            _validate_percentage_groups(
                cached["saneamento_2022"],
                SANITATION_PERCENTAGE_GROUPS_2022,
                "cache SIDRA - saneamento 2022",
            )
            return cached

        # O cache criado antes da inclusão da tabela 136 não continha 2010.
        # Reconsulta somente população e preserva as extrações de saneamento.
        cached["populacao"] = fetch_population()
        cached["populacao"].write_parquet(paths["populacao"])
        return cached

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


def _build_population_contract(
    panel: pl.DataFrame,
    population: pl.DataFrame,
) -> pl.DataFrame:
    """Usa a população como grade e anexa contagens SINAN por residência."""
    keys = ["ano", "codigo_municipio"]
    _assert_unique(population, keys, "população municipal")
    _assert_unique(panel, keys, "painel SINAN por residência")
    population_grid = _left_join_preserving_rows(
        population,
        panel,
        on=keys,
        source="painel SINAN por residência",
    )

    # Preserva a série histórica SINAN nos anos sem denominador populacional.
    # Nos anos censitários, códigos fora da grade oficial não entram no
    # contrato (são retidos em auditoria específica).
    panel_only = panel.join(population.select(keys), on=keys, how="anti").filter(
        ~pl.col("ano").is_in(list(CENSUS_MUNICIPALITY_COUNTS))
    )
    municipality_catalog = (
        population.sort("ano")
        .group_by("codigo_municipio")
        .agg(pl.col("municipio").drop_nulls().last())
    )
    panel_only = _left_join_preserving_rows(
        panel_only,
        municipality_catalog,
        on=["codigo_municipio"],
        source="catálogo municipal da população",
    )
    population_columns = [
        column
        for column in population.columns
        if column not in [*keys, "municipio"]
    ]
    panel_only = panel_only.with_columns(
        [
            pl.lit(None, dtype=population.schema[column]).alias(column)
            for column in population_columns
        ]
    ).select(population_grid.columns)
    joined = pl.concat([population_grid, panel_only], how="vertical_relaxed").with_columns(
        pl.col("notificacoes").is_not_null().alias("tem_notificacao_registrada"),
        pl.when(pl.col("notificacoes").is_not_null())
        .then(pl.lit("com_notificacao_registrada"))
        .otherwise(pl.lit("sem_notificacao_registrada"))
        .alias("situacao_registro_sinan"),
        (pl.col("populacao").is_not_null() & (pl.col("populacao") > 0)).alias(
            "correspondencia_populacao"
        ),
    )

    count_columns = [
        "notificacoes",
        "casos_confirmados",
        "notificacoes_data_invalida",
        "notificacoes_codigo_notificacao_invalido",
        "notificacoes_municipio_notificacao_divergente",
        "municipios_notificacao_distintos",
    ]
    available_count_columns = [
        column for column in count_columns if column in joined.columns
    ]
    joined = joined.with_columns(
        [
            pl.col(column).fill_null(0).cast(pl.UInt64).alias(column)
            for column in available_count_columns
        ]
    ).with_columns(
        pl.when(pl.col("correspondencia_populacao"))
        .then(pl.col("notificacoes") / pl.col("populacao") * 100_000)
        .otherwise(None)
        .alias("taxa_notificacoes_100k"),
        pl.when(pl.col("correspondencia_populacao"))
        .then(pl.col("casos_confirmados") / pl.col("populacao") * 100_000)
        .otherwise(None)
        .alias("taxa_confirmados_100k"),
    )
    _assert_unique(joined, keys, "contrato SINAN-população")
    return joined.sort(keys)


def _build_sanitation_contract(
    population_contract: pl.DataFrame,
    sanitation: pl.DataFrame,
    year: int,
    percentage_groups: dict[str, tuple[str, ...]],
) -> pl.DataFrame:
    """Anexa SIDRA à grade censitária sem permitir joins um-para-muitos."""
    source = f"saneamento SIDRA {year}"
    census_grid = population_contract.filter(pl.col("ano") == year)
    _assert_unique(census_grid, ["ano", "codigo_municipio"], f"grade Censo {year}")

    sanitation_year = sanitation.filter(pl.col("ano") == year)
    _assert_unique(sanitation_year, ["ano", "codigo_municipio"], source)
    _validate_percentage_groups(sanitation_year, percentage_groups, source)
    if "municipio" in sanitation_year.columns:
        sanitation_year = sanitation_year.drop("municipio")

    integrated = _left_join_preserving_rows(
        census_grid,
        sanitation_year,
        on=["ano", "codigo_municipio"],
        source=source,
    )
    percentage_columns = [
        column for columns in percentage_groups.values() for column in columns
    ]
    integrated = integrated.with_columns(
        pl.all_horizontal(
            [pl.col(column).is_not_null() for column in percentage_columns]
        ).alias("correspondencia_sidra")
    ).sort("codigo_municipio")
    _assert_unique(integrated, ["ano", "codigo_municipio"], f"contrato {source}")
    return integrated


def _sinan_without_census_population(
    panel: pl.DataFrame,
    population: pl.DataFrame,
) -> pl.DataFrame:
    """Audita códigos bem formatados que não pertencem à grade censitária."""
    census_keys = population.filter(pl.col("ano").is_in([2010, 2022])).select(
        "ano", "codigo_municipio"
    )
    _assert_unique(census_keys, ["ano", "codigo_municipio"], "grade censitária")
    return (
        panel.filter(pl.col("ano").is_in([2010, 2022]))
        .join(census_keys, on=["ano", "codigo_municipio"], how="anti")
        .sort("ano", "codigo_municipio")
    )


def _build_integration_audit(
    population_contract: pl.DataFrame,
    sanitation_2010: pl.DataFrame,
    sanitation_2022: pl.DataFrame,
    sinan_quality: pl.DataFrame | None = None,
    sinan_without_population: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Resume grade, chave, denominador e cobertura dos contratos censitários."""
    contracts = [
        (
            "populacao_2010",
            2010,
            population_contract.filter(pl.col("ano") == 2010),
            "correspondencia_populacao",
        ),
        (
            "populacao_2022",
            2022,
            population_contract.filter(pl.col("ano") == 2022),
            "correspondencia_populacao",
        ),
        ("saneamento_2010", 2010, sanitation_2010, "correspondencia_sidra"),
        ("saneamento_2022", 2022, sanitation_2022, "correspondencia_sidra"),
    ]
    rows = []
    for base, year, frame, correspondence_column in contracts:
        total = frame.height
        correspondences = int(frame[correspondence_column].sum() or 0)
        with_notification = int(frame["tem_notificacao_registrada"].sum() or 0)
        unique_keys = frame.select("ano", "codigo_municipio").unique().height
        valid_population = int(frame["correspondencia_populacao"].sum() or 0)
        expected = CENSUS_MUNICIPALITY_COUNTS[year]
        quality_year = (
            sinan_quality.filter(pl.col("ano_arquivo") == year)
            if sinan_quality is not None
            else None
        )
        unmatched_year = (
            sinan_without_population.filter(pl.col("ano") == year)
            if sinan_without_population is not None
            else None
        )

        def quality_sum(column: str) -> int:
            if quality_year is None or column not in quality_year.columns:
                return 0
            return int(quality_year[column].sum() or 0)

        rows.append(
            {
                "base": base,
                "ano": year,
                "linhas_esperadas": expected,
                "linhas_totais": total,
                # Nome anterior preservado para consumidores existentes.
                "linhas_elegiveis": total,
                "chaves_unicas": unique_keys,
                "grade_censo_completa": total == expected and unique_keys == total,
                "populacao_valida": valid_population,
                "correspondencias": correspondences,
                "percentual_correspondencia": (
                    correspondences / total * 100 if total else 0.0
                ),
                "municipios_com_notificacao": with_notification,
                "municipios_sem_notificacao": total - with_notification,
                "percentual_cobertura_sinan": (
                    with_notification / total * 100 if total else 0.0
                ),
                "notificacoes": int(frame["notificacoes"].sum() or 0),
                "casos_confirmados": int(frame["casos_confirmados"].sum() or 0),
                "registros_sinan_brutos": quality_sum("registros"),
                "registros_codigo_residencia_invalido": quality_sum(
                    "codigo_municipio_residencia_invalido"
                ),
                "registros_codigo_notificacao_invalido": quality_sum(
                    "codigo_municipio_notificacao_invalido"
                ),
                "registros_residencia_notificacao_divergente": quality_sum(
                    "municipio_residencia_notificacao_divergente"
                ),
                "municipios_sinan_sem_correspondencia_populacao": (
                    unmatched_year.height if unmatched_year is not None else 0
                ),
                "notificacoes_sem_correspondencia_populacao": (
                    int(unmatched_year["notificacoes"].sum() or 0)
                    if unmatched_year is not None
                    else 0
                ),
            }
        )
    return pl.DataFrame(rows).sort("ano", "base")


def build_integrated_outputs(
    source_dir: Path = SINAN_DIR,
    output_dir: Path = PROCESSED_DIR,
    refresh_ibge: bool = False,
) -> dict[str, pl.DataFrame]:
    """Agrega notificações e cria os três contratos analíticos finais."""
    sinan = build_sinan_outputs(source_dir, output_dir)
    ibge = build_ibge_outputs(output_dir, refresh=refresh_ibge)
    panel = sinan["painel"]
    _validate_census_population(ibge["populacao"])

    population_panel = _build_population_contract(panel, ibge["populacao"])
    sanitation = _build_sanitation_contract(
        population_panel,
        ibge["saneamento_2010"],
        2010,
        SANITATION_PERCENTAGE_GROUPS_2010,
    )
    sanitation_2022 = _build_sanitation_contract(
        population_panel,
        ibge["saneamento_2022"],
        2022,
        SANITATION_PERCENTAGE_GROUPS_2022,
    )
    sinan_without_population = _sinan_without_census_population(
        panel, ibge["populacao"]
    )

    population_panel.write_parquet(output_dir / "sinan_populacao_municipio_ano.parquet")
    sanitation.write_parquet(output_dir / "sinan_saneamento_2010.parquet")
    sanitation_2022.write_parquet(output_dir / "sinan_saneamento_2022.parquet")
    sinan_without_population.write_csv(
        output_dir / "auditoria_sinan_sem_correspondencia_populacao_censo.csv"
    )

    audit = _build_integration_audit(
        population_panel,
        sanitation,
        sanitation_2022,
        sinan_quality=sinan["qualidade"],
        sinan_without_population=sinan_without_population,
    )
    audit.write_csv(output_dir / "auditoria_integracao.csv")
    return {
        **sinan,
        **ibge,
        "populacao_integrada": population_panel,
        "saneamento_integrado": sanitation,
        "saneamento_2022_integrado": sanitation_2022,
        "sinan_sem_correspondencia_populacao": sinan_without_population,
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
