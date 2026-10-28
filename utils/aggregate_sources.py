"""Agregação exploratória SINAN–SIDRA–INMET por UF e mês de calendário.

Reutiliza a classificação histórica e a triagem meteorológica do projeto.
Saneamento vem do agregado oficial estadual, preservando seus denominadores.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from utils.analysis_pipeline import (
    CENSUS_MUNICIPALITY_COUNTS, PROCESSED_DIR, SINAN_DIR, SIDRA_API,
    _assert_unique, _fetch_sidra, _sidra_value, scan_sinan,
)
from utils.inmet_pipeline import clean_inmet, daily_weather, verify_publication


UF_CODES = dict(zip(
    "11 12 13 14 15 16 17 21 22 23 24 25 26 27 28 29 31 32 33 35 41 42 43 50 51 52 53".split(),
    "RO AC AM RR PA AP TO MA PI CE RN PB PE AL SE BA MG ES RJ SP PR SC RS MS MT GO DF".split(),
))
REGIONS = {uf: region for region, ufs in {
    "Norte": "RO AC AM RR PA AP TO", "Nordeste": "MA PI CE RN PB PE AL SE BA",
    "Sudeste": "MG ES RJ SP", "Sul": "PR SC RS", "Centro-Oeste": "MS MT GO DF",
}.items() for uf in ufs.split()}
SANITATION = ["pct_agua_rede", "pct_esgotamento", "pct_coleta_lixo"]
WEATHER = {
    "temperatura_media_c": "temp_media_c",
    "chuva_acumulada_mm": "precip_media_mm",
    "umidade_media_pct": "umidade_media_pct",
}


def sanitation_queries(year: int) -> dict[str, tuple[str, str, int]]:
    """URL, classificação e quantidade de categorias mutuamente exclusivas."""
    if year == 2010:
        base = f"{SIDRA_API}/t/3218/n3/all/v/1000096/p/2010/"
        return {
            "pct_agua_rede": (base + "c61/92853/c299/0/c67/0/c309/0", "Forma de abastecimento de água (Código)", 1),
            "pct_esgotamento": (base + "c61/0/c299/9678,9679/c67/0/c309/0", "Existência de banheiro ou sanitário e esgotamento sanitário (Código)", 2),
            "pct_coleta_lixo": (base + "c61/0/c299/0/c67/92863,92864/c309/0", "Destino do lixo (Código)", 2),
        }
    if year == 2022:
        return {
            "pct_agua_rede": (f"{SIDRA_API}/t/6803/n3/all/v/1000381/p/2022/c1821/72144", "Existência de ligação à rede geral de distribuição de água e principal forma de abastecimento de água (Código)", 1),
            "pct_esgotamento": (f"{SIDRA_API}/t/6805/n3/all/v/1000381/p/2022/c11558/72110,72111", "Tipo de esgotamento sanitário (Código)", 2),
            "pct_coleta_lixo": (f"{SIDRA_API}/t/6892/n3/all/v/1000381/p/2022/c67/72120,72121", "Destino do lixo (Código)", 2),
        }
    raise ValueError("Saneamento disponível somente para os Censos 2010 e 2022.")


def load_sanitation_uf(year: int, cache_dir: Path, refresh: bool = False) -> pl.DataFrame:
    """Consulta os percentuais estaduais oficiais e verifica o cache por checksum."""
    queries = sanitation_queries(year)
    path = cache_dir / f"sidra_saneamento_uf_{year}.parquet"
    metadata_path = path.with_suffix(".json")
    urls = {key: query[0] for key, query in queries.items()}
    if path.exists() and metadata_path.exists() and not refresh:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if metadata["urls"] != urls or metadata["sha256"] != hashlib.sha256(path.read_bytes()).hexdigest():
            raise ValueError("Cache estadual SIDRA incompatível; use refresh=True para atualizá-lo.")
        result = pl.read_parquet(path)
    else:
        result = pl.DataFrame({"UF_codigo": list(UF_CODES)})
        for indicator, (url, category, expected_categories) in queries.items():
            raw = _fetch_sidra(url)
            selected = raw.select(
                pl.col("Unidade da Federação (Código)").alias("UF_codigo"),
                pl.col(category).alias("categoria"), _sidra_value().alias("valor"),
            )
            _assert_unique(selected, ["UF_codigo", "categoria"], indicator)
            if selected.filter(pl.col("valor").is_null() | ~pl.col("valor").is_between(0, 100)).height:
                raise ValueError(f"SIDRA: valores ausentes ou fora de 0–100 em {indicator}.")
            counts = selected.group_by("UF_codigo").len()
            if set(counts["UF_codigo"]) != set(UF_CODES) or counts.filter(pl.col("len") != expected_categories).height:
                raise ValueError(f"SIDRA: UFs/categorias incompletas em {indicator}.")
            summary = selected.group_by("UF_codigo").agg(pl.col("valor").sum().alias(indicator))
            result = result.join(summary, on="UF_codigo", validate="1:1")
        result = result.with_columns(pl.lit(year).alias("ano"))
        _validate_sanitation(result, year)
        cache_dir.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp.parquet")
        result.write_parquet(temporary)
        metadata = {"ano": year, "urls": urls, "consultado_em_utc": datetime.now(timezone.utc).isoformat(),
                    "sha256": hashlib.sha256(temporary.read_bytes()).hexdigest()}
        temporary.replace(path)
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    _validate_sanitation(result, year)
    return result


def _validate_sanitation(frame: pl.DataFrame, year: int) -> None:
    _assert_unique(frame, ["ano", "UF_codigo"], "SIDRA estadual")
    if frame.height != 27 or set(frame["UF_codigo"]) != set(UF_CODES) or set(frame["ano"]) != {year}:
        raise ValueError("SIDRA estadual: esperadas as 27 UFs do ano solicitado.")
    if frame.filter(pl.any_horizontal([pl.col(c).is_null() | ~pl.col(c).is_between(0, 100) for c in SANITATION])).height:
        raise ValueError("SIDRA estadual: percentual ausente ou fora de 0–100.")


def aggregate_sinan_monthly(raw: pl.LazyFrame, population: pl.DataFrame, year: int) -> dict[str, pl.DataFrame]:
    """Agrega pela data efetiva e residência oficial, com exclusões identificadas."""
    _assert_unique(population, ["codigo_municipio"], "população do ano")
    if population.filter(pl.col("populacao").is_null() | (pl.col("populacao") <= 0)).height:
        raise ValueError("População ausente ou não positiva.")
    official = population.select("codigo_municipio").lazy().with_columns(pl.lit(True).alias("residencia_oficial"))
    classified = raw.join(official, on="codigo_municipio", how="left", validate="m:1").with_columns(
        pl.when(pl.col("data_notificacao").is_null()).then(pl.lit("sem_data_calendario"))
        .when(pl.col("data_notificacao").dt.year() != year).then(pl.lit("fora_ano_calendario"))
        .when(pl.col("codigo_municipio").is_null()).then(pl.lit("residencia_invalida"))
        .when(pl.col("residencia_oficial").is_null()).then(pl.lit("residencia_sem_ibge"))
        .otherwise(pl.lit("incluido")).alias("situacao"),
    )
    audit = classified.group_by("arquivo", "ano_arquivo", "situacao").agg(
        pl.len().alias("notificacoes"), pl.col("caso_confirmado").sum().alias("casos_confirmados"),
    ).sort("ano_arquivo", "situacao").collect(engine="streaming")
    exclusions = classified.filter(
        pl.col("situacao").is_in(["residencia_invalida", "residencia_sem_ibge"])
        | ((pl.col("situacao") == "sem_data_calendario") & (pl.col("ano_arquivo") == year))
    ).select("arquivo", "ano_arquivo", "data_notificacao_original", "codigo_municipio_original",
             "classificacao_original", "situacao").collect(engine="streaming")
    counts = classified.filter(pl.col("situacao") == "incluido").with_columns(
        pl.col("codigo_municipio").str.slice(0, 2).alias("UF_codigo"),
        pl.col("data_notificacao").dt.month().cast(pl.Int32).alias("mes"),
    ).group_by("UF_codigo", "mes").agg(
        pl.len().alias("notificacoes"), pl.col("caso_confirmado").sum().alias("casos_confirmados"),
        (pl.col("ano_arquivo") != year).sum().alias("notificacoes_outro_ano_fonte"),
    ).collect(engine="streaming")
    states = population.with_columns(pl.col("codigo_municipio").str.slice(0, 2).alias("UF_codigo")).group_by("UF_codigo").agg(
        pl.col("populacao").sum(), pl.len().alias("municipios_populacao"),
    )
    months = pl.DataFrame({"mes": range(1, 13)}, schema={"mes": pl.Int32})
    monthly = states.join(months, how="cross").join(counts, on=["UF_codigo", "mes"], how="left", validate="1:1").with_columns(
        pl.col("notificacoes", "casos_confirmados", "notificacoes_outro_ano_fonte").fill_null(0),
        pl.col("UF_codigo").replace_strict(UF_CODES).alias("UF"), pl.lit(year).alias("ano"),
        pl.date(year, pl.col("mes"), 1).alias("competencia"),
    ).with_columns(
        pl.col("UF").replace_strict(REGIONS).alias("regiao"),
        (pl.col("casos_confirmados").cast(pl.Float64) / pl.col("populacao") * 100_000).alias("taxa_confirmados_100k"),
        (pl.col("notificacoes").cast(pl.Float64) / pl.col("populacao") * 100_000).alias("taxa_notificacoes_100k"),
    ).sort("UF", "mes")
    included = audit.filter(pl.col("situacao") == "incluido")
    if monthly["notificacoes"].sum() != included["notificacoes"].sum() or monthly["casos_confirmados"].sum() != included["casos_confirmados"].sum():
        raise AssertionError("Agregação SINAN não reconcilia com a auditoria.")
    return {"mensal": monthly, "auditoria": audit, "excluidos": exclusions}


def aggregate_weather_monthly(daily: pl.DataFrame, year: int, minimum_coverage: float = 0.8) -> pl.DataFrame:
    """Painel fixo: chuva só com todos os dias; médias com cobertura configurada."""
    if not 0 < minimum_coverage <= 1:
        raise ValueError("Cobertura mensal deve estar em (0, 1].")
    _assert_unique(daily, ["UF", "DATA"], "INMET diário")
    if set(daily["UF"]) - set(UF_CODES.values()):
        raise ValueError("INMET diário contém UF desconhecida.")
    if set(daily["DATA"].dt.year()) - {year}:
        raise ValueError("INMET diário contém datas fora do ano solicitado.")
    # Completar o calendário antes da agregação impede confundir 1 dia com 100% do mês.
    dates = pl.DataFrame({"DATA": pl.date_range(date(year, 1, 1), date(year, 12, 31), eager=True)})
    grid = pl.DataFrame({"UF": list(UF_CODES.values())}).join(dates, how="cross")
    complete = grid.join(daily, on=["UF", "DATA"], how="left", validate="1:1").with_columns(
        pl.col("DATA").dt.month().cast(pl.Int32).alias("mes"),
    )
    summary = complete.group_by("UF", "mes").agg(
        pl.len().alias("dias_calendario"),
        pl.col("n_estacoes_painel_fixo").max().fill_null(0).alias("n_estacoes_painel_fixo"),
        pl.col("estacoes_painel_fixo").drop_nulls().first(),
        *[expression for name, source in WEATHER.items() for expression in (
            pl.col(f"fixo_{source}").count().alias(f"dias_validos_{name}"),
            (pl.col(f"fixo_{source}").sum() if name == "chuva_acumulada_mm" else pl.col(f"fixo_{source}").mean()).alias(name),
        )],
    ).with_columns(
        *[(100 * pl.col(f"dias_validos_{name}") / pl.col("dias_calendario")).alias(f"cobertura_{name}_pct") for name in WEATHER],
        *[pl.when(pl.col(f"dias_validos_{name}") >= pl.col("dias_calendario") * (1 if name == "chuva_acumulada_mm" else minimum_coverage))
          .then(pl.col(name)).otherwise(None).alias(name) for name in WEATHER],
    )
    # Roster é definido no ano e deve aparecer também em meses sem uma leitura.
    return summary.sort("UF", "mes").with_columns(
        pl.col("n_estacoes_painel_fixo").max().over("UF"),
        pl.col("estacoes_painel_fixo").forward_fill().backward_fill().over("UF"),
    ).with_columns(
        pl.when(pl.col("n_estacoes_painel_fixo") == 0).then(pl.lit("sem_painel_elegivel"))
        .when(pl.all_horizontal([pl.col(name).is_not_null() for name in WEATHER])).then(pl.lit("disponivel"))
        .otherwise(pl.lit("cobertura_insuficiente")).alias("situacao_inmet"),
    )


def summarize_annual(monthly: pl.DataFrame) -> pl.DataFrame:
    """Uma observação anual por UF; população e Censo não são somados 12 vezes."""
    _assert_unique(monthly, ["UF", "ano", "mes"], "base mensal integrada")
    if monthly.group_by("UF", "ano").agg(pl.col("mes").n_unique().alias("n")).filter(pl.col("n") != 12).height:
        raise ValueError("Resumo anual exige os 12 meses de cada UF.")
    result = monthly.group_by("UF", "ano").agg(
        pl.col("regiao").first(), pl.col("populacao").first(),
        *[pl.col(name).first() for name in SANITATION],
        pl.col("notificacoes").sum(), pl.col("casos_confirmados").sum(),
        pl.col("notificacoes_outro_ano_fonte").sum(),
        (pl.col("situacao_inmet") == "disponivel").sum().alias("meses_clima_completo"),
        pl.when(pl.col("chuva_acumulada_mm").count() == 12).then(pl.col("chuva_acumulada_mm").sum()).otherwise(None).alias("chuva_anual_mm"),
        *[pl.when(pl.col(name).count() == 12).then(
            (pl.col(name) * pl.col(f"dias_validos_{name}")).sum() / pl.col(f"dias_validos_{name}").sum()
        ).otherwise(None).alias(name) for name in ("temperatura_media_c", "umidade_media_pct")],
    ).with_columns(
        (pl.col("casos_confirmados").cast(pl.Float64) / pl.col("populacao") * 100_000).alias("taxa_confirmados_100k"),
        (pl.col("notificacoes").cast(pl.Float64) / pl.col("populacao") * 100_000).alias("taxa_notificacoes_100k"),
    )
    return result.sort("UF")


def build_aggregations(year: int = 2010, weather_path: Path | None = None,
                       source_dir: Path = SINAN_DIR, processed_dir: Path = PROCESSED_DIR,
                       refresh_sidra: bool = False, fixed_stations: int = 3,
                       minimum_station_coverage: float = 0.8,
                       minimum_monthly_coverage: float = 0.8) -> dict:
    """Gera agregados e auditorias; INMET ausente permanece explicitamente nulo."""
    sanitation_queries(year)  # Valida o recorte censitário antes de qualquer escrita.
    if year >= date.today().year:
        raise ValueError("Selecione um ano encerrado.")
    for required_year in (year - 1, year, year + 1):
        if not (source_dir / f"DENGBR{required_year % 100:02d}.parquet").exists():
            raise FileNotFoundError(f"SINAN {required_year} ausente; são necessários o ano-alvo e seus vizinhos.")
    population_path = processed_dir / "ibge_populacao_municipio_ano.parquet"
    if not population_path.exists():
        raise FileNotFoundError(f"Execute utils.analysis_pipeline para gerar {population_path}.")
    population = pl.read_parquet(population_path).filter(pl.col("ano") == year)
    if population.height != CENSUS_MUNICIPALITY_COUNTS[year]:
        raise ValueError(f"Catálogo populacional {year} incompleto.")
    outputs = aggregate_sinan_monthly(scan_sinan(source_dir), population, year)
    sanitation = load_sanitation_uf(year, processed_dir / "cache_agregacao_uf", refresh_sidra)
    monthly = outputs["mensal"].join(sanitation, on=["ano", "UF_codigo"], how="left", validate="m:1")
    if monthly.height != 27 * 12:
        raise ValueError("Esperadas 324 linhas (27 UFs × 12 meses).")
    weather_status = "arquivo_ausente"
    if weather_path is not None and weather_path.exists():
        verify_publication(weather_path)
        cleaned, _ = clean_inmet(pl.scan_parquet(weather_path))
        first, last = date(year, 1, 1), date(year, 12, 31)
        target = cleaned.filter(
            pl.col("DATA").is_between(first, last)
            | ((pl.col("DATA") == last + timedelta(days=1)) & (pl.col("HORA_UTC_NUM") == 0))
            | (pl.col("DATA").is_null() & (pl.col("ANO_REFERENCIA").cast(pl.Int32, strict=False) == year))
        )
        daily = daily_weather(target, start_date=first, end_date=last, fixed_stations=fixed_stations,
                              minimum_station_coverage=minimum_station_coverage).collect(engine="streaming")
        weather = aggregate_weather_monthly(daily, year, minimum_monthly_coverage)
        monthly = monthly.join(weather, on=["UF", "mes"], how="left", validate="1:1")
        weather_status = "arquivo_processado"
    else:
        monthly = monthly.with_columns(
            pl.col("competencia").dt.days_in_month().alias("dias_calendario"),
            pl.lit(0).alias("n_estacoes_painel_fixo"), pl.lit(None, dtype=pl.String).alias("estacoes_painel_fixo"),
            pl.lit(weather_status).alias("situacao_inmet"),
            *[expr for name in WEATHER for expr in (
                pl.lit(None, dtype=pl.Float64).alias(name),
                pl.lit(0).alias(f"dias_validos_{name}"),
                pl.lit(0.0).alias(f"cobertura_{name}_pct"),
            )],
        )
    monthly = monthly.with_columns(
        pl.all_horizontal([pl.col(name).is_not_null() for name in [*SANITATION, *WEATHER]]).alias("tres_fontes_disponiveis"),
    ).sort("UF", "mes")
    sources = [{"arquivo": str(p.resolve()), "bytes": p.stat().st_size, "mtime_ns": p.stat().st_mtime_ns}
               for p in sorted(source_dir.glob("DENGBR*.parquet"))]
    metadata = {
        "ano_calendario": year, "gerado_em_utc": datetime.now(timezone.utc).isoformat(),
        "sinan_arquivos": sources, "populacao": str(population_path.resolve()),
        "sidra_urls": {key: item[0] for key, item in sanitation_queries(year).items()},
        "inmet": str(weather_path.resolve()) if weather_path else None, "situacao_inmet": weather_status,
        "estacoes_fixas_por_uf": fixed_stations, "cobertura_minima_estacao": minimum_station_coverage,
        "cobertura_minima_mensal_medias": minimum_monthly_coverage, "cobertura_mensal_chuva": 1.0,
        "periodo_sinan": "data efetiva de notificação nos arquivos disponíveis da coorte; inclui datas de outros anos-fonte",
        "exclusoes": "data ilegível e residência sem correspondência no catálogo do Censo; consultar auditorias",
        "agua": "rede geral" if year == 2010 else "rede geral como forma principal de abastecimento",
        "esgotamento": "rede ou fossa séptica" if year == 2010 else "rede geral/pluvial ou fossa ligada à rede",
        "comparabilidade": "Definições censitárias diferem entre 2010 e 2022; analisar cada ano separadamente.",
    }
    return {**outputs, "mensal": monthly, "anual": summarize_annual(monthly), "metadados": metadata}


def export_aggregations(outputs: dict, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for name in ("mensal", "anual"):
        outputs[name].write_parquet(destination / f"{name}_uf.parquet")
        outputs[name].write_csv(destination / f"{name}_uf.csv")
    outputs["auditoria"].write_csv(destination / "auditoria_sinan_resumo.csv")
    outputs["excluidos"].write_csv(destination / "auditoria_sinan_excluidos.csv")
    (destination / "metadados.json").write_text(json.dumps(outputs["metadados"], ensure_ascii=False, indent=2), encoding="utf-8")
