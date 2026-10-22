"""Base UF × dia de calendário para exploração de clima e dengue.

Dia de notificação é uma data administrativa local; o dia meteorológico é UTC.
As correlações descritivas são calculadas dentro de UF, com desfecho e amostra
comuns. Não são estimativas de efeito climático ou de saneamento.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from utils.analysis_pipeline import SINAN_DIR, PROCESSED_DIR, scan_sinan
from utils.inmet_pipeline import clean_inmet, daily_weather, verify_publication


def build_daily_uf(weather_path: Path, year: int = 2010, source_dir: Path = SINAN_DIR,
                   output_dir: Path = PROCESSED_DIR, fixed_stations: int = 3,
                   minimum_station_coverage: float = 0.8) -> pl.DataFrame:
    if year >= date.today().year:
        raise ValueError("Comparações anuais exigem ano encerrado.")
    expected = source_dir / f"DENGBR{year % 100:02d}.parquet"
    if not expected.exists():
        raise FileNotFoundError(f"Fonte anual SINAN ausente: {expected}")
    sinan = scan_sinan(source_dir, years=[year])
    invalid = sinan.filter(
        pl.col("data_notificacao").is_null()
        | (pl.col("data_notificacao").dt.year() != year)
        | pl.col("codigo_municipio").is_null()
    ).select(pl.len()).collect().item()
    if invalid:
        raise ValueError(f"SINAN {year}: {invalid} registros sem tempo/residência válidos; reconcilie antes da agregação")
    counts = sinan.with_columns(pl.col("codigo_municipio").str.slice(0, 2).alias("UF_CODIGO")).group_by(
        "UF_CODIGO", "data_notificacao"
    ).agg(pl.len().alias("notificacoes_dengue"), pl.col("caso_confirmado").sum().alias("casos_confirmados"))
    population_path = output_dir / "ibge_populacao_municipio_ano.parquet"
    if not population_path.exists():
        raise FileNotFoundError(f"Execute o pipeline municipal primeiro: {population_path}")
    official_codes = pl.scan_parquet(population_path).filter(pl.col("ano") == year).select("codigo_municipio").unique()
    if not official_codes.select(pl.len()).collect().item():
        raise ValueError(f"Sem catálogo populacional para {year}")
    unmatched = sinan.select("codigo_municipio").unique().join(official_codes, on="codigo_municipio", how="anti").collect()
    if unmatched.height:
        output_dir.mkdir(parents=True, exist_ok=True)
        unmatched.write_csv(output_dir / f"clima_codigos_sem_populacao_{year}.csv")
        raise ValueError("SINAN contém municípios sem correspondência no catálogo do mesmo ano; consulte a auditoria")
    uf_codes = {"11":"RO", "12":"AC", "13":"AM", "14":"RR", "15":"PA", "16":"AP", "17":"TO",
                "21":"MA", "22":"PI", "23":"CE", "24":"RN", "25":"PB", "26":"PE", "27":"AL", "28":"SE", "29":"BA",
                "31":"MG", "32":"ES", "33":"RJ", "35":"SP", "41":"PR", "42":"SC", "43":"RS",
                "50":"MS", "51":"MT", "52":"GO", "53":"DF"}
    unknown = counts.filter(~pl.col("UF_CODIGO").is_in(uf_codes)).collect()
    if unknown.height:
        raise ValueError("UF de residência SINAN fora do catálogo IBGE")
    counts = counts.with_columns(pl.col("UF_CODIGO").replace_strict(uf_codes).alias("UF")).rename({"data_notificacao":"DATA"}).drop("UF_CODIGO").collect()
    verify_publication(weather_path)
    raw_weather = pl.scan_parquet(weather_path)
    # Aceita a saída canônica do ingestor e os headers dos parquets legados.
    cleaned, _ = clean_inmet(raw_weather)
    # Viradas de ano podem estar no arquivo de um ano adjacente. Agregar pela
    # data observada, mas não ocultar datas inválidas do arquivo do ano-alvo.
    first_day, last_day = date(year, 1, 1), date(year, 12, 31)
    target_weather = cleaned.filter(
        pl.col("DATA").is_between(first_day, last_day)
        | ((pl.col("DATA") == last_day + timedelta(days=1)) & (pl.col("HORA_UTC_NUM") == 0))
        | (pl.col("DATA").is_null() & (pl.col("ANO_REFERENCIA").cast(pl.Int32, strict=False) == year))
    )
    weather = daily_weather(
        target_weather, start_date=first_day, end_date=last_day,
        fixed_stations=fixed_stations, minimum_station_coverage=minimum_station_coverage,
    ).collect(engine="streaming")
    dates = pl.DataFrame({"DATA": pl.date_range(date(year, 1, 1), date(year, 12, 31), eager=True)})
    grid = pl.DataFrame({"UF": list(uf_codes.values())}).join(dates, how="cross")
    result = grid.join(counts, on=["UF", "DATA"], how="left", validate="1:1").with_columns(
        pl.col("notificacoes_dengue", "casos_confirmados").fill_null(0)
    ).join(weather, on=["UF", "DATA"], how="left", validate="1:1").sort("UF", "DATA").with_columns(
        # Metadados da coorte são constantes no ano, mesmo em dias sem medição.
        pl.col("n_estacoes_painel_fixo").forward_fill().backward_fill().over("UF").fill_null(0),
        pl.col("estacoes_painel_fixo").forward_fill().backward_fill().over("UF"),
    )
    # Ausência de meteorologia permanece nula; nunca vira dia seco.
    output_dir.mkdir(parents=True, exist_ok=True)
    result.write_parquet(output_dir / f"INMET_x_PYSUS_{year}_diario_uf.parquet")
    return result


def within_uf_correlations(frame: pl.DataFrame, lag_days: int = 28) -> pl.DataFrame:
    """Compara estações disponíveis e painel fixo nos mesmos dias de cada UF.

    UFs sem uma coorte fixa elegível continuam na saída com n=0 e r nulo;
    não se publica a correlação dinâmica como se a sensibilidade tivesse passado.
    """
    columns = ["temp_media_c", "precip_media_mm", "umidade_media_pct"]
    fixed_columns = [f"fixo_{column}" for column in columns]
    if lag_days < 0:
        raise ValueError("Defasagem deve ser não negativa.")
    if frame.select(pl.struct("UF", "DATA").n_unique()).item() != frame.height:
        raise ValueError("UF/data duplicados; a correlação exige observações únicas")
    if frame["DATA"].null_count() or frame["UF"].null_count():
        raise ValueError("UF/data não podem ser nulos.")
    if frame.select(pl.col("DATA").dt.year().n_unique()).item() != 1:
        raise ValueError("Compare um ano por vez: a composição fixa é definida por UF/ano.")
    if not set(fixed_columns).issubset(frame.columns):
        raise ValueError("Regenere a integração climática com o painel fixo de estações.")
    shifted = frame.sort("UF", "DATA").with_columns([
        pl.col(column).shift(lag_days).over("UF").alias(column) for column in [*columns, *fixed_columns]
    ])
    # Exigir dias adjacentes impede interpretar um deslocamento de linhas
    # como dias quando o chamador fornece uma série com lacunas.
    gaps = frame.sort("UF", "DATA").with_columns(pl.col("DATA").diff().over("UF").dt.total_days().alias("gap")).filter(pl.col("gap") > 1)
    if gaps.height:
        raise ValueError("Complete a grade de calendário antes de calcular defasagens.")
    complete = shifted.drop_nulls([*columns, *fixed_columns, "notificacoes_dengue"])
    results = []
    for method, prefix in [("estacoes_disponiveis", ""), ("painel_fixo", "fixo_")]:
        summary = complete.group_by("UF").agg(
            pl.len().alias("dias_completos_comuns"),
            *[pl.when((pl.len() >= 30) & (pl.col(prefix + column).std() > 0) & (pl.col("notificacoes_dengue").std() > 0))
              .then(pl.corr(prefix + column, "notificacoes_dengue")).otherwise(None).alias(f"r_{column}") for column in columns],
        )
        results.append(frame.select("UF").unique().join(summary, on="UF", how="left").with_columns(
            pl.col("dias_completos_comuns").fill_null(0),
            pl.lit(method).alias("metodo"), pl.lit(lag_days).alias("defasagem_dias"),
        ))
    return pl.concat(results).sort("UF", "metodo")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("weather", type=Path)
    parser.add_argument("--year", type=int, default=2010)
    parser.add_argument("--fixed-stations", type=int, default=3)
    parser.add_argument("--minimum-station-coverage", type=float, default=0.8)
    args = parser.parse_args()
    print(build_daily_uf(args.weather, args.year, fixed_stations=args.fixed_stations,
                         minimum_station_coverage=args.minimum_station_coverage))


if __name__ == "__main__":
    main()
