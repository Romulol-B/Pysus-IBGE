"""Ingestão INMET estrita, qualidade e agregação com cobertura explícita.

Os limites físicos são filtros de triagem, não certificados de qualidade.
Os dados brutos permanecem no parquet agregado; a limpeza é uma visão lazy.
Dias climáticos usam UTC, sem afirmar representar dias civis locais.
"""

from __future__ import annotations

import argparse
import csv
from datetime import date
import hashlib
import json
import os
import re
import unicodedata
import uuid
from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parents[1]
META = ["ANO_REFERENCIA", "ESTACAO", "CODIGO_WMO", "UF", "REGIAO"]
UF_SIGLAS = frozenset("RO AC AM RR PA AP TO MA PI CE RN PB PE AL SE BA MG ES RJ SP PR SC RS MS MT GO DF".split())
STATION_PATTERN = r"^(?:[A-Z][0-9]{3}|[0-9]{5})$"


def _publish_outputs(staged: dict[Path, Path], manifest_path: Path) -> None:
    """Publica um conjunto preparado; falhas de rename restauram as versões anteriores.

    A marca pending impede leitores cooperantes de aceitar uma publicação
    interrompida. Backups ficam recuperáveis se o processo for encerrado à força.
    """
    pending = manifest_path.with_suffix(manifest_path.suffix + ".pending")
    transaction = uuid.uuid4().hex
    backups = {target: target.with_name(f".{target.name}.{transaction}.backup") for target in staged if target.exists()}
    metadata = {"versao": transaction, "arquivos": [
        {"path": str(target.resolve()), "sha256": digest(temporary)} for target, temporary in staged.items()
    ]}
    # 'x' também impede duas publicações simultâneas no mesmo destino.
    with pending.open("x", encoding="utf-8") as stream:
        json.dump({**metadata, "backups": {str(k): str(v) for k, v in backups.items()},
                   "staged": {str(k): str(v) for k, v in staged.items()}}, stream)
    published = []
    manifest_temporary = manifest_path.with_name(f".{manifest_path.name}.{transaction}.tmp")
    try:
        for target, backup in backups.items():
            os.link(target, backup)
        for target, temporary in staged.items():
            temporary.replace(target)
            published.append(target)
        manifest_temporary.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        manifest_temporary.replace(manifest_path)
    except BaseException:
        for target in reversed(published):
            if target in backups:
                backups[target].replace(target)
            else:
                target.unlink(missing_ok=True)
        # Se restaurar falhar, a exceção deixa pending/backups para recuperação.
        for backup in backups.values():
            backup.unlink(missing_ok=True)
        pending.unlink(missing_ok=True)
        manifest_temporary.unlink(missing_ok=True)
        raise
    for backup in backups.values():
        backup.unlink(missing_ok=True)
    pending.unlink()


def verify_publication(path: Path) -> None:
    """Verifica integridade quando o parquet possui manifesto de publicação."""
    manifest_path = path.with_suffix(".publication.json")
    if manifest_path.with_suffix(manifest_path.suffix + ".pending").exists():
        raise ValueError(f"Publicação INMET interrompida: {manifest_path}")
    if manifest_path.exists():
        metadata = json.loads(manifest_path.read_text(encoding="utf-8"))
        records = metadata.get("arquivos", [])
        if not any(Path(record["path"]).resolve() == path.resolve() for record in records):
            raise ValueError("Manifesto não contém o parquet solicitado.")
        for record in records:
            output = Path(record["path"])
            if not output.exists() or digest(output) != record["sha256"]:
                raise ValueError(f"Saídas INMET de versões distintas ou alteradas: {output}")


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _key(name: str) -> str:
    name = "".join(c for c in unicodedata.normalize("NFKD", name) if not unicodedata.combining(c))
    return re.sub(r"[^A-Z0-9]+", "_", name.upper()).strip("_")


def ingest_inmet(source: Path, destination: Path, csv_destination: Path | None = None) -> pl.DataFrame:
    """Normaliza encoding, une schemas por nome e reconcilia todos os arquivos."""
    source, destination = source.resolve(), destination.resolve()
    if csv_destination is not None:
        csv_destination = csv_destination.resolve()
        if csv_destination == destination:
            raise ValueError("CSV e parquet precisam de destinos distintos.")
    manifest_path = destination.with_suffix(".publication.json")
    if manifest_path.exists():
        previous = json.loads(manifest_path.read_text(encoding="utf-8"))
        previous_paths = {Path(record["path"]).resolve() for record in previous["arquivos"]}
        requested_paths = {destination} | ({csv_destination} if csv_destination is not None else set())
        if previous_paths != requested_paths:
            raise ValueError("Mantenha os destinos da publicação anterior, incluindo --csv-output, para não deixar saídas de versões distintas.")
    files = sorted(p for p in source.rglob("*") if p.is_file() and p.name.upper().startswith("INMET_") and p.suffix.lower() == ".csv")
    if not files:
        raise FileNotFoundError(f"Nenhum CSV INMET em {source}")
    cache = destination.parent / ".inmet_utf8"
    cache.mkdir(parents=True, exist_ok=True)
    frames, audit = [], []
    for path in files:
        row = {"arquivo": str(path.relative_to(source)), "status": "erro"}
        try:
            fingerprint = digest(path)
            converted = cache / f"{fingerprint}.csv"
            cache_metadata = converted.with_suffix(".metadata.json")
            cache_valid = False
            if converted.exists() and cache_metadata.exists():
                metadata = json.loads(cache_metadata.read_text(encoding="utf-8"))
                cache_valid = metadata.get("source_sha256") == fingerprint and metadata.get("utf8_sha256") == digest(converted)
            if not cache_valid:
                temporary = converted.with_suffix(".tmp")
                with path.open(encoding="latin-1", newline="") as inp, temporary.open("w", encoding="utf-8", newline="") as out:
                    for line in inp:
                        out.write(line)
                temporary.replace(converted)
                cache_metadata.write_text(json.dumps({"source_sha256":fingerprint, "utf8_sha256":digest(converted)}), encoding="utf-8")
            with converted.open(encoding="utf-8", newline="") as stream:
                for _ in range(8):
                    next(stream)
                reader = csv.reader(stream, delimiter=";")
                header = next(reader)
                count = 0
                for record in reader:
                    if len(record) != len(header):
                        raise ValueError(f"linha {count + 10}: {len(record)} campos, esperados {len(header)}")
                    count += 1
            if not count:
                raise ValueError("arquivo sem observações")
            names = [_key(c) if c.strip() else f"EMPTY_{i}" for i, c in enumerate(header)]
            if len(names) != len(set(names)):
                raise ValueError("cabeçalhos ambíguos após normalização")
            frame = pl.scan_csv(converted, separator=";", skip_rows=8, infer_schema=False, new_columns=names, ignore_errors=False)
            frame = frame.select([c for c in names if not c.startswith("EMPTY_")])
            parts = path.stem.split("_")
            if len(parts) < 5 or not re.fullmatch(r"\d{4}", path.parent.name):
                raise ValueError("nome/caminho não identifica ano, região, UF, estação e WMO")
            if parts[2].upper() not in UF_SIGLAS or not re.fullmatch(STATION_PATTERN, parts[3].upper()):
                raise ValueError("UF ou identificador de estação inválido no nome do arquivo")
            frame = frame.with_columns(
                pl.lit(path.parent.name).alias("ANO_REFERENCIA"),
                pl.lit(parts[1]).alias("REGIAO"), pl.lit(parts[2]).alias("UF"),
                pl.lit(parts[3]).alias("CODIGO_WMO"), pl.lit(parts[4]).alias("ESTACAO"),
                pl.lit(str(path.relative_to(source))).alias("ARQUIVO_ORIGEM"),
            )
            scanned_count = frame.select(pl.len()).collect(engine="streaming").item()
            if scanned_count != count:
                raise ValueError(f"reconciliação de linhas: CSV={count}, Polars={scanned_count}")
            frames.append(frame)
            row.update(status="ok", registros=count, sha256=fingerprint, erro="")
        except Exception as error:
            row["erro"] = str(error)
        audit.append(row)
    destination.parent.mkdir(parents=True, exist_ok=True)
    audit_frame = pl.DataFrame(audit)
    audit_frame.write_csv(destination.with_suffix(".auditoria.csv"))
    if any(row["status"] != "ok" for row in audit):
        raise ValueError(f"Ingestão interrompida; verifique {destination.with_suffix('.auditoria.csv')}")
    combined = pl.concat(frames, how="diagonal_relaxed")
    transaction = uuid.uuid4().hex
    temporary = destination.with_name(f".{destination.name}.{transaction}.tmp")
    staged = {destination: temporary}
    try:
        combined.sink_parquet(temporary)
        total = pl.scan_parquet(temporary).select(pl.len()).collect(engine="streaming").item()
        if total != sum(row["registros"] for row in audit):
            raise ValueError("Contagem agregada difere das fontes; saída anterior preservada.")
        if csv_destination is not None:
            csv_destination.parent.mkdir(parents=True, exist_ok=True)
            csv_temporary = csv_destination.with_name(f".{csv_destination.name}.{transaction}.tmp")
            staged[csv_destination] = csv_temporary
            # Exportar do parquet preparado, antes de substituir qualquer saída.
            pl.scan_parquet(temporary).sink_csv(csv_temporary, separator=";")
        _publish_outputs(staged, manifest_path)
    finally:
        # Se uma restauração falhar, preservar materiais para recuperação.
        if not manifest_path.with_suffix(manifest_path.suffix + ".pending").exists():
            for staged_path in staged.values():
                staged_path.unlink(missing_ok=True)
    return audit_frame


def clean_inmet(frame: pl.LazyFrame) -> tuple[pl.LazyFrame, list[str]]:
    """Padroniza headers antigos/novos e conserva flags de falhas e de triagem."""
    names = frame.collect_schema().names()
    rename = {name: _key(name) for name in names}
    grouped = {}
    for original, canonical in rename.items():
        grouped.setdefault(canonical, []).append(original)
    # Duas grafias de radiação podem coexistir em parquets legados. Exigir
    # ausência de conflito antes de coalescer, sem somar percentuais de presença.
    for canonical, originals in grouped.items():
        if len(originals) > 1:
            mismatch = frame.select(
                (pl.sum_horizontal([pl.col(c).is_not_null().cast(pl.Int8) for c in originals]) > 1).sum()
            ).collect(engine="streaming").item()
            if mismatch:
                raise ValueError(f"{canonical}: colunas simultâneas; reconcilie valores antes de mesclar")
    frame = frame.select([pl.coalesce([pl.col(c).cast(pl.String) for c in originals]).alias(canonical) for canonical, originals in grouped.items()])
    names = frame.collect_schema().names()
    dates = [c for c in ("DATA_YYYY_MM_DD", "DATA") if c in names]
    hours = [c for c in ("HORA_UTC", "HORA") if c in names]
    if not dates or not hours or not {"CODIGO_WMO", "UF", "ANO_REFERENCIA"}.issubset(names):
        raise ValueError("INMET sem data/hora/WMO/UF/ano obrigatórios")
    frame = frame.with_columns(pl.col("UF", "CODIGO_WMO").str.strip_chars().str.to_uppercase())
    parsed_date = pl.coalesce([pl.col(c).str.strip_chars().str.to_date(fmt, strict=False) for c in dates for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y")])
    parsed_hour = pl.coalesce([
        pl.col(c).str.strip_chars().str.extract(r"^(\d{2})(?::?00)(?:\s+UTC)?$", 1).cast(pl.Int8, strict=False)
        for c in hours
    ])
    frame = frame.with_columns(parsed_date.alias("DATA"), parsed_hour.alias("HORA_UTC_NUM"))
    frame = frame.with_columns(
        (pl.col("DATA").is_null() | pl.col("HORA_UTC_NUM").is_null() | ~pl.col("HORA_UTC_NUM").is_between(0, 23)).fill_null(True).alias("TEMPO_INVALIDO"),
        (pl.col("DATA").dt.year() != pl.col("ANO_REFERENCIA").cast(pl.Int32, strict=False)).fill_null(True).alias("ANO_REFERENCIA_DIVERGENTE"),
    )
    measurements = []
    text = {*META, "DATA_YYYY_MM_DD", "DATA", "HORA", "HORA_UTC", "ARQUIVO_ORIGEM"}
    for name in names:
        if name in text or name.startswith("EMPTY_"):
            continue
        raw = pl.col(name).str.strip_chars()
        numeric = raw.str.replace(",", ".", literal=True).cast(pl.Float64, strict=False)
        missing = raw.is_null() | raw.is_in(["", "-9999", "-9999.0", "-9999,0"])
        lower, upper = None, None
        if "ORVALHO" in name: lower, upper = -80, 55
        elif "TEMPERATURA" in name: lower, upper = -20, 55
        elif "UMIDADE" in name: lower, upper = 0, 100
        elif "PRESSAO" in name: lower, upper = 500, 1100
        elif "PRECIPITACAO" in name: lower, upper = 0, 300
        elif "RADIACAO" in name: lower, upper = 0, 6000
        elif "DIRECAO" in name: lower, upper = 0, 360
        elif "VENTO" in name: lower, upper = 0, 100
        if lower is None:
            raise ValueError(f"Coluna não reconhecida: {name}; classifique-a antes da análise")
        invalid = (~missing & (numeric.is_null() | ~numeric.is_finite() | ~numeric.is_between(lower, upper))).fill_null(False)
        frame = frame.with_columns(
            pl.col(name).alias(f"BRUTO_{name}"),
            invalid.alias(f"INVALIDO_{name}"),
            pl.when(~missing & ~invalid).then(numeric).otherwise(None).alias(name),
        )
        measurements.append(name)
    return frame, measurements


def daily_weather(
    frame: pl.LazyFrame,
    minimum_hours: int = 18,
    start_date: date | None = None,
    end_date: date | None = None,
    fixed_stations: int = 3,
    minimum_station_coverage: float = 0.8,
) -> pl.LazyFrame:
    """Publica médias disponíveis e sensibilidade com uma composição fixa por UF/ano.

    Precipitação é atribuída ao início do intervalo de uma hora terminado na
    observação. A coorte fixa usa somente cobertura meteorológica, nunca dengue:
    escolhe as k estações com mais dias completos comuns às três variáveis,
    exigindo cobertura mínima no calendário solicitado. Dias sem todas as k
    estações válidas permanecem nulos, preservando pesos constantes.
    """
    if not isinstance(minimum_hours, int) or not 1 <= minimum_hours <= 24:
        raise ValueError("minimum_hours deve estar entre 1 e 24.")
    if not isinstance(fixed_stations, int) or fixed_stations < 1:
        raise ValueError("fixed_stations deve ser um inteiro positivo.")
    if not 0 < minimum_station_coverage <= 1:
        raise ValueError("Cobertura mínima deve estar em (0, 1].")
    names = frame.collect_schema().names()
    def find(prefix):
        matches = [c for c in names if c.startswith(prefix)]
        if len(matches) != 1:
            raise ValueError(f"Esperada uma variável {prefix}: {matches}")
        return matches[0]
    variables = {"temp_media_c": find("TEMPERATURA_DO_AR_BULBO_SECO"),
                 "precip_media_mm": find("PRECIPITACAO_TOTAL"),
                 "umidade_media_pct": find("UMIDADE_RELATIVA_DO_AR")}
    frame = frame.with_columns(pl.col("UF", "CODIGO_WMO").cast(pl.String).str.strip_chars().str.to_uppercase())
    invalid_identifier = (~pl.col("UF").is_in(sorted(UF_SIGLAS)) | ~pl.col("CODIGO_WMO").str.contains(STATION_PATTERN)).fill_null(True)
    invalid = frame.filter(pl.col("TEMPO_INVALIDO") | invalid_identifier).select(pl.len()).collect(engine="streaming").item()
    if invalid:
        raise ValueError(f"{invalid} registros sem tempo/WMO/UF válido; agregação interrompida antes do join")
    first, last = frame.select(pl.col("DATA").min().alias("inicio"), pl.col("DATA").max().alias("fim")).collect(engine="streaming").row(0)
    if first is None:
        raise ValueError("Sem observações INMET no intervalo solicitado.")
    start_date, end_date = start_date or first, end_date or last
    if start_date > end_date:
        raise ValueError("Período meteorológico invertido.")
    keys = ["CODIGO_WMO", "DATA", "HORA_UTC_NUM"]
    duplicate = frame.group_by(keys).len().filter(pl.col("len") > 1).limit(1).collect(engine="streaming")
    if duplicate.height:
        raise ValueError("Observações WMO/data/hora duplicadas; agregação interrompida")
    station_uf = frame.group_by("CODIGO_WMO").agg(pl.col("UF").n_unique().alias("n")).filter(pl.col("n") != 1).limit(1).collect(engine="streaming")
    if station_uf.height:
        raise ValueError("Código WMO associado a múltiplas UFs")
    station_keys = ["UF", "CODIGO_WMO", "DATA"]
    instantaneous = frame.group_by(station_keys).agg([
        pl.when(pl.col(column).count() >= minimum_hours).then(pl.col(column).mean()).otherwise(None).alias(output)
        for output, column in variables.items() if output != "precip_media_mm"
    ])
    rain_column = variables["precip_media_mm"]
    rainfall = frame.with_columns(
        (pl.col("DATA").cast(pl.Datetime) + pl.duration(hours=pl.col("HORA_UTC_NUM")) - pl.duration(hours=1)).dt.date().alias("DATA")
    ).group_by(station_keys).agg(
        pl.when(pl.col(rain_column).count() == 24).then(pl.col(rain_column).sum()).otherwise(None).alias("precip_media_mm")
    )
    station_day = instantaneous.join(rainfall, on=station_keys, how="full", coalesce=True).filter(
        pl.col("DATA").is_between(start_date, end_date)
    ).with_columns(pl.col("DATA").dt.year().alias("ANO"))
    available = station_day.group_by("UF", "DATA").agg(
        pl.col("CODIGO_WMO").n_unique().alias("n_estacoes_presentes"),
        *[expr for output in variables for expr in (
            pl.col(output).mean().alias(output),
            pl.col(output).count().alias(f"n_estacoes_validas_{output}"),
        )],
    ).with_columns(pl.col("DATA").dt.year().alias("ANO"))

    expected_days = pl.DataFrame({
        "ANO": list(range(start_date.year, end_date.year + 1)),
        "dias_esperados": [
            (min(end_date, date(year, 12, 31)) - max(start_date, date(year, 1, 1))).days + 1
            for year in range(start_date.year, end_date.year + 1)
        ],
    }).with_columns(pl.col("ANO").cast(pl.Int32)).lazy()
    complete_day = pl.all_horizontal([pl.col(column).is_not_null() for column in variables])
    coverage = station_day.group_by("UF", "ANO", "CODIGO_WMO").agg(
        complete_day.sum().alias("dias_completos")
    ).join(expected_days, on="ANO", how="left").filter(
        pl.col("dias_completos") >= pl.col("dias_esperados") * minimum_station_coverage
    ).sort(["UF", "ANO", "dias_completos", "CODIGO_WMO"], descending=[False, False, True, False]).with_columns(
        pl.col("CODIGO_WMO").cum_count().over("UF", "ANO").alias("ordem")
    ).filter(pl.col("ordem") <= fixed_stations)
    roster_summary = coverage.group_by("UF", "ANO").agg(
        pl.len().alias("n_estacoes_painel_fixo"),
        pl.col("CODIGO_WMO").sort().str.join(",").alias("estacoes_painel_fixo"),
    ).filter(pl.col("n_estacoes_painel_fixo") == fixed_stations)
    roster = coverage.join(roster_summary.select("UF", "ANO"), on=["UF", "ANO"], how="semi")
    fixed = station_day.join(roster.select("UF", "ANO", "CODIGO_WMO"), on=["UF", "ANO", "CODIGO_WMO"], how="inner").group_by("UF", "DATA").agg([
        pl.when(pl.col(column).count() == fixed_stations).then(pl.col(column).mean()).otherwise(None).alias(f"fixo_{column}")
        for column in variables
    ])
    return available.join(fixed, on=["UF", "DATA"], how="left").join(
        roster_summary, on=["UF", "ANO"], how="left"
    ).with_columns(pl.col("n_estacoes_painel_fixo").fill_null(0)).drop("ANO").sort("UF", "DATA")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--output", type=Path, default=ROOT / "data/INMET_dataset_agregado.parquet")
    parser.add_argument("--csv-output", type=Path, help="exporta também CSV com ';' para a landing Databricks")
    args = parser.parse_args()
    print(ingest_inmet(args.source, args.output, args.csv_output))


if __name__ == "__main__":
    main()
