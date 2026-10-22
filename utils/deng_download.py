"""Baixa anos encerrados do SINAN para o diretório usado pelo pipeline."""

import argparse
import hashlib
import json
import os
import shutil
import zipfile
from datetime import date
from pathlib import Path

SOURCE_DIR = Path(__file__).resolve().parents[1] / "sinan"
OFFICIAL_CSV_YEARS = {2007, 2008, 2009, 2012}


def _official_csv(year, cache):
    """Recupera anos ausentes do catálogo, sem substituir por outra coorte."""
    import httpx
    import polars as pl

    url = f"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/Dengue/csv/DENGBR{year % 100:02d}.csv.zip"
    cache.mkdir(parents=True, exist_ok=True)
    archive_path = cache / f"DENGBR{year % 100:02d}.csv.zip"
    temporary = archive_path.with_suffix(".zip.tmp")
    with httpx.stream("GET", url, timeout=180, follow_redirects=True) as response:
        response.raise_for_status()
        with temporary.open("wb") as stream:
            for chunk in response.iter_bytes():
                stream.write(chunk)
    temporary.replace(archive_path)
    csv_path = archive_path.with_suffix("")
    with zipfile.ZipFile(archive_path) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(members) != 1:
            raise ValueError(f"Arquivo oficial {year}: esperado exatamente um CSV")
        with archive.open(members[0]) as source, csv_path.open("wb") as destination:
            shutil.copyfileobj(source, destination)
    frame = pl.read_csv(csv_path, infer_schema=False, encoding="utf8", try_parse_dates=False)
    return frame.to_pandas(), {"url": url, "sha256_fonte": _digest(archive_path), "formato_fonte": "CSV.ZIP"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=int, default=2000)
    parser.add_argument("--end", type=int, default=2023)
    parser.add_argument("--output", type=Path, default=SOURCE_DIR)
    parser.add_argument("--origin", choices=["DADOSGOV", "FTP"], default="DADOSGOV")
    parser.add_argument("--cache", type=Path, help="cache isolado por origem")
    args = parser.parse_args()
    if not 2000 <= args.start <= args.end < date.today().year:
        parser.error("Escolha anos encerrados, a partir de 2000.")
    args.output.mkdir(parents=True, exist_ok=True)
    # Nunca reutilizar o cache legado que mistura origens no mesmo basename.
    # PySUS captura esse caminho em argumentos padrão durante o import:
    # set_cache() depois do import não move todos os clientes internos.
    cache = (args.cache or args.output / f".pysus-{args.origin.lower()}-cache-v1").resolve()
    os.environ["PYSUS_CACHEPATH"] = str(cache)
    import pysus
    from pysus.api.progress import disable_progress_bars

    disable_progress_bars()
    manifest_path = args.output / "manifesto_download.json"
    records = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else []
    manifest = {record["ano"]: record for record in records}
    for year in range(args.start, args.end + 1):
        destination = args.output / f"DENGBR{year % 100:02d}.parquet"
        previous = manifest.get(year, {})
        if (previous.get("origem") == args.origin and previous.get("arquivo") == destination.name
                and previous.get("registros", 0) > 0 and destination.exists()
                and previous.get("sha256") == _digest(destination)):
            print(f"{year}: publicação {args.origin} verificada; reutilizando {destination}", flush=True)
            continue
        if args.origin == "DADOSGOV" and year in OFFICIAL_CSV_YEARS:
            frame, provenance = _official_csv(year, cache / "official-csv")
        else:
            origin = pysus.dadosgov if args.origin == "DADOSGOV" else pysus.ftp
            paths = origin.sinan(disease="DENG", year=year, download=False, show_progress=False)
            if len(paths) != 1:
                raise ValueError(f"{args.origin} {year}: esperada uma fonte, recebidas {paths}; não combinar espelhos.")
            frame = origin.sinan(disease="DENG", year=year, as_dataframe=True, show_progress=False)
            provenance = {"path_fonte": str(paths[0]), "formato_fonte": "PARQUET"}
        if frame.empty:
            raise ValueError(f"SINAN {year}: fonte vazia; nenhum zero será inferido.")
        frame.columns = [column.upper() for column in frame.columns]
        if len(set(frame.columns)) != len(frame.columns):
            raise ValueError(f"SINAN {year}: nomes duplicados após normalização.")
        temporary = destination.with_suffix(".parquet.tmp")
        frame.to_parquet(temporary, index=False)
        temporary.replace(destination)
        manifest[year] = {"ano": year, "arquivo": destination.name, "registros": len(frame),
                         "origem": args.origin, **provenance,
                         "sha256": _digest(destination),
                         "extraido_em": date.today().isoformat()}
        # Checkpoint por ano permite retomar sem repetir anos já verificados.
        manifest_temporary = manifest_path.with_suffix(".json.tmp")
        manifest_temporary.write_text(json.dumps([manifest[y] for y in sorted(manifest)], indent=2), encoding="utf-8")
        manifest_temporary.replace(manifest_path)
        print(f"{year}: {len(frame):,} registros em {destination}", flush=True)


def _digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


if __name__ == "__main__":
    main()
