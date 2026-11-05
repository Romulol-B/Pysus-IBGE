"""Reconcilia os contratos SINAN local e Databricks nos mesmos arquivos de SP."""

import argparse
import hashlib
import json
from pathlib import Path

import polars as pl

from utils.analysis_pipeline import PROCESSED_DIR, SINAN_DIR, _classification_status


def reconciliar(pasta, populacao):
    grade = pl.read_parquet(populacao).filter(
        pl.col("ano").is_in([2010, 2022]) & pl.col("codigo_municipio").str.starts_with("35")
    ).select("ano", "codigo_municipio")
    if grade.group_by("ano").len().sort("ano")["len"].to_list() != [645, 645] or grade.is_duplicated().any():
        raise ValueError("A grade deve conter 645 municípios únicos de SP em cada ano")
    resultados = []
    for ano in (2010, 2022):
        bruto = pl.scan_parquet(pasta / f"DENGBR{ano % 100:02d}.parquet").select(
            pl.col(c).cast(pl.String).str.strip_chars().replace("", None)
            for c in ("ID_MN_RESI", "DT_NOTIFIC", "CLASSI_FIN", "CRITERIO", "EVOLUCAO")
        ).filter(pl.col("ID_MN_RESI").str.starts_with("35"))
        classe, criterio = pl.col("CLASSI_FIN"), pl.col("CRITERIO").is_in(["1", "2"])
        classes = ["1", "2", "3", "4"] if ano == 2010 else ["10", "11", "12"]
        graves = ["2", "3", "4"] if ano == 2010 else ["11", "12"]
        confirmado = classe.is_in(classes) & criterio
        flags = {"casos_confirmados": confirmado, "casos_graves": classe.is_in(graves) & criterio,
                 "obitos_dengue": confirmado & (pl.col("EVOLUCAO") == "2")}
        data = pl.coalesce(pl.col("DT_NOTIFIC").str.to_date(f, strict=False)
                           for f in ("%Y-%m-%d", "%Y%m%d"))
        municipio = pl.when(pl.col("ID_MN_RESI").str.contains(r"^\d{6,7}$")).then(
            pl.col("ID_MN_RESI").str.slice(0, 6))
        local = _classification_status(classe, "CLASSI_FIN") == "confirmado"
        grupos = bruto.with_columns(
            pl.lit(ano).alias("ano"),
            municipio.is_in(grade.filter(pl.col("ano") == ano)["codigo_municipio"].implode())
            .fill_null(False).alias("municipio_na_grade"),
            pl.when(data.is_null()).then(pl.lit("data_invalida"))
            .when(data.dt.year() == ano).then(pl.lit("mesmo_ano"))
            .otherwise(pl.lit("outro_ano_civil")).alias("periodo"),
            pl.when(local & ~classe.is_in(classes).fill_null(False)).then(pl.lit("classe_de_outra_epoca"))
            .when(classe.is_in(classes) & ~criterio.fill_null(False)).then(pl.lit("criterio_fora_1_2_ou_ausente"))
            .otherwise(pl.lit("mesma_elegibilidade")).alias("regra_confirmacao"),
        ).group_by("ano", "municipio_na_grade", "periodo", "regra_confirmacao").agg(
            pl.len().alias("notificacoes"), local.sum().alias("confirmados_local"),
            *[expr for nome, flag in flags.items() for expr in
              (flag.sum().alias(nome), flag.is_null().sum().alias(f"{nome}_desconhecidos"))],
        ).collect()
        resultados.append(grupos)
    return pl.concat(resultados).sort("ano", "municipio_na_grade", "periodo", "regra_confirmacao")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sinan", type=Path, default=SINAN_DIR)
    parser.add_argument("--populacao", type=Path, default=PROCESSED_DIR / "ibge_populacao_municipio_ano.parquet")
    parser.add_argument("--output", type=Path, default=PROCESSED_DIR / "reconciliacao_contratos_sp.csv")
    args = parser.parse_args()
    quadro = reconciliar(args.sinan, args.populacao)
    entradas = {}
    for arquivo in [args.populacao, *(args.sinan / f"DENGBR{a}.parquet" for a in (10, 22))]:
        with arquivo.open("rb") as stream:
            entradas[str(arquivo)] = hashlib.file_digest(stream, "sha256").hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    quadro.write_csv(args.output)
    args.output.with_suffix(".json").write_text(json.dumps({
        "entradas_sha256": entradas,
        "escopo": "Mesmo par de arquivos 2010/2022; não inclui contribuições de arquivos vizinhos.",
        "validacao": "Cálculo local das regras; não comprova a execução Spark nem a coorte de origem.",
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(quadro)


if __name__ == "__main__":
    main()
