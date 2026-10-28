import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import polars as pl

from utils.aggregate_sources import (
    SANITATION, UF_CODES, aggregate_sinan_monthly, aggregate_weather_monthly,
    build_aggregations, export_aggregations, summarize_annual,
)


class AggregationsTest(unittest.TestCase):
    def sinan(self):
        population = pl.DataFrame({"codigo_municipio": ["355030", "350950", "330455"],
                                   "populacao": [1000, 9000, 5000]})
        raw = pl.DataFrame({
            "arquivo": ["DENGBR10.parquet", "DENGBR09.parquet", "DENGBR10.parquet",
                        "DENGBR10.parquet", "DENGBR10.parquet", "DENGBR10.parquet"],
            "ano_arquivo": [2010, 2009, 2010, 2010, 2010, 2010],
            "data_notificacao": [date(2010, 1, 2), date(2010, 1, 3), date(2011, 1, 1),
                                  date(2010, 2, 1), None, date(2010, 2, 3)],
            "codigo_municipio": ["355030", "350950", "355030", None, "355030", "359999"],
            "caso_confirmado": [True, False, True, True, True, True],
            "data_notificacao_original": ["20100102", "20100103", "20110101", "20100201", "erro", "20100203"],
            "codigo_municipio_original": ["355030", "350950", "355030", None, "355030", "359999"],
            "classificacao_original": ["1", "5", "1", "1", "1", "1"],
        }).lazy()
        return aggregate_sinan_monthly(raw, population, 2010)

    def daily(self):
        dates = pl.date_range(date(2010, 1, 1), date(2010, 1, 31), eager=True)
        return pl.DataFrame({"UF": ["SP"] * 31, "DATA": dates,
                             "fixo_temp_media_c": [25.0] * 31,
                             "fixo_precip_media_mm": [2.0] * 31,
                             "fixo_umidade_media_pct": [70.0] * 31,
                             "n_estacoes_painel_fixo": [3] * 31,
                             "estacoes_painel_fixo": ["A001,A002,A003"] * 31})

    def test_calendar_dates_residence_and_population_reconcile(self):
        result = self.sinan()
        monthly = result["mensal"]
        self.assertEqual(monthly.height, 24)
        january = monthly.filter((pl.col("UF") == "SP") & (pl.col("mes") == 1)).row(0, named=True)
        self.assertEqual(january["notificacoes"], 2)
        self.assertEqual(january["casos_confirmados"], 1)
        self.assertEqual(january["notificacoes_outro_ano_fonte"], 1)
        self.assertEqual(january["populacao"], 10000)
        self.assertAlmostEqual(january["taxa_confirmados_100k"], 10.0)
        self.assertEqual(result["excluidos"].height, 3)
        self.assertEqual(result["auditoria"]["notificacoes"].sum(), 6)
        self.assertEqual(monthly.filter(pl.col("UF") == "RJ")["notificacoes"].sum(), 0)

    def test_rain_requires_all_calendar_days_and_missing_never_becomes_zero(self):
        daily = self.daily().with_columns(
            pl.when(pl.col("DATA") == date(2010, 1, 31)).then(None).otherwise(pl.col("fixo_precip_media_mm")).alias("fixo_precip_media_mm")
        )
        monthly = aggregate_weather_monthly(daily, 2010)
        january = monthly.filter((pl.col("UF") == "SP") & (pl.col("mes") == 1)).row(0, named=True)
        self.assertIsNone(january["chuva_acumulada_mm"])
        self.assertEqual(january["dias_validos_chuva_acumulada_mm"], 30)
        self.assertEqual(january["temperatura_media_c"], 25.0)
        self.assertEqual(january["situacao_inmet"], "cobertura_insuficiente")
        february = monthly.filter((pl.col("UF") == "SP") & (pl.col("mes") == 2)).row(0, named=True)
        self.assertEqual(february["dias_calendario"], 28)
        self.assertIsNone(february["chuva_acumulada_mm"])
        self.assertEqual(february["n_estacoes_painel_fixo"], 3)
        self.assertEqual(february["situacao_inmet"], "cobertura_insuficiente")
        absent_uf = monthly.filter(pl.col("UF") == "RJ")
        self.assertEqual(absent_uf["situacao_inmet"].unique().to_list(), ["sem_painel_elegivel"])

    def test_sparse_days_do_not_count_as_a_complete_month(self):
        monthly = aggregate_weather_monthly(self.daily().head(1), 2010)
        january = monthly.filter((pl.col("UF") == "SP") & (pl.col("mes") == 1)).row(0, named=True)
        self.assertEqual(january["dias_calendario"], 31)
        self.assertIsNone(january["temperatura_media_c"])
        self.assertIsNone(january["chuva_acumulada_mm"])

    def test_complete_rain_and_real_dry_days_are_preserved(self):
        monthly = aggregate_weather_monthly(self.daily(), 2010)
        self.assertEqual(monthly.filter((pl.col("UF") == "SP") & (pl.col("mes") == 1))["chuva_acumulada_mm"].item(), 62.0)
        dry = aggregate_weather_monthly(self.daily().with_columns(pl.lit(0.0).alias("fixo_precip_media_mm")), 2010)
        self.assertEqual(dry.filter((pl.col("UF") == "SP") & (pl.col("mes") == 1))["chuva_acumulada_mm"].item(), 0.0)

    def test_duplicate_daily_observations_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "duplicada"):
            aggregate_weather_monthly(pl.concat([self.daily(), self.daily().head(1)]), 2010)

    def test_annual_denominator_is_not_multiplied_by_twelve(self):
        result = self.sinan()
        monthly = result["mensal"].join(aggregate_weather_monthly(self.daily(), 2010), on=["UF", "mes"], validate="1:1").with_columns(
            *[pl.lit(80.0).alias(c) for c in SANITATION]
        )
        annual = summarize_annual(monthly)
        sp = annual.filter(pl.col("UF") == "SP").row(0, named=True)
        self.assertEqual(sp["populacao"], 10000)
        self.assertEqual(sp["casos_confirmados"], 1)
        self.assertAlmostEqual(sp["taxa_confirmados_100k"], 10.0)
        self.assertEqual(sp["pct_agua_rede"], 80.0)
        self.assertIsNone(sp["chuva_anual_mm"])
        self.assertIsNone(sp["temperatura_media_c"])
        with tempfile.TemporaryDirectory() as tmp:
            export_aggregations({**result, "mensal": monthly, "anual": annual, "metadados": {"ano": 2010}}, Path(tmp))
            self.assertEqual(pl.read_parquet(Path(tmp) / "anual_uf.parquet").height, 2)
            self.assertTrue((Path(tmp) / "auditoria_sinan_excluidos.csv").exists())

    def test_large_counts_keep_float_rates_without_integer_overflow(self):
        monthly = self.sinan()["mensal"].join(aggregate_weather_monthly(self.daily(), 2010), on=["UF", "mes"], validate="1:1").with_columns(
            *[pl.lit(80.0).alias(c) for c in SANITATION],
            pl.lit(500_000, dtype=pl.UInt32).alias("casos_confirmados"),
            pl.lit(600_000, dtype=pl.UInt32).alias("notificacoes"),
        )
        sp = summarize_annual(monthly).filter(pl.col("UF") == "SP").row(0, named=True)
        self.assertEqual(sp["taxa_confirmados_100k"], 60_000_000.0)
        self.assertEqual(sp["taxa_notificacoes_100k"], 72_000_000.0)

    def test_full_weather_pipeline_and_missing_weather_branch(self):
        """Um ano horário completo verifica virada UTC, painel, joins e taxas."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, processed = root / "sinan", root / "processados"
            source.mkdir()
            processed.mkdir()
            population = pl.DataFrame({"ano": [2010] * 27,
                "codigo_municipio": [code + "0001" for code in UF_CODES], "populacao": [1000] * 27})
            population.write_parquet(processed / "ibge_populacao_municipio_ano.parquet")
            for year, day in [(2009, 1), (2010, 2), (2011, 3)]:
                pl.DataFrame({"DT_NOTIFIC": [f"2010-01-{day:02d}"], "NU_ANO": [str(year)],
                              "ID_MUNICIP": ["350001"], "ID_MN_RESI": ["350001"],
                              "CLASSI_FIN": ["10"]}).write_parquet(source / f"DENGBR{year % 100:02d}.parquet")
            hours = [datetime(2010, 1, 1) + timedelta(hours=h) for h in range(365 * 24 + 1)]
            weather = pl.DataFrame({"DATA": [d.strftime("%Y-%m-%d") for d in hours],
                "HORA_UTC": [d.strftime("%H00") for d in hours],
                "ANO_REFERENCIA": [str(d.year) for d in hours],
                "TEMPERATURA_DO_AR_BULBO_SECO_HORARIA_C": ["25"] * len(hours),
                "PRECIPITACAO_TOTAL_HORARIO_MM": ["1"] * len(hours),
                "UMIDADE_RELATIVA_DO_AR_HORARIA_PCT": ["70"] * len(hours),
                "UF": ["SP"] * len(hours),
            }).join(pl.DataFrame({"CODIGO_WMO": ["A001", "A002", "A003"]}), how="cross")
            path = root / "inmet.parquet"
            weather.write_parquet(path)
            sanitation = pl.DataFrame({"UF_codigo": list(UF_CODES), "ano": [2010] * 27,
                                       **{c: [80.0] * 27 for c in SANITATION}})
            with patch.dict("utils.aggregate_sources.CENSUS_MUNICIPALITY_COUNTS", {2010: 27}), \
                    patch("utils.aggregate_sources.load_sanitation_uf", return_value=sanitation):
                result = build_aggregations(2010, path, source, processed)
                sp = result["anual"].filter(pl.col("UF") == "SP").row(0, named=True)
                self.assertEqual(result["mensal"].height, 324)
                self.assertEqual(sp["chuva_anual_mm"], 365 * 24)
                self.assertEqual(sp["temperatura_media_c"], 25)
                self.assertEqual(sp["umidade_media_pct"], 70)
                self.assertEqual(sp["meses_clima_completo"], 12)
                self.assertEqual(sp["taxa_confirmados_100k"], 300)
                self.assertEqual(result["mensal"]["tres_fontes_disponiveis"].sum(), 12)
                self.assertTrue(result["mensal"].filter(pl.col("UF") == "RJ")["chuva_acumulada_mm"].is_null().all())
                partial = build_aggregations(2010, root / "ausente.parquet", source, processed)
                self.assertEqual(partial["mensal"]["tres_fontes_disponiveis"].sum(), 0)
                self.assertEqual(partial["mensal"]["situacao_inmet"].unique().to_list(), ["arquivo_ausente"])
                self.assertTrue(partial["mensal"]["chuva_acumulada_mm"].is_null().all())


if __name__ == "__main__":
    unittest.main()
