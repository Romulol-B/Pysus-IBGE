import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import polars as pl

from analysis_pipeline import (
    _build_integration_audit,
    _build_population_contract,
    _build_sanitation_contract,
    _normalize_municipality_code,
    _sinan_without_census_population,
    _validate_percentage_groups,
    build_sinan_outputs,
    fetch_population,
    scan_sinan,
)


class SinanHarmonizationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.source = Path(self.temp_dir.name) / "sinan"
        self.output = Path(self.temp_dir.name) / "processados"
        self.source.mkdir()

        pl.DataFrame(
            {
                "DT_NOTIFIC": ["2000-01-02", "2000-02-03"],
                "NU_ANO": ["2000", "2000"],
                "ID_MUNICIP": ["3550308", "3304557"],
                "ID_MN_RESI": ["3304557", "3550308"],
                "DENGUE": ["1", "2"],
                "CS_RACA": ["1", "9"],
                "CON_EVOLUC": ["1", ""],
            }
        ).write_parquet(self.source / "DENGBR00.parquet")

        pl.DataFrame(
            {
                "DT_NOTIFIC": ["20070102", "20070203"],
                "NU_ANO": ["2007", "2007"],
                "ID_MUNICIP": ["355030", "330455"],
                "ID_MN_RESI": ["355030", None],
                "CLASSI_FIN": ["10", "5"],
                "CS_RACA": ["4", None],
                "EVOLUCAO": ["2", "9"],
            }
        ).write_parquet(self.source / "DENGBR07.parquet")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_municipality_codes_are_normalized_without_inventing_values(self) -> None:
        frame = pl.DataFrame(
            {
                "original": [
                    "3550308",
                    "355030",
                    " 3304557 ",
                    "12345",
                    "ABC123",
                    None,
                ]
            }
        ).with_columns(
            _normalize_municipality_code(pl.col("original")).alias("normalizado")
        )
        self.assertEqual(
            frame["normalizado"].to_list(),
            ["355030", "355030", "330455", None, None, None],
        )

    def test_schema_eras_use_residence_and_preserve_notification(self) -> None:
        frame = scan_sinan(self.source).collect().sort("arquivo", "data_notificacao")
        self.assertEqual(frame.height, 4)
        self.assertEqual(
            frame["codigo_municipio"].to_list(),
            ["330455", "355030", "355030", None],
        )
        self.assertEqual(
            frame["codigo_municipio_notificacao"].to_list(),
            ["355030", "330455", "355030", "330455"],
        )
        self.assertEqual(
            frame["municipio_residencia_notificacao_divergente"].to_list(),
            [True, True, False, False],
        )
        self.assertEqual(
            frame["status_classificacao"].to_list(),
            ["confirmado", "descartado", "confirmado", "descartado"],
        )
        self.assertEqual(frame["data_notificacao"].null_count(), 0)
        self.assertEqual(
            frame["raca"].to_list(),
            ["branca", "ignorado", "parda", "nao_preenchido"],
        )

    def test_municipal_panel_preserves_valid_residence_totals_and_audits_invalid(self) -> None:
        outputs = build_sinan_outputs(self.source, self.output)
        panel = outputs["painel"]
        self.assertEqual(panel["notificacoes"].sum(), 3)
        self.assertEqual(panel["casos_confirmados"].sum(), 2)
        self.assertEqual(
            panel.select("ano", "codigo_municipio").unique().height,
            panel.height,
        )
        self.assertEqual(outputs["codigos_invalidos"].height, 1)
        self.assertEqual(
            outputs["qualidade"]["codigo_municipio_residencia_invalido"].sum(),
            1,
        )
        self.assertEqual(outputs["fluxo_residencia_notificacao"]["notificacoes"].sum(), 3)
        self.assertTrue((self.output / "sinan_municipio_ano.parquet").exists())
        self.assertTrue(
            (self.output / "auditoria_sinan_codigos_municipio_invalidos.csv").exists()
        )
        self.assertTrue(
            (self.output / "auditoria_sinan_fluxo_residencia_notificacao.parquet").exists()
        )


class PopulationSourceTest(unittest.TestCase):
    @patch("analysis_pipeline._validate_census_population")
    @patch("analysis_pipeline._fetch_sidra")
    def test_population_2010_uses_table_136_variable_93_and_total_category(
        self,
        fetch_mock,
        _validate_mock,
    ) -> None:
        def fake_fetch(url: str) -> pl.DataFrame:
            if "/t/6579/" in url:
                periods = re.search(r"/p/([0-9,]+)$", url).group(1).split(",")
                return pl.DataFrame(
                    {
                        "Município (Código)": ["3550308"] * len(periods),
                        "Município": ["São Paulo (SP)"] * len(periods),
                        "Ano": periods,
                        "Valor": ["100"] * len(periods),
                    }
                )
            if "/t/136/" in url:
                return pl.DataFrame(
                    {
                        "Município (Código)": ["3550308"],
                        "Município": ["São Paulo (SP)"],
                        "Ano": ["2010"],
                        "Valor": ["11253503"],
                        "Cor ou raça (Código)": ["0"],
                    }
                )
            return pl.DataFrame(
                {
                    "Município (Código)": ["3550308"],
                    "Município": ["São Paulo (SP)"],
                    "Ano": ["2022"],
                    "Valor": ["11451999"],
                }
            )

        fetch_mock.side_effect = fake_fetch
        population = fetch_population()
        requested_urls = [call.args[0] for call in fetch_mock.call_args_list]
        self.assertIn(
            "https://apisidra.ibge.gov.br/values/t/136/n6/all/v/93/p/2010/c86/0",
            requested_urls,
        )
        census_2010 = population.filter(pl.col("ano") == 2010).row(0, named=True)
        self.assertEqual(census_2010["populacao"], 11_253_503)
        self.assertEqual(
            census_2010["fonte_populacao"],
            "SIDRA 136 v93 c86=0 - Censo 2010",
        )


class MunicipalContractTest(unittest.TestCase):
    @staticmethod
    def population() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "ano": [2010, 2010, 2022, 2022],
                "codigo_municipio": ["000001", "000002", "000001", "000003"],
                "municipio": ["A", "B", "A", "C"],
                "populacao": [1_000, 2_000, 2_000, 4_000],
                "fonte_populacao": ["censo"] * 4,
            }
        )

    @staticmethod
    def panel() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "ano": [2010, 2022, 2010, 2023],
                "codigo_municipio": ["000001", "000001", "999999", "000001"],
                "notificacoes": [10, 20, 7, 3],
                "casos_confirmados": [4, 5, 2, 1],
            }
        )

    def test_census_is_complete_grid_rates_use_denominators_and_history_is_kept(self) -> None:
        contract = _build_population_contract(self.panel(), self.population())
        census_contract = contract.filter(pl.col("ano").is_in([2010, 2022]))
        self.assertEqual(census_contract.height, 4)
        self.assertEqual(
            contract.select("ano", "codigo_municipio").unique().height,
            contract.height,
        )
        self.assertEqual(contract.filter(pl.col("ano") == 2023).height, 1)
        self.assertIsNone(
            contract.filter(pl.col("ano") == 2023)["populacao"].item()
        )

        with_record = contract.filter(
            (pl.col("ano") == 2010) & (pl.col("codigo_municipio") == "000001")
        ).row(0, named=True)
        self.assertTrue(with_record["tem_notificacao_registrada"])
        self.assertEqual(
            with_record["situacao_registro_sinan"], "com_notificacao_registrada"
        )
        self.assertAlmostEqual(with_record["taxa_notificacoes_100k"], 1_000.0)
        self.assertAlmostEqual(with_record["taxa_confirmados_100k"], 400.0)

        without_record = contract.filter(
            (pl.col("ano") == 2010) & (pl.col("codigo_municipio") == "000002")
        ).row(0, named=True)
        self.assertFalse(without_record["tem_notificacao_registrada"])
        self.assertEqual(
            without_record["situacao_registro_sinan"], "sem_notificacao_registrada"
        )
        self.assertEqual(without_record["notificacoes"], 0)
        self.assertEqual(without_record["casos_confirmados"], 0)
        self.assertEqual(without_record["taxa_notificacoes_100k"], 0.0)
        self.assertEqual(without_record["taxa_confirmados_100k"], 0.0)

        rate_2022 = contract.filter(
            (pl.col("ano") == 2022) & (pl.col("codigo_municipio") == "000001")
        )["taxa_notificacoes_100k"].item()
        self.assertAlmostEqual(rate_2022, 1_000.0)

    def test_duplicate_dimension_key_is_rejected_before_join(self) -> None:
        duplicated = pl.concat([self.population(), self.population().head(1)])
        with self.assertRaisesRegex(ValueError, "duplicada"):
            _build_population_contract(self.panel(), duplicated)

    def test_valid_looking_sinan_code_outside_census_is_audited(self) -> None:
        unmatched = _sinan_without_census_population(self.panel(), self.population())
        self.assertEqual(unmatched.height, 1)
        self.assertEqual(unmatched["codigo_municipio"].item(), "999999")
        self.assertEqual(unmatched["notificacoes"].item(), 7)

    def test_sanitation_join_preserves_grid_and_exposes_coverage(self) -> None:
        population_contract = _build_population_contract(
            self.panel(), self.population()
        )
        groups = {"dimensao": ("pct_a", "pct_b")}
        sanitation_2010 = pl.DataFrame(
            {
                "ano": [2010],
                "codigo_municipio": ["000001"],
                "municipio": ["A"],
                "pct_a": [60.0],
                "pct_b": [40.0],
            }
        )
        sanitation_2022 = pl.DataFrame(
            {
                "ano": [2022],
                "codigo_municipio": ["000001"],
                "municipio": ["A"],
                "pct_a": [55.0],
                "pct_b": [45.0],
            }
        )
        integrated_2010 = _build_sanitation_contract(
            population_contract, sanitation_2010, 2010, groups
        )
        integrated_2022 = _build_sanitation_contract(
            population_contract, sanitation_2022, 2022, groups
        )

        self.assertEqual(integrated_2010.height, 2)
        self.assertEqual(integrated_2010["correspondencia_sidra"].sum(), 1)
        self.assertEqual(
            integrated_2010.select("ano", "codigo_municipio").unique().height,
            integrated_2010.height,
        )

        audit = _build_integration_audit(
            population_contract, integrated_2010, integrated_2022
        )
        sanitation_audit = audit.filter(pl.col("base") == "saneamento_2010").row(
            0, named=True
        )
        self.assertEqual(sanitation_audit["municipios_com_notificacao"], 1)
        self.assertEqual(sanitation_audit["municipios_sem_notificacao"], 1)
        self.assertAlmostEqual(sanitation_audit["percentual_cobertura_sinan"], 50.0)
        self.assertAlmostEqual(sanitation_audit["percentual_correspondencia"], 50.0)
        self.assertTrue(
            audit["percentual_cobertura_sinan"].is_between(0, 100).all()
        )
        self.assertTrue(audit["percentual_correspondencia"].is_between(0, 100).all())

    def test_duplicate_sanitation_key_is_rejected(self) -> None:
        population_contract = _build_population_contract(
            self.panel(), self.population()
        )
        duplicated = pl.DataFrame(
            {
                "ano": [2010, 2010],
                "codigo_municipio": ["000001", "000001"],
                "pct_a": [60.0, 60.0],
                "pct_b": [40.0, 40.0],
            }
        )
        with self.assertRaisesRegex(ValueError, "duplicada"):
            _build_sanitation_contract(
                population_contract,
                duplicated,
                2010,
                {"dimensao": ("pct_a", "pct_b")},
            )

    def test_percentages_must_be_bounded_and_sum_to_one_hundred(self) -> None:
        groups = {"dimensao": ("pct_a", "pct_b")}
        valid = pl.DataFrame({"pct_a": [60.0, 0.0], "pct_b": [40.0, 100.0]})
        _validate_percentage_groups(valid, groups, "teste")

        out_of_range = pl.DataFrame({"pct_a": [120.0], "pct_b": [-20.0]})
        with self.assertRaisesRegex(ValueError, "fora de"):
            _validate_percentage_groups(out_of_range, groups, "teste")

        does_not_close = pl.DataFrame({"pct_a": [50.0], "pct_b": [40.0]})
        with self.assertRaisesRegex(ValueError, "não fecham 100%"):
            _validate_percentage_groups(does_not_close, groups, "teste")


if __name__ == "__main__":
    unittest.main()
