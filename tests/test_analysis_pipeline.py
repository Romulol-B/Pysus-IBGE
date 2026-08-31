import tempfile
import unittest
from pathlib import Path

import polars as pl

from analysis_pipeline import build_sinan_outputs, scan_sinan


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
                "CLASSI_FIN": ["10", "5"],
                "CS_RACA": ["4", None],
                "EVOLUCAO": ["2", "9"],
            }
        ).write_parquet(self.source / "DENGBR07.parquet")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_schema_eras_are_harmonized(self) -> None:
        frame = scan_sinan(self.source).collect().sort("arquivo", "data_notificacao")
        self.assertEqual(frame.height, 4)
        self.assertEqual(frame["codigo_municipio"].to_list(), ["355030", "330455", "355030", "330455"])
        self.assertEqual(frame["status_classificacao"].to_list(), ["confirmado", "descartado", "confirmado", "descartado"])
        self.assertEqual(frame["data_notificacao"].null_count(), 0)
        self.assertEqual(frame["raca"].to_list(), ["branca", "ignorado", "parda", "nao_preenchido"])

    def test_municipal_panel_preserves_totals(self) -> None:
        outputs = build_sinan_outputs(self.source, self.output)
        panel = outputs["painel"]
        self.assertEqual(panel["notificacoes"].sum(), 4)
        self.assertEqual(panel["casos_confirmados"].sum(), 2)
        self.assertEqual(panel.select("ano", "codigo_municipio").unique().height, panel.height)
        self.assertTrue((self.output / "sinan_municipio_ano.parquet").exists())


if __name__ == "__main__":
    unittest.main()
