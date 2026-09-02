import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely.geometry import Point

import spatial_analysis as spatial


class SpatialMunicipalContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.boundary_path = Path(self.temp_dir.name) / "municipios.geojson"
        self.data_path = Path(self.temp_dir.name) / "indicadores.parquet"
        # A existência do arquivo evita qualquer tentativa de download no teste.
        self.boundary_path.touch()

        self.boundaries = gpd.GeoDataFrame(
            {
                "codarea": ["1100015", "1100023", "1100031"],
                "geometry": [Point(0, 0), Point(1, 0), Point(2, 0)],
            },
            crs="EPSG:4326",
        )
        self.indicators = pd.DataFrame(
            {
                "codigo_municipio": ["110001", "110002", "110003"],
                "municipio": ["Município A", "Município B", "Município C"],
                "populacao": [10_000, 20_000, 30_000],
                "notificacoes": [12, 0, 5],
                "casos_confirmados": [10, 0, 3],
                "taxa_confirmados_100k": [100.0, 0.0, 10.0],
                "pct_agua_rede_geral_principal": [90.0, 80.0, 70.0],
                "pct_esgoto_rede_geral_ou_pluvial": [60.0, 50.0, 40.0],
                "pct_lixo_coletado_domicilio": [95.0, 85.0, 75.0],
                "correspondencia_populacao": [True, True, True],
                "correspondencia_sidra": [True, True, True],
                "tem_notificacao_registrada": [True, False, True],
                "situacao_registro_sinan": [
                    "com_notificacao_registrada",
                    "sem_notificacao_registrada",
                    "com_notificacao_registrada",
                ],
            }
        )

    def tearDown(self) -> None:
        plt.close("all")
        self.temp_dir.cleanup()

    def _load(self, indicators: pd.DataFrame | None = None):
        with (
            patch.object(spatial.gpd, "read_file", return_value=self.boundaries.copy()),
            patch.object(
                spatial.pd,
                "read_parquet",
                return_value=(indicators if indicators is not None else self.indicators).copy(),
            ),
        ):
            return spatial.load_spatial_data(self.data_path, self.boundary_path)

    @staticmethod
    def _audit_values(audit: pd.DataFrame) -> dict[str, float]:
        return audit.set_index("metrica")["valor"].to_dict()

    def test_flag_is_preserved_and_zero_rate_remains_in_analysis(self) -> None:
        full_map, analysis_map, audit = self._load()

        self.assertEqual(len(full_map), 3)
        self.assertEqual(len(analysis_map), 3)
        self.assertEqual(
            full_map["tem_notificacao_registrada"].tolist(),
            [True, False, True],
        )
        self.assertEqual(
            full_map["situacao_registro_sinan"].tolist(),
            self.indicators["situacao_registro_sinan"].tolist(),
        )
        without_notification = analysis_map.loc[
            analysis_map["tem_notificacao_registrada"].eq(False)
        ]
        self.assertEqual(len(without_notification), 1)
        self.assertEqual(without_notification.iloc[0]["taxa_confirmados_100k"], 0.0)

        values = self._audit_values(audit)
        self.assertEqual(values["municipios_com_notificacao_registrada"], 2)
        self.assertEqual(values["municipios_sem_notificacao_registrada"], 1)
        self.assertEqual(values["cobertura_percentual_populacao"], 100.0)
        self.assertEqual(values["cobertura_percentual_saneamento"], 100.0)
        self.assertEqual(values["cobertura_percentual_geometria"], 100.0)

    def test_invalid_codes_are_audited_and_excluded(self) -> None:
        invalid = self.indicators.iloc[[0]].copy()
        invalid["codigo_municipio"] = "codigo-invalido"
        indicators = pd.concat([self.indicators, invalid], ignore_index=True)

        full_map, analysis_map, audit = self._load(indicators)

        self.assertEqual(len(full_map), 3)
        self.assertEqual(len(analysis_map), 3)
        self.assertFalse(analysis_map["codigo_municipio"].isna().any())
        values = self._audit_values(audit)
        self.assertEqual(values["registros_codigo_municipio_invalido"], 1)

    def test_coverage_map_uses_notification_contract_labels(self) -> None:
        full_map, _, _ = self._load()

        figure, axes = spatial.plot_data_coverage(full_map)

        labels = [text.get_text() for text in axes.get_legend().get_texts()]
        self.assertEqual(
            labels,
            ["Com notificação registrada", "Sem notificação registrada"],
        )
        plt.close(figure)

    def test_missing_notification_flag_fails_instead_of_being_inferred(self) -> None:
        indicators = self.indicators.drop(columns="tem_notificacao_registrada")

        with self.assertRaisesRegex(ValueError, "tem_notificacao_registrada"):
            self._load(indicators)


if __name__ == "__main__":
    unittest.main()
