# PySUS–IBGE: dengue e saneamento básico

Projeto da disciplina SCC0957 — Prática de Ciência de Dados II. O objetivo é
produzir perfis municipais de dengue pelo município de residência e investigar
associações exploratórias com condições domiciliares de saneamento.

## Notebooks

- `sinan.ipynb`: exploração inicial do SINAN e visualizações descritivas.
- `seneamento.ipynb`: análise das tabelas municipais do SIDRA antes do merge.
- `integracao_sinan_saneamento.ipynb`: harmonização do SINAN, auditoria e
  integração municipal nos anos compatíveis.
- `analise_espacial_pysal.ipynb`: EDA geográfica de 2022 com pesos espaciais,
  Moran global/bivariado, LISA e mapas municipais.

## Fontes

- SINAN/DATASUS: notificações de dengue de 2000 a 2023, obtidas com PySUS.
- SIDRA 136: população residente do Censo 2010, variável 93 e total da
  classificação de cor ou raça (`c86/0`).
- SIDRA 3218: condições domiciliares do Censo 2010.
- SIDRA 6803, 6805 e 6892: água, esgotamento e lixo no Censo 2022.
- SIDRA 6579 e 4709: estimativas populacionais e população do Censo 2022.
- API de Malhas do IBGE: geometria municipal simplificada usada na análise
  espacial; o arquivo é mantido no cache não versionado.

Os dados climáticos são preparados em um repositório separado e não fazem
parte dos contratos analíticos desta etapa.

## Execução

```bash
uv sync
uv run python analysis_pipeline.py
uv run python spatial_analysis.py
uv run jupyter lab
```

Na primeira execução sem cache, use `--refresh-ibge` para consultar o SIDRA. Os
arquivos gerados ficam em `sinan/processados/`, diretório deliberadamente não
versionado junto com os dados brutos.

## Contratos produzidos

- `sinan_municipio_ano.parquet`
- `sinan_populacao_municipio_ano.parquet`
- `sinan_saneamento_2010.parquet`
- `sinan_saneamento_2022.parquet`

Os resultados espaciais tabulares ficam em
`sinan/processados/analise_espacial/`. As cinco visualizações exportadas ficam
em `figuras/analise_espacial/`.

Nos recortes censitários, a base do IBGE define a grade completa de municípios.
Quando não há linha correspondente no SINAN, as contagens são zero e a coluna
`situacao_registro_sinan` registra `sem_notificacao_registrada`; códigos
municipais inválidos permanecem em uma auditoria separada. As taxas usam a
população residente do mesmo ano como denominador.

A carga validada da Tabela 136 contém os 5.565 municípios existentes em 2010,
sem chaves duplicadas e sem denominadores nulos ou não positivos. A seleção é
reproduzida pela consulta `/t/136/n6/all/v/93/p/2010/c86/0` e identificada no
parquet populacional como `SIDRA 136 v93 c86=0 - Censo 2010`.

A documentação metodológica e os resultados estão no vault Obsidian
`SCC0957-Pratica-de-Ciencia-de-Dados-II-Obsidian`.
