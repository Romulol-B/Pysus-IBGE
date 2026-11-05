# PySUS–IBGE: dengue, saneamento e clima

Projeto da disciplina SCC0957 — Prática de Ciência de Dados II. O fluxo atual usa Databricks para analisar **São Paulo em 2010 e 2022**. Os módulos locais em `utils/` conservam o processamento nacional histórico, com contratos analíticos distintos.

## Fluxo Databricks

Os notebooks estão em [notebooks/databricks](notebooks/databricks/). O prefixo indica a camada: `00_landing`, `01_bronze`, `02_silver`, `03_gold`. As dependências determinam a ordem dentro de cada camada.

| Etapa | Notebook | Dependência |
|---|---|---|
| Configuração | `setup_ambiente.ipynb` | Unity Catalog, credencial e localização externa S3 |
| Conferência das entradas | `00_landing_validar_sinan.py`, `00_landing_validar_inmet.py` | Volumes e arquivos |
| Bronze | `01_bronze_sidra.ipynb`, `01_bronze_sinan_sp.py`, `01_bronze_inmet_sp.py` | API SIDRA e Landing |
| Silver SIDRA | `02_silver_sidra.ipynb` | Bronze SIDRA; auxiliar `02_silver_padronizar_municipios.ipynb` |
| Silver SINAN | `02_silver_sinan_sp.py` | Bronze SINAN e Silver SIDRA |
| Silver INMET | `02_silver_inmet_sp.py` | Bronze INMET |
| Painel municipal | `02_silver_dengue_sidra_sp.py` | Silver SINAN e SIDRA |
| Painel mensal | `02_silver_dengue_clima_sp.py` | Silver SINAN, SIDRA e INMET |
| Relatório Gold | `03_gold_agregacao_sinan_sidra_inmet.ipynb` | Ambos os painéis |
| Análises complementares | `03_gold_correlacoes_sp.py`, `03_gold_correlacoes_geograficas_pysal_sp.py` | Painéis; espacial usa somente o municipal |

O [job de nove tarefas](jobs/sinan_sidra_inmet_agregacao_sp.job.json) cobre Bronze, Silver e agregação. Setup, conferências Landing e análises complementares são etapas separadas. Ajuste o prefixo dos caminhos do workspace ao importar o JSON; os nomes finais correspondem aos arquivos locais.

### Preparação e execução

1. Importe os notebooks na mesma pasta do workspace, **incluindo `setup_pipeline.py`**, usado por `%run` para as validações e publicações comuns.
2. Execute `setup_ambiente`. A identidade executora precisa poder criar catálogo/schemas/volumes, ler a Landing e escrever tabelas. A credencial e a localização externa S3 devem estar configuradas previamente. `DESTINATARIO` é opcional e concede leitura na Silver depois da criação dos objetos.
3. Disponibilize `DENGBR10.parquet` e `DENGBR22.parquet` em `/Volumes/dengue_sus/landing/sinan_s3/`. Copie junto `manifesto_download.json`, quando disponível. A Bronze verifica checksum, nome, origem e contagem e rejeita coortes misturadas.
4. Disponibilize `INMET_dataset_agregado.csv` em `/Volumes/dengue_sus/landing/inmet/`, junto do manifesto `INMET_dataset_agregado.publication.json` da mesma publicação. Uma marca `.publication.json.pending` bloqueia a carga. O CSV deve conter os anos-alvo e, quando disponíveis, as leituras de 00:00 de 01/01/2011 e 01/01/2023.
5. Execute as conferências Landing. A SINAN usa 2010 por padrão; a Bronze exige os dois anos. Arquivos sem manifesto recebem checksum de conteúdo e indicação **`manifesto_verificado=false`**; sua origem histórica não é inferida.
6. Preencha o parâmetro de job **`versao_codigo`** com a revisão dos notebooks importados, por exemplo um commit ou identificador de revisão local. É uma declaração do operador, não uma verificação automática do código no workspace. `execucao_id` recebe `{{job.run_id}}`.
7. Para execução manual, preencha os widgets `execucao_id` e `versao_codigo` com os mesmos valores em todas as tarefas. Use um identificador novo para cada carga completa. Tabelas antigas sem metadados exigem uma nova carga desde a Bronze.
8. Confira as validações e auditorias antes de interpretar os resultados. Para executar somente a Gold sobre uma carga existente, reutilize a identificação dessa carga. Após reexecutar um produtor, reexecute seus consumidores.

As configurações serverless existentes usam ambiente 6. As células `%pip` fixam versões dos pacotes instalados pelos notebooks; o `uv.lock` configura o ambiente **local**, não o Databricks. A resolução dessas dependências e a execução integral precisam ser verificadas no compute de destino.

A conferência Landing INMET usa `/Volumes/dengue_sus/landing/inmet/_validacao/conexao.csv`. Envie esse arquivo com o conteúdo abaixo antes de executar `00_landing_validar_inmet`:

```csv
id;fonte
1;INMET
2;INMET
```

### Contratos e consistência

- **SINAN:** residência, ano-fonte e uma notificação por linha, sem deduplicação de pessoas. Confirmados exigem classificação do período e `CRITERIO` 1/2. Graves: classes 2/3/4 em 2010 e 11/12 em 2022; óbitos: confirmados com `EVOLUCAO=2`. O mensal com clima inclui somente datas do mesmo ano-fonte.
- **Ausências:** contagens elegíveis e `*_desconhecidos` são separadas. Desconhecido significa regra indeterminada por campo nulo/vazio; outros códigos informados continuam na auditoria. Fonte anual vazia bloqueia o processamento. A grade municipal recebe zero somente quando não existe notificação vinculada, com flags de vínculo explícitas.
- **SIDRA:** população do Censo como denominador; categorias de saneamento próprias de cada Censo, sem harmonização temporal presumida. Grade esperada: 645 municípios por ano.
- **INMET:** estações disponíveis, dias UTC, 24 leituras válidas para chuva diária e 18 para temperatura/umidade. Chuva mensal exige todos os dias; médias mensais exigem 80%. A leitura de 00:00 do ano seguinte completa o dia anterior quando existe. A Silver diária registra os WMO utilizados por variável.
- **Espacial:** malha 2022 nos dois anos, KNN 8 padronizado por linha, 9.999 permutações, semente 42, taxas brutas e BH. LISA explicita `alternative="directed"`. É um produto distinto do histórico local com BY e hipótese bilateral.

`setup_pipeline` registra execução, revisão declarada, fontes e versões Delta em `userMetadata` no histórico das tabelas existentes. Os consumidores verificam se todas as saídas do produtor pertencem ao mesmo lote, fixam as versões lidas e recusam entradas substituídas ou inconsistentes. **Não há transação única entre tabelas nem rollback do conjunto.** Uma falha parcial deixa saídas físicas que os leitores deste projeto bloqueiam; leitores externos precisam aplicar as mesmas verificações. Não execute cargas manuais concorrentes sobre as mesmas tabelas.

A Gold é uma camada de relatórios, sem novas tabelas de controle ou tabelas Gold. Configure `DESTINO` no notebook de agregação para exportar CSV, HTML e metadados para um Volume. Cada exportação cria uma pasta nova e grava `_SUCCESS` por último. Pastas sem essa marca estão incompletas. Sem `DESTINO`, os resultados ficam apenas na sessão.

## Reprodução local e histórico

```bash
uv sync
uv run python -m utils.reconcile_databricks
uv run python -m unittest discover -s tests -v
```

A reconciliação lê `data/raw/sinan/DENGBR10.parquet`, `DENGBR22.parquet` e `data/processados/ibge_populacao_municipio_ano.parquet`. Produz `reconciliacao_contratos_sp.csv` e um JSON de checksums, sem alterar as fontes. Os argumentos `--sinan`, `--populacao` e `--output` permitem outros caminhos. Ela reproduz as regras sobre os mesmos arquivos; não executa Spark nem reconstrói o calendário com arquivos vizinhos.

Para refazer o **pipeline histórico nacional**, com acesso às fontes:

```bash
uv run python -m utils.deng_download --start 2000 --end 2023
uv run python -m utils.analysis_pipeline --refresh-ibge --allow-undated
```

Python 3.13+ e dependências estão em `pyproject.toml`/`uv.lock`. Os destinos padrão acompanham a organização existente: brutos em `data/raw/sinan/`, resultados em `data/processados/`; dados permanecem ignorados pelo Git. O downloader registra coorte e checksums, usa DadosGov e complementa 2007–2009/2012 com CSVs oficiais. FTP é opção explícita de outra coorte. `--allow-undated` preserva datas ilegíveis no ano-fonte com auditoria.

O fluxo histórico confirma pela classificação, sem exigir o mesmo critério Spark. Sua integração mensal pode incorporar outros anos-fonte; a agregação climática usa painel fixo. A análise espacial local usa BY, LISA bilateral, 999 permutações por padrão e sensibilidades populacionais. Coeficientes nacionais de execuções anteriores não validam os resultados atuais de SP. Os geradores `create_analysis_notebooks` e `create_spatial_notebook` são legados e **não fazem parte do fluxo recomendado**: ainda contêm destinos de notebooks retirados.

As instruções e contratos detalhados ficam no [relatório Obsidian](relatorio/README.md) e o estado das correções em [PLANO_DE_CORRECOES.md](PLANO_DE_CORRECOES.md). Guias antigos em `docs/` são arquivos locais ignorados; a documentação compartilhada é README + `relatorio/`.

Os notebooks são preparados localmente, sem `git add`, commit ou push. O usuário importa, valida e versiona pelo Databricks; a sincronização posterior deve preservar alterações locais pendentes. Nesta revisão não houve execução remota.
