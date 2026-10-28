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
- [`agregacao_sinan_ibge_inmet_uf_mes.ipynb`](notebooks/Agregacao/DENGUE+IBGE+IMNET/agregacao_sinan_ibge_inmet_uf_mes.ipynb):
  integração SINAN–SIDRA–INMET por UF e mês de calendário, com resumo anual,
  auditorias, séries temporais, dispersões integradas e cobertura meteorológica.
  Começa em 2010 e também aceita 2022 como recorte separado. Consulta os percentuais
  estaduais diretamente no SIDRA e usa painel fixo de estações INMET. Sem o Parquet
  climático, executa a parte SINAN–IBGE e identifica as variáveis climáticas ausentes.
  Os dados e gráficos HTML ficam em `sinan/processados/agregacao_tres_fontes/<ano>/`.

## Fontes

- SINAN/DATASUS: notificações de dengue de 2000 a 2023, obtidas com PySUS.
- SIDRA 136: população residente do Censo 2010, variável 93 e total da
  classificação de cor ou raça (`c86/0`).
- SIDRA 3218: condições domiciliares do Censo 2010.
- SIDRA 6803, 6805 e 6892: água, esgotamento e lixo no Censo 2022.
- SIDRA 6579 e 4709: estimativas populacionais e população do Censo 2022.
- API de Malhas do IBGE: geometria municipal simplificada usada na análise
  espacial; o arquivo é mantido no cache não versionado.

O clima é preparado por `utils.inmet_pipeline` e integrado por
`utils.climate_integration`. Ele permanece separado dos contratos municipais
de saneamento: não se misturam escalas UF–dia e município–ano na comparação
de coeficientes. O novo notebook de agregação usa `utils.aggregate_sources`
para reunir as fontes em uma escala comum UF–mês, com saneamento censitário
constante no ano e auditoria própria pela data efetiva da notificação.

## Execução

```bash
uv sync
uv run python -m utils.deng_download --start 2000 --end 2023
uv run python -m utils.analysis_pipeline --refresh-ibge --allow-undated
uv run python -m utils.spatial_analysis
uv run jupyter lab
```

Na primeira execução sem cache, use `--refresh-ibge` para consultar o SIDRA. Os
arquivos gerados ficam em `sinan/processados/`, diretório deliberadamente não
versionado junto com os dados brutos.

Execute os comandos na raiz. São necessários Python 3.13 ou superior, dependências de
`pyproject.toml`, acesso ao DATASUS/SIDRA/IBGE ou caches verificados, e todos os
arquivos anuais esperados em `sinan/DENGBRxx.parquet`. O download inclui 2023
e usa por padrão a coorte DadosGov, com cache isolado e manifesto incremental
de contagens, origem e checksums. Os anos 2007–2009 e 2012, ausentes do catálogo
DadosGov do PySUS, vêm dos CSVs ZIP oficiais do Ministério da Saúde, sem troca
automática para a coorte FTP. Reexecuções reutilizam apenas anos verificados.
`--origin FTP` é uma opção explícita para outra coorte: seus totais não são
intercambiáveis com os resultados históricos deste repositório.
Anos ainda abertos, arquivos vazios, campos obrigatórios ausentes e datas
ilegíveis interrompem a análise por padrão. A coorte original contém uma data
corrompida em 2008: `--allow-undated` permite conservar essa notificação somente
no período-fonte anual, com auditoria individual e mês civil desconhecido no
CSV mensal, sem inventar uma data. Gráficos por data excluem esse registro e
informam a quantidade excluída. Os contratos anuais mantêm o ano do arquivo
como período de referência (`periodo_referencia = ano_fonte_sinan`), por escolha
explícita: não são necessariamente anos civis completos. Divergências de
`NU_ANO` e datas em anos adjacentes são auditadas e preservadas. As taxas usam
a população do ano de referência; são razões descritivas do período da fonte,
não incidências de um ano civil reconstituído. O CSV mensal inclui o ano civil
real da notificação em `ano_calendario_notificacao`, sem excluir essas datas.
Ausência de
fonte anual não é evidência de zero notificações.

As classificações finais são `CON_CLASSI` até 2006 e `CLASSI_FIN` nos demais
anos; `DENGUE` na ficha antiga é antecedente de doença e nunca é usado como
confirmação. A série é de notificações confirmadas segundo essas fichas, não
uma contagem de pessoas únicas nem uma estimativa de todas as infecções.

Para clima, organize CSVs INMET em pastas por ano e execute:

```bash
uv run python -m utils.inmet_pipeline base-INM --csv-output data/INMET_dataset_agregado.csv
uv run python -m utils.climate_integration data/INMET_dataset_agregado.parquet --year 2010
```

A ingestão preserva valores como texto, une schemas explicitamente por nome,
normaliza variantes de cabeçalho, reconcilia contagens e gera auditoria por
arquivo. Nenhum erro de parsing é ignorado. O cache UTF-8 é identificado e
validado por checksum; falhas preservam a saída anterior. A visão de limpeza
publica flags de valores rejeitados e mantém os originais. Limites físicos
são triagem, não prova de qualidade. Direção do vento usa estatística circular.
Horas e dias climáticos são UTC; a data administrativa de notificação não é
uma medida exata da exposição nem é convertida artificialmente em UTC.

A precipitação diária exige 24 intervalos horários válidos por estação.
Cada leitura é atribuída ao início da hora precedente: 00:00 pertence à
última hora do dia anterior. O recorte anual inclui a leitura de 00:00 de
1º de janeiro do ano seguinte quando disponível; sem ela, a precipitação
do último dia permanece nula. Temperatura e
umidade exigem ao menos 18. As médias por UF incluem apenas estações válidas
para cada variável e informam seus números; dias sem medição permanecem
nulos. Contagens de WMO, completude e limites de triagem não garantem
representatividade espacial. As correlações são descritivas dentro de UF,
com o mesmo desfecho e dias completos comuns; sazonalidade, atrasos e
dependência temporal impedem interpretação causal. Não se conclui que
saneamento explica mais que clima comparando coeficientes de escalas distintas.

A sensibilidade à composição da rede usa, por padrão, três estações fixas
por UF/ano, escolhidas exclusivamente pela cobertura meteorológica. Cada uma
precisa ter as três variáveis válidas em pelo menos 80% dos dias do calendário
solicitado. A lista de WMO aparece em `estacoes_painel_fixo`. A média fixa só
é publicada nos dias em que todas as estações selecionadas têm dados válidos
para a variável; os pesos não mudam quando uma estação falha. Configure
`--fixed-stations` e `--minimum-station-coverage` antes da análise. O painel
fixo é uma sensibilidade de composição, não uma amostra representativa do estado.
As tabelas de correlação comparam `estacoes_disponiveis` e `painel_fixo` nos
mesmos dias completos e preservam UFs sem painel elegível com n=0 e r nulo.

Parquet e CSV INMET são preparados antes de substituir qualquer saída.
Falhas de publicação restauram as versões anteriores. Um manifesto
`.publication.json` registra os checksums do conjunto; a marca `.pending`
sinaliza publicação interrompida e deve ser resolvida antes de consumir os
dados (backups são preservados em caso de encerramento abrupto). A integração
climática verifica o manifesto quando presente. Consumidores externos, inclusive
a landing Databricks, devem usar os arquivos da mesma publicação. Ao atualizar,
mantenha os mesmos destinos, incluindo o argumento `--csv-output` usado antes.

O scanner pode receber `years=[2010]`; a integração climática abre somente
o ano solicitado. Arquivos de outros anos não são validados nesse recorte.

O diretório `data/` é versionado apenas com o marcador `.gitkeep`. Os dados
locais dentro dele são ignorados pelo Git.

## Contratos produzidos

- `sinan_municipio_ano.parquet`
- `sinan_populacao_municipio_ano.parquet`
- `sinan_saneamento_2010.parquet`
- `sinan_saneamento_2022.parquet`

Os resultados espaciais tabulares ficam em
`sinan/processados/analise_espacial/`. As cinco visualizações exportadas ficam
em `figuras/analise_espacial/`.

A malha é solicitada explicitamente para 2022 e usa o cache
`ibge_municipios_2022_min.geojson`, acompanhado de `.metadata.json` com ano,
URL e SHA-256. Caches antigos sem proveniência não são aceitos. Para uma malha
local fornecida manualmente, documente o ano 2022 e seu checksum nesse mesmo
formato; mudar o nome do arquivo não estabelece sua compatibilidade temporal.
LISA publica p-valores brutos e ajustados por FDR Benjamini–Yekutieli, e os
mapas usam os valores ajustados. Com 999 permutações, o piso do p-valor bruto
é 0,001; poucas descobertas após ajuste não provam ausência de agrupamento.
Também são publicadas sensibilidades global/local com taxas ajustadas à
população pelo método de Assunção–Reis. Compare as duas análises antes de
interpretar padrões como risco, mantendo a ressalva de subnotificação.

A hipótese local é explicitamente bilateral (`alternative="two-sided"`,
conforme esda 2.10 no lockfile). Nas tabelas globais, `p_sim` conserva o
pseudo-p unilateral do PySAL e `p_sim_bilateral` explicita o dobro limitado
a 1 para interpretação bilateral; não se confunde `two_tailed` analítico
com o cálculo por permutação.

Os seis notebooks locais de SINAN, integração, SIDRA e análise espacial foram
reexecutados com a coorte restaurada e contêm resultados salvos. As figuras anteriores
à correção ficam em `figuras/analise_espacial/anteriores_nao_validadas/` apenas
para referência histórica e não representam os resultados corrigidos.
Para regenerar os notebooks leitores, sem executar análises:

```bash
uv run python -m utils.create_analysis_notebooks
uv run python -m utils.create_spatial_notebook
```

### Conclusões atuais

Em 2022, as correlações de saneamento são fracas: água r = 0,1621 e esgoto
r = 0,1222, sem demonstrar relação causal. Há autocorrelação espacial global
positiva da taxa (I = 0,4313; p bilateral = 0,002), estável nas sensibilidades
avaliadas. Nenhum cluster LISA local permanece significativo após FDR-BY a 5%,
inclusive com taxa ajustada à população; não se sustentam as antigas localizações
de clusters significativos. A ausência de descobertas locais não elimina o
padrão global. Na série de 2000–2006, somente a classificação final corrigida
fundamenta a contagem de confirmados; o antecedente `DENGUE` não mede confirmação,
incidência ou tendência de casos confirmados.

Nos recortes censitários, a base do IBGE define a grade completa de municípios.
Quando não há linha correspondente no SINAN, as contagens são zero e a coluna
`situacao_registro_sinan` registra `sem_notificacao_registrada`; códigos
municipais inválidos permanecem em uma auditoria separada. As taxas usam a
população residente do mesmo ano como denominador.

O pipeline exige que a Tabela 136 contenha os 5.565 municípios de 2010,
sem chaves duplicadas e sem denominadores nulos ou não positivos. A seleção é
reproduzida pela consulta `/t/136/n6/all/v/93/p/2010/c86/0` e identificada no
parquet populacional como `SIDRA 136 v93 c86=0 - Censo 2010`.

A documentação metodológica e os resultados estão no vault Obsidian
`SCC0957-Pratica-de-Ciencia-de-Dados-II-Obsidian`.
