# Correções e impacto nos resultados

## Problemas e mudanças

- **SINAN:** `DENGUE` era antecedente de infecção, não diagnóstico. A confirmação usa `CON_CLASSI` até 2006 e `CLASSI_FIN` depois. Corrigidos recortes por UF hospitalar e agregações por notificação: a análise usa residência.
- **IBGE e taxas:** corrigidos denominadores incompatíveis, conversões numéricas e junções por nome. Adotados códigos municipais, população do ano de referência, grade censitária completa e auditorias. Arquivo ausente não vira zero.
- **Datas:** preservados ano-fonte e datas de anos adjacentes. A única data corrompida permanece na contagem anual, auditada como mês desconhecido, mediante `--allow-undated`.
- **Downloads:** corrigidas corrupção e duplicação por colisão entre espelhos. Separados caches, adicionados checksums, retomada e 2023. A troca temporária para FTP alterou a coorte; restaurados DadosGov e CSVs oficiais.
- **INMET:** corrigidos leitura, cabeçalhos, sentinelas, identificadores, limites de orvalho, direção circular do vento e horário da chuva. Exigidas cobertura mínima, amostras comuns e sensibilidade com estações fixas. Adicionada publicação conjunta Parquet/CSV com recuperação de falhas.
- **Espacial:** corrigidos alinhamento dos pesos, quadrantes LISA e `alpha`. Validados valores, geometrias e malha de 2022; adicionadas sensibilidades de vizinhança/taxa, testes bilaterais e FDR-BY.
- **Execução:** corrigidos caminhos, empacotamento e gravação da landing Databricks. Centralizadas transformações e reexecutados seis notebooks. Python 3.13 funcional resolveu a falha de `libffi`.

## Comparação com os resultados do repositório

A coorte restaurada tem **25.031.001 registros** e reproduz os totais antigos de classificação, raça/cor e evolução quando usadas as regras antigas.

| Resultado | Antes | Depois |
|---|---:|---:|
| Municípios na integração de 2022 | 4.678 | 5.570 |
| Notificações integradas, 2022 | 1.405.095 | 1.399.370 |
| Confirmados integrados, 2022 | 1.254.238 | 1.250.896 |
| Correlação água–taxa, 2022 | 0,1464 | 0,1621 |
| Correlação esgoto–taxa, 2022 | 0,1008 | 0,1222 |
| Moran global da taxa, 2022 | ≈0,41 | 0,4313 |

Em 2022, as diferenças foram pequenas: **−0,41% nas notificações e −0,27% nos confirmados**. As cinco correlações mantiveram os sinais e permaneceram fracas. Mantêm-se as conclusões descritivas de associação fraca e autocorrelação global positiva, sem interpretação causal.

**Conclusão local:** nenhum cluster LISA permanece significativo após FDR-BY, tanto na taxa bruta quanto na ajustada. Não há suporte para as antigas identificações de clusters municipais significativos; isso não contradiz a autocorrelação global positiva.

**Conclusão histórica:** a série de 2000–2006 usa a classificação final corrigida. As contagens baseadas no antecedente `DENGUE` foram substituídas e não sustentam conclusões sobre magnitude ou tendência dos confirmados. Essa correção não foi pequena.

As conclusões foram atualizadas nos cinco notebooks de análise, no README e nos geradores, preservando os resultados salvos. Os textos agora distinguem a autocorrelação global positiva da ausência de clusters locais significativos e utilizam a classificação histórica corrigida, sem interpretação causal das associações.

