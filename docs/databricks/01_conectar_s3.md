# Etapa 1 — conectar o S3 ao Databricks

Objetivo: ler duas linhas de exemplo no Databricks Free Edition antes de enviar
o CSV completo do INMET. Este guia prepara a conexão; sua execução na plataforma
é a validação pendente deste commit.

| Recurso | Valor |
| --- | --- |
| Bucket / região | `pcd2-dengue` / `us-east-2` |
| Prefixo do projeto | `s3://pcd2-dengue/pysus-ibge/` |
| IAM Role | `databricks-dengue-s3-read` |
| Política IAM | `databricks-dengue-s3-read-policy` |
| Storage Credential | `dengue_s3_read` |
| External Location | `dengue_landing_s3` |
| Volume externo | `dengue_sus.landing.inmet` |
| Origem do volume | `s3://pcd2-dengue/pysus-ibge/landing/inmet/` |

O Databricks terá leitura dos originais no S3. Nas próximas etapas, os arquivos
Parquet derivados e as tabelas Delta usarão armazenamento gerenciado pelo
Databricks. Não são necessárias access keys no notebook.

## 1. Conferir permissões e bucket

No Databricks, abra **Catalog** e confirme que `dengue_sus` existe. Para configurar
a conexão, seu usuário precisa de `CREATE STORAGE CREDENTIAL` no metastore e
`CREATE EXTERNAL LOCATION` no metastore e na credencial. Confira essas permissões
com o proprietário/admin caso as opções de criação estejam indisponíveis.

Para executar o notebook, você precisará de:

- `USE CATALOG` e `CREATE SCHEMA` em `dengue_sus`;
- `USE SCHEMA` e `CREATE VOLUME` em `dengue_sus.landing`;
- `CREATE EXTERNAL VOLUME` em `dengue_landing_s3`;
- `READ VOLUME` em `dengue_sus.landing.inmet` para ler o arquivo.

Quem cria schema e volume recebe a propriedade desses objetos. Se eles já
existirem, confira seus privilégios na aba **Permissions**. Referência:
[permissões de volumes](https://docs.databricks.com/aws/en/volumes/privileges).

Na AWS, entre na conta proprietária do bucket:

1. No menu da conta, copie o **Account ID**, com 12 dígitos. Ele substituirá
   `<AWS_ACCOUNT_ID>` nos JSONs abaixo.
2. Abra **S3 → Buckets → pcd2-dengue** e confira a região `us-east-2`.
3. Em **Permissions → Object Ownership**, confira **Bucket owner enforced**
   (ACLs desabilitadas). Mantenha o bloqueio de acesso público ativado.
4. Em **Properties → Default encryption**, confira se usa SSE-S3 ou SSE-KMS.
   Para SSE-KMS, siga também a seção de KMS abaixo e confira a criptografia do
   objeto enviado, pois ela pode diferir do padrão do bucket.

## 2. Criar a função IAM

Abra **IAM → Roles → Create role → Custom trust policy**. Cole:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
    },
    "Action": "sts:AssumeRole",
    "Condition": {"StringEquals": {"sts:ExternalId": "0000"}}
  }]
}
```

Avance sem anexar políticas e nomeie a função `databricks-dengue-s3-read`.
Guarde seu ARN. O `0000` é temporário e será substituído na etapa 4.

Na função criada, abra **Permissions → Add permissions → Create inline policy →
JSON**. Substitua `<AWS_ACCOUNT_ID>` e cole:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:GetBucketLocation",
      "Resource": "arn:aws:s3:::pcd2-dengue"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::pcd2-dengue",
      "Condition": {
        "StringLike": {"s3:prefix": ["pysus-ibge/", "pysus-ibge/*"]}
      }
    },
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::pcd2-dengue/pysus-ibge/*"
    },
    {
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "arn:aws:iam::<AWS_ACCOUNT_ID>:role/databricks-dengue-s3-read"
    }
  ]
}
```

Salve como `databricks-dengue-s3-read-policy`. Essa política separa a consulta da
região do bucket, a listagem restrita ao prefixo e a leitura dos objetos.
[Referência AWS para restrição por prefixo](https://docs.aws.amazon.com/AmazonS3/latest/userguide/amazon-s3-policy-keys.html).

### Somente se os objetos usarem SSE-KMS

Adicione à lista `Statement` da política de permissões da função:

```json
{
  "Effect": "Allow",
  "Action": "kms:Decrypt",
  "Resource": "<ARN_DA_CHAVE_KMS>"
}
```

Em **KMS → Customer managed keys → chave usada → Key policy**, adicione esta
declaração à lista `Statement` existente, preservando as demais:

```json
{
  "Sid": "AllowDatabricksDengueRead",
  "Effect": "Allow",
  "Principal": {
    "AWS": "arn:aws:iam::<AWS_ACCOUNT_ID>:role/databricks-dengue-s3-read"
  },
  "Action": "kms:Decrypt",
  "Resource": "*"
}
```

Nesse documento de política da chave, `*` representa a própria chave. Se ela
for gerenciada pela AWS, sua política não é editável; confira o acesso da função
na mesma conta e registre qualquer falha de descriptografia antes de continuar.
[Permissões KMS para leitura de objetos S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html).

## 3. Criar a credencial no Databricks

Abra **Catalog → Connect → Credentials → Create credential**:

- Tipo: **AWS IAM Role**.
- Nome: `dengue_s3_read`.
- IAM Role ARN: o ARN copiado na etapa 2.
- Em **Advanced options**, marque **Limit to read-only use**.

Crie a credencial e copie o **External ID** exibido.

## 4. Finalizar a confiança e validar

Na AWS, abra a função → **Trust relationships → Edit trust policy**.
Substitua os dois campos entre `<...>` e salve:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "AWS": [
        "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",
        "arn:aws:iam::<AWS_ACCOUNT_ID>:role/databricks-dengue-s3-read"
      ]
    },
    "Action": "sts:AssumeRole",
    "Condition": {"StringEquals": {"sts:ExternalId": "<EXTERNAL_ID>"}}
  }]
}
```

No Databricks, abra `dengue_s3_read` → **Validate Configuration**. Caso solicitado,
use o caminho `s3://pcd2-dengue/pysus-ibge/`. A função precisa passar também pela
verificação **Self Assume Role**.

As etapas de confiança e credencial seguem o
[procedimento oficial do Databricks](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual).

## 5. Registrar a localização externa

No Databricks, abra **Catalog → Connect → External Locations → Create external
location → Manual** e preencha:

| Campo | Valor |
| --- | --- |
| Nome | `dengue_landing_s3` |
| Storage type | S3 |
| URL | `s3://pcd2-dengue/pysus-ibge/` |
| Storage credential | `dengue_s3_read` |

Mantenha eventos de arquivos desativados nesta carga manual. Valide o acesso de
leitura antes de seguir. Se o objeto já existir, confira URL e credencial sem
substituí-lo. Referência:
[conexão S3](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3).

## 6. Enviar o arquivo pequeno

Salve localmente `conexao.csv`, em UTF-8, com este conteúdo sintético:

```csv
id;fonte
1;INMET
2;INMET
```

No console S3, dentro de `pcd2-dengue`, crie/abra as pastas
`pysus-ibge/landing/inmet/_validacao/`. Use **Upload → Add files → Upload** para
enviar o CSV. A chave completa deve ser:

```text
pysus-ibge/landing/inmet/_validacao/conexao.csv
```

Faça esse envio antes de executar o notebook, para que o diretório de origem
do volume já exista. O upload usa sua sessão AWS; a função do Databricks continua
somente com leitura.

## 7. Executar o notebook duas vezes

Importe [01_validar_landing_inmet.py](../../notebooks/databricks/01_validar_landing_inmet.py)
no **Workspace → Import → File**, como notebook Python. Selecione o compute
serverless e use **Run all**.

O notebook cria o schema e o volume externo somente se não existirem, confere
o tipo e o caminho do volume e lê exclusivamente o CSV de validação. A criação
usa a sintaxe [CREATE VOLUME](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-volume);
a conferência usa [DESCRIBE VOLUME](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-volume).

Resultado esperado: a tabela com `1 / INMET` e `2 / INMET`, seguida de
`Conexão validada: 2 linhas lidas do S3 pelo volume externo.`
Execute **Run all** novamente: o resultado deve permanecer igual.

| Se ocorrer | Conferir |
| --- | --- |
| Opção de criação indisponível | Privilégios e disponibilidade do recurso na Free Edition; informar a mensagem antes de avançar. |
| `Self Assume Role` falhou | ARN da própria função nas duas políticas e External ID final. |
| `AccessDenied` ao listar/ler | Prefixo solicitado, política IAM, eventuais negações na bucket policy e KMS do objeto. |
| Validação exige escrita | Credencial marcada como read-only; registrar o erro, sem ampliar a política automaticamente. |
| Volume existente com origem diferente | Inspecionar o objeto indicado; o notebook interrompe sem alterá-lo. |
| Arquivo não encontrado | Chave exata, nome `conexao.csv` e conclusão do upload no S3. |
| Permissão de volume negada | `USE CATALOG`, `USE SCHEMA` e `READ VOLUME`. |
| CSV divergente | Copiar novamente as três linhas fornecidas, incluindo o cabeçalho. |

Envie o resultado das duas execuções ou a célula e mensagem da falha. A próxima
entrega, após essa validação, será o carregamento do CSV completo na landing.
