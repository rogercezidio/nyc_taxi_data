# 🌟 iFood Case - Data Architecture

Este repositório apresenta a solução desenvolvida para o desafio técnico de Data Architect do iFood, com foco na ingestão, processamento e análise de dados de corridas de táxi da cidade de Nova York, utilizando a stack do Databricks com Delta Lake e Unity Catalog.

---

## 🏠 Visão Geral da Solução

A solução segue a abordagem **medalhão (raw → bronze → silver → gold)** com validações e enriquecimentos em cada etapa:

1. **Ingestão:** download dos arquivos Parquet do site da NYC TLC (Jan-Mai/2023) e salva em volume RAW.
2. **Bronze:** padroniza colunas, valida esquema, aplica castings e descarta arquivos inválidos para quarentena.
3. **Silver:** aplica filtros de qualidade, enriquecimentos temporais e restrição ao período válido.
4. **Gold:** cria uma dimensão de datas e uma tabela fato agregada com métricas de corridas.

---

## 📊 Tecnologias Utilizadas

- **Databricks (com Unity Catalog)**
- **Delta Lake**
- **PySpark / Spark SQL**
- **Volumes UC (Managed e External)**
- **Python 3.10+**
- **dotenv + dbutils.secrets**

---

## 🔗 Estrutura do Repositório

```text
ifood-case/
├─ src/               # Código-fonte da pipeline e configurações
├─ analysis/          # Notebooks com queries e insights
├─ requirements.txt   # Dependências para execução local
└─ README.md          # Documentação da solução
```

---

## 📆 Dados Utilizados

Fonte oficial da [NYC Taxi & Limousine Commission (TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

Período considerado: **Janeiro a Maio de 2023**

Tipos de táxi processados:
- `yellow`
- `green`
- `fhv`
- `fhvhv`

---

## 📃 Tabelas Criadas

### Dimensões
- `gold.dim_date`: dimensão de datas (2023)

### Fatos
- `gold.fact_trips`: métricas agregadas por data, hora e tipo de táxi

### Intermediárias
- `bronze.<type>_tripdata_bronze`
- `silver.<type>_tripdata_silver`

---

## 🧹 Perguntas Respondidas (analysis/)

1. **Média de `total_amount` por mês (Yellow Taxis)**
2. **Média de `passenger_count` por hora do dia (Maio/2023)**

---

## ⚙️ Execução 

1. Configure um cluster no Databricks com **Unity Catalog habilitado**.
2. No AWS IAM, crie uma **role com permissão para acessar seu bucket S3** e confiança na conta Databricks. Depois, crie a Storage Credential com:

   ```sql
   CREATE STORAGE CREDENTIAL minha_credencial_uc
   WITH IAM_ROLE = 'arn:aws:iam::<seu_account_id>:role/<sua_role>'
   COMMENT 'Credencial UC para acesso ao bucket';
   ```
   Se preferir use Cloudformation:

   🔗 Referência: https://docs.databricks.com/aws/pt/connect/unity-catalog/cloud-storage/external-locations

3. Crie um arquivo `.env` na raiz com as variáveis:

   ```env
   BUCKET_BASE=s3://seu-bucket/path
   TAG=nyc
   CATALOG=ifood_case
   CREDS_NAME=minha_credencial_uc
   ```

4. Execute os notebooks na ordem:

   1. `01_ingertion_raw`
   2. `02_etl_bronze`
   3. `03_etl_silver`
   4. `04_etl_gold`
---

## 📊 Extras
- Arquivos com problemas são movidos automaticamente para a camada `quarentine`.
- Todas as tabelas Delta possuem **constraints de integridade** para garantir consistência.

---

## 🙌 Considerações Finais

A solução é escalável, auditável e aproveita boas práticas de engenharia de dados, como separação por camadas, controle de qualidade, uso de particionamento e enriquecimento semântico.

Sinta-se livre para explorar os notebooks e adaptar para outros cenários analíticos!
