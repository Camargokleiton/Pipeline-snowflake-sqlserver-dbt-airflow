# E-commerce Data Pipeline

An end-to-end portfolio project that generates synthetic e-commerce data, orchestrates ingestion with Apache Airflow, lands raw data in Snowflake, and transforms it with dbt into analytics-ready layers.

> **Project status:** SQL Server-to-Snowflake ingestion is orchestrated by Airflow. dbt transformations and tests are run separately from Airflow.

## Architecture

```mermaid
flowchart LR
    G["Synthetic data generator"] -->|"Seed customers, products, orders..."| S["SQL Server"]
    S -->|"Airflow + Parquet"| B[("Snowflake · BRONZE")]
    B -->|"dbt staging views"| V[("Snowflake · SILVER")]
    V -->|"dbt marts"| M[("Snowflake · GOLD")]
    A["Apache Airflow"] -. "schedules and orchestrates ingestion" .-> G
    A -.-> B
    D["dbt"] -. "run and test manually" .-> V
    D -.-> M
    P[("PostgreSQL · Airflow metadata")] --- A
```

### Data layers

| Layer | Purpose | Implementation |
| --- | --- | --- |
| **Bronze** | Raw copies of the SQL Server source tables, including extraction timestamps. | Airflow extracts to Parquet and loads `RAW_*` tables into `ERP_DATABASE.BRONZE`. |
| **Silver** | Cleaned and standardized staging models. | dbt creates views in `ERP_DATABASE.SILVER`. |
| **Gold** | Analytics-ready customer/product dimensions and an order-level fact table. | dbt creates `DIM_CUSTOMERS`, `DIM_PRODUCTS`, and `FCT_ORDERS` tables in `ERP_DATABASE.GOLD`. |

### Gold marts

| Model | Grain | Contents |
| --- | --- | --- |
| `dim_customers` | One row per customer | Customer identity, contact, and location attributes. |
| `dim_products` | One row per product | Product, pricing, inventory, and joined category attributes. |
| `fct_orders` | One row per order | Order totals, shipping, item-line count, total quantity, item subtotal, and payment details. Item and payment data are aggregated before joining so they do not multiply order rows. |

## Technology stack

- **Apache Airflow 3.3.2** — scheduling and ingestion orchestration.
- **SQL Server 2022** — synthetic e-commerce source database.
- **Snowflake** — analytical warehouse and Bronze, Silver, and Gold schemas.
- **dbt Core + dbt-snowflake** — SQL transformations and data tests.
- **Docker Compose** — local orchestration for Airflow, PostgreSQL, and SQL Server.
- **Parquet + PyArrow** — columnar interchange format for ingestion.

## Repository layout

```text
.
├── data_font/
│   ├── DDL/                              # SQL Server reference DDL
│   └── fake_ingest_sqlserver_database/   # Synthetic data generator
├── dbt_ecommerce/
│   ├── models/
│   │   ├── staging/                     # Bronze sources and Silver models
│   │   └── marts/                       # Gold dimensions and facts
│   ├── snapshots/
│   └── macros/
├── docker/
│   ├── airflow/                         # Custom Airflow image
│   └── dags/                            # Seed and ingestion DAGs
├── docker-compose.yml
└── README.md
```

## Getting started

### Prerequisites

- Docker Desktop with Docker Compose.
- A Snowflake account, warehouse, and role with permission to create databases, schemas, stages, and tables in the target database.
- dbt Core and the `dbt-snowflake` adapter installed locally to run transformations.

### 1. Configure local environment variables

Create a root `.env` file for Docker Compose. Provide values for the variables referenced in [`docker-compose.yml`](./docker-compose.yml), including:

- `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB`
- `MSSQL_SA_PASSWORD` and `MSSQL_PORT`
- `AIRFLOW_WWW_USER_USERNAME` and `AIRFLOW_WWW_USER_PASSWORD`
- `AIRFLOW_API_SECRET_KEY` and `AIRFLOW_API_AUTH_JWT_SECRET`
- Optional seed sizes: `FAKE_CATEGORIES`, `FAKE_CUSTOMERS`, `FAKE_PRODUCTS`, and `FAKE_ORDERS`

Use strong, unique values for passwords and generate **different random values** for the two Airflow API secrets. The SQL Server SA password must meet Microsoft's password complexity requirements. `.env` files are ignored by Git: **never commit credentials or paste them into this README**.

The standalone generator also supports a local `data_font/fake_ingest_sqlserver_database/.env`; it is not needed when the generator is run by Airflow, which receives its connection settings from Docker Compose.

### 2. Start the local services

From the repository root:

```powershell
docker compose up -d --build
docker compose ps
```

Airflow's initialization service creates the metadata schema and web user. Open [http://localhost:8080](http://localhost:8080) and sign in with the Airflow credentials configured in `.env`. Newly discovered DAGs may be paused on a fresh installation; enable them in the Airflow UI if necessary.

### 3. Configure the Snowflake Airflow connection

In **Airflow UI → Admin → Connections**, create a connection with:

| Field | Value |
| --- | --- |
| Connection ID | `snowflake` |
| Connection type | Snowflake |
| Account, username, password | Your Snowflake account credentials |
| Extra | Your Snowflake `warehouse` and `role` |

Grant the configured Snowflake role the privileges needed by the ingestion DAG. Do not commit connection secrets.

### 4. Generate and ingest data

The `dag_seed_sqlserver` DAG runs on a 20-minute schedule. It creates/ensures the SQL Server schema, inserts a synthetic batch, then triggers `dag_sqlserver_to_snowflake_bronze` to load the source tables into Snowflake. Both DAGs can also be triggered from the Airflow UI.

The Bronze tables are `RAW_CUSTOMERS`, `RAW_CATEGORIES`, `RAW_PRODUCTS`, `RAW_ORDERS`, `RAW_ORDER_ITEMS`, and `RAW_PAYMENTS` in `ERP_DATABASE.BRONZE`.

### 5. Configure and run dbt

Create `~/.dbt/profiles.yml` (on Windows, `%USERPROFILE%\.dbt\profiles.yml`) with the Snowflake connection details. Keep the password in an environment variable rather than in the profile:

```yaml
dbt_ecommerce:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "<your-account>"
      user: "<your-user>"
      password: "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}"
      role: "<your-role>"
      database: ERP_DATABASE
      warehouse: "<your-warehouse>"
      schema: BRONZE
      threads: 4
```

Set `DBT_SNOWFLAKE_PASSWORD` in your local shell, then run from the repository root:

```powershell
dbt debug --project-dir dbt_ecommerce --profiles-dir "$HOME\.dbt"
dbt run --project-dir dbt_ecommerce --profiles-dir "$HOME\.dbt" --select staging
dbt test --project-dir dbt_ecommerce --profiles-dir "$HOME\.dbt" --select staging
dbt run --project-dir dbt_ecommerce --profiles-dir "$HOME\.dbt" --select path:models/marts
dbt test --project-dir dbt_ecommerce --profiles-dir "$HOME\.dbt" --select path:models/marts
```

The profile's `schema: BRONZE` is the default schema; the project's schema-generation macro routes staging models to `SILVER` and marts to `GOLD`. Bronze source definitions are in [`src_bronze.yml`](./dbt_ecommerce/models/staging/src_bronze.yml).

## Operational notes

- Airflow automates data generation and Bronze ingestion. **dbt is not currently part of the Airflow DAGs**; run the dbt commands separately after ingestion.
- Staging models are views; Gold marts are tables.
- The ingestion DAG truncates each Bronze target table before loading its latest SQL Server extract. This is a full refresh, not incremental CDC.
- The generator appends synthetic rows on each run. Adjust the `FAKE_*` settings to control the size of each batch.
- `docker compose down` stops the services. Docker volumes preserve SQL Server and Airflow metadata; removing volumes deletes that local state.

## Validation performed

- Both Airflow DAGs completed a seed-to-Bronze run successfully.
- All six dbt Silver staging views and all three Gold mart tables built successfully.
- All 26 dbt data tests passed across Silver and Gold, including uniqueness, required fields, and relationships.
- The complete nine-model dbt build finished successfully with no errors or warnings.

## English / Português

See the full Portuguese guide below.

---

# Pipeline de Dados de E-commerce

Projeto de portfólio de engenharia de dados que gera dados sintéticos de e-commerce, orquestra a ingestão com Apache Airflow, carrega os dados brutos no Snowflake e os transforma com dbt em camadas prontas para análise.

> **Status do projeto:** A ingestão do SQL Server para o Snowflake é orquestrada pelo Airflow. As transformações e os testes do dbt são executados separadamente do Airflow.

## Arquitetura

```mermaid
flowchart LR
    G["Gerador de dados sintéticos"] -->|"Gera clientes, produtos, pedidos..."| S["SQL Server"]
    S -->|"Airflow + Parquet"| B[("Snowflake · BRONZE")]
    B -->|"Views de staging com dbt"| V[("Snowflake · SILVER")]
    V -->|"Marts com dbt"| M[("Snowflake · GOLD")]
    A["Apache Airflow"] -. "agenda e orquestra a ingestão" .-> G
    A -.-> B
    D["dbt"] -. "execução e testes manuais" .-> V
    D -.-> M
    P[("PostgreSQL · metadados do Airflow")] --- A
```

### Camadas de dados

| Camada | Objetivo | Implementação |
| --- | --- | --- |
| **Bronze** | Cópias brutas das tabelas de origem do SQL Server, com data de extração. | Airflow extrai para Parquet e carrega tabelas `RAW_*` em `ERP_DATABASE.BRONZE`. |
| **Silver** | Modelos de staging limpos e padronizados. | dbt cria views em `ERP_DATABASE.SILVER`. |
| **Gold** | Dimensões de clientes/produtos e fato analítico no nível do pedido. | dbt cria as tabelas `DIM_CUSTOMERS`, `DIM_PRODUCTS` e `FCT_ORDERS` em `ERP_DATABASE.GOLD`. |

### Marts Gold

| Modelo | Granularidade | Conteúdo |
| --- | --- | --- |
| `dim_customers` | Uma linha por cliente | Identificação, contato e localização do cliente. |
| `dim_products` | Uma linha por produto | Produto, preços, estoque e atributos da categoria relacionada. |
| `fct_orders` | Uma linha por pedido | Totais, frete, quantidade de itens, quantidade total, subtotal dos itens e dados de pagamento. Itens e pagamentos são agregados antes dos joins para não multiplicar pedidos. |

## Tecnologias

- **Apache Airflow 3.3.2** — agendamento e orquestração da ingestão.
- **SQL Server 2022** — banco de origem com dados sintéticos de e-commerce.
- **Snowflake** — data warehouse analítico e schemas Bronze, Silver e Gold.
- **dbt Core + dbt-snowflake** — transformações SQL e testes de dados.
- **Docker Compose** — ambiente local para Airflow, PostgreSQL e SQL Server.
- **Parquet + PyArrow** — formato colunar usado na ingestão.

## Estrutura do repositório

```text
.
├── data_font/
│   ├── DDL/                              # DDL de referência do SQL Server
│   └── fake_ingest_sqlserver_database/   # Gerador de dados sintéticos
├── dbt_ecommerce/
│   ├── models/
│   │   ├── staging/                     # Fontes Bronze e modelos Silver
│   │   └── marts/                       # Dimensões e fatos Gold
│   ├── snapshots/
│   └── macros/
├── docker/
│   ├── airflow/                         # Imagem customizada do Airflow
│   └── dags/                            # DAGs de geração e ingestão
├── docker-compose.yml
└── README.md
```

## Como executar

### Pré-requisitos

- Docker Desktop com Docker Compose.
- Conta Snowflake, warehouse e role com permissão para criar databases, schemas, stages e tabelas no banco de destino.
- dbt Core e o adaptador `dbt-snowflake` instalados localmente para executar as transformações.

### 1. Configure as variáveis de ambiente locais

Crie um arquivo `.env` na raiz para o Docker Compose. Preencha as variáveis referenciadas em [`docker-compose.yml`](./docker-compose.yml), incluindo:

- `POSTGRES_USER`, `POSTGRES_PASSWORD` e `POSTGRES_DB`
- `MSSQL_SA_PASSWORD` e `MSSQL_PORT`
- `AIRFLOW_WWW_USER_USERNAME` e `AIRFLOW_WWW_USER_PASSWORD`
- `AIRFLOW_API_SECRET_KEY` e `AIRFLOW_API_AUTH_JWT_SECRET`
- Tamanho opcional das cargas: `FAKE_CATEGORIES`, `FAKE_CUSTOMERS`, `FAKE_PRODUCTS` e `FAKE_ORDERS`

Use senhas fortes e únicas e gere valores aleatórios **diferentes** para os dois segredos da API do Airflow. A senha SA do SQL Server deve cumprir os requisitos de complexidade da Microsoft. Arquivos `.env` são ignorados pelo Git: **nunca envie credenciais ao repositório nem as cole neste README**.

O gerador executado diretamente também aceita um `.env` local em `data_font/fake_ingest_sqlserver_database/`; ele não é necessário quando o Airflow executa o gerador, pois o Docker Compose fornece as configurações de conexão.

### 2. Inicie os serviços locais

Na raiz do repositório:

```powershell
docker compose up -d --build
docker compose ps
```

O serviço de inicialização do Airflow cria o schema de metadados e o usuário web. Acesse [http://localhost:8080](http://localhost:8080) e entre com as credenciais do Airflow definidas no `.env`. Em uma instalação nova, as DAGs detectadas podem estar pausadas; se necessário, habilite-as pela interface do Airflow.

### 3. Configure a conexão do Airflow com o Snowflake

Na interface do Airflow, acesse **Admin → Connections** e crie uma conexão:

| Campo | Valor |
| --- | --- |
| Connection ID | `snowflake` |
| Connection type | Snowflake |
| Account, username, password | Credenciais da sua conta Snowflake |
| Extra | `warehouse` e `role` do Snowflake |

Conceda à role configurada os privilégios necessários para a DAG de ingestão. Não envie segredos da conexão ao repositório.

### 4. Gere e ingira os dados

A DAG `dag_seed_sqlserver` é agendada a cada 20 minutos. Ela cria ou garante o schema no SQL Server, insere uma carga sintética e então dispara a DAG `dag_sqlserver_to_snowflake_bronze`, que carrega as tabelas de origem no Snowflake. Também é possível disparar as duas DAGs manualmente pela interface do Airflow.

As tabelas Bronze são `RAW_CUSTOMERS`, `RAW_CATEGORIES`, `RAW_PRODUCTS`, `RAW_ORDERS`, `RAW_ORDER_ITEMS` e `RAW_PAYMENTS`, no schema `ERP_DATABASE.BRONZE`.

### 5. Configure e execute o dbt

Crie `~/.dbt/profiles.yml` (no Windows, `%USERPROFILE%\.dbt\profiles.yml`) com os dados da conexão Snowflake. Mantenha a senha em uma variável de ambiente, não no arquivo de perfil:

```yaml
dbt_ecommerce:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "<sua-conta>"
      user: "<seu-usuario>"
      password: "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}"
      role: "<sua-role>"
      database: ERP_DATABASE
      warehouse: "<seu-warehouse>"
      schema: BRONZE
      threads: 4
```

Defina `DBT_SNOWFLAKE_PASSWORD` no terminal local e execute, a partir da raiz do repositório:

```powershell
dbt debug --project-dir dbt_ecommerce --profiles-dir "$HOME\.dbt"
dbt run --project-dir dbt_ecommerce --profiles-dir "$HOME\.dbt" --select staging
dbt test --project-dir dbt_ecommerce --profiles-dir "$HOME\.dbt" --select staging
dbt run --project-dir dbt_ecommerce --profiles-dir "$HOME\.dbt" --select path:models/marts
dbt test --project-dir dbt_ecommerce --profiles-dir "$HOME\.dbt" --select path:models/marts
```

O `schema: BRONZE` no perfil é o schema padrão; a macro do projeto direciona os modelos de staging para `SILVER` e os marts para `GOLD`. As fontes Bronze estão definidas em [`src_bronze.yml`](./dbt_ecommerce/models/staging/src_bronze.yml).

## Observações operacionais

- O Airflow automatiza a geração dos dados e a ingestão no Bronze. **O dbt ainda não faz parte das DAGs do Airflow**; execute os comandos do dbt separadamente após a ingestão.
- Os modelos de staging são views; os marts Gold são tabelas.
- A DAG de ingestão trunca cada tabela Bronze de destino antes de carregar a extração mais recente do SQL Server. É uma carga completa, não CDC incremental.
- O gerador acrescenta dados sintéticos a cada execução. Ajuste as variáveis `FAKE_*` para controlar o tamanho de cada carga.
- `docker compose down` para os serviços. Os volumes Docker preservam os dados locais do SQL Server e os metadados do Airflow; removê-los apaga esse estado local.

## Validações realizadas

- As duas DAGs do Airflow concluíram com sucesso uma execução da geração até a carga Bronze.
- As seis views de staging Silver e as três tabelas marts Gold foram criadas com sucesso pelo dbt.
- Os 26 testes dbt das camadas Silver e Gold passaram, incluindo unicidade, campos obrigatórios e relacionamentos.
- O build completo dos nove modelos dbt terminou sem erros ou avisos.
