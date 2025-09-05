# Case Study: Data Engineering

---

**⚠️ PLEASE DO NOT FORK THIS REPO AS OTHERS MAY SEE YOUR CODE. INSTEAD YOU CAN USE THE
[USE THIS TEMPLATE](https://github.com/new?template_name=case-study-data-engineering&template_owner=MDPI-AG)
BUTTON TO CREATE YOUR OWN REPOSITORY.**

---

## Targeted Workflow

Extract → Store raw → Preprocess → Load to PostgreSQL → Transform with dbt → Analyze

## Getting Started

Install the required packages using pip. It is recommended to use a virtual environment
to avoid conflicts with other projects.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Start the Postgres database and MinIO S3 using Docker:

```bash
docker-compose up -d
```

This will start:
- **Postgres database** on port 5432 (accessible with any Postgres client)
- **MinIO S3** on port 9000 (API) and 9001 (Web Console)

## Issues with CrossRef API

If you face any issues with accessing the CrossRef API, you can add `&mailto=your@email`
into the URL. This way CrossRef assigns your API request to a prioritized pool. Use
a real email address.

## Suggested Project Structure

```plaintext
.
├── data                         # Local storage for ingested data
│   ├── raw                     # Raw dumps from API
│   └── processed               # Cleaned/preprocessed files (if needed)
│
├── src                         # All Python source code
│   ├── extract                 # Code to call APIs and fetch raw data
│   │   ├── __init__.py
│   │   └── extractor.py
│   │
│   ├── preprocess              # Normalize / clean / deduplicate raw data
│   │   ├── __init__.py
│   │   └── normalize.py
│   │
│   ├── load                    # Load preprocessed data into Postgres
│   │   ├── __init__.py
│   │   └── loader.py
│   │
│   ├── utils                   # Config, logging, etc.
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── logger.py
│   │   └── s3_client.py        # MinIO S3 client utility
│   │
│   └── pipeline.py             # Orchestrates all the steps end-to-end
│
├── dbt                         # dbt project directory
│   ├── models
│   │   ├── staging             # Raw to cleaned staging models
│   │   └── marts               # Final models / business logic
│   ├── seeds
│   └── snapshots
│
├── docker-compose.yml          # Docker Compose file to run Postgres & MinIO
├── main.py                     # Entrypoint that runs the pipeline
├── README.md
└── requirements.txt
```

## Some Ideas for Todos

- improve the fetching of raw data: loop a few pages of the API responses
- improve the deduplicator logic: some items with different DOI's may belong together (e.g., a preprint and a journal article version of the same work)
- start the dbt schema and sql models to run some analytics on the data, e.g., sum citations per year, or per journal, or per publisher
- decompose the `main.py` entrypoints into distinct pipelines and use an orchestrator such as Airflow or Prefect
- Dockerize the python app

## dbt

If you are not familiar with dbt, you can check their [sandox project](https://github.com/dbt-labs/jaffle-shop/)
on GitHub to get started. You can also check the [dbt documentation](https://docs.getdbt.com/docs/introduction)
for more information.

---

## 🚀 What's Been Implemented

✅ Switched from `psycopg2` to `psycopg` and resolved database connection issues.

✅ Enhanced the CrossRef API data fetching to loop through multiple pages of API responses.

✅ Added some basic preprocessing logic to clean the raw data (using Polars).

✅ Implemented MinIO S3 object storage for raw data files.

✅ Completed dbt analytics framework.

✅ Orchestrated Pipeline with Prefect.

✅ Configured code quality tools: flake8, black, isort, pre-commit.

### MinIO S3 Object Storage Usage

**What it does:** Provides S3-compatible object storage for raw data files, supporting the modern data lake architecture.

**How to use:**

1. **Start the services:**
   ```bash
   docker-compose up -d
   ```
   
2. **Run the pipeline:**
   ```bash
   python main_orchestrated.py
   ```
   Raw data will be automatically stored in both `./data/raw/` and MinIO S3 bucket `crossref-raw`.

3. **Access MinIO Web Console:**
   - Open http://localhost:9001 in your browser
   - Login with `minioadmin` / `minioadmin123`
   - Browse uploaded raw data files

### 📊 dbt Analytics Framework

The project includes a complete dbt analytics framework with staging models, dimension tables, and analytical queries for citation analysis.

**How to use:**

1. **Install dbt dependencies:**
   ```bash
   cd dbt && dbt deps
   ```

2. **Test database connection:**
   ```bash
   dbt debug
   ```

3. **Run all models:**
   ```bash
   dbt run
   ```

4. **Run tests:**
   ```bash
   dbt test
   ```

5. **Generate documentation:**
   ```bash
   dbt docs generate && dbt docs serve
   ```

## 🔄 Prefect Pipeline Orchestration

The project has been refactored from a monolithic `main.py` script into a modular, orchestrated pipeline using **Prefect** for workflow management. This provides better observability, error handling, and scalability.

### Pipeline Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Extract        │    │  Preprocess      │    │  Load           │
│  Pipeline       │──▶ │  Pipeline        │──▶ │  Pipeline       │
│                 │    │                  │    │                 │
│ • Fetch API     │    │ • Normalize      │    │ • Load to DB    │
│ • Save to S3    │    │ • Deduplicate    │    │ • Data quality  │
│ • Raw storage   │    │ • Clean data     │    │ • Insert records│
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Pipeline Components

#### 1. **Extract Pipeline** (`pipelines/extract_pipeline.py`)
- **Purpose**: Fetch data from CrossRef API and store raw data
- **Tasks**:
  - `setup_extract_config()` - Initialize configuration
  - `setup_extract_logger()` - Setup logging
  - `fetch_crossref_data()` - API data fetching with pagination
  - `extract_raw_data()` - Retrieve data from S3 storage
- **Output**: Raw CrossRef data stored in MinIO S3 and local files

#### 2. **Preprocess Pipeline** (`pipelines/preprocess_pipeline.py`)
- **Purpose**: Clean, normalize, and deduplicate raw data
- **Tasks**:
  - `setup_preprocess_logger()` - Setup logging
  - `normalize_data()` - Standardize data format and structure
  - `deduplicate_data()` - Remove duplicate records
  - `save_processed_data()` - Save clean data to JSON
- **Output**: Clean, deduplicated data ready for database loading

#### 3. **Load Pipeline** (`pipelines/load_pipeline.py`)
- **Purpose**: Load processed data into PostgreSQL database
- **Tasks**:
  - `setup_load_config()` - Database configuration
  - `setup_load_logger()` - Setup logging
  - `load_data_to_database()` - Insert records into database
- **Output**: Data persisted in PostgreSQL for dbt analytics

**How to use:**

1. **Start the services:**
```bash
sudo docker-compose up -d
```

2. **Run the pipeline:**

**Option A: Run the full orchestrated pipeline**
```bash
python main_orchestrated.py
```

**Option B: Use the CLI runner for individual components**
```bash
python run_individual_pipelines.py extract --max-pages 5
python run_individual_pipelines.py preprocess
python run_individual_pipelines.py load
python run_individual_pipelines.py all --max-pages 3
```

3. **Scheduling (Optional)**

For automated execution, use the included Prefect deployment configuration:

```bash
# Deploy pipeline with daily schedule
prefect deploy --name crossref-etl-pipeline

# The pipeline will run daily at 6 AM UTC as configured in prefect.yaml
```

## 🔧 Code Quality Tools

This project uses several code quality tools to maintain consistent style and catch issues early:

### **flake8** - Style Guide Enforcement
**Purpose**: Checks Python code against PEP 8 style guidelines and detects programming errors.

**Configuration**: See `.flake8` for project-specific settings:
- Maximum line length: 120 characters  
- Ignores docstring warnings (D-series codes)
- Excludes build directories and virtual environments

**Usage**:
```bash
# Check all files for style issues
flake8 .

# Check specific file
flake8 src/extract/extractor.py
```

### **black** - Code Formatter
**Purpose**: Automatically formats Python code to ensure consistent style.

**Usage**:
```bash
# Format all Python files
black .

# Check what would be changed without modifying files
black --check .
```

### **isort** - Import Sorter
**Purpose**: Automatically sorts and organizes Python import statements.

**Usage**:
```bash
# Sort imports in all files
isort .

# Check import sorting without modifying files
isort --check-only .
```

### **pre-commit** - Git Hook Framework
**Purpose**: Runs code quality checks automatically before each commit.

**Setup**:
```bash
# Install pre-commit hooks (one-time setup)
pre-commit install

# Run hooks manually on all files
pre-commit run --all-files
```

**Hooks configured** (see `.pre-commit-config.yaml`):
- Trim trailing whitespace
- Fix end of files
- Check YAML syntax
- Check for large files
- Check for merge conflicts
- Remove debug statements
- Run black formatter
- Run isort import sorter
- Run flake8 style checker
