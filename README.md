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

✅ Implemented MinIO S3 object storage for raw data files.

✅ Completes dbt analytics framework:
- **Staging Models**: Clean and standardized CrossRef data (`stg_crossref_publications`, `stg_crossref_authors`)
- **Dimension Models**: Publications and authors with calculated metrics (`dim_publications`, `dim_authors`)
- **Analytics Models**: Citations analysis by year, journal, and publisher (`analytics_citations_by_*`)
- **Data Quality**: Schema tests, documentation, and data quality scoring

✅ **Prefect Pipeline Orchestration**:
- **Modular Architecture**: Decomposed monolithic `main.py` into distinct pipeline components
- **Extract Pipeline**: Fetches CrossRef API data and stores in MinIO S3
- **Preprocess Pipeline**: Normalizes, deduplicates, and saves processed data
- **Load Pipeline**: Loads clean data into PostgreSQL database
- **Orchestration**: Complete workflow management with retry logic and monitoring

## MinIO S3 Object Storage Usage

**What it does:** Provides S3-compatible object storage for raw data files, supporting the modern data lake architecture.

**Setup:**
1. **Docker Services:**
   - API endpoint: `http://localhost:9000`
   - Web Console: `http://localhost:9001`
   - Default credentials: `minioadmin` / `minioadmin123`

2. **S3 Client Utility:** Created `src/utils/s3_client.py` with methods for:
   - Bucket creation and management
   - JSON data upload/download
   - File operations
   - Object listing

3. **Pipeline Integration:** Updated `src/extract/extractor.py` to:
   - Save raw CrossRef data to both local files AND MinIO S3
   - Auto-create S3 buckets (`crossref-raw` by default)
   - Upload each API response page as separate objects

**How to use:**

1. **Start the services:**
   ```bash
   docker-compose up -d
   ```

2. **Access MinIO Web Console:**
   - Open http://localhost:9001 in your browser
   - Login with `minioadmin` / `minioadmin123`
   - Browse uploaded raw data files

3. **Run the pipeline:**
   ```bash
   python main.py
   ```
   Raw data will be automatically stored in both `./data/raw/` and MinIO S3 bucket `crossref-raw`.

4. **Configure S3 settings**:
   ```python
   config = Config({
       "S3_HOST": "localhost",
       "S3_PORT": 9000,
       "S3_ACCESS_KEY": "minioadmin",
       "S3_SECRET_KEY": "minioadmin123",
       "S3_BUCKET_RAW": "crossref-raw",
       # ... other configs
   })
   ```

**Benefits:**
- **Data Lake Foundation:** Raw data is now stored in S3-compatible storage for dbt integration
- **Scalability:** Object storage scales better than local file systems
- **Data Lineage:** Clear separation between raw (S3) and processed (PostgreSQL) data
- **dbt Ready:** Raw S3 data can be directly accessed by dbt models for transformations

---

## 📊 dbt Analytics Framework

### Overview
The project now includes a complete dbt analytics framework with staging models, dimension tables, and analytical queries for citation analysis.

### dbt Models Structure
```
dbt2/models/
├── staging/
│   ├── stg_crossref_publications.sql    # Clean publications data
│   └── stg_crossref_authors.sql         # Normalized author data
├── marts/
│   ├── dim_publications.sql             # Publications with metrics
│   ├── dim_authors.sql                  # Author analytics & h-index
│   ├── analytics_citations_by_year.sql  # Annual citation trends
│   ├── analytics_citations_by_journal.sql # Journal rankings
│   └── analytics_citations_by_publisher.sql # Publisher analysis
├── schema.yml                           # Tests & documentation
└── sources.yml                          # Source data definitions
```

### Key Analytics Features
- **Citation Impact Scoring**: Publications ranked by citation impact
- **Author Metrics**: H-index estimation, productivity scores, career analysis
- **Temporal Analysis**: Year-over-year citation and publication trends
- **Journal Rankings**: Impact scores, citation rates, productivity metrics
- **Publisher Analysis**: Market share, influence scores, portfolio analysis
- **Data Quality**: Automated testing and quality scoring (0-1)

### Running dbt Models

**Note:** The active dbt project is in `dbt/` directory (scilit_analytics).

1. **Install dbt dependencies:**
   ```bash
   cd dbt
   dbt deps
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

---

## 🔄 Prefect Pipeline Orchestration

### Overview
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

### Usage Instructions

#### 1. **Prerequisites Setup**

Ensure Docker services are running:
```bash
# Start PostgreSQL and MinIO services
sudo docker-compose up -d

# Verify services are healthy
curl -f http://localhost:9000/minio/health/live
```

Install Prefect (already included in requirements.txt):
```bash
pip install prefect
```

#### 2. **Running the Complete Pipeline**

**Option A: Run the full orchestrated pipeline**
```bash
python main_orchestrated.py
```

**Option B: Use the CLI runner for individual components**
```bash
# Run individual pipeline components
python run_individual_pipelines.py extract --max-pages 5
python run_individual_pipelines.py preprocess
python run_individual_pipelines.py load
python run_individual_pipelines.py all --max-pages 3
```

#### 3. **Monitoring and Observability**

Prefect provides built-in monitoring through:
- **Task-level logging**: Each task logs its progress and results
- **Flow execution tracking**: Monitor pipeline runs and completion status
- **Error handling**: Automatic retry logic for failed tasks
- **Data lineage**: Track data flow between pipeline stages

#### 4. **Configuration**

Pipeline configuration is centralized in each pipeline module:

```python
# Common configuration across all pipelines
config = Config({
    "API_ENDPOINT": "https://api.crossref.org/works?sort=published&order=desc&rows=200",
    "DB_HOST": "localhost",
    "DB_PORT": 5432,
    "DB_NAME": "my_database",
    "DB_USER": "my_user",
    "DB_PASSWORD": "my_password",
    "S3_HOST": "localhost",
    "S3_PORT": 9000,
    "S3_ACCESS_KEY": "minioadmin",
    "S3_SECRET_KEY": "minioadmin123",
    "S3_BUCKET_RAW": "crossref-raw",
    "LOG_FILE": "logs/app.log",
    "LOG_LEVEL": "INFO"
})
```

#### 5. **Scheduling (Optional)**

For automated execution, use the included Prefect deployment configuration:

```bash
# Deploy pipeline with daily schedule
prefect deploy --name crossref-etl-pipeline

# The pipeline will run daily at 6 AM UTC as configured in prefect.yaml
```

### Key Benefits

✅ **Modularity**: Each pipeline stage can be developed, tested, and run independently
✅ **Observability**: Comprehensive logging and monitoring at task and flow levels
✅ **Error Handling**: Built-in retry logic and failure handling
✅ **Scalability**: Easy to scale individual components or add new pipeline stages
✅ **Data Lineage**: Clear tracking of data transformations through the pipeline
✅ **Development**: Simplified testing and debugging of individual components

### Files Added/Modified

```
├── pipelines/                          # New pipeline modules
│   ├── __init__.py
│   ├── extract_pipeline.py            # Extract workflow
│   ├── preprocess_pipeline.py         # Preprocess workflow
│   └── load_pipeline.py               # Load workflow
├── main_orchestrated.py               # New orchestrated entrypoint
├── run_individual_pipelines.py        # CLI tool for individual components
├── prefect.yaml                       # Deployment configuration
└── main.py                            # Original monolithic script (preserved)
```

---

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
