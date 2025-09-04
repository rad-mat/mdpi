from prefect import task, flow
from src.utils.config import Config
from src.utils.logger import setup_logger
from src.extract.extractor import Extractor
from typing import List, Dict, Any


@task
def setup_extract_config() -> Config:
    """Setup configuration for extract pipeline"""
    return Config(config={
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
        "S3_SECURE": "false",
        "S3_BUCKET_RAW": "crossref-raw",
        "LOG_FILE": "logs/app.log",
        "LOG_LEVEL": "INFO",
    })


@task
def setup_extract_logger(config: Config):
    """Setup logger for extract pipeline"""
    return setup_logger(
        name="extract_pipeline",
        log_file=config.log_file,
        level=config.log_level,
    )


@task
def fetch_crossref_data(config: Config, logger, max_pages: int = 5) -> None:
    """Fetch data from CrossRef API and save to S3"""
    extractor = Extractor(config, logger)
    extractor.fetch_and_save_data(max_pages=max_pages)
    logger.info(f"Successfully fetched {max_pages} pages of data from CrossRef API")


@task
def extract_raw_data(config: Config, logger) -> List[Dict[Any, Any]]:
    """Extract raw data from S3 storage"""
    extractor = Extractor(config, logger)
    raw_data = extractor.extract_raw_data()
    logger.info(f"Extracted {len(raw_data)} raw items from S3")
    return raw_data


@flow(name="Extract Pipeline")
def run_extract_pipeline(max_pages: int = 5) -> List[Dict[Any, Any]]:
    """
    Main extract pipeline flow that fetches data from CrossRef API
    and extracts it from S3 storage
    """
    config = setup_extract_config()
    logger = setup_extract_logger(config)
    
    fetch_crossref_data(config, logger, max_pages)
    raw_data = extract_raw_data(config, logger)
    
    return raw_data