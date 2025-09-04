from typing import Any, Dict, List

from prefect import flow, task

from src.extract.extractor import Extractor
from src.utils.config import Config
from src.utils.pipeline_logger import get_pipeline_logger
from src.utils.shared_config import get_default_config


@task
def setup_extract_config() -> Config:
    """Setup configuration for extract pipeline"""
    return get_default_config()


@task
def setup_extract_logger(config: Config):
    """Setup logger for extract pipeline"""
    return get_pipeline_logger("extract_pipeline", config)


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
