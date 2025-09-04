from typing import Any, Dict, List

from prefect import flow, task

from src.load.loader import Loader
from src.utils.config import Config
from src.utils.logger import setup_logger


@task
def setup_load_config() -> Config:
    """Setup configuration for load pipeline"""
    return Config(
        config={
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
        }
    )


@task
def setup_load_logger(config: Config):
    """Setup logger for load pipeline"""
    return setup_logger(
        name="load_pipeline",
        log_file=config.log_file,
        level=config.log_level,
    )


@task
def load_data_to_database(
    unique_data: List[Dict[Any, Any]], config: Config, logger
) -> None:
    """Load processed data into the database"""
    loader = Loader(config, logger)
    loader.load_data(unique_data)
    logger.info(f"Successfully loaded {len(unique_data)} items into database")


@flow(name="Load Pipeline")
def run_load_pipeline(unique_data: List[Dict[Any, Any]]) -> None:
    """
    Main load pipeline flow that loads processed data into the database
    """
    config = setup_load_config()
    logger = setup_load_logger(config)

    load_data_to_database(unique_data, config, logger)
