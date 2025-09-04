from typing import Any, Dict, List

from prefect import flow, task

from src.load.loader import Loader
from src.utils.config import Config
from src.utils.pipeline_logger import get_pipeline_logger
from src.utils.shared_config import get_default_config


@task
def setup_load_config() -> Config:
    """Setup configuration for load pipeline"""
    return get_default_config()


@task
def setup_load_logger(config: Config):
    """Setup logger for load pipeline"""
    return get_pipeline_logger("load_pipeline", config)


@task
def load_data_to_database(unique_data: List[Dict[Any, Any]], config: Config, logger) -> None:
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
