from prefect import task, flow
from src.preprocess.normalizer import Normalizer
from src.preprocess.deduplicator import Deduplicator
from src.utils.logger import setup_logger
from typing import List, Dict, Any
import json
from datetime import datetime


@task
def setup_preprocess_logger():
    """Setup logger for preprocess pipeline"""
    return setup_logger(
        name="preprocess_pipeline",
        log_file="logs/app.log",
        level="INFO",
    )


@task
def normalize_data(raw_data: List[Dict[Any, Any]], logger) -> List[Dict[Any, Any]]:
    """Normalize raw data using the Normalizer"""
    normalizer = Normalizer()
    normalized_data = []
    
    for item in raw_data:
        try:
            normalized_item = normalizer.normalize(item)
            normalized_data.append(normalized_item)
        except KeyError as e:
            logger.error(f"KeyError: {e}")
            logger.error("Data format may have changed. Please check the API response.")
    
    logger.info(f"Normalized {len(normalized_data)} items.")
    return normalized_data


@task
def deduplicate_data(normalized_data: List[Dict[Any, Any]], logger) -> List[Dict[Any, Any]]:
    """Remove duplicates from normalized data"""
    deduplicator = Deduplicator()
    unique_data = deduplicator.deduplicate(normalized_data)
    logger.info(f"Deduplicated data to {len(unique_data)} items.")
    return unique_data


@task
def save_processed_data(unique_data: List[Dict[Any, Any]], logger) -> str:
    """Save processed data to JSON file"""
    now = datetime.now()
    filename = now.strftime("%Y%m%d_%H%M%S") + "_data.json"
    filepath = f"./data/processed/{filename}"
    
    with open(filepath, "w") as f:
        json.dump(unique_data, f, indent=4)
    
    logger.info(f"Saved processed data to {filepath}")
    return filepath


@flow(name="Preprocess Pipeline")
def run_preprocess_pipeline(raw_data: List[Dict[Any, Any]]) -> List[Dict[Any, Any]]:
    """
    Main preprocess pipeline flow that normalizes and deduplicates data
    """
    logger = setup_preprocess_logger()
    
    normalized_data = normalize_data(raw_data, logger)
    unique_data = deduplicate_data(normalized_data, logger)
    save_processed_data(unique_data, logger)
    
    return unique_data