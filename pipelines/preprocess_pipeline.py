import json
from datetime import datetime
from typing import Any, Dict, List

from prefect import flow, task

from src.preprocess.deduplicator import Deduplicator
from src.preprocess.normalizer import Normalizer
from src.preprocess.transformer import DataTransformer
from src.utils.logger import setup_logger


@task
def setup_preprocess_logger():
    """Setup logger for preprocess pipeline"""
    return setup_logger(
        name="preprocess_pipeline",
        log_file="logs/app.log",
        level="INFO",
    )


@task
def transform_data(raw_data: List[Dict[Any, Any]], logger) -> List[Dict[Any, Any]]:
    """Transform and clean raw data using the DataTransformer"""
    transformer = DataTransformer()
    
    try:
        transformed_data = transformer.transform_crossref_data(raw_data)
        
        # Log transformation summary
        summary = transformer.get_transformation_summary(raw_data, transformed_data)
        logger.info(f"Transformed {summary['original_count']} items to {summary['transformed_count']} items")
        logger.info(f"Applied transformations: {', '.join(summary['transformations_applied'])}")
        
        return transformed_data
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        logger.warning("Falling back to original data without transformation")
        return raw_data


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
def deduplicate_data(
    normalized_data: List[Dict[Any, Any]], logger
) -> List[Dict[Any, Any]]:
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
    Main preprocess pipeline flow that transforms, normalizes and deduplicates data
    """
    logger = setup_preprocess_logger()

    # Step 1: Transform and clean raw data using Polars
    transformed_data = transform_data(raw_data, logger)
    
    # Step 2: Normalize data (existing logic)
    normalized_data = normalize_data(transformed_data, logger)
    
    # Step 3: Deduplicate data
    unique_data = deduplicate_data(normalized_data, logger)
    
    # Step 4: Save processed data
    save_processed_data(unique_data, logger)

    return unique_data
