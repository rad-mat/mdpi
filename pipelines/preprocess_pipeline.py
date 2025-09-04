from typing import Any, Dict, List

from prefect import flow, task
from src.preprocess.transformer import DataTransformer
from src.utils.data_processing import (
    deduplicate_data_items,
    normalize_data_items,
    save_processed_data_to_file,
)
from src.utils.pipeline_logger import get_pipeline_logger
from src.utils.shared_config import get_default_config


@task
def setup_preprocess_logger():
    """Setup logger for preprocess pipeline"""
    config = get_default_config()
    return get_pipeline_logger("preprocess_pipeline", config)


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
    """Normalize raw data using shared processing utility"""
    return normalize_data_items(raw_data, logger)


@task
def deduplicate_data(normalized_data: List[Dict[Any, Any]], logger) -> List[Dict[Any, Any]]:
    """Remove duplicates from normalized data using shared processing utility"""
    return deduplicate_data_items(normalized_data, logger)


@task
def save_processed_data(unique_data: List[Dict[Any, Any]], logger) -> str:
    """Save processed data to JSON file using shared processing utility"""
    return save_processed_data_to_file(unique_data, logger)


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
