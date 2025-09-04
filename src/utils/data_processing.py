"""
Shared data processing utilities to eliminate code duplication between legacy and pipeline code.
"""

import json
from datetime import datetime
from typing import Any, Dict, List

from src.preprocess.deduplicator import Deduplicator
from src.preprocess.normalizer import Normalizer


def normalize_data_items(raw_data: List[Dict[Any, Any]], logger) -> List[Dict[Any, Any]]:
    """
    Normalize a list of raw data items using the Normalizer.
    This is shared logic extracted from main.py and preprocess_pipeline.py.

    Args:
        raw_data: List of raw data dictionaries to normalize
        logger: Logger instance for error reporting

    Returns:
        List of normalized data items
    """
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


def deduplicate_data_items(normalized_data: List[Dict[Any, Any]], logger) -> List[Dict[Any, Any]]:
    """
    Remove duplicates from normalized data items.
    This is shared logic extracted from main.py and preprocess_pipeline.py.

    Args:
        normalized_data: List of normalized data dictionaries
        logger: Logger instance for logging

    Returns:
        List of deduplicated data items
    """
    deduplicator = Deduplicator()
    unique_data = deduplicator.deduplicate(normalized_data)
    logger.info(f"Deduplicated data to {len(unique_data)} items.")
    return unique_data


def save_processed_data_to_file(unique_data: List[Dict[Any, Any]], logger) -> str:
    """
    Save processed data to a timestamped JSON file.
    This is shared logic extracted from main.py and preprocess_pipeline.py.

    Args:
        unique_data: List of processed data dictionaries to save
        logger: Logger instance for logging

    Returns:
        str: Path to the saved file
    """
    now = datetime.now()
    filename = now.strftime("%Y%m%d_%H%M%S") + "_data.json"
    filepath = f"./data/processed/{filename}"

    with open(filepath, "w") as f:
        json.dump(unique_data, f, indent=4)

    logger.info(f"Saved processed data to {filepath}")
    return filepath
