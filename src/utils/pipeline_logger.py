"""
Shared logger factory to eliminate logger setup duplication across pipelines.
"""

from src.utils.config import Config
from src.utils.logger import setup_logger


def get_pipeline_logger(pipeline_name: str, config: Config):
    """
    Factory function to create a standardized logger for pipeline components.
    This eliminates duplication of similar logger setup code across pipelines.

    Args:
        pipeline_name (str): Name of the pipeline (e.g., 'extract_pipeline', 'load_pipeline')
        config (Config): Configuration instance containing log settings

    Returns:
        Logger: Configured logger instance for the specified pipeline
    """
    return setup_logger(
        name=pipeline_name,
        log_file=config.log_file,
        level=config.log_level,
    )
