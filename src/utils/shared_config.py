"""
Shared configuration factory to eliminate configuration duplication across pipelines.
"""

from src.utils.config import Config


def get_default_config() -> Config:
    """
    Factory function to create the default configuration used across all pipelines.
    This eliminates duplication of the same config dictionary in multiple files.

    Returns:
        Config: Default configuration instance with standard settings
    """
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
