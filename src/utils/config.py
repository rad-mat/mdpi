from typing import Dict


class Config:
    """
    Configuration class for the application.

    Attributes:
        api_endpoint (str): API endpoint for CrossRef.
        log_file (str): Path to the log file.
        log_level (str): Logging level.
        db_host (str): Database host.
        db_port (int): Database port.
        db_name (str): Database name.
        db_user (str): Database user.
        db_password (str): Database password.
        s3_host (str): MinIO S3 host.
        s3_port (int): MinIO S3 port.
        s3_access_key (str): MinIO S3 access key.
        s3_secret_key (str): MinIO S3 secret key.
        s3_secure (bool): Whether to use HTTPS for S3 connection.
        s3_bucket_raw (str): S3 bucket name for raw data.
    """

    def __init__(self, config: Dict[str, str]):
        self.api_endpoint = config.get("API_ENDPOINT", "")

        self.db_host = config.get("DB_HOST", "localhost")
        self.db_port = int(config.get("DB_PORT", 5432))
        self.db_name = config.get("DB_NAME", "my_database")
        self.db_user = config.get("DB_USER", "my_user")
        self.db_password = config.get("DB_PASSWORD", "my_password")

        self.s3_host = config.get("S3_HOST", "localhost")
        self.s3_port = int(config.get("S3_PORT", 9000))
        self.s3_access_key = config.get("S3_ACCESS_KEY", "minioadmin")
        self.s3_secret_key = config.get("S3_SECRET_KEY", "minioadmin123")
        self.s3_secure = config.get("S3_SECURE", "false").lower() == "true"
        self.s3_bucket_raw = config.get("S3_BUCKET_RAW", "crossref-raw")

        self.log_file = config.get("LOG_FILE", "app.log")
        self.log_level = config.get("LOG_LEVEL", "INFO").upper()

        self.__validate_config()

    def __validate_config(self):
        """
        Validate the configuration values.
        Raises:
            ValueError: If any required configuration is missing or invalid.
        """
        if not self.api_endpoint:
            raise ValueError("API_ENDPOINT is required.")

        if not self.db_host:
            raise ValueError("DB_HOST is required.")
        if not self.db_name:
            raise ValueError("DB_NAME is required.")
        if not self.db_user:
            raise ValueError("DB_USER is required.")
        if not self.db_password:
            raise ValueError("DB_PASSWORD is required.")
        if not isinstance(self.db_port, int) or self.db_port <= 0:
            raise ValueError("DB_PORT must be a positive integer.")

        if not self.s3_host:
            raise ValueError("S3_HOST is required.")
        if not isinstance(self.s3_port, int) or self.s3_port <= 0:
            raise ValueError("S3_PORT must be a positive integer.")
        if not self.s3_access_key:
            raise ValueError("S3_ACCESS_KEY is required.")
        if not self.s3_secret_key:
            raise ValueError("S3_SECRET_KEY is required.")
        if not self.s3_bucket_raw:
            raise ValueError("S3_BUCKET_RAW is required.")

        if not isinstance(self.log_file, str) or not self.log_file.strip():
            raise ValueError("LOG_FILE must be a non-empty string.")
        if not isinstance(self.log_level, str) or not self.log_level.strip():
            raise ValueError("LOG_LEVEL must be a non-empty string.")
