import io
import json
from datetime import datetime
from typing import Any, Dict, Optional

from minio import Minio
from minio.error import S3Error


class S3Client:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.client = None
        self._initialize_client()

    def _initialize_client(self):
        try:
            self.client = Minio(
                endpoint=f"{self.config.s3_host}:{self.config.s3_port}",
                access_key=self.config.s3_access_key,
                secret_key=self.config.s3_secret_key,
                secure=self.config.s3_secure,
            )
            self.logger.info("MinIO S3 client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize S3 client: {e}")
            raise

    def create_bucket_if_not_exists(self, bucket_name: str) -> bool:
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                self.logger.info(f"Created bucket: {bucket_name}")
            else:
                self.logger.info(f"Bucket {bucket_name} already exists")
            return True
        except S3Error as e:
            self.logger.error(f"Failed to create bucket {bucket_name}: {e}")
            return False

    def upload_json(self, bucket_name: str, object_name: str, data: Dict[Any, Any]) -> bool:
        try:
            json_data = json.dumps(data, indent=2, default=str)
            json_bytes = json_data.encode("utf-8")

            self.client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=io.BytesIO(json_bytes),
                length=len(json_bytes),
                content_type="application/json",
            )

            self.logger.info(f"Uploaded {object_name} to bucket {bucket_name}")
            return True
        except S3Error as e:
            self.logger.error(f"Failed to upload {object_name}: {e}")
            return False

    def upload_file(self, bucket_name: str, object_name: str, file_path: str) -> bool:
        try:
            self.client.fput_object(bucket_name=bucket_name, object_name=object_name, file_path=file_path)
            self.logger.info(f"Uploaded file {file_path} as {object_name} to bucket {bucket_name}")
            return True
        except S3Error as e:
            self.logger.error(f"Failed to upload file {file_path}: {e}")
            return False

    def download_json(self, bucket_name: str, object_name: str) -> Optional[Dict[Any, Any]]:
        try:
            response = self.client.get_object(bucket_name, object_name)
            data = json.loads(response.read().decode("utf-8"))
            self.logger.info(f"Downloaded {object_name} from bucket {bucket_name}")
            return data
        except S3Error as e:
            self.logger.error(f"Failed to download {object_name}: {e}")
            return None
        finally:
            if "response" in locals():
                response.close()
                response.release_conn()

    def list_objects(self, bucket_name: str, prefix: str = "") -> list:
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
            object_list = [obj.object_name for obj in objects]
            self.logger.info(f"Listed {len(object_list)} objects from bucket {bucket_name}")
            return object_list
        except S3Error as e:
            self.logger.error(f"Failed to list objects from bucket {bucket_name}: {e}")
            return []

    def generate_object_name(self, prefix: str = "crossref", extension: str = "json") -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{prefix}/{timestamp}.{extension}"
