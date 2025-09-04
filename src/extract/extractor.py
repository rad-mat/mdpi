import glob
import json
import os
from datetime import datetime
from logging import Logger

import requests
from requests.exceptions import HTTPError
from tqdm import tqdm

from src.utils.config import Config
from src.utils.s3_client import S3Client


class Extractor:
    """
    DataLoader class for loading data from CrossRef API.

    Attributes:
        config (Config): Configuration object containing API endpoint and key.
        headers (Dict[str, str]): Headers for the API request.
    """

    def __init__(self, config: Config, logger: Logger):
        self.config = config
        self.headers = {
            "Accept": "application/json",
        }
        self.s3_client = S3Client(config, logger)

        self.logger = logger
        self.logger.info("DataLoader initialized with config: %s", config.__dict__)
        self.logger.info("Headers set for API request: %s", self.headers)
        self.logger.info("DataLoader initialized successfully.")

    def fetch_and_save_data(self, max_pages):
        """
        Fetch data from CrossRef API by looping through multiple pages.

        Args:
            max_pages (int): Maximum number of pages to fetch
        """
        self.logger.info("Fetching data from CrossRef API...")

        all_data = []

        for page_offset in tqdm(range(0, max_pages * 200, 200), desc="Fetching pages", unit="page"):
            try:
                # Add offset parameter for pagination
                url = f"{self.config.api_endpoint}&offset={page_offset}"
                self.logger.info(f"Fetching page with offset {page_offset}: {url}")

                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                data = response.json()

                # Check if we have items in the response
                if "message" in data and "items" in data["message"]:
                    items = data["message"]["items"]
                    if not items:  # No more items, break the loop
                        self.logger.info("No more items found, stopping pagination")
                        break

                    all_data.append(data)
                    self.logger.info(f"Fetched {len(items)} items from page offset {page_offset}")
                else:
                    self.logger.warning(f"No 'message' or 'items' found in response for offset {page_offset}")
                    break

            except HTTPError as http_err:
                self.logger.error(f"HTTP error occurred for offset {page_offset}: {http_err}")
                break
            except Exception as err:
                self.logger.error(f"An error occurred for offset {page_offset}: {err}")
                break

        self.logger.info(f"Fetched data from {len(all_data)} pages successfully.")

        # Create S3 bucket for raw data if it doesn't exist
        self.s3_client.create_bucket_if_not_exists(self.config.s3_bucket_raw)

        # Save each page's data to separate files (local backup) and S3
        for i, data in enumerate(all_data):
            now = datetime.now()
            filename = now.strftime("%Y%m%d_%H%M%S") + f"_page_{i+1}_data.json"
            filepath = f"./data/raw/{filename}"

            # Ensure directory exists
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            # Save locally
            with open(filepath, "w") as f:
                json.dump(data, f, indent=4)

            self.logger.info(f"Saved page {i+1} data to {filepath}")

            # Save to S3
            s3_object_name = f"crossref/raw/{filename}"
            if self.s3_client.upload_json(self.config.s3_bucket_raw, s3_object_name, data):
                self.logger.info(f"Uploaded page {i+1} data to S3: {s3_object_name}")
            else:
                self.logger.error(f"Failed to upload page {i+1} data to S3")

    def extract_raw_data(self):
        """
        Extract raw data from the json files in ./data/raw directory.
        """
        self.logger.info("Extracting raw data from JSON files...")

        data = []
        json_files = glob.glob(os.path.join("./data/raw", "*.json"))
        for file in tqdm(json_files, desc="Extracting data", unit="file"):
            with open(file, "r") as f:
                try:
                    json_data = json.load(f)

                    # only get json_data.items
                    if "message" in json_data and "items" in json_data["message"]:
                        items = json_data.get("message", {}).get("items", [])
                        for item in items:
                            if isinstance(item, dict):
                                data.append(item)
                            else:
                                self.logger.warning(f"Item is not a dictionary: {item}")
                    else:
                        self.logger.warning(f"No 'message' or 'items' found in file {file}")
                except json.JSONDecodeError as e:
                    self.logger.error(f"Error decoding JSON from file {file}: {e}")
                    continue

        self.logger.info("Raw data extraction completed.")
        return data
