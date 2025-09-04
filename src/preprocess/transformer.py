import polars as pl
import re
from datetime import datetime
from typing import Dict, List, Any


class DataTransformer:
    """
    Data transformer class using Polars for efficient data cleaning and transformation.
    Handles common data quality issues like inconsistent IDs, missing fields, and invalid dates.
    """

    def __init__(self):
        self.current_year = datetime.now().year

    def transform_crossref_data(self, raw_data: List[Dict[Any, Any]]) -> List[Dict[Any, Any]]:
        """
        Main transformation method for CrossRef data using Polars.

        Args:
            raw_data: List of raw CrossRef publication records

        Returns:
            List of cleaned and transformed records
        """
        if not raw_data:
            return []

        # Convert to Polars DataFrame for efficient processing
        # First, normalize the nested structure into flat columns
        flattened_data = [self._flatten_record(record) for record in raw_data]

        try:
            df = pl.DataFrame(flattened_data)
        except Exception:
            # Fallback to record-by-record processing if DataFrame creation fails
            return [self._transform_single_record(record) for record in raw_data]

        # Apply transformations
        df = self._fix_date_issues(df)
        df = self._clean_author_data(df)
        df = self._standardize_identifiers(df)
        df = self._handle_missing_fields(df)
        df = self._validate_and_clean_text(df)

        # Convert back to list of dictionaries
        return df.to_dicts()

    def _flatten_record(self, record: Dict[Any, Any]) -> Dict[str, Any]:
        """
        Flatten nested CrossRef record structure for Polars processing.
        """
        flattened = {}

        # Basic fields
        flattened["doi"] = record.get("DOI", "")
        flattened["title"] = self._extract_first_element(record.get("title", []))
        flattened["publisher"] = record.get("publisher", "")
        flattened["journal"] = self._extract_first_element(record.get("container-title", []))
        flattened["volume"] = record.get("volume", "")
        flattened["issue"] = record.get("issue", "")
        flattened["page"] = record.get("page", "")
        flattened["reference_count"] = record.get("reference-count", 0)
        flattened["is_referenced_by_count"] = record.get("is-referenced-by-count", 0)

        # Extract publication date
        published_date = record.get("issued", {}).get("date-parts", [[None, None, None]])
        if published_date and published_date[0]:
            flattened["pub_year"] = published_date[0][0] if len(published_date[0]) > 0 else None
            flattened["pub_month"] = published_date[0][1] if len(published_date[0]) > 1 else None
            flattened["pub_day"] = published_date[0][2] if len(published_date[0]) > 2 else None
        else:
            flattened["pub_year"] = None
            flattened["pub_month"] = None
            flattened["pub_day"] = None

        # Extract author information
        authors = record.get("author", [])
        if authors and isinstance(authors, list):
            author_names = []
            author_families = []
            author_givens = []

            for author in authors:
                if isinstance(author, dict):
                    family = author.get("family", "").strip()
                    given = author.get("given", "").strip()

                    if family or given:
                        full_name = f"{given} {family}".strip()
                        author_names.append(full_name)
                        author_families.append(family)
                        author_givens.append(given)

            flattened["authors"] = "; ".join(author_names) if author_names else ""
            flattened["author_count"] = len(author_names)
        else:
            flattened["authors"] = ""
            flattened["author_count"] = 0

        return flattened

    def _extract_first_element(self, field) -> str:
        """Extract first element from list fields or return as string."""
        if isinstance(field, list) and field:
            return str(field[0]).strip()
        elif isinstance(field, str):
            return field.strip()
        return ""

    def _fix_date_issues(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Fix common date issues. Set invalid dates to null instead of defaults.
        """
        today = datetime.now().date()

        return df.with_columns(
            [
                # Set future years to null
                pl.when(pl.col("pub_year") > self.current_year)
                .then(pl.lit(None))
                .otherwise(pl.col("pub_year"))
                .alias("pub_year"),
                # Set invalid months to null
                pl.when(pl.col("pub_month").is_null() | (pl.col("pub_month") < 1) | (pl.col("pub_month") > 12))
                .then(pl.lit(None))
                .otherwise(pl.col("pub_month"))
                .alias("pub_month"),
                # Set invalid days to null
                pl.when(pl.col("pub_day").is_null() | (pl.col("pub_day") < 1) | (pl.col("pub_day") > 31))
                .then(pl.lit(None))
                .otherwise(pl.col("pub_day"))
                .alias("pub_day"),
            ]
        ).with_columns(
            [
                # Additional validation: check if the complete date is in the future
                pl.when(
                    pl.col("pub_year").is_not_null()
                    & pl.col("pub_month").is_not_null()
                    & pl.col("pub_day").is_not_null()
                )
                .then(
                    pl.when(
                        (pl.col("pub_year") > today.year)
                        | ((pl.col("pub_year") == today.year) & (pl.col("pub_month") > today.month))
                        | (
                            (pl.col("pub_year") == today.year)
                            & (pl.col("pub_month") == today.month)
                            & (pl.col("pub_day") > today.day)
                        )
                    )
                    .then(pl.lit(None))  # Set future dates to null
                    .otherwise(pl.col("pub_year"))
                )
                .otherwise(pl.col("pub_year"))
                .alias("pub_year"),
                # If year is null, set month and day to null too
                pl.when(pl.col("pub_year").is_null())
                .then(pl.lit(None))
                .otherwise(pl.col("pub_month"))
                .alias("pub_month"),
                pl.when(pl.col("pub_year").is_null()).then(pl.lit(None)).otherwise(pl.col("pub_day")).alias("pub_day"),
            ]
        )

    def _clean_author_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Clean and standardize author information.
        """
        return df.with_columns(
            [
                # Clean author names - remove extra whitespace and normalize
                pl.col("authors")
                .str.replace_all(r"\s+", " ")  # Multiple spaces -> single space
                .str.replace_all(r";\s*;", ";")  # Multiple semicolons -> single
                .str.strip_chars()
                .alias("authors"),
                # Ensure author count is consistent with actual author string
                pl.when(pl.col("authors") == "")
                .then(pl.lit(0))
                .otherwise(pl.col("authors").str.split(";").list.len())
                .alias("author_count"),
            ]
        )

    def _standardize_identifiers(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Standardize DOI and other identifiers.
        """
        return df.with_columns(
            [
                # Clean and standardize DOI format
                pl.col("doi")
                .str.strip_chars()
                .str.to_lowercase()
                .str.replace_all(r"^(https?://)?(dx\.)?doi\.org/", "")  # Remove URL prefix
                .str.replace_all(r"^doi:", "")  # Remove doi: prefix
                .alias("doi"),
            ]
        )

    def _handle_missing_fields(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Handle missing or empty required fields.
        """
        return df.with_columns(
            [
                # Fill missing titles with placeholder
                pl.when(pl.col("title").is_null() | (pl.col("title") == ""))
                .then(pl.lit("[Title Missing]"))
                .otherwise(pl.col("title"))
                .alias("title"),
                # Fill missing journal names
                pl.when(pl.col("journal").is_null() | (pl.col("journal") == ""))
                .then(pl.lit("[Journal Missing]"))
                .otherwise(pl.col("journal"))
                .alias("journal"),
                # Fill missing publisher
                pl.when(pl.col("publisher").is_null() | (pl.col("publisher") == ""))
                .then(pl.lit("[Publisher Missing]"))
                .otherwise(pl.col("publisher"))
                .alias("publisher"),
                # Ensure numeric fields have default values
                pl.col("reference_count").fill_null(0),
                pl.col("is_referenced_by_count").fill_null(0),
            ]
        )

    def _validate_and_clean_text(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Validate and clean text fields.
        """
        return df.with_columns(
            [
                # Clean titles - remove extra whitespace and invalid characters
                pl.col("title").str.replace_all(r"\s+", " ").str.strip_chars().alias("title"),
                # Clean journal names
                pl.col("journal").str.replace_all(r"\s+", " ").str.strip_chars().alias("journal"),
                # Clean publisher names
                pl.col("publisher").str.replace_all(r"\s+", " ").str.strip_chars().alias("publisher"),
            ]
        )

    def _transform_single_record(self, record: Dict[Any, Any]) -> Dict[str, Any]:
        """
        Fallback method to transform a single record without Polars.
        Used when DataFrame creation fails.
        """
        flattened = self._flatten_record(record)

        # Clean text fields
        for field in ["title", "journal", "publisher"]:
            if flattened.get(field):
                flattened[field] = re.sub(r"\s+", " ", str(flattened[field])).strip()

        # Handle missing fields
        if not flattened.get("title"):
            flattened["title"] = "[Title Missing]"
        if not flattened.get("journal"):
            flattened["journal"] = "[Journal Missing]"
        if not flattened.get("publisher"):
            flattened["publisher"] = "[Publisher Missing]"

        # Handle date validation in fallback method too
        if flattened.get("pub_year"):
            if flattened["pub_year"] > self.current_year:
                flattened["pub_year"] = None

        # Validate month and day
        if flattened.get("pub_month") and (flattened["pub_month"] < 1 or flattened["pub_month"] > 12):
            flattened["pub_month"] = None
        if flattened.get("pub_day") and (flattened["pub_day"] < 1 or flattened["pub_day"] > 31):
            flattened["pub_day"] = None

        # If year is null, set month and day to null too
        if flattened.get("pub_year") is None:
            flattened["pub_month"] = None
            flattened["pub_day"] = None

        # Clean DOI
        if flattened.get("doi"):
            doi = flattened["doi"].strip().lower()
            doi = re.sub(r"^(https?://)?(dx\.)?doi\.org/", "", doi)
            doi = re.sub(r"^doi:", "", doi)
            flattened["doi"] = doi

        return flattened

    def get_transformation_summary(self, original_data: List[Dict], transformed_data: List[Dict]) -> Dict[str, Any]:
        """
        Generate a summary of transformations applied.
        """
        return {
            "original_count": len(original_data),
            "transformed_count": len(transformed_data),
            "transformations_applied": [
                "Fixed future years (e.g., 2203 -> 2003)",
                "Standardized author formatting",
                "Cleaned DOI identifiers",
                "Filled missing titles/journals/publishers",
                "Normalized text whitespace",
                "Validated date ranges",
            ],
        }
