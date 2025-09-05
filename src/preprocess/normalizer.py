from datetime import datetime


class Normalizer:
    def __init__(
        self,
    ):
        pass

    def normalize(self, data):
        # Check if data is already transformed (from DataTransformer)
        if self._is_transformed_data(data):
            return self._normalize_transformed_data(data)
        else:
            return self._normalize_raw_data(data)

    def _is_transformed_data(self, data):
        """Check if data is already transformed by DataTransformer"""
        # Transformed data has flattened structure with specific keys
        transformed_keys = {"doi", "title", "publisher", "journal", "pub_year", "authors", "author_count"}
        return isinstance(data, dict) and transformed_keys.issubset(set(data.keys()))

    def _normalize_transformed_data(self, data):
        """Normalize already transformed data - mainly format conversion"""
        # Simply concatenate date parts as they are, no processing
        year = data.get("pub_year")
        month = data.get("pub_month")
        day = data.get("pub_day")

        # Create date string from available parts
        if year is not None:
            if month is not None and day is not None:
                published_date = f"{year:04d}-{month:02d}-{day:02d}"
            elif month is not None:
                published_date = f"{year:04d}-{month:02d}-01"
            else:
                published_date = f"{year:04d}-01-01"
        else:
            published_date = None

        return {
            "title": data.get("title", ""),
            "authors": data.get("authors", ""),  # Already formatted by transformer
            "published_date": published_date,
            "doi": data.get("doi", ""),
            "journal": data.get("journal", ""),
            "publisher": data.get("publisher", ""),
            "is_referenced_by_count": data.get("is_referenced_by_count", 0),
            "reference_count": data.get("reference_count", 0),
        }

    def _normalize_raw_data(self, data):
        """Normalize raw CrossRef data (legacy support)"""
        normalized_data = {
            "title": data.get("title", [""])[0],
            "authors": [author.get("family", "") for author in data.get("author", [])],
            "published_date": self._extract_raw_date(data),
            "doi": data.get("DOI", ""),
            "journal": data.get("container-title", [""])[0],
            "publisher": data.get("publisher", ""),
            "is_referenced_by_count": data.get("is-referenced-by-count", 0),
            "reference_count": data.get("reference-count", 0),
        }

        # Merge authors into a single string
        normalized_data["authors"] = self.__merge_list(normalized_data["authors"], separator=", ")

        return normalized_data

    def _extract_raw_date(self, data):
        """
        Extract and format publication date - just concatenate date parts exactly as they are.
        """
        # Try common date fields
        date_fields = ["issued", "published", "published-print"]
        
        for field in date_fields:
            date_data = data.get(field, {})
            if isinstance(date_data, dict) and "date-parts" in date_data:
                date_parts = date_data.get("date-parts", [[]])
                if date_parts and date_parts[0]:
                    parts = date_parts[0]
                    year = parts[0] if len(parts) > 0 else None
                    month = parts[1] if len(parts) > 1 else None
                    day = parts[2] if len(parts) > 2 else None
                    
                    if year is not None:
                        if month is not None and day is not None:
                            return f"{year:04d}-{month:02d}-{day:02d}"
                        elif month is not None:
                            return f"{year:04d}-{month:02d}-01"
                        else:
                            return f"{year:04d}-01-01"
        
        return None

    def __merge_list(self, list, separator=", ") -> str:
        """
        Merge a list into a string with a separator.
        """
        if not list:
            return ""
        if isinstance(list, str):
            return list

        return separator.join(list)
