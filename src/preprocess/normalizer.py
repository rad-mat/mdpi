from datetime import datetime


class Normalizer:
    def __init__(self,):
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
        transformed_keys = {'doi', 'title', 'publisher', 'journal', 'pub_year', 'authors', 'author_count'}
        return isinstance(data, dict) and transformed_keys.issubset(set(data.keys()))
    
    def _normalize_transformed_data(self, data):
        """Normalize already transformed data - mainly format conversion"""
        # Create published_date from individual components, or null if any component is missing
        year = data.get('pub_year')
        month = data.get('pub_month')
        day = data.get('pub_day')
        
        # Only create date if all components are present and valid
        if year is not None and month is not None and day is not None:
            published_date = f"{year:04d}-{month:02d}-{day:02d}"
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
            "published_date": data.get("issued", {}).get("date-parts", [[None]])[0][0],
            "doi": data.get("DOI", ""),
            "journal": data.get("container-title", [""])[0],
            "publisher": data.get("publisher", ""),
            "is_referenced_by_count": data.get("is-referenced-by-count", 0),
            "reference_count": data.get("reference-count", 0),
        }

        # Polyfill date
        date_parts = normalized_data["published_date"]
        if isinstance(date_parts, list):
            date_parts = date_parts[0] if date_parts else []
        elif isinstance(date_parts, str):
            date_parts = [int(part) for part in date_parts.split("-")]
        normalized_data["published_date"] = self.__polyfill_date(date_parts)

        # Merge authors into a single string
        normalized_data["authors"] = self.__merge_list(normalized_data["authors"], separator=", ")

        return normalized_data

    def __polyfill_date(self, date_parts) -> str:
        if not date_parts:
            return "1970-01-01"

        # dateparts is an array of year, month, day - fill missing parts with 01

        if not isinstance(date_parts, list):
            date_parts = [date_parts]

        year = date_parts[0]
        month = date_parts[1] if len(date_parts) > 1 else 1
        day = date_parts[2] if len(date_parts) > 2 else 1

        # sanitize year - if more than 12 months into the future, set to current year
        current_year = datetime.now().year
        if year > current_year + 1:
            year = current_year

        # sanitize month: 01 - 12 range
        if month < 1 or month > 12:
            month = 1

        # sanitize day: 01 - 31 range
        if day < 1 or day > 31:
            day = 1

        # format date as YYYY-MM-DD
        date = f"{year:04d}-{month:02d}-{day:02d}"

        return date

    def __merge_list(self, list, separator=", ") -> str:
        """
        Merge a list into a string with a separator.
        """
        if not list:
            return ""
        if isinstance(list, str):
            return list

        return separator.join(list)
