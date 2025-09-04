#!/usr/bin/env python3
"""
Script to run individual pipeline components for testing and development
"""
import argparse

from pipelines.extract_pipeline import run_extract_pipeline


def main():
    parser = argparse.ArgumentParser(description="Run individual pipeline components")
    parser.add_argument(
        "pipeline",
        choices=["extract", "preprocess", "load", "all"],
        help="Pipeline component to run",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum pages to fetch (extract pipeline only)",
    )

    args = parser.parse_args()

    if args.pipeline == "extract":
        print("Running extract pipeline...")
        raw_data = run_extract_pipeline(max_pages=args.max_pages)
        print(f"Extract pipeline completed. Extracted {len(raw_data)} records.")

    elif args.pipeline == "preprocess":
        print("Running preprocess pipeline...")
        print("Note: This requires raw data from extract pipeline")
        # For testing, you would need to load raw data from S3 or pass it as parameter

    elif args.pipeline == "load":
        print("Running load pipeline...")
        print("Note: This requires processed data from preprocess pipeline")
        # For testing, you would need processed data

    elif args.pipeline == "all":
        print("Running full pipeline...")
        from main_orchestrated import run_full_pipeline

        result = run_full_pipeline(max_pages=args.max_pages)
        print(f"Full pipeline completed! Processed {result['processed_records']} records.")


if __name__ == "__main__":
    main()
