from prefect import flow

from pipelines.extract_pipeline import run_extract_pipeline
from pipelines.load_pipeline import run_load_pipeline
from pipelines.preprocess_pipeline import run_preprocess_pipeline


@flow(name="CrossRef Data Processing Pipeline")
def run_full_pipeline(max_pages: int = 5):
    """
    Main orchestration flow that runs the complete data processing pipeline:
    1. Extract data from CrossRef API
    2. Preprocess (normalize and deduplicate) the data
    3. Load the processed data into the database
    """
    # Extract pipeline
    raw_data = run_extract_pipeline(max_pages=max_pages)

    # Preprocess pipeline
    processed_data = run_preprocess_pipeline(raw_data)

    # Load pipeline
    run_load_pipeline(processed_data)

    return {"processed_records": len(processed_data)}


if __name__ == "__main__":
    # Run the full pipeline with default settings
    result = run_full_pipeline()
    print(f"Pipeline completed successfully! Processed {result['processed_records']} records.")
