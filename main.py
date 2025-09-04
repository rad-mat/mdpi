"""
Legacy main.py pipeline - now replaced by orchestrated pipeline.
This file is kept for backwards compatibility but uses the new orchestrated approach.
For new development, use main_orchestrated.py instead.
"""

from main_orchestrated import run_full_pipeline

if __name__ == "__main__":
    print("Running legacy main.py - this now uses the orchestrated pipeline...")
    print("For new development, use main_orchestrated.py instead.")

    # Run the orchestrated pipeline with the same parameters as the legacy version
    result = run_full_pipeline(max_pages=5)
    print(f"Pipeline completed! Processed {result['processed_records']} records.")

    # Note: dbt models are available in the dbt/ directory
    print("To run dbt analytics: cd dbt && dbt run")
