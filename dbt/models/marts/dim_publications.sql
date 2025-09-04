{{ config(materialized='table') }}

with publications as (
    select
        id,
        title,
        published_date,
        published_year,
        doi,
        journal,
        publisher,
        is_referenced_by_count,
        reference_count,
        is_missing_title,
        is_missing_doi,
        is_missing_journal,
        is_missing_authors,
        dbt_updated_at
    from {{ ref('stg_crossref_publications') }}
),

publication_metrics as (
    select
        *,
        -- Calculate citation impact scores
        case
            when is_referenced_by_count > 0 then
                ln(is_referenced_by_count + 1) * 10  -- log scale impact score
            else 0
        end as citation_impact_score,

        -- Publication age in years
        extract(year from current_date) - published_year as publication_age_years,

        -- Data quality score (0-1)
        (case when not is_missing_title then 0.25 else 0 end +
         case when not is_missing_doi then 0.25 else 0 end +
         case when not is_missing_journal then 0.25 else 0 end +
         case when not is_missing_authors then 0.25 else 0 end) as data_quality_score,

        -- Citation rate per year
        case
            when extract(year from current_date) - published_year > 0 then
                is_referenced_by_count::float / (extract(year from current_date) - published_year)
            else is_referenced_by_count::float
        end as citations_per_year

    from publications
)

select * from publication_metrics
