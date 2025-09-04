{{ config(materialized='view') }}

with source_data as (
    select
        id,
        title,
        authors,
        published_date,
        doi,
        journal,
        publisher,
        is_referenced_by_count,
        reference_count
    from {{ var('raw_schema') }}.{{ var('raw_table') }}
),

cleaned_data as (
    select
        id,
        -- Clean and standardize title
        trim(coalesce(title, '')) as title,

        -- Handle authors array
        case
            when authors is null or array_length(authors, 1) is null then array[]::text[]
            else authors
        end as authors,

        -- Ensure valid date
        case
            when published_date is null then '1970-01-01'::date
            else published_date
        end as published_date,

        -- Extract year from published date
        extract(year from
            case
                when published_date is null then '1970-01-01'::date
                else published_date
            end
        ) as published_year,

        -- Clean DOI
        lower(trim(coalesce(doi, ''))) as doi,

        -- Clean journal name
        trim(coalesce(journal, '')) as journal,

        -- Clean publisher name
        trim(coalesce(publisher, '')) as publisher,

        -- Ensure non-negative citation counts
        case
            when is_referenced_by_count < 0 then 0
            else coalesce(is_referenced_by_count, 0)
        end as is_referenced_by_count,

        case
            when reference_count < 0 then 0
            else coalesce(reference_count, 0)
        end as reference_count,

        -- Add data quality flags
        case when trim(coalesce(title, '')) = '' then true else false end as is_missing_title,
        case when trim(coalesce(doi, '')) = '' then true else false end as is_missing_doi,
        case when trim(coalesce(journal, '')) = '' then true else false end as is_missing_journal,
        case when array_length(authors, 1) is null then true else false end as is_missing_authors,

        -- Add current timestamp for tracking
        current_timestamp as dbt_updated_at

    from source_data
)

select * from cleaned_data
