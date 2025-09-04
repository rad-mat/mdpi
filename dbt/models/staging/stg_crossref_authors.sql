{{ config(materialized='view') }}

with publications as (
    select
        id as publication_id,
        doi,
        authors,
        title,
        published_year
    from {{ ref('stg_crossref_publications') }}
    where array_length(authors, 1) > 0
),

authors_unnested as (
    select
        publication_id,
        doi,
        title,
        published_year,
        unnest(authors) as author_name,
        generate_series(1, array_length(authors, 1)) as author_position
    from publications
),

authors_cleaned as (
    select
        publication_id,
        doi,
        title,
        published_year,
        trim(coalesce(author_name, '')) as author_name,
        author_position,
        -- Generate a simple author ID based on name
        md5(lower(trim(coalesce(author_name, '')))) as author_id,
        current_timestamp as dbt_updated_at
    from authors_unnested
    where trim(coalesce(author_name, '')) != ''
)

select * from authors_cleaned
