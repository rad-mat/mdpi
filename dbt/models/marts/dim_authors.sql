{{ config(materialized='table') }}

with author_publications as (
    select 
        author_id,
        author_name,
        publication_id,
        doi,
        author_position,
        published_year
    from {{ ref('stg_crossref_authors') }}
),

author_metrics as (
    select 
        ap.author_id,
        min(ap.author_name) as author_name,  -- Use min to get consistent name
        count(distinct ap.publication_id) as total_publications,
        min(ap.published_year) as first_publication_year,
        max(ap.published_year) as latest_publication_year,
        max(ap.published_year) - min(ap.published_year) + 1 as career_span_years,
        sum(pub.is_referenced_by_count) as total_citations,
        avg(pub.is_referenced_by_count) as avg_citations_per_paper,
        sum(case when ap.author_position = 1 then 1 else 0 end) as first_author_papers,
        count(distinct pub.journal) as journals_published_in,
        count(distinct pub.publisher) as publishers_worked_with,
        current_timestamp as dbt_updated_at
    from author_publications ap
    left join {{ ref('dim_publications') }} pub 
        on ap.publication_id = pub.id
    group by ap.author_id
),

author_productivity as (
    select 
        *,
        -- H-index approximation (simplified)
        case 
            when total_publications >= 10 then 
                sqrt(total_citations * total_publications / 2)::int
            else total_publications 
        end as estimated_h_index,
        
        -- Publications per year
        case 
            when career_span_years > 0 then 
                total_publications::float / career_span_years
            else total_publications::float
        end as publications_per_year,
        
        -- First author ratio
        case 
            when total_publications > 0 then 
                first_author_papers::float / total_publications
            else 0
        end as first_author_ratio
        
    from author_metrics
)

select * from author_productivity