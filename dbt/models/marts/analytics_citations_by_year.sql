{{ config(materialized='table') }}

with yearly_citations as (
    select 
        published_year,
        count(*) as total_publications,
        sum(is_referenced_by_count) as total_citations,
        sum(reference_count) as total_references,
        avg(is_referenced_by_count) as avg_citations_per_paper,
        avg(reference_count) as avg_references_per_paper,
        max(is_referenced_by_count) as max_citations,
        min(is_referenced_by_count) as min_citations,
        percentile_cont(0.5) within group (order by is_referenced_by_count) as median_citations,
        count(case when is_referenced_by_count > 0 then 1 end) as cited_publications,
        count(case when is_referenced_by_count = 0 then 1 end) as uncited_publications
    from {{ ref('dim_publications') }}
    where published_year between 1990 and extract(year from current_date)
    group by published_year
),

yearly_trends as (
    select 
        *,
        -- Calculate year-over-year growth
        lag(total_publications) over (order by published_year) as prev_year_publications,
        lag(total_citations) over (order by published_year) as prev_year_citations,
        
        -- Citation rate
        case 
            when total_publications > 0 then 
                cited_publications::float / total_publications 
            else 0 
        end as citation_rate,
        
        current_timestamp as dbt_updated_at
        
    from yearly_citations
),

final as (
    select 
        *,
        -- Growth rates
        case 
            when prev_year_publications > 0 then 
                ((total_publications - prev_year_publications)::float / prev_year_publications) * 100
            else null 
        end as publications_growth_rate_pct,
        
        case 
            when prev_year_citations > 0 then 
                ((total_citations - prev_year_citations)::float / prev_year_citations) * 100
            else null 
        end as citations_growth_rate_pct
        
    from yearly_trends
)

select * from final
order by published_year