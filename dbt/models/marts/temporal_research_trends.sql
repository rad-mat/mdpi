{{ config(materialized='table') }}

-- Temporal Research Trends and Evolution
with yearly_metrics as (
    select 
        published_year,
        count(*) as total_publications,
        count(distinct journal) as active_journals,
        count(distinct publisher) as active_publishers,
        sum(is_referenced_by_count) as total_citations,
        avg(is_referenced_by_count) as avg_citations_per_paper,
        max(is_referenced_by_count) as highest_cited_paper,
        sum(reference_count) as total_references,
        avg(reference_count) as avg_references_per_paper,
        avg(data_quality_score) as avg_data_quality,
        count(case when is_referenced_by_count = 0 then 1 end)::float / count(*) as uncited_rate,
        count(case when is_referenced_by_count >= 100 then 1 end) as highly_cited_count
    from {{ ref('dim_publications') }}
    group by published_year
),

growth_trends as (
    select 
        published_year,
        total_publications,
        total_citations,
        active_journals,
        avg_citations_per_paper,
        highly_cited_count,
        uncited_rate,
        lag(total_publications) over (order by published_year) as prev_year_pubs,
        lag(total_citations) over (order by published_year) as prev_year_citations
    from yearly_metrics
),

growth_rates as (
    select *,
        case when prev_year_pubs > 0 then 
            ((total_publications - prev_year_pubs)::float / prev_year_pubs) * 100 
        end as pub_growth_rate,
        case when prev_year_citations > 0 then 
            ((total_citations - prev_year_citations)::float / prev_year_citations) * 100 
        end as citation_growth_rate
    from growth_trends
),

decade_analysis as (
    select 
        (published_year / 10) * 10 as decade_start,
        (published_year / 10) * 10 || 's' as decade_label,
        count(*) as total_publications_decade,
        sum(is_referenced_by_count) as total_citations_decade,
        avg(is_referenced_by_count) as avg_citations_per_paper_decade
    from {{ ref('dim_publications') }}
    group by (published_year / 10) * 10
)

select 
    published_year,
    total_publications,
    total_citations,
    avg_citations_per_paper,
    active_journals,
    highly_cited_count,
    pub_growth_rate,
    citation_growth_rate,
    uncited_rate
from growth_rates
order by published_year desc