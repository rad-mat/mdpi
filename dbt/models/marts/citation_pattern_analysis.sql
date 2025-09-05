{{ config(materialized='table') }}

-- Advanced Citation Pattern Analysis
with citation_base as (
    select 
        id,
        title,
        published_year,
        journal,
        publisher,
        is_referenced_by_count as citations,
        reference_count,
        data_quality_score,
        publication_age_years,
        citations_per_year,
        case 
            when is_referenced_by_count = 0 then 'Uncited'
            when is_referenced_by_count between 1 and 5 then 'Low Impact'
            when is_referenced_by_count between 6 and 20 then 'Moderate Impact'
            when is_referenced_by_count between 21 and 100 then 'High Impact'
            else 'Exceptional Impact'
        end as citation_tier
    from {{ ref('dim_publications') }}
    where published_year >= 1990
),

citation_distribution as (
    select 
        citation_tier,
        count(*) as paper_count,
        sum(citations) as total_citations,
        avg(citations) as avg_citations,
        percentile_cont(0.5) within group (order by citations) as median_citations,
        max(citations) as max_citations,
        min(citations) as min_citations
    from citation_base
    group by citation_tier
),

journal_citation_patterns as (
    select 
        journal,
        count(*) as total_papers,
        sum(citations) as total_journal_citations,
        avg(citations) as avg_citations_per_paper,
        percentile_cont(0.5) within group (order by citations) as median_citations,
        max(citations) as highest_cited_paper,
        count(case when citations = 0 then 1 end)::float / count(*) as uncited_rate
    from citation_base
    where trim(journal) != ''
    group by journal
    having count(*) >= 20
),

aging_patterns as (
    select 
        publication_age_years,
        count(*) as papers_at_age,
        avg(citations) as avg_citations_at_age,
        sum(citations) as total_citations_at_age
    from citation_base
    where publication_age_years between 1 and 20
    group by publication_age_years
)

select 
    citation_tier as analysis_category,
    paper_count as metric_value,
    'Papers in tier' as metric_description,
    avg_citations as secondary_metric
from citation_distribution