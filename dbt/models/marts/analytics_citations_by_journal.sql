{{ config(materialized='table') }}

with journal_citations as (
    select 
        journal,
        publisher,
        count(*) as total_publications,
        count(distinct published_year) as years_active,
        min(published_year) as first_publication_year,
        max(published_year) as latest_publication_year,
        sum(is_referenced_by_count) as total_citations,
        sum(reference_count) as total_references,
        avg(is_referenced_by_count) as avg_citations_per_paper,
        avg(reference_count) as avg_references_per_paper,
        max(is_referenced_by_count) as max_citations,
        percentile_cont(0.5) within group (order by is_referenced_by_count) as median_citations,
        count(case when is_referenced_by_count > 0 then 1 end) as cited_publications,
        avg(data_quality_score) as avg_data_quality_score
    from {{ ref('dim_publications') }}
    where trim(journal) != ''
    group by journal, publisher
),

journal_rankings as (
    select 
        *,
        -- Rank journals by various metrics
        row_number() over (order by total_citations desc) as rank_by_total_citations,
        row_number() over (order by avg_citations_per_paper desc) as rank_by_avg_citations,
        row_number() over (order by total_publications desc) as rank_by_publication_count,
        
        -- Impact metrics
        case 
            when total_publications > 0 then 
                cited_publications::float / total_publications 
            else 0 
        end as citation_rate,
        
        -- Journal productivity
        case 
            when years_active > 0 then 
                total_publications::float / years_active 
            else total_publications::float
        end as publications_per_year,
        
        current_timestamp as dbt_updated_at
        
    from journal_citations
),

final as (
    select 
        *,
        -- Overall journal score (weighted combination of metrics)
        (
            (avg_citations_per_paper / nullif((select max(avg_citations_per_paper) from journal_rankings), 0)) * 0.4 +
            (citation_rate) * 0.3 +
            (publications_per_year / nullif((select max(publications_per_year) from journal_rankings), 0)) * 0.3
        ) * 100 as journal_impact_score
        
    from journal_rankings
)

select * from final
where total_publications >= 5  -- Filter out journals with very few publications
order by total_citations desc