{{ config(materialized='table') }}

with publisher_citations as (
    select 
        publisher,
        count(*) as total_publications,
        count(distinct journal) as total_journals,
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
    where trim(publisher) != ''
    group by publisher
),

publisher_rankings as (
    select 
        *,
        -- Rank publishers by various metrics
        row_number() over (order by total_citations desc) as rank_by_total_citations,
        row_number() over (order by avg_citations_per_paper desc) as rank_by_avg_citations,
        row_number() over (order by total_publications desc) as rank_by_publication_count,
        row_number() over (order by total_journals desc) as rank_by_journal_count,
        
        -- Impact metrics
        case 
            when total_publications > 0 then 
                cited_publications::float / total_publications 
            else 0 
        end as citation_rate,
        
        -- Publisher productivity
        case 
            when years_active > 0 then 
                total_publications::float / years_active 
            else total_publications::float
        end as publications_per_year,
        
        case 
            when years_active > 0 then 
                total_journals::float / years_active 
            else total_journals::float
        end as journals_per_year,
        
        current_timestamp as dbt_updated_at
        
    from publisher_citations
),

market_share as (
    select 
        pr.*,
        -- Market share calculations
        pr.total_publications::float / sum(pr.total_publications) over () as publication_market_share,
        pr.total_citations::float / sum(pr.total_citations) over () as citation_market_share,
        pr.total_journals::float / sum(pr.total_journals) over () as journal_market_share
    from publisher_rankings pr
),

final as (
    select 
        *,
        -- Publisher influence score
        (
            (avg_citations_per_paper / nullif((select max(avg_citations_per_paper) from market_share), 0)) * 0.3 +
            (citation_rate) * 0.25 +
            (publication_market_share) * 0.25 +
            (citation_market_share) * 0.2
        ) * 100 as publisher_influence_score
        
    from market_share
)

select * from final
where total_publications >= 10  -- Filter out publishers with very few publications
order by total_citations desc