{{ config(materialized='table') }}

-- Publication Quality and Data Integrity Assessment
with publication_quality as (
    select 
        id,
        title,
        published_year,
        journal,
        publisher,
        is_referenced_by_count as citations,
        reference_count,
        data_quality_score,
        is_missing_title,
        is_missing_doi,
        is_missing_journal,
        is_missing_authors,
        case when length(trim(title)) < 10 then true else false end as suspiciously_short_title,
        case when length(trim(title)) > 300 then true else false end as suspiciously_long_title,
        case when is_referenced_by_count > 1000 then true else false end as suspiciously_high_citations,
        case when reference_count = 0 then true else false end as no_references_flag,
        case when reference_count > 200 then true else false end as excessive_references_flag,
        case 
            when reference_count > 0 then is_referenced_by_count::float / reference_count
            else null 
        end as citation_reference_ratio
    from {{ ref('dim_publications') }}
),

quality_distribution as (
    select 
        case 
            when data_quality_score = 1.0 then 'Perfect (1.0)'
            when data_quality_score >= 0.75 then 'High (0.75-0.99)'
            when data_quality_score >= 0.5 then 'Medium (0.5-0.74)'
            when data_quality_score >= 0.25 then 'Low (0.25-0.49)'
            else 'Very Low (0-0.24)'
        end as quality_tier,
        count(*) as paper_count,
        avg(citations) as avg_citations,
        avg(reference_count) as avg_references,
        sum(citations) as total_citations,
        count(case when citations > 0 then 1 end)::float / count(*) as citation_rate
    from publication_quality
    group by 
        case 
            when data_quality_score = 1.0 then 'Perfect (1.0)'
            when data_quality_score >= 0.75 then 'High (0.75-0.99)'
            when data_quality_score >= 0.5 then 'Medium (0.5-0.74)'
            when data_quality_score >= 0.25 then 'Low (0.25-0.49)'
            else 'Very Low (0-0.24)'
        end
),

publisher_quality as (
    select 
        publisher,
        count(*) as total_papers,
        avg(data_quality_score) as avg_quality_score,
        count(case when data_quality_score >= 0.75 then 1 end)::float / count(*) as high_quality_rate,
        avg(citations) as avg_citations,
        avg(citation_reference_ratio) as avg_citation_ref_ratio
    from publication_quality
    where trim(publisher) != ''
    group by publisher
    having count(*) >= 100
)

select 
    quality_tier,
    paper_count,
    avg_citations,
    avg_references,
    total_citations,
    citation_rate
from quality_distribution
order by 
    case quality_tier
        when 'Perfect (1.0)' then 1
        when 'High (0.75-0.99)' then 2  
        when 'Medium (0.5-0.74)' then 3
        when 'Low (0.25-0.49)' then 4
        when 'Very Low (0-0.24)' then 5
    end