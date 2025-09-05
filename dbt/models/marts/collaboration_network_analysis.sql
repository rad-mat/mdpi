{{ config(materialized='table') }}

-- Author Collaboration Network Analysis
with author_pairs as (
    select 
        a1.author_name as author_1,
        a2.author_name as author_2,
        a1.publication_id,
        p.published_year,
        p.is_referenced_by_count as paper_citations,
        p.journal
    from {{ ref('stg_crossref_authors') }} a1
    join {{ ref('stg_crossref_authors') }} a2
        on a1.publication_id = a2.publication_id
        and a1.author_name < a2.author_name
    join {{ ref('dim_publications') }} p
        on a1.publication_id = p.id
),

collaboration_metrics as (
    select 
        author_1,
        author_2,
        count(*) as collaboration_count,
        count(distinct published_year) as years_collaborated,
        count(distinct journal) as journals_collaborated,
        sum(paper_citations) as total_collaboration_citations,
        avg(paper_citations) as avg_collaboration_citations,
        min(published_year) as first_collaboration,
        max(published_year) as latest_collaboration
    from author_pairs
    group by author_1, author_2
),

strong_collaborations as (
    select *,
        case 
            when collaboration_count >= 5 then 'Very Strong'
            when collaboration_count >= 3 then 'Strong'
            when collaboration_count = 2 then 'Moderate'
            else 'Weak'
        end as collaboration_strength
    from collaboration_metrics
    where collaboration_count >= 2
)

select 
    author_1,
    author_2,
    collaboration_count,
    collaboration_strength,
    total_collaboration_citations,
    years_collaborated,
    journals_collaborated
from strong_collaborations
order by collaboration_count desc, total_collaboration_citations desc