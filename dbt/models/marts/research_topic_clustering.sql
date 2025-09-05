{{ config(materialized='table') }}

-- Research Topic Clustering and Trend Analysis
with title_tokens as (
    select 
        id,
        title,
        published_year,
        journal,
        is_referenced_by_count,
        regexp_split_to_table(
            lower(regexp_replace(title, '[^a-zA-Z0-9\s]', ' ', 'g')), 
            '\s+'
        ) as word
    from {{ ref('dim_publications') }}
    where length(trim(title)) > 10
),

meaningful_words as (
    select *
    from title_tokens
    where length(word) >= 4
      and word not in (
          'and', 'the', 'for', 'with', 'from', 'this', 'that', 'they', 'were', 'been',
          'have', 'has', 'had', 'will', 'would', 'could', 'should', 'their', 'there',
          'then', 'than', 'when', 'where', 'what', 'which', 'while', 'these', 'those',
          'such', 'some', 'more', 'most', 'many', 'much', 'very', 'well', 'also',
          'only', 'just', 'like', 'into', 'over', 'after', 'under', 'about', 'through'
      )
      and word ~ '^[a-z]+$'
),

word_metrics as (
    select 
        word,
        count(*) as word_frequency,
        count(distinct id) as papers_with_word,
        count(distinct journal) as journals_with_word,
        count(distinct published_year) as years_appeared,
        sum(is_referenced_by_count) as total_citations_for_word,
        avg(is_referenced_by_count) as avg_citations_per_paper,
        min(published_year) as first_appearance,
        max(published_year) as latest_appearance,
        (count(*) * avg(is_referenced_by_count)) as word_impact_score
    from meaningful_words
    group by word
    having count(*) >= 5
),

word_trends as (
    select *,
        case 
            when first_appearance >= 2015 then 'Emerging'
            when first_appearance >= 2010 then 'Growing'
            when first_appearance >= 2005 then 'Established'
            else 'Legacy'
        end as term_lifecycle
    from word_metrics
)

select 
    word,
    word_frequency,
    papers_with_word,
    journals_with_word,
    years_appeared,
    total_citations_for_word,
    avg_citations_per_paper,
    word_impact_score,
    term_lifecycle,
    first_appearance,
    latest_appearance
from word_trends
order by word_impact_score desc