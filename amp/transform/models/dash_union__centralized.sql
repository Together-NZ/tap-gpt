{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

WITH final_result AS (
    {{ dash_table_general_process.dash_union_non_search(source_name='dash_union', table_name='dash_table', sub_brands='sub_brands') }}
    UNION ALL
    {{ dash_table_general_process.dash_union_search(source_name='dash_union_search', table_name='dash_table_search', sub_brands='sub_brands') }}
)
SELECT
    COALESCE(t2.present, t1.publisher) AS publisher,
    t1.* EXCEPT (publisher)
FROM final_result AS t1
LEFT JOIN `together-internal.publisher_naming.publisher_naming` AS t2
    ON LOWER(t1.publisher) = LOWER(t2.publisher)
