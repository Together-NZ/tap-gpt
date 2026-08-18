{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}
WITH final_result AS (
  {{ dash_table_general_process.dash_union_non_search(source_name='dash_union__sorted', table_name='dash_table__sorted',sub_brands=env_var('SUB_BRANDS', 'null')) }}
  UNION ALL
  {{ dash_table_general_process.dash_union_search(source_name='dash_union_search__sorted', table_name='dash_table_search__sorted',sub_brands=env_var('SUB_BRANDS', 'null')) }}
)
SELECT
  COALESCE(t2.present, t1.publisher) AS publisher,
  t1.*
EXCEPT(publisher)
FROM final_result AS t1
LEFT JOIN `together-internal.publisher_naming.publisher_naming` AS t2
  ON LOWER(t1.publisher) = LOWER(t2.publisher)
