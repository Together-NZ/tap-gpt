{{ config(
    materialized='table',
) }}
WITH dash_table AS (
{{ dash_table_general_process.ttd(source_name='ttd_transformed__geely', table_name='ttd__geely') }}
UNION ALL
{{ dash_table_general_process.cm360(source_name='cm360_transformed__geely', table_name='cm360_direct_buy__geely') }}
UNION ALL
{{ dash_table_general_process.meta(source_name='facebook_transformed__geely', table_name='facebook__geely') }}
UNION ALL
{{ dash_table_general_process.linkedin(source_name='linkedin_transformed__geely', table_name='linkedin__geely') }}
UNION ALL
{{ dash_table_general_process.google_ads(source_name='google_ads_search_transformed__geely', table_name='google_ads_demand__geely') }}
),
with_channel AS (
SELECT * EXCEPT (publisher, channel),
dc.publisher,
dc.channel
FROM dash_table AS dt
JOIN `together-internal.channel.publisher_channel` AS dc
  ON lower(trim(dt.publisher)) = lower(trim(dc.publisher))
),
{{ dash_table_general_process.dash_table_general_process() }}
