{{ config(
    materialized='table',
) }}
WITH dash_table AS (
{{ dash_table_general_process.google_ads(source_name='google_ads', table_name='google_ads_demand') }}
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
