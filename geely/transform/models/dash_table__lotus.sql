{{ config(
    materialized='table',
) }}
WITH dash_table AS (
{{ dash_table_general_process.ttd(source_name='ttd_transformed__lotus', table_name='ttd__lotus') }}
UNION ALL
{{ dash_table_general_process.cm360(source_name='cm360_transformed__lotus', table_name='cm360_direct_buy__lotus') }}
UNION ALL
{{ dash_table_general_process.linkedin(source_name='linkedin_transformed__lotus', table_name='linkedin__lotus') }}
UNION ALL
{{ dash_table_general_process.hivestack(source_name='hivestack_transformed__lotus', table_name='hivestack__lotus') }}
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
