{{ config(
    materialized='table',
) }}
WITH dash_table AS (
{{ dash_table_general_process.ttd(source_name='ttd_transformed__cffc', table_name='ttd_transformed__cffc') }}
UNION ALL
{{ dash_table_general_process.dv360_standard(source_name='dv360_transformed__cffc', table_name='dv360_standard__cffc', yt_source_name='dv360_transformed__cffc', yt_table_name='dv360_youtube__cffc') }}
UNION ALL
{{ dash_table_general_process.dv360_youtube(source_name='dv360_transformed__cffc', table_name='dv360_youtube__cffc') }}
UNION ALL
{{ dash_table_general_process.snapchat(source_name='snapchat_transformed__cffc', table_name='snapchat__cffc') }}
UNION ALL
{{ dash_table_general_process.hivestack(source_name='hivestack_transformed__cffc', table_name='hivestack__cffc') }}
UNION ALL
{{ dash_table_general_process.meta(source_name='facebook_transformed__cffc', table_name='facebook__cffc') }}
UNION ALL
{{ dash_table_general_process.cm360(source_name='cm360_transformed__cffc', table_name='cm360_direct_buy__cffc') }}
UNION ALL
{{ dash_table_general_process.linkedin(source_name='linkedin_transformed__cffc', table_name='linkedin__cffc') }}
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
