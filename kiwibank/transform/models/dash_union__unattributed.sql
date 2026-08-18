{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}
SELECT * FROM `kiwibank-main.dash_table__ao_social_boosting.dash_union__ao_social_boosting`
WHERE campaign_name LIKE '%argargbservsevggcservghsthvcwrthcxstgxsdgxd%'