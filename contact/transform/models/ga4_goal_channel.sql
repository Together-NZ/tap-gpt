{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'publisher', 'channel', 'funnel'],
) }}
(SELECT date,
'purchase_energy' AS eventName,
eventCount,
NULL AS eventValue,
campaign_name,publisher,sessionSourceMedium,
sessionCampaignName,sessionSourceMediumraw,
site_name,channel,campaign_name_selection,sessionManualAdContent,
funnel,media_format,NULL AS row_num
FROM `contact-energy-main.ga4_transformed.ga4_goal_energy_purchase`
) UNION ALL (
    SELECT * FROM `contact-energy-main.ga4_transformed.ga4_goal_channel_no_purchases`
)
UNION ALL (
    SELECT date,
'purchase_broadband' AS eventName,
eventCount,
NULL AS eventValue,
campaign_name,publisher,sessionSourceMedium,
sessionCampaignName,sessionSourceMediumraw,
site_name,channel,campaign_name_selection,sessionManualAdContent,
funnel,media_format,NULL AS row_num
FROM `contact-energy-main.ga4_transformed.ga4_goal_broadband_purchase`
)