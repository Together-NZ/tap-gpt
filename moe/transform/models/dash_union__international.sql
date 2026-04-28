{{
    config(
        materialized='table',
    )
}}
WITH funnel_old AS (
SELECT *  FROM `moe-main.dash_table.dash_union` WHERE campaign_name NOT IN (
    SELECT DISTINCT campaign_name FROM (
    SELECT * FROM `moe-main.dash_table.dash_union` WHERE ( LOWER(campaign_name) LIKE '%domestic%')
UNION ALL
SELECT * FROM `moe-main.dash_table.dash_union` WHERE LOWER(campaign_name) LIKE '%hivestack%'
UNION ALL
SELECT * FROM `moe-main.dash_table.dash_union` WHERE ((LOWER(campaign_name) LIKE '%nz%' OR LOWER(campaign_name) LIKE '%new zealand%') 
AND (LOWER(campaign_name) NOT LIKE '%international%' 
AND LOWER(campaign_name) NOT LIKE '%oversea%'))
UNION ALL 
SELECT * FROM `moe-main.dash_table.dash_union` WHERE (LOWER(campaign_name) NOT LIKE '%international%' 
AND LOWER(campaign_name) NOT LIKE '%oversea%' AND LOWER(campaign_name) NOT LIKE '%hivestack%' 
AND LOWER(campaign_name) NOT LIKE '%domestic%'
AND LOWER(campaign_name) NOT LIKE '%nz%'
AND LOWER(campaign_name) NOT LIKE '%new zealand%')
))),
new_funnel_paid_media AS (
SELECT * EXCEPT(funnel), 
CASE WHEN LOWER(campaign_name) LIKE '%canada%' THEN 'Canada'
WHEN LOWER(campaign_name) LIKE '%us%' OR LOWER(campaign_name) LIKE '%usa%' OR LOWER(campaign_name) LIKE '%united states%' OR LOWER(campaign_name) LIKE '%america%' THEN 'USA'
WHEN LOWER(campaign_name) LIKE '%germany%' OR LOWER(campaign_name) LIKE '%german%' OR LOWER(campaign_name) LIKE '%german%' THEN 'Germany'
WHEN LOWER(campaign_name) LIKE '%finland%' OR LOWER(campaign_name) LIKE '%fi%' OR LOWER(campaign_name) LIKE '%finnish%' THEN 'Finland'
WHEN LOWER(campaign_name) LIKE '%norway%'  OR LOWER(campaign_name) LIKE '%norwegian%' THEN 'Norway'
WHEN LOWER(campaign_name) LIKE '%denmark%' OR LOWER(campaign_name) LIKE '%dk%' OR LOWER(campaign_name) LIKE '%danish%' THEN 'Denmark'
WHEN LOWER(campaign_name) LIKE '%netherlands%'  OR LOWER(campaign_name) LIKE '%dutch%' THEN 'Netherlands'
WHEN (LOWER(campaign_name) LIKE '%uk%' OR LOWER(campaign_name) LIKE '%united kingdom%' OR LOWER(campaign_name) LIKE '%british%') AND NOT (
    LOWER(campaign_name) LIKE '%ireland%' OR LOWER(campaign_name) LIKE '%irish%' OR LOWER(campaign_name) LIKE '%ir%'
) THEN 'UK'
WHEN (LOWER(campaign_name) LIKE '%ireland%' OR LOWER(campaign_name) LIKE '%irish%' OR LOWER(campaign_name) LIKE '%ir%') AND NOT (
    LOWER(campaign_name) LIKE '%uk%' OR LOWER(campaign_name) LIKE '%united kingdom%' OR LOWER(campaign_name) LIKE '%british%'
) THEN 'Ireland'
WHEN (LOWER(campaign_name) LIKE '%uk%' OR LOWER(campaign_name) LIKE '%united kingdom%' OR LOWER(campaign_name) LIKE '%british%') AND (
    LOWER(campaign_name) LIKE '%ireland%' OR LOWER(campaign_name) LIKE '%irish%' OR LOWER(campaign_name) LIKE '%ir%'
) THEN 'UK & Ireland'
ELSE 'Other'
END AS funnel
FROM funnel_old 
)

SELECT * FROM new_funnel_paid_media
