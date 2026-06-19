{{ config(
    materialized='table',
) }}
WITH dash_table AS (
{{dash_table_general_process.tiktok(source_name='tiktok_transformed', table_name='tiktok')          }} WHERE campaign_name LIKE '%00005%'
UNION ALL
{{dash_table_general_process.meta(source_name='facebook_transformed', table_name='facebook')}} WHERE campaign_name LIKE '%00005%'
UNION ALL
{{dash_table_general_process.linkedin(source_name='linkedin_transformed', table_name='linkedin')}} WHERE campaign_name LIKE '%00005%'
UNION ALL
{{dash_table_general_process.dv360_standard(source_name='dv360_transformed', table_name='dv360_standard',yt_source_name='dv360_transformed',yt_table_name='dv360_youtube')}} AND campaign_name LIKE '%00005%'
UNION ALL
{{dash_table_general_process.dv360_youtube(source_name='dv360_transformed', table_name='dv360_youtube')}} WHERE campaign_name LIKE '%00005%'
UNION ALL
{{dash_table_general_process.hivestack(source_name='hivestack_transformed', table_name='hivestack')}} WHERE campaign_name LIKE '%00005%'
UNION ALL
{{dash_table_general_process.google_ads(source_name='google_ads__everyday_banking_retail_deposit', table_name='google_ads_demand__everyday_banking_retail_deposit1')}}
UNION ALL
{{dash_table_general_process.google_ads(source_name='google_ads__everyday_banking_retail_deposit', table_name='google_ads_demand__everyday_banking_retail_deposit2')}}
UNION ALL
{{dash_table_general_process.cm360(source_name='cm360_transformed', table_name='cm360_direct_buy')}} WHERE campaign_name LIKE '%00005%'
UNION ALL
{{dash_table_general_process.ttd(source_name='ttd_transformed', table_name='ttd')}} WHERE campaign_name LIKE '%00005%'
),
with_channel AS (
SELECT * EXCEPT (publisher, channel),
dc.publisher,
dc.channel
FROM dash_table as dt JOIN `together-internal.channel.publisher_channel` as dc
ON lower(dt.publisher) = lower(dc.publisher) 
),
campaign_base AS (
       SELECT *,
              CASE WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_'))>=2 
              THEN SPLIT(campaign_name,'_')[1] 
              ELSE campaign_name
       END as campaign_name_raw
       FROM with_channel

),
campaign_name_selection_duplicate AS (
       SELECT COUNT(*) AS indicator,lower(campaign_name_raw) AS lower_campaign FROM (SELECT DISTINCT campaign_name_raw FROM campaign_base)
       GROUP BY LOWER(campaign_name_raw) HAVING COUNT(*)>1
),
duplicate_raw AS (
       SELECT distinct campaign_name_raw, ROW_NUMBER() OVER (PARTITION BY LOWER(campaign_name_raw) ORDER BY (campaign_name_raw)) as row_number from campaign_base cb join campaign_name_selection_duplicate cd
       ON LOWER(cb.campaign_name_raw) = LOWER(cd.lower_campaign) 
),
deduplicate_raw AS (
       select * from duplicate_raw where row_number = 1
)
SELECT camb.* EXCEPT(campaign_name_raw),
NULL AS campaign_id,
NULL AS device,
trim(CASE WHEN 
       lower(camb.campaign_name_raw) = lower(deduplicate_raw.campaign_name_raw) 
       
       THEN deduplicate_raw.campaign_name_raw
       ELSE camb.campaign_name_raw
END )AS campaign_name_selection,
CASE WHEN 
       EXISTS(SELECT 1 FROM UNNEST(SPLIT(creative_name,'_'))  as a
       WHERE lower(a) in UNNEST(ARRAY['aud','dg','disp','native','pdooh','rmdisp','social','vid','vidod','yt']))
       THEN  (SELECT X FROM UNNEST(SPLIT(creative_name,'_') ) as X WHERE lower(X) IN UNNEST(['aud','dg','disp','native','pdooh','rmdisp','social','vid','vidod','yt'])
       LIMIT 1)
       WHEN  EXISTS(SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_'))  as a
       WHERE lower(a) in UNNEST(ARRAY['aud','dg','disp','native','pdooh','rmdisp','social','vid','vidod','yt']))
       THEN  (SELECT X FROM UNNEST(SPLIT(campaign_name,'_') ) as X WHERE lower(X) IN UNNEST(['aud','dg','disp','native','pdooh','rmdisp','social','vid','vidod','yt'])
       LIMIT 1)
       WHEN EXISTS(SELECT 1 FROM UNNEST(SPLIT(creative_name,'_'))  as a
       WHERE lower(a) LIKE '%demand gen%')
       THEN 'DG'
       WHEN EXISTS(SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_'))  as a
       WHERE lower(a) LIKE '%demand gen%')
       THEN 'DG'
       else 'OTHER'
END as media_format,
CASE WHEN 
       EXISTS(SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_')) as a
       WHERE LOWER(a) IN UNNEST(ARRAY['consider','attract','choose']))
       THEN (SELECT X FROM UNNEST(SPLIT(campaign_name,'_') ) as X WHERE LOWER(X) IN UNNEST(['consider','attract','choose'])
       LIMIT 1)
       else 'OTHER'
END AS funnel,
CASE 
WHEN LOWER(campaign_name) like '%gi%' or lower(campaign_name) like '%general insurance%' 
or lower(campaign_name) like '%car%' or lower(campaign_name) like '%home%' or lower(campaign_name) like '%content%'
THEN 'General Insurance'
ELSE 'Wealth'
END AS sub_brands,


 FROM campaign_base camb LEFT JOIN deduplicate_raw ON LOWER(deduplicate_raw.campaign_name_raw) = LOWER(camb.campaign_name_raw)




