{{ config(
    materialized='table',
) }}
WITH dash_table AS (
SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail,
    CAST(0 AS INT64) AS video_completion,
    CAST(0 AS INT64) AS video_25_completion,
    CAST(0 AS INT64) AS video_50_completion,
    CAST(0 AS INT64) AS video_75_completion,
    CAST(0 AS INT64) AS video_views,
    campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, null as conversions
FROM `kiwibank-main.tiktok_transformed.tiktok`
UNION ALL
SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail,
    video_completion, video_25_completion, video_50_completion, video_75_completion, video_played AS video_views,
    campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, conversions
FROM `kiwibank-main.facebook_transformed.facebook`
UNION ALL
SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail,
    video_completion, video_25_completion, video_50_completion, video_75_completion, video_views,
    campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, null as conversions
FROM `kiwibank-main.linkedin_transformed.linkedin`
UNION ALL
SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail,
    video_completion, video_25_completion, video_50_completion, video_75_completion, video_views,
    campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, null as conversions
FROM `kiwibank-main.ttd_transformed.ttd`
UNION ALL
SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail,
    video_completion, video_25_completion, video_50_completion, video_75_completion, video_views,
    campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, null as conversions
FROM `kiwibank-main.dv360_transformed.dv360_standard`
UNION ALL
SELECT media_cost, impressions, clicks,
    ad_name AS creative_name,
    audience_name,
    ad_format AS ad_format,
    ad_format_detail AS ad_format_detail,
    CAST(0 AS INT64) AS video_completion,
    CAST(0 AS INT64) AS video_25_completion,
    CAST(0 AS INT64) AS video_50_completion,
    CAST(0 AS INT64) AS video_75_completion,
    CAST(0 AS INT64) AS video_views,
    campaign_name, publisher, campaign_descr,
    creative_descr,
    date, conversions
FROM `kiwibank-main.google_ads_search_transformed__everyday_banking_retail_deposit.google_ads_demand__everyday_banking_retail_deposit1`
UNION ALL
SELECT media_cost, impressions, clicks,
    ad_name AS creative_name,
    audience_name,
    ad_format AS ad_format,
    ad_format_detail AS ad_format_detail,
    CAST(0 AS INT64) AS video_completion,
    CAST(0 AS INT64) AS video_25_completion,
    CAST(0 AS INT64) AS video_50_completion,
    CAST(0 AS INT64) AS video_75_completion,
    CAST(0 AS INT64) AS video_views,
    campaign_name, publisher, campaign_descr,
    creative_descr,
    date, conversions
FROM `kiwibank-main.google_ads_search_transformed__everyday_banking_retail_deposit.google_ads_demand__everyday_banking_retail_deposit2`
UNION ALL
SELECT media_cost, impressions, clicks,
    ad_name AS creative_name,
    audience_name,
    ad_format AS ad_format,
    ad_format_detail AS ad_format_detail,
    CAST(0 AS INT64) AS video_completion,
    CAST(0 AS INT64) AS video_25_completion,
    CAST(0 AS INT64) AS video_50_completion,
    CAST(0 AS INT64) AS video_75_completion,
    CAST(0 AS INT64) AS video_views,
    campaign_name, publisher, campaign_descr,
    creative_descr,
    date, conversions
FROM `kiwibank-main.google_ads_search_transformed__credit_card.google_ads_demand__credit_card`
UNION ALL
SELECT media_cost, impressions, clicks,
    ad_name AS creative_name,
    audience_name,
    ad_format AS ad_format,
    ad_format_detail AS ad_format_detail,
    CAST(0 AS INT64) AS video_completion,
    CAST(0 AS INT64) AS video_25_completion,
    CAST(0 AS INT64) AS video_50_completion,
    CAST(0 AS INT64) AS video_75_completion,
    CAST(0 AS INT64) AS video_views,
    campaign_name, publisher, campaign_descr,
    creative_descr,
    date, conversions
FROM `kiwibank-main.google_ads_search_transformed__fraud_and_scams.google_ads_demand__fraud_and_scams`
UNION ALL
SELECT media_cost, impressions, clicks,
    ad_name AS creative_name,
    audience_name,
    ad_format AS ad_format,
    ad_format_detail AS ad_format_detail,
    CAST(0 AS INT64) AS video_completion,
    CAST(0 AS INT64) AS video_25_completion,
    CAST(0 AS INT64) AS video_50_completion,
    CAST(0 AS INT64) AS video_75_completion,
    CAST(0 AS INT64) AS video_views,
    campaign_name, publisher, campaign_descr,
    creative_descr,
    date, conversions
FROM `kiwibank-main.google_ads_search_transformed__everyday_banking_join_kiwibank.google_ads_demand__everyday_banking_join_kiwibank`
UNION ALL
SELECT media_cost, impressions, clicks,
    ad_name AS creative_name,
    audience_name,
    ad_format AS ad_format,
    ad_format_detail AS ad_format_detail,
    CAST(0 AS INT64) AS video_completion,
    CAST(0 AS INT64) AS video_25_completion,
    CAST(0 AS INT64) AS video_50_completion,
    CAST(0 AS INT64) AS video_75_completion,
    CAST(0 AS INT64) AS video_views,
    campaign_name, publisher, campaign_descr,
    creative_descr,
    date, conversions
FROM `kiwibank-main.google_ads_search_transformed__business_banking.google_ads_demand__business_banking`
UNION ALL

SELECT media_cost, impressions, clicks,
    ad_name AS creative_name,
    audience_name,
    ad_format AS ad_format,
    ad_format_detail AS ad_format_detail,
    CAST(0 AS INT64) AS video_completion,
    CAST(0 AS INT64) AS video_25_completion,
    CAST(0 AS INT64) AS video_50_completion,
    CAST(0 AS INT64) AS video_75_completion,
    CAST(0 AS INT64) AS video_views,
    campaign_name, publisher, campaign_descr,
    creative_descr,
    date, conversions
FROM `kiwibank-main.google_ads_search_transformed__home_loans.google_ads_demand__home_loans`
UNION ALL 
SELECT media_cost, impressions, clicks, creative_name, NULL AS audience_name, null ad_format, NULL AS ad_format_detail, 0 AS video_completion
,0 as video_25_completion,0 as video_50_completion,0 as video_75_completion, 0  as video_views,
campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,conversions,
platform FROM `kiwibank-main.gpt_transformed.gpt`
),
with_channel AS (
SELECT * EXCEPT (publisher, channel),
dc.publisher,
dc.channel
FROM dash_table as dt JOIN `together-internal.channel.publisher_channel` as dc
ON lower(dt.publisher) = lower(dc.publisher)
),
{{dash_table_general_process.dash_table_general_process()}}
