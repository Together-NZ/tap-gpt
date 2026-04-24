{{ config(
    materialized='table',
) }}
SELECT * FROM `kiwibank-main.ga4_transformed.ga4_goal_channel`
WHERE campaign_name NOT IN (
    select distinct campaign_name FROM `kiwibank-main.dash_table__ao_social_boosting.dash_union__ao_social_boosting`
    UNION ALL
    select distinct campaign_name FROM `kiwibank-main.dash_table__business_banking.dash_union__business_banking`  
    UNION ALL
    select distinct campaign_name FROM `kiwibank-main.dash_table__credit_card.dash_union__credit_card`  
    UNION ALL
    select distinct campaign_name FROM `kiwibank-main.dash_table__everyday_banking_join_kiwibank.dash_union__everyday_banking_join_kiwibank`  
    UNION ALL
    select distinct campaign_name FROM `kiwibank-main.dash_table__everyday_banking_retail_deposit.dash_union__everyday_banking_retail_deposit`  
    UNION ALL
    select distinct campaign_name FROM `kiwibank-main.dash_table__fraud_and_scams.dash_union__fraud_and_scams`  
    UNION ALL
    select distinct campaign_name FROM `kiwibank-main.dash_table__home_loan.dash_union__home_loan`  
)