{{ config(
    materialized='table',
) }}


SELECT * FROM `kiwibank-main.ga4_transformed.ga4_goal_channel_session` WHERE campaign_name IN (
    SELECT DISTINCT campaign_name FROM `kiwibank-main.dash_table__ao_social_boosting.dash_union__ao_social_boosting`
    UNION ALL
    SELECT DISTINCT campaign_name FROM `kiwibank-main.dash_table__business_banking.dash_union__business_banking`
    UNION ALL
    SELECT DISTINCT campaign_name FROM `kiwibank-main.dash_table__credit_card.dash_union__credit_card`
    UNION ALL
    SELECT DISTINCT campaign_name FROM `kiwibank-main.dash_table__everyday_banking_join_kiwibank.dash_union__everyday_banking_join_kiwibank`
    UNION ALL
    SELECT DISTINCT campaign_name FROM `kiwibank-main.dash_table__everyday_banking_retail_deposit.dash_union__everyday_banking_retail_deposit`
    UNION ALL
    SELECT DISTINCT campaign_name FROM `kiwibank-main.dash_table__fraud_and_scams.dash_union__fraud_and_scams`
    UNION ALL
    SELECT DISTINCT campaign_name FROM `kiwibank-main.dash_table__home_loans.dash_union__home_loans`
)