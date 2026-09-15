{{ config(
    materialized='table',
    schema='gpt_transformed__noel_leeming',
    alias='gpt__noel_leeming',
) }}

WITH
{{ gpt.campaigns(source_name='gpt_raw__noel_leeming', table_name='campaigns') }},
{{ gpt.ad_groups(source_name='gpt_raw__noel_leeming', table_name='ad_groups') }},
{{ gpt.ads(source_name='gpt_raw__noel_leeming', table_name='ads') }},
{{ gpt.joint_meta_data() }},
{{ gpt.insights(source_name='gpt_raw__noel_leeming', table_name='ad_insights') }},
{{ gpt.conversions(source_name='gpt_raw__noel_leeming', table_name='ad_conversions') }},
{{ gpt.final() }}
