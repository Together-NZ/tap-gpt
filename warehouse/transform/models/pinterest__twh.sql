{{ config(
    materialized='table',
    schema='pinterest_transformed__twh',
    alias='pinterest__twh',
) }}

WITH
{{ pinterest.reports(source_name='pinterest_raw__twh', table_name='reports') }},
{{ pinterest.ad_groups(source_name='pinterest_raw__twh', table_name='ad_groups') }},
{{ pinterest.campaigns(source_name='pinterest_raw__twh', table_name='campaigns') }},
{{ pinterest.ads(source_name='pinterest_raw__twh', table_name='ads') }},
{{ pinterest.final_calculation() }}
