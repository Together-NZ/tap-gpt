{{ config(
    materialized='table',
    schema='pinterest_transformed__twhs',
    alias='pinterest__twhs',
) }}

WITH
{{ pinterest.reports(source_name='pinterest_raw__twhs', table_name='reports') }},
{{ pinterest.ad_groups(source_name='pinterest_raw__twhs', table_name='ad_groups') }},
{{ pinterest.campaigns(source_name='pinterest_raw__twhs', table_name='campaigns') }},
{{ pinterest.ads(source_name='pinterest_raw__twhs', table_name='ads') }},
{{ pinterest.final_calculation() }}
