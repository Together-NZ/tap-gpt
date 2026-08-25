{{ config(
    materialized='table',
) }}
{{cm360.cm360_direct_buy_no_dv360(source_name='cm360_raw__kiwibank', table_name='no_dv360_stream'
,lower_advertiser_name='kiwibank')}}