{{ config(
    materialized='table',
    schema='cm360_transformed__twh',
    alias='cm360_direct_buy__twh',
) }}

WITH cm360_data AS (
    {{ cm360.cm360_direct_buy(source_name='cm360_raw', table_name='cm360_report_stream', lower_advertiser_name='warehouse') }}
)

SELECT * FROM cm360_data
WHERE LOWER(advertiser) LIKE '%warehouse%'
  AND LOWER(advertiser) NOT LIKE '%stationery%'
