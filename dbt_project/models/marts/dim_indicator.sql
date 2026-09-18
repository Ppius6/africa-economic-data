select 
    indicator_code,
    indicator_name,
    unit,
    category
from {{ ref('stg_indicators') }}