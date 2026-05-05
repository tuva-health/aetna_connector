select 
    *
    , data_source || '.' || member_id as person_id
from {{ ref('stg_pharmacy_claim') }}