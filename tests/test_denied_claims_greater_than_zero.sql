-- Ensure denied claims are never greater than 0. If they are, they need to be excluded from claims. Currently, no denied claims are greater than 0.
select 
    * 
from {{ ref('stg_medical_claim')}}
where clm_ln_status_cd = 'D'
    and paid_amt > 0