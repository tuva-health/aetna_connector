select * from {{ ref('int_medical_claim_adr') }}
qualify row_number() over (
    partition by claim_id, claim_line_number
    order by paid_date desc
) = 1
