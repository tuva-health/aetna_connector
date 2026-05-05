select distinct
    attr_npi
    , attr_name
from {{ ref('stg_member') }}
where attr_npi is not null
qualify row_number() over (partition by attr_npi order by _run_time desc) = 1
