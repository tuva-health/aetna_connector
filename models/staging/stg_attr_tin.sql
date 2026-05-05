select distinct
    attr_tax_id
    , attr_tax_id_name
from {{ ref('stg_member') }}
where attr_tax_id is not null
qualify row_number() over (partition by attr_tax_id order by _run_time desc) = 1
