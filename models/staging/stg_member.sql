select 
    dw_member_id as member_id
    , dw_member_id as person_id
    , trim(race_ethnicity) as race_ethnicity
    , date_id
    , dual_elig
    , lob
    , market
    , attr_npi
    , attr_tax_id
    , filename
    , efftv_dt
    , termn_dt
    , _run_time
    , case
        when length(efftv_dt) > 12
            then to_date(efftv_dt, 'yyyy-MM-dd HH:mm:ss')
        when efftv_dt like '____-__-__'
            then to_date(efftv_dt, 'yyyy-MM-dd')
        else to_date(efftv_dt, 'MM/dd/yyyy')
    end as member_effective_date
    , case
        when length(termn_dt) > 12
            then to_date(termn_dt, 'yyyy-MM-dd HH:mm:ss')
        when termn_dt like '____-__-__'
            then to_date(termn_dt, 'yyyy-MM-dd')
        else to_date(termn_dt, 'MM/dd/yyyy')
    end as member_term_date
from {{ source('aetna', 'member') }}
qualify row_number() over (partition by dw_member_id, lob order by date_id desc, _run_time desc) = 1

