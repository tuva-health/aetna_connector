select 
    dw_member_id as member_id
    , dw_member_id as person_id
    , trim(race_ethnicity) as race_ethnicity
    , dual_elig
    , market
    , attr_npi
    , attr_tax_id
    , filename
    , efftv_dt
    , termn_dt
    , current_timestamp
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

