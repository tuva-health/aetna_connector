select distinct
      to_date(eff_mm, 'yyyyMM') as enrollment_start_date
    , last_day(cast(to_date(eff_mm, 'yyyyMM') as date)) as enrollment_end_date
    -- , individual_id
    , cast(src_member_id as {{ dbt.type_string() }}) as emp_src_member_id
    -- , msk_prefix_nm
    , cast(mem_last_nm  as {{ dbt.type_string() }}) as member_last_nm
    , cast(mem_first_nm as {{ dbt.type_string() }}) as member_first_nm
    , cast(suffix_nm as {{ dbt.type_string() }}) as member_suffix_nm
    , cast(mbr_gender as {{ dbt.type_string() }}) as mbr_gender_cd
    , cast(mem_birth_dt as date) as member_birth_dt
    , cast(mem_address_line_1 as {{ dbt.type_string() }}) as member_address_line_1_txt
    , cast(mem_address_line_2 as {{ dbt.type_string() }}) as member_address_line_2_txt
    , cast(mem_city_nm as {{ dbt.type_string() }}) as member_city_nm
    , cast(mem_state_postal_cd as {{ dbt.type_string() }}) as member_state_postal_cd
    , cast(mem_county_cd as {{ dbt.type_string() }}) as member_county_cd
    , cast(mem_zip_cd as {{ dbt.type_string() }}) as member_zip_cd
    , cast(mem_phone_num as {{ dbt.type_string() }}) as member_phone
    -- , src_member_id
    , cast(member_id as {{ dbt.type_string() }}) as member_id
    , cast(mbr_rtp_type_cd as {{ dbt.type_string() }}) as mbr_rtp_type_cd
    , cast(nullif(mem_ssn_nbr,'********') as {{ dbt.type_string() }}) as member_ssn_nbr
    -- , ps_unique_id
    -- , customer_nbr
    , cast(group_nbr as {{ dbt.type_string() }}) as group_nbr
    , cast(business_in_cd as {{ dbt.type_string() }}) as business_ln_cd
    , product_ln_cd
    -- , subgroup_nbr
    -- , account_nbr
    -- , subscriber_last_nm
    -- , subscriber_first_nm
    -- , subscriber_gender_cd
    -- , subscriber_brth_dt
    -- , subs_zip_cd
    -- , subs_st_postal_cd
    -- , county_cd
    -- , subscriber_ssn_nbr
    , filename as file_id
    , filename as file_name
    , date_id as file_date
    , _run_time as ingest_datetime
    -- extension columns
    , market as x_market
    , 'aetna' || '|' || market || '|' || lob as data_source
    , 'aetna' as payer
    , case
        when filename like '%IPAOFNY_ME_AETA%' then 'MA'
        when filename like '%IPANYCIA_AETA%' or business_in_cd in ('CP', 'E3') then 'COMM'
        when business_in_cd in ('D1','D2','E2') then 'MCD'
        when business_in_cd = 'ME' then 'MCR'
        when business_in_cd = 'U' then 'UNKNOWN'
    end as payer_type
    , cast(attr_npi_nbr as string) as attr_npi_nbr
    , cast(attr_tax_id_nbr as string) as attr_tax_id_nbr
from {{ source('aetna', 'enrollment') }} as enroll
where attr_org_cd is not null
