/*
Some unmapped source columns are commented out below.
*/
with source_data as (
    select
     elig.enrollment_start_date
    , elig.enrollment_end_date
    -- , individual_id
    , elig.emp_src_member_id
    -- , msk_prefix_nm
    , elig.member_last_nm
    , elig.member_first_nm
    , elig.member_suffix_nm
    , elig.mbr_gender_cd
    , elig.member_birth_dt
    , elig.member_address_line_1_txt
    , elig.member_address_line_2_txt
    , elig.member_city_nm
    , elig.member_state_postal_cd
    , elig.member_county_cd
    , elig.member_zip_cd
    , elig.member_phone
    -- , src_member_id
    , elig.member_id
    , elig.mbr_rtp_type_cd
    , elig.member_ssn_nbr
    -- , ps_unique_id
    -- , customer_nbr
    , elig.group_nbr
    , elig.business_ln_cd
    , case
        when elig.file_id like '%IPAOFNY_ME_AETA%' then 'MA'
        when elig.file_id like '%IPANYCIA_AETA%' or elig.business_ln_cd in ('CP', 'E3') then 'COMM'
        when elig.business_ln_cd in ('D1','D2','E2') then 'MCD'
        when elig.business_ln_cd = 'ME' then 'MCR'
        when elig.business_ln_cd = 'U' then 'UNKNOWN'
    end as payer_type
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
    , elig.file_id
    , elig.file_date
    , elig.ingest_datetime
    -- , plsp_prod_cd
    , lkup.code_desc as plan
    -- , business_ln_cd
    -- , fund_ctg_cd
    -- , coverage_type_cd
    -- , ntwk_srv_area_id
    -- , cfo_cd
    -- , cust_segment_cd
    -- , cust_subseg_cd
    -- , medical_ind -- filler column
    -- , drug_ind
    -- , sbstnc_abuse_ind
    -- , dental_ind -- filler column
    -- , mental_health_ind
    -- , orig_covg_eff_dt
    -- , covg_cncl_dt
    -- , pcp_tax_id_nbr
    -- , pcp_prvdr_id
    -- , pcp_print_nm
    -- , pcp_address_line_1_txt
    -- , pcp_address_line_2_txt
    -- , pcp_city_nm
    -- , pcp_state_postal_cd
    -- , pcp_zip_cd
    -- , pcp_npi_no
    -- , specialty_ctg_cd
    -- , xchng_id
    , data_source
    , payer
    , elig.attr_npi_nbr
    , elig.attr_tax_id_nbr
    from {{ ref('stg_eligibility') }} as elig
    left join {{ ref('aetna_codes')}} as lkup
        on elig.product_ln_cd = lkup.code_value
        and code = 'PRODUCT_LN_CD'
)

, mapped_data as (
    select
        sd.data_source || '.' || sd.member_id as person_id
        , sd.member_id
        , case
            when sd.mbr_gender_cd = 'M' then 'male'
            when sd.mbr_gender_cd = 'F' then 'female'
            else 'unknown'
        end as gender
        , sd.member_birth_dt as birth_date
        , sd.enrollment_start_date
        , sd.enrollment_end_date
        , payer
        , sd.payer_type
        , sd.plan
        , case when mem.dual_elig like '%DUAL%' then 8 end as dual_status_code
        , sd.group_nbr as group_id
        , sd.member_first_nm as first_name
        , sd.member_last_nm as last_name
        , sd.emp_src_member_id as subscriber_id
        , sd.mbr_rtp_type_cd as subscriber_relation
        , nullif(sd.member_ssn_nbr, '********') as social_security_number
        , sd.member_address_line_1_txt || ' ' || coalesce(sd.member_address_line_2_txt, '') as address
        , sd.member_city_nm as city
        , sd.member_state_postal_cd as state
        , sd.member_zip_cd as zip_code
        , case when sd.member_phone = '' then null else sd.member_phone end as phone
        , sd.member_suffix_nm as name_suffix
        , stg_race.race
        , stg_race.ethnicity
        , sd.file_id as file_name
        , sd.file_date
        , sd.ingest_datetime
        , data_source    
        , sd.attr_npi_nbr
        , sd.attr_tax_id_nbr
        , rank() over (partition by sd.payer_type order by sd.file_date desc, sd.ingest_datetime desc) as record_rank
    from source_data as sd
    left join {{ ref('stg_race')}} as stg_race
        on sd.member_id = stg_race.person_id
    left join {{ ref('stg_member') }} as mem
        on mem.person_id = sd.member_id
        and mem.member_effective_date >= sd.enrollment_start_date
        and mem.member_term_date <= sd.enrollment_end_date
)

, final as (
    select
        cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(member_id as {{ dbt.type_string() }}) as member_id
        , cast(subscriber_id as {{ dbt.type_string() }}) as subscriber_id
        , cast(gender as {{ dbt.type_string() }}) as gender
        , cast(race as {{ dbt.type_string() }}) as race
        , cast(ethnicity as {{ dbt.type_string() }}) as ethnicity
        , cast(birth_date as date) as birth_date
        , cast(null as date) as death_date
        , cast(null as integer) as death_flag
        , cast(enrollment_start_date as date) as enrollment_start_date
        , cast(enrollment_end_date as date) as enrollment_end_date
        , cast(payer as {{ dbt.type_string() }}) as payer
        , cast(payer_type as {{ dbt.type_string() }}) as payer_type
        , cast(plan as {{ dbt.type_string() }}) as plan
        , cast(null as {{ dbt.type_string() }}) as original_reason_entitlement_code
        , cast(dual_status_code as {{ dbt.type_string() }}) as dual_status_code
        , cast(null as {{ dbt.type_string() }}) as medicare_status_code
        , cast(group_id as {{ dbt.type_string() }}) as group_id
        , cast(null as {{ dbt.type_string() }}) as group_name
        , cast(first_name as {{ dbt.type_string() }}) as first_name
        , cast(last_name as {{ dbt.type_string() }}) as last_name
        , cast(social_security_number as {{ dbt.type_string() }}) as social_security_number
        , cast(subscriber_relation as {{ dbt.type_string() }}) as subscriber_relation
        , cast(address as {{ dbt.type_string() }}) as address
        , cast(city as {{ dbt.type_string() }}) as city
        , cast(state as {{ dbt.type_string() }}) as state
        , cast(zip_code as {{ dbt.type_string() }}) as zip_code
        , cast(phone as {{ dbt.type_string() }}) as phone
        , cast(name_suffix as {{ dbt.type_string() }}) as name_suffix
        , cast(null as {{ dbt.type_string() }}) as middle_name
        , cast(null as {{ dbt.type_string() }}) as email
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(file_name as {{ dbt.type_string() }}) as file_name
        , file_date
        , ingest_datetime
        , case when len(attr_npi_nbr) < 10 then null else attr_npi_nbr end as attr_npi_nbr
        , case when len(attr_tax_id_nbr) < 9 then null else attr_tax_id_nbr end as attr_tax_id_nbr
    from mapped_data
    where record_rank = 1
)

select * from final
