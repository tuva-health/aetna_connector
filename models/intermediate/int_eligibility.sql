/*
Some unmapped source columns are commented out below.
*/
with source_data as (
    select
     enrollment_start_date
    , enrollment_end_date
    -- , individual_id
    , emp_src_member_id
    -- , msk_prefix_nm
    , member_last_nm
    , member_first_nm
    , member_suffix_nm
    , mbr_gender_cd
    , member_birth_dt
    , member_address_line_1_txt
    -- , member_address_line_2_txt
    , member_city_nm
    , member_state_postal_cd
    , member_county_cd
    , member_zip_cd
    , member_phone
    -- , src_member_id
    , member_id
    , mbr_rtp_type_cd
    , member_ssn_nbr
    -- , ps_unique_id
    -- , customer_nbr
    , group_nbr
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
    , file_id
    -- , plsp_prod_cd
    -- , product_ln_cd
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
    from {{ ref('stg_eligibility') }}
)

, mapped_data as (
    select
        member_id as person_id
        , member_id
        , case
            when mbr_gender_cd = 'M' then 'male'
            when mbr_gender_cd = 'F' then 'female'
            else 'unknown'
        end as gender
        , member_birth_dt as birth_date
        , enrollment_start_date
        , enrollment_end_date
        , 'Aetna' as payer
        , 'commercial' as payer_type
        , 'Aetna' as plan
        , group_nbr as group_id
        , member_first_nm as first_name
        , member_last_nm as last_name
        , emp_src_member_id as subscriber_id
        , mbr_rtp_type_cd as subscriber_relation
        , member_ssn_nbr as social_security_number
        , member_address_line_1_txt as address
        , member_city_nm as city
        , member_state_postal_cd as state
        , member_zip_cd as zip_code
        , case when member_phone = '' then null else member_phone end as phone
        , member_suffix_nm as name_suffix
        , file_id as file_name
        , 'Aetna' as data_source
    from source_data
)

, final as (
    select
        cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(member_id as {{ dbt.type_string() }}) as member_id
        , cast(subscriber_id as {{ dbt.type_string() }}) as subscriber_id
        , cast(gender as {{ dbt.type_string() }}) as gender
        , cast(null as {{ dbt.type_string() }}) as race
        , cast(birth_date as date) as birth_date
        , cast(null as date) as death_date
        , cast(null as integer) as death_flag
        , cast(enrollment_start_date as date) as enrollment_start_date
        , cast(enrollment_end_date as date) as enrollment_end_date
        , cast(payer as {{ dbt.type_string() }}) as payer
        , cast(payer_type as {{ dbt.type_string() }}) as payer_type
        , cast(plan as {{ dbt.type_string() }}) as plan
        , cast(null as {{ dbt.type_string() }}) as original_reason_entitlement_code
        , cast(null as {{ dbt.type_string() }}) as dual_status_code
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
        , cast(null as {{ dbt.type_string() }}) as ethnicity
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(file_name as {{ dbt.type_string() }}) as file_name
        , cast(null as date) as file_date
        , cast(null as datetime) as ingest_datetime
    from mapped_data
)

select * from final
