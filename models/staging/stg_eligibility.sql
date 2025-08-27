select
     cast(enrollment_start_date as date) as enrollment_start_date
    , last_day(cast(enrollment_start_date as date)) as enrollment_end_date
    , cast(individual_id as {{ dbt.type_string() }}) as individual_id
    , cast(emp_src_member_id as {{ dbt.type_string() }}) as emp_src_member_id
    , cast(trim(msk_prefix_nm) as {{ dbt.type_string() }}) as msk_prefix_nm
    , cast(trim(member_last_nm) as {{ dbt.type_string() }}) as member_last_nm
    , cast(trim(member_first_nm) as {{ dbt.type_string() }}) as member_first_nm
    , cast(trim(member_suffix_nm) as {{ dbt.type_string() }}) as member_suffix_nm
    , cast(mbr_gender_cd as {{ dbt.type_string() }}) as mbr_gender_cd
    , cast(member_birth_dt as date) as member_birth_dt
    , cast(trim(member_address_line_1_txt) as {{ dbt.type_string() }}) as member_address_line_1_txt
    , cast(trim(member_address_line_2_txt) as {{ dbt.type_string() }}) as member_address_line_2_txt
    , cast(trim(member_city_nm) as {{ dbt.type_string() }}) as member_city_nm
    , cast(member_state_postal_cd as {{ dbt.type_string() }}) as member_state_postal_cd
    , cast(member_county_cd as {{ dbt.type_string() }}) as member_county_cd
    , cast(member_zip_cd as {{ dbt.type_string() }}) as member_zip_cd
    , cast(member_phone as {{ dbt.type_string() }}) as member_phone
    , cast(src_member_id as {{ dbt.type_string() }}) as src_member_id
    , cast(member_id as {{ dbt.type_string() }}) as member_id
    , cast(mbr_rtp_type_cd as {{ dbt.type_string() }}) as mbr_rtp_type_cd
    , cast(member_ssn_nbr as {{ dbt.type_string() }}) as member_ssn_nbr
    , cast(ps_unique_id as {{ dbt.type_string() }}) as ps_unique_id
    , cast(customer_nbr as {{ dbt.type_string() }}) as customer_nbr
    , cast(group_nbr as {{ dbt.type_string() }}) as group_nbr
    , cast(subgroup_nbr as {{ dbt.type_string() }}) as subgroup_nbr
    , cast(account_nbr as {{ dbt.type_string() }}) as account_nbr
    , cast(subscriber_last_nm as {{ dbt.type_string() }}) as subscriber_last_nm
    , cast(subscriber_first_nm as {{ dbt.type_string() }}) as subscriber_first_nm
    , cast(subscriber_gender_cd as {{ dbt.type_string() }}) as subscriber_gender_cd
    , cast(subscriber_brth_dt as {{ dbt.type_string() }}) as subscriber_brth_dt
    , cast(subs_zip_cd as {{ dbt.type_string() }}) as subs_zip_cd
    , cast(subs_st_postal_cd as {{ dbt.type_string() }}) as subs_st_postal_cd
    , cast(subscriber_county_cd as {{ dbt.type_string() }}) as subscriber_county_cd
    , cast(subscriber_ssn_nbr as {{ dbt.type_string() }}) as subscriber_ssn_nbr
    , cast(file_id as {{ dbt.type_string() }}) as file_id
    , cast(plsp_prod_cd as {{ dbt.type_string() }}) as plsp_prod_cd
    , cast(product_ln_cd as {{ dbt.type_string() }}) as product_ln_cd
    , cast(business_ln_cd as {{ dbt.type_string() }}) as business_ln_cd
    , cast(fund_ctg_cd as {{ dbt.type_string() }}) as fund_ctg_cd
    , cast(coverage_type_cd as {{ dbt.type_string() }}) as coverage_type_cd
    , cast(ntwk_srv_area_id as {{ dbt.type_string() }}) as ntwk_srv_area_id
    , cast(cfo_cd as {{ dbt.type_string() }}) as cfo_cd
    , cast(cust_segment_cd as {{ dbt.type_string() }}) as cust_segment_cd
    , cast(cust_subseg_cd as {{ dbt.type_string() }}) as cust_subseg_cd
    , cast(medical_ind as {{ dbt.type_string() }}) as medical_ind -- filler column
    , cast(drug_ind as {{ dbt.type_string() }}) as drug_ind
    , cast(sbstnc_abuse_ind as {{ dbt.type_string() }}) as sbstnc_abuse_ind
    , cast(dental_ind as {{ dbt.type_string() }}) as dental_ind -- filler column
    , cast(mental_health_ind as {{ dbt.type_string() }}) as mental_health_ind
    , cast(orig_covg_eff_dt as date) as orig_covg_eff_dt
    , cast(covg_cncl_dt as date) as covg_cncl_dt
    , cast(pcp_tax_id_nbr as {{ dbt.type_string() }}) as pcp_tax_id_nbr
    , cast(pcp_prvdr_id as {{ dbt.type_string() }}) as pcp_prvdr_id
    , cast(pcp_print_nm as {{ dbt.type_string() }}) as pcp_print_nm
    , cast(pcp_address_line_1_txt as {{ dbt.type_string() }}) as pcp_address_line_1_txt
    , cast(pcp_address_line_2_txt as {{ dbt.type_string() }}) as pcp_address_line_2_txt
    , cast(pcp_city_nm as {{ dbt.type_string() }}) as pcp_city_nm
    , cast(pcp_state_postal_cd as {{ dbt.type_string() }}) as pcp_state_postal_cd
    , cast(pcp_zip_cd as {{ dbt.type_string() }}) as pcp_zip_cd
    , cast(pcp_npi_no as {{ dbt.type_string() }}) as pcp_npi_no
    , cast(specialty_ctg_cd as {{ dbt.type_string() }}) as specialty_ctg_cd
    , cast(xchng_id as {{ dbt.type_string() }}) as xchng_id
    /*
    not applicable
    end of record marker
    */
from {{ source('aetna', 'aetna_eligibility_raw') }}
