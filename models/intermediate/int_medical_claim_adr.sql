with source_data as (
select
      ps_unique_id
    , customer_nbr
    , group_nbr
    , filler_1
    , subgroup_nbr
    , account_nbr
    , file_id
    , clm_ln_type_cd
    , non_prfrrd_srv_cd
    , plsp_prod_cd
    , product_ln_cd
    , classification_cd
    , bnft_pkg_id
    , plan_id
    , benefit_tier
    , fund_ctg_cd
    , src_subscriber_id
    , subscriber_last_nm
    , subscriber_first_nm
    , subscriber_gender_cd
    , subscriber_brth_dt
    , subs_zip_cd
    , subs_st_postal_cd
    , coverage_type_cd
    , subscriber_ssn_nbr
    , member_id
    , member_number
    , member_last_nm
    , member_first_nm
    , mbr_gender_cd
    , mbr_rtp_type_cd
    , member_birth_dt
    , src_clm_id
    , acas_gen_seq_nbr
    , prev_clm_seg_id
    , derived_tcn_nbr
    , src_claim_line_id
    , claim_line_id
    , ntwk_srv_area_id
    , paid_prvdr_nsa_id
    , srv_capacity_cd
    , pcp_tax_id_format_cd
    , pcp_tax_id_nbr
    , pcp_nm
    , srv_prvdr_tax_id_format_cd
    , srv_prvdr_tax_id_nbr
    , srv_prvdr_id
    , srv_prvdr_nm
    , srv_prvdr_address_line_1_txt
    , srv_prvdr_address_line_2_txt
    , srv_prvdr_city_nm
    , srv_prvdr_state_postal_cd
    , srv_prvdr_zip_cd
    , srv_prvdr_provider_type_cd
    , srv_prvdr_specialty_cd
    , payee_cd
    , paid_prvdr_par_cd
    , received_dt
    , adjn_dt
    , srv_start_dt
    , srv_stop_dt
    , date_processed
    , filler_2
    , filler_3
    , filler_4
    , mdc_cd
    , drg_cd
    , prcdr_cd
    , prcdr_modifier_cd
    , prcdr_type_cd
    , filler_5
    , filler_6
    , filler_7
    , type_srv_cd
    , benefit_cd
    , tooth_1_nbr
    , plc_srv_cd
    , dschrg_status_cd
    , revenue_cd
    , hcfa_bill_type_cd
    , unit_cnt
    , src_unit_cnt
    , src_billed_amt
    , billed_amt
    , not_covered_amt_1
    , not_covered_amt_2
    , not_covered_amt_3
    , clm_ln_msg_cd_1
    , clm_ln_msg_cd_2
    , clm_ln_msg_cd_3
    , covered_amt
    , allowed_amt
    , filler_8
    , srv_copay_amt
    , src_srv_copay_amt
    , deductible_amt
    , coinsurance_amt
    , src_coins_amt
    , bnft_payable_amt
    , paid_amt
    , cob_paid_amt
    , ahf_bfd_amt
    , ahf_paid_amt
    , negot_savings_amt
    , r_c_savings_amt
    , cob_savings_amt
    , src_cob_svngs_amt
    , pri_payer_cvg_cd
    , cob_type_cd
    , cob_cd
    , prcdr_cd_ndc
    , src_clm_mbr_id
    , clm_ln_status_cd
    , src_member_id
    , reversal_cd
    , admit_cnt
    , admin_savings_amt
    , adj_prvdr_dsgnn_cd
    , aex_plan_dsgntn_cd
    , benefit_tier_cd
    , aex_prvdr_spctg_cd
    , prod_distnctn_cd
    , billed_eligible_amt
    , srv_provider_class_cd
    , poa_cd_1
    , poa_cd_2
    , poa_cd_3
    , filler_9
    , filler_10
    , filler_11
    , pricing_mthd_cd
    , type_class_cd
    , specialty_ctg_cd
    , srv_prvdr_npi
    , ttl_ded_met_ind
    , ttl_interest_amt
    , ttl_surcharge_amt
    , filler_12
    , hcfa_plc_srv_cd
    , hcfa_admit_src_cd
    , hcfa_admit_type_cd
    , src_admit_dt
    , src_discharge_dt
    , prcdr_modifier_cd_2
    , prcdr_modifier_cd_3
    , poa_cd_4
    , poa_cd_5
    , poa_cd_6
    , poa_cd_7
    , poa_cd_8
    , poa_cd_9
    , poa_cd_10
    , pri_icd9_dx_cd
    , icd9_dx_cd_2
    , icd9_dx_cd_3
    , icd9_dx_cd_4
    , icd9_dx_cd_5
    , icd9_dx_cd_6
    , icd9_dx_cd_7
    , icd9_dx_cd_8
    , icd9_dx_cd_9
    , icd9_dx_cd_10
    , icd9_prcdr_cd_1
    , icd9_prcdr_cd_2
    , icd9_prcdr_cd_3
    , icd9_prcdr_cd_4
    , icd9_prcdr_cd_5
    , icd9_prcdr_cd_6
    , ahf_det_order_cd
    , ahf_mbr_coins_amt
    , ahf_mbr_copay_amt
    , ahf_mbr_ded_amt
    , filler_13
    , filler_14
    , icd_10_ind
    , xchng_id
    , filler_15
    , row_number() over (
        partition by
            src_clm_id
            , src_claim_line_id
        order by date_processed desc
    ) as row_num
from {{ ref('stg_medical_claim') }}
)

, source_data_deduped as (
    select *
    from source_data
    where row_num = 1
)

, mapped_data as (
    select
        left(src_clm_id, 9) as claim_id
        , src_claim_line_id as claim_line_number
        , clm_ln_status_cd as claim_line_status_code
        , member_id
        , member_id as person_id
        , srv_start_dt as claim_line_start_date
        , srv_stop_dt as claim_line_end_date
        , srv_start_dt as claim_start_date
        , srv_stop_dt as claim_end_date
        , date_processed as paid_date
        , src_admit_dt as admission_date
        , src_discharge_dt as discharge_date
        , hcfa_admit_src_cd as admit_source_code
        , hcfa_admit_type_cd as admit_type_code
        , dschrg_status_cd as discharge_disposition_code
        , revenue_cd as revenue_center_code
        , unit_cnt as service_unit_quantity
        , hcfa_bill_type_cd as bill_type_code
        , hcfa_plc_srv_cd as place_of_service_code
        , null as drg_code_type
        , drg_cd as drg_code
        , prcdr_cd as hcpcs_code
        , prcdr_modifier_cd as hcpcs_modifier_1
        , prcdr_modifier_cd_2 as hcpcs_modifier_2
        , prcdr_modifier_cd_3 as hcpcs_modifier_3
        , srv_prvdr_npi as rendering_npi
        , srv_prvdr_tax_id_nbr as rendering_tin
        , paid_amt as paid_amount
        , allowed_amt as allowed_amount
        , srv_copay_amt as copayment_amount
        , deductible_amt as deductible_amount
        , coinsurance_amt as coinsurance_amount
        , billed_amt as charge_amount
        , case when icd_10_ind = 'Y' then 'icd-10-cm'
               when icd_10_ind = 'N' then 'icd-9-cm'
            end as diagnosis_code_type
        , pri_icd9_dx_cd as diagnosis_code_1
        , icd9_dx_cd_2 as diagnosis_code_2
        , icd9_dx_cd_3 as diagnosis_code_3
        , icd9_dx_cd_4 as diagnosis_code_4
        , icd9_dx_cd_5 as diagnosis_code_5
        , icd9_dx_cd_6 as diagnosis_code_6
        , icd9_dx_cd_7 as diagnosis_code_7
        , icd9_dx_cd_8 as diagnosis_code_8
        , icd9_dx_cd_9 as diagnosis_code_9
        , icd9_dx_cd_10 as diagnosis_code_10
        , poa_cd_1 as diagnosis_poa_1
        , poa_cd_2 as diagnosis_poa_2
        , poa_cd_3 as diagnosis_poa_3
        , poa_cd_4 as diagnosis_poa_4
        , poa_cd_5 as diagnosis_poa_5
        , poa_cd_6 as diagnosis_poa_6
        , poa_cd_7 as diagnosis_poa_7
        , poa_cd_8 as diagnosis_poa_8
        , poa_cd_9 as diagnosis_poa_9
        , poa_cd_10 as diagnosis_poa_10
        , case when icd_10_ind = 'Y' then 'icd-10-pcs'
               when icd_10_ind = 'N' then 'icd-9-pcs'
            end as procedure_code_type
        , icd9_prcdr_cd_1 as procedure_code_1
        , icd9_prcdr_cd_2 as procedure_code_2
        , icd9_prcdr_cd_3 as procedure_code_3
        , icd9_prcdr_cd_4 as procedure_code_4
        , icd9_prcdr_cd_5 as procedure_code_5
        , icd9_prcdr_cd_6 as procedure_code_6
        , 'Aetna' as plan
        , 'Aetna' as data_source
        , 'Aetna' as payer
    from source_data
)

, claim_line_totals as (
    select
        claim_id
        , claim_line_number
        , sum(paid_amount) as sum_paid_amount
        , sum(allowed_amount) as sum_allowed_amount
        , sum(coinsurance_amount) as sum_coinsurance_amount
        , sum(copayment_amount) as sum_copayment_amount
        , sum(deductible_amount) as sum_deductible_amount
        , sum(service_unit_quantity) as sum_service_unit_quantity
        , sum(charge_amount) as sum_charge_amount
    from mapped_data
    group by claim_id, claim_line_number
)

, claim_types as (
      select
    claim_id
    , max(
        bill_type_code is not null
        or drg_code is not null
        or admit_type_code is not null
        or admit_source_code is not null
        or discharge_disposition_code is not null
        or revenue_center_code is not null
    ) as is_institutional
    , max(
        bill_type_code is null
        and drg_code is null
        and admit_type_code is null
        and admit_source_code is null
        and discharge_disposition_code is null
        and revenue_center_code is null
        and place_of_service_code is not null
    ) as is_professional
  from mapped_data
  group by claim_id
)

, rejections as (
    select
        claim_id
        /*
        Records with a "D" in claim line status code are voided and reprocessed
        under a new claim ID. Here we identify records that should be voided.
        */
        , max(claim_line_status_code = 'D') as has_denied_status
    from mapped_data
    group by claim_id
)

, claims_to_exclude as (
    select distinct claim_id
    from claim_line_totals
    where (sum_allowed_amount < 0 or sum_paid_amount < 0 or sum_service_unit_quantity < 0)
)

, final as (
select
      cast(md.claim_id as {{ dbt.type_string() }}) as claim_id
    , cast(md.claim_line_number as integer) as claim_line_number
    , cast(case when is_institutional then 'institutional'
                when is_professional and not is_institutional then 'professional'
                when not is_professional and not is_institutional then 'undetermined'
            end as {{ dbt.type_string() }}) as claim_type
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(person_id as {{ dbt.type_string() }}) as member_id
    , cast(payer as {{ dbt.type_string() }}) as payer
    , cast(plan as {{ dbt.type_string() }}) as plan
    , cast(claim_start_date as date) as claim_start_date
    , cast(claim_end_date as date) as claim_end_date
    , cast(claim_line_start_date as date) as claim_line_start_date
    , cast(claim_line_end_date as date) as claim_line_end_date
    , cast(admission_date as date) as admission_date
    , cast(discharge_date as date) as discharge_date
    , cast(admit_source_code as {{ dbt.type_string() }}) as admit_source_code
    , cast(admit_type_code as {{ dbt.type_string() }}) as admit_type_code
    , cast(discharge_disposition_code as {{ dbt.type_string() }}) as discharge_disposition_code
    , cast(place_of_service_code as {{ dbt.type_string() }}) as place_of_service_code
    , cast(bill_type_code as {{ dbt.type_string() }}) as bill_type_code
    , cast(drg_code_type as {{ dbt.type_string() }}) as drg_code_type
    , cast(drg_code as {{ dbt.type_string() }}) as drg_code
    , cast(revenue_center_code as {{ dbt.type_string() }}) as revenue_center_code
    , cast(service_unit_quantity as integer) as service_unit_quantity
    , cast(hcpcs_code as {{ dbt.type_string() }}) as hcpcs_code
    , cast(hcpcs_modifier_1 as {{ dbt.type_string() }}) as hcpcs_modifier_1
    , cast(hcpcs_modifier_2 as {{ dbt.type_string() }}) as hcpcs_modifier_2
    , cast(hcpcs_modifier_3 as {{ dbt.type_string() }}) as hcpcs_modifier_3
    , cast(null as {{ dbt.type_string() }}) as hcpcs_modifier_4
    , cast(null as {{ dbt.type_string() }}) as hcpcs_modifier_5
    , cast(rendering_npi as {{ dbt.type_string() }}) as rendering_npi
    , cast(rendering_tin as {{ dbt.type_string() }}) as rendering_tin
    , cast(null as {{ dbt.type_string() }}) as billing_npi
    , cast(null as {{ dbt.type_string() }}) as billing_tin
    , cast(null as {{ dbt.type_string() }}) as facility_npi
    , cast(paid_date as date) as paid_date
    , cast(clt.sum_paid_amount as numeric(38, 2)) as paid_amount
    , cast(clt.sum_allowed_amount as numeric(38, 2)) as allowed_amount
    , cast(clt.sum_charge_amount as numeric(38, 2)) as charge_amount
    , cast(clt.sum_coinsurance_amount as numeric(38, 2)) as coinsurance_amount
    , cast(clt.sum_copayment_amount as numeric(38, 2)) as copayment_amount
    , cast(clt.sum_deductible_amount as numeric(38, 2)) as deductible_amount
    , cast(clt.sum_paid_amount + clt.sum_coinsurance_amount + clt.sum_copayment_amount + clt.sum_deductible_amount
        as numeric(38, 2)) as total_cost_amount
    , cast(diagnosis_code_type as {{ dbt.type_string() }}) as diagnosis_code_type
    , cast(diagnosis_code_1 as {{ dbt.type_string() }}) as diagnosis_code_1
    , cast(diagnosis_code_2 as {{ dbt.type_string() }}) as diagnosis_code_2
    , cast(diagnosis_code_3 as {{ dbt.type_string() }}) as diagnosis_code_3
    , cast(diagnosis_code_4 as {{ dbt.type_string() }}) as diagnosis_code_4
    , cast(diagnosis_code_5 as {{ dbt.type_string() }}) as diagnosis_code_5
    , cast(diagnosis_code_6 as {{ dbt.type_string() }}) as diagnosis_code_6
    , cast(diagnosis_code_7 as {{ dbt.type_string() }}) as diagnosis_code_7
    , cast(diagnosis_code_8 as {{ dbt.type_string() }}) as diagnosis_code_8
    , cast(diagnosis_code_9 as {{ dbt.type_string() }}) as diagnosis_code_9
    , cast(diagnosis_code_10 as {{ dbt.type_string() }}) as diagnosis_code_10
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_11
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_12
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_13
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_14
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_15
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_16
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_17
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_18
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_19
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_20
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_21
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_22
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_23
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_24
    , cast(null as {{ dbt.type_string() }}) as diagnosis_code_25
    , cast(diagnosis_poa_1 as {{ dbt.type_string() }}) as diagnosis_poa_1
    , cast(diagnosis_poa_2 as {{ dbt.type_string() }}) as diagnosis_poa_2
    , cast(diagnosis_poa_3 as {{ dbt.type_string() }}) as diagnosis_poa_3
    , cast(diagnosis_poa_4 as {{ dbt.type_string() }}) as diagnosis_poa_4
    , cast(diagnosis_poa_5 as {{ dbt.type_string() }}) as diagnosis_poa_5
    , cast(diagnosis_poa_6 as {{ dbt.type_string() }}) as diagnosis_poa_6
    , cast(diagnosis_poa_7 as {{ dbt.type_string() }}) as diagnosis_poa_7
    , cast(diagnosis_poa_8 as {{ dbt.type_string() }}) as diagnosis_poa_8
    , cast(diagnosis_poa_9 as {{ dbt.type_string() }}) as diagnosis_poa_9
    , cast(diagnosis_poa_10 as {{ dbt.type_string() }}) as diagnosis_poa_10
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_11
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_12
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_13
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_14
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_15
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_16
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_17
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_18
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_19
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_20
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_21
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_22
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_23
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_24
    , cast(null as {{ dbt.type_string() }}) as diagnosis_poa_25
    , cast(procedure_code_type as {{ dbt.type_string() }}) as procedure_code_type
    , cast(procedure_code_1 as {{ dbt.type_string() }}) as procedure_code_1
    , cast(procedure_code_2 as {{ dbt.type_string() }}) as procedure_code_2
    , cast(procedure_code_3 as {{ dbt.type_string() }}) as procedure_code_3
    , cast(procedure_code_4 as {{ dbt.type_string() }}) as procedure_code_4
    , cast(procedure_code_5 as {{ dbt.type_string() }}) as procedure_code_5
    , cast(procedure_code_6 as {{ dbt.type_string() }}) as procedure_code_6
    , cast(null as {{ dbt.type_string() }}) as procedure_code_7
    , cast(null as {{ dbt.type_string() }}) as procedure_code_8
    , cast(null as {{ dbt.type_string() }}) as procedure_code_9
    , cast(null as {{ dbt.type_string() }}) as procedure_code_10
    , cast(null as {{ dbt.type_string() }}) as procedure_code_11
    , cast(null as {{ dbt.type_string() }}) as procedure_code_12
    , cast(null as {{ dbt.type_string() }}) as procedure_code_13
    , cast(null as {{ dbt.type_string() }}) as procedure_code_14
    , cast(null as {{ dbt.type_string() }}) as procedure_code_15
    , cast(null as {{ dbt.type_string() }}) as procedure_code_16
    , cast(null as {{ dbt.type_string() }}) as procedure_code_17
    , cast(null as {{ dbt.type_string() }}) as procedure_code_18
    , cast(null as {{ dbt.type_string() }}) as procedure_code_19
    , cast(null as {{ dbt.type_string() }}) as procedure_code_20
    , cast(null as {{ dbt.type_string() }}) as procedure_code_21
    , cast(null as {{ dbt.type_string() }}) as procedure_code_22
    , cast(null as {{ dbt.type_string() }}) as procedure_code_23
    , cast(null as {{ dbt.type_string() }}) as procedure_code_24
    , cast(null as {{ dbt.type_string() }}) as procedure_code_25
    , cast(null as date) as procedure_date_1
    , cast(null as date) as procedure_date_2
    , cast(null as date) as procedure_date_3
    , cast(null as date) as procedure_date_4
    , cast(null as date) as procedure_date_5
    , cast(null as date) as procedure_date_6
    , cast(null as date) as procedure_date_7
    , cast(null as date) as procedure_date_8
    , cast(null as date) as procedure_date_9
    , cast(null as date) as procedure_date_10
    , cast(null as date) as procedure_date_11
    , cast(null as date) as procedure_date_12
    , cast(null as date) as procedure_date_13
    , cast(null as date) as procedure_date_14
    , cast(null as date) as procedure_date_15
    , cast(null as date) as procedure_date_16
    , cast(null as date) as procedure_date_17
    , cast(null as date) as procedure_date_18
    , cast(null as date) as procedure_date_19
    , cast(null as date) as procedure_date_20
    , cast(null as date) as procedure_date_21
    , cast(null as date) as procedure_date_22
    , cast(null as date) as procedure_date_23
    , cast(null as date) as procedure_date_24
    , cast(null as date) as procedure_date_25
    , cast(null as integer) as in_network_flag
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_string() }}) as file_date
    , cast(null as datetime) as ingest_datetime
from mapped_data as md
inner join claim_line_totals as clt
on md.claim_id = clt.claim_id
and md.claim_line_number = clt.claim_line_number
left outer join claim_types as ct
on md.claim_id = ct.claim_id
left outer join rejections as r
on md.claim_id = r.claim_id
left outer join claims_to_exclude as cte
on md.claim_id = cte.claim_id
where not r.has_denied_status
and cte.claim_id is null
)

select * from final
