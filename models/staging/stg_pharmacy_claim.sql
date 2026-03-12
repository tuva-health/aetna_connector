select
    rx_claim_id as claim_id
    , row_number() over (partition by member_id order by rx_claim_id, disp_dt, prescription_nbr) as claim_line_number
    , cast(member_id as {{ dbt.type_string() }}) as person_id
    , cast(member_id as {{ dbt.type_string() }}) as member_id
    , cast('Aetna' as {{ dbt.type_string() }}) as payer
    , cast(product_ln_cd as {{ dbt.type_string() }}) as plan
    , cast(prescriber_id as {{ dbt.type_string() }}) as prescribing_provider_npi
    , null as dispensing_provider_npi
    , cast(disp_dt as date) as dispensing_date
    , lpad(ndc_cd,11,'0') as ndc_code
    , cast(unts_dispensed_qty as float) as service_unit_quantity
    , cast(days_supply_cnt as float) as days_supply_cnt
    , null as refills
    , cast(process_dt as date) as paid_date
    , cast(paid_amt as float) as paid_amount
    , null as allowed_amount
    , null as charge_amount
    , null as coinsurance_amount
    , cast(srv_copay_amt as float) as copayment_amount
    , cast(app_to_per_ded_amt as float) as deductible_amount
    , null as in_network_flag
    , 'Aetna' as data_source
    -- Fill these in since these need to be established by the user
    , null as file_name
    , null as file_date
    , null as ingest_datetime
from {{ source('aetna', 'aetna_pharmacy_claims_raw') }}
where clm_status = 'P'