select
    cast(claim_id as {{ dbt.type_string() }}) as claim_id
    , cast(claim_line_number as integer) as claim_line_number
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(member_id as {{ dbt.type_string() }}) as member_id
    , cast(payer as {{ dbt.type_string() }}) as payer
    , cast({{ the_tuva_project.quote_column('plan') }} as {{ dbt.type_string() }}) as {{ the_tuva_project.quote_column('plan') }}
    , cast(prescribing_provider_npi as {{ dbt.type_string() }}) as prescribing_provider_npi
    , cast(dispensing_provider_npi as {{ dbt.type_string() }}) as dispensing_provider_npi
    , cast(dispensing_date as date) as dispensing_date
    , cast(ndc_code as {{ dbt.type_string() }}) as ndc_code
    , cast(quantity as integer) as quantity
    , cast(days_supply as integer) as days_supply
    , cast(refills as integer) as refills
    , cast(paid_date as date) as paid_date
    , round(paid_amount, 2) as paid_amount
    , round(allowed_amount, 2) as allowed_amount
    , round(charge_amount, 2) as charge_amount
    , round(coinsurance_amount, 2) as coinsurance_amount
    , round(copayment_amount, 2) as copayment_amount
    , round(deductible_amount, 2) as deductible_amount
    , cast(in_network_flag as integer) as in_network_flag
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(file_name as {{ dbt.type_string() }}) as file_name
    , file_date
    , ingest_datetime
from {{ ref('int_pharmacy_claim') }}

