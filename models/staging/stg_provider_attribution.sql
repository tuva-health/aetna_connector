select 
    member_id
    , person_id
    , person_id as patient_id
    , payer
    , data_source
    , payer_type
    , attr_npi_nbr AS payer_attributed_provider -- Storing NPI here
    , null as payer_attributed_provider_practice
    , null as payer_attributed_provider_organization
    , payer_type  as payer_attributed_provider_lob
    , attr_tax_id_nbr as custom_attributed_provider -- Storing TIN here
    , null as custom_attributed_provider_practice
    , null as custom_attributed_provider_organization
    , payer_type as custom_attributed_provider_lob
    , file_name
    , ingest_datetime
    , file_date
    , enrollment_start_date as year_month 
    , plan
from {{ ref('int_eligibility') }}

