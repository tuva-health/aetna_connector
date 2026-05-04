with race as (
select distinct 
      person_id
    , race_ethnicity
    , case 
        when race_ethnicity = 'White' then 'white'
        when race_ethnicity = 'African American/Black' then 'black or african american'
        when race_ethnicity = 'Asian' then 'asian'
        when race_ethnicity = 'American Indian/Alaskan Native' then 'american indian or alaska native'
        when race_ethnicity = 'Pacific Islander' then 'native hawaiian or other pacific islander'
        when race_ethnicity = 'Hispanic/Latino' then 'other race'
        when race_ethnicity = 'Other Race/Ethnicity' then 'other race'
        else 'unknown'
    end as race
from {{ ref('stg_member') }}
)

select
    person_id
    , race
    , case 
        when race = 'unknown' then 'unknown'
        when race_ethnicity = 'Hispanic/Latino' then 'Hispanic or Latino'
        else 'Not Hispanic or Latino'
    end as ethnicity
from race
