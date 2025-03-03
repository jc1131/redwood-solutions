with response_combined as (
    select * from {{ ref('int_response_combined') }}
)
select
form_response_combine_pk
,form_response_fk as form_response_pk

from response_combined