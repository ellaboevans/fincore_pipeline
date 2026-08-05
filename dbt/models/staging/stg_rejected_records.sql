with source as (

    select *
    from {{ source('raw', 'rejected_records') }}

)

select
    rejection_id,
    source_name,
    run_date,
    record_identifier,
    raw_record,
    rejection_rule,
    rejection_reason,
    rejected_at,
    loaded_at

from source