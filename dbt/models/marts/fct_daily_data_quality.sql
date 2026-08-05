with rejected as (

    select *
    from {{ ref('stg_rejected_records') }}

),

aggregated as (

    select
        run_date,
        source_name,
        rejection_rule,
        count(*) as rejected_record_count,
        max(rejected_at) as latest_rejection_at,
        max(loaded_at) as latest_loaded_at

    from rejected

    group by
        run_date,
        source_name,
        rejection_rule

)

select *
from aggregated