with source as (

    select *
    from {{ source('raw', 'portfolio_pnl') }}

),

renamed as (

    select
        portfolio_id,
        portfolio_name,
        pnl_date,
        position_count,
        total_market_value_usd,
        total_unrealized_pnl_usd,
        max_market_value_usd,
        max_daily_loss_usd,
        max_position_concentration_pct,
        market_value_limit_breached,
        daily_loss_limit_breached,
        loaded_at

    from source

)

select *
from renamed