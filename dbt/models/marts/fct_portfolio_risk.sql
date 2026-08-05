with portfolio_pnl as (

    select *
    from {{ ref('stg_portfolio_pnl') }}

),

final as (

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

        case
            when market_value_limit_breached
              or daily_loss_limit_breached
                then 'BREACH'
            else 'WITHIN_LIMIT'
        end as risk_status,

        case
            when max_market_value_usd is null
              or max_market_value_usd = 0
                then null
            else
                abs(total_market_value_usd)
                / max_market_value_usd
        end as market_value_limit_utilization,

        max(loaded_at) as latest_loaded_at

    from portfolio_pnl

    group by
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
        daily_loss_limit_breached

)

select *
from final