with source as (
    select *
    from {{ source('raw', 'processed_trades') }}
),

renamed as (
    select
        order_id,
        portfolio_id,
        upper(trim(symbol)) as symbol,
        upper(trim(side)) as side,
        quantity,
        trade_price,
        average_cost,
        upper(trim(currency)) as currency,
        transact_time,
        trade_date,
        close_price,
        upper(trim(market_currency)) as market_currency,
        fx_rate_to_usd,
        realized_pnl_local,
        realized_pnl_usd,
        pnl_unresolvable,
        loaded_at

    from source
)

select *
from renamed