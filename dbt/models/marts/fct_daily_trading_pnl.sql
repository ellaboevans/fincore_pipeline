with trades as (
    select *
    from {{ ref('stg_processed_trades') }}
),

aggregated as (
    select
        trade_date,
        portfolio_id,
        symbol,
        
        count(*) as trade_count,

        sum(
            case
                when side = 'BUY' then quantity
                else 0
            end
        ) as buy_quantity,

        sum(
            case
                when side = 'SELL' then quantity
                else 0
            end
        ) as sell_quantity,

        sum(realized_pnl_local) as realized_pnl_local,
        sum(realized_pnl_usd) as realized_pnl_usd,

        sum(
            case
                when pnl_unresolvable then 1
                else 0
            end
        ) as unresolvable_trade_count,

        max(loaded_at) as latest_loaded_at

    from trades

    group by
        trade_date,
        portfolio_id,
        symbol
)

select *
from aggregated