CREATE SCHEMA IF NOT EXISTS monitoring;


CREATE OR REPLACE VIEW monitoring.v_pipeline_load_summary AS
SELECT
    dag_id,
    airflow_run_id,
    run_date,

    COUNT(*) AS load_task_count,

    SUM(rows_deleted) AS total_rows_deleted,
    SUM(rows_inserted) AS total_rows_inserted,

    COUNT(*) FILTER (
        WHERE load_status = 'SUCCESS'
    ) AS successful_load_count,

    COUNT(*) FILTER (
        WHERE load_status = 'FAILED'
    ) AS failed_load_count,

    MIN(started_at) AS load_started_at,
    MAX(completed_at) AS load_completed_at,

    EXTRACT(
        EPOCH FROM (
            MAX(completed_at) - MIN(started_at)
        )
    ) AS load_duration_seconds

FROM audit.pipeline_load_log
GROUP BY
    dag_id,
    airflow_run_id,
    run_date;


CREATE OR REPLACE VIEW monitoring.v_table_load_metrics AS
SELECT
    load_id,
    dag_id,
    airflow_run_id,
    run_date,
    target_table,
    rows_deleted,
    rows_inserted,
    load_status,
    started_at,
    completed_at,

    EXTRACT(
        EPOCH FROM (
            completed_at - started_at
        )
    ) AS duration_seconds,

    error_message

FROM audit.pipeline_load_log;


CREATE OR REPLACE VIEW monitoring.v_daily_trade_metrics AS
SELECT
    trade_date AS metric_date,

    COUNT(*) AS processed_trade_count,

    COUNT(*) FILTER (
        WHERE pnl_unresolvable
    ) AS unresolvable_trade_count,

    COUNT(*) FILTER (
        WHERE side = 'BUY'
    ) AS buy_trade_count,

    COUNT(*) FILTER (
        WHERE side = 'SELL'
    ) AS sell_trade_count,

    COALESCE(
        SUM(realized_pnl_usd),
        0
    ) AS total_realized_pnl_usd,

    COUNT(DISTINCT portfolio_id) AS portfolio_count,

    COUNT(DISTINCT symbol) AS symbol_count

FROM raw.processed_trades
GROUP BY trade_date;


CREATE OR REPLACE VIEW monitoring.v_daily_rejection_metrics AS
SELECT
    run_date AS metric_date,

    COUNT(*) AS rejected_record_count,

    COUNT(*) FILTER (
        WHERE source_name = 'trades'
    ) AS rejected_trade_count,

    COUNT(*) FILTER (
        WHERE source_name = 'market_data'
    ) AS rejected_market_count,

    COUNT(*) FILTER (
        WHERE source_name = 'portfolio'
    ) AS rejected_portfolio_count,

    COUNT(DISTINCT rejection_rule) AS rejection_rule_count

FROM raw.rejected_records
GROUP BY run_date;


CREATE OR REPLACE VIEW monitoring.v_daily_quality_metrics AS
WITH trades AS (

    SELECT
        trade_date AS metric_date,
        COUNT(*) AS accepted_trade_count,
        COUNT(*) FILTER (
            WHERE pnl_unresolvable
        ) AS unresolvable_count

    FROM raw.processed_trades
    GROUP BY trade_date

),

rejections AS (

    SELECT
        run_date AS metric_date,
        COUNT(*) FILTER (
            WHERE source_name = 'trades'
        ) AS rejected_trade_count,
        COUNT(*) AS total_rejected_count

    FROM raw.rejected_records
    GROUP BY run_date

)

SELECT
    COALESCE(
        trades.metric_date,
        rejections.metric_date
    ) AS metric_date,

    COALESCE(
        trades.accepted_trade_count,
        0
    ) AS accepted_trade_count,

    COALESCE(
        rejections.rejected_trade_count,
        0
    ) AS rejected_trade_count,

    COALESCE(
        rejections.total_rejected_count,
        0
    ) AS total_rejected_count,

    COALESCE(
        trades.unresolvable_count,
        0
    ) AS unresolvable_count,

    CASE
        WHEN
            COALESCE(trades.accepted_trade_count, 0)
            + COALESCE(rejections.rejected_trade_count, 0)
            = 0
        THEN 0

        ELSE
            COALESCE(
                rejections.rejected_trade_count,
                0
            )::NUMERIC
            /
            (
                COALESCE(
                    trades.accepted_trade_count,
                    0
                )
                +
                COALESCE(
                    rejections.rejected_trade_count,
                    0
                )
            )
    END AS trade_reject_rate,

    (
        CASE
            WHEN
                COALESCE(trades.accepted_trade_count, 0)
                + COALESCE(rejections.rejected_trade_count, 0)
                = 0
            THEN FALSE

            ELSE
                (
                    COALESCE(
                        rejections.rejected_trade_count,
                        0
                    )::NUMERIC
                    /
                    (
                        COALESCE(
                            trades.accepted_trade_count,
                            0
                        )
                        +
                        COALESCE(
                            rejections.rejected_trade_count,
                            0
                        )
                    )
                ) < 0.02
                AND COALESCE(
                    trades.unresolvable_count,
                    0
                ) < 100
        END
    ) AS quality_gate_passed

FROM trades
FULL OUTER JOIN rejections
    ON trades.metric_date = rejections.metric_date;


CREATE OR REPLACE VIEW monitoring.v_portfolio_risk_summary AS
SELECT
    pnl_date AS metric_date,

    COUNT(*) AS portfolio_count,

    COUNT(*) FILTER (
        WHERE market_value_limit_breached
    ) AS market_value_breach_count,

    COUNT(*) FILTER (
        WHERE daily_loss_limit_breached
    ) AS daily_loss_breach_count,

    COUNT(*) FILTER (
        WHERE
            market_value_limit_breached
            OR daily_loss_limit_breached
    ) AS total_breached_portfolios,

    COALESCE(
        SUM(total_market_value_usd),
        0
    ) AS total_market_value_usd,

    COALESCE(
        SUM(total_unrealized_pnl_usd),
        0
    ) AS total_unrealized_pnl_usd

FROM raw.portfolio_pnl
GROUP BY pnl_date;


CREATE OR REPLACE VIEW monitoring.v_rejections_by_rule AS
SELECT
    run_date AS metric_date,
    source_name,
    rejection_rule,
    COUNT(*) AS rejected_record_count

FROM raw.rejected_records

GROUP BY
    run_date,
    source_name,
    rejection_rule;