CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS audit;


-- ============================================================
-- Processed trade output from Spark
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.processed_trades (
    order_id               VARCHAR(100)    NOT NULL,
    portfolio_id           VARCHAR(50)     NOT NULL,
    symbol                 VARCHAR(50)     NOT NULL,
    side                   VARCHAR(10)     NOT NULL,
    quantity               NUMERIC(20, 4)  NOT NULL,
    trade_price            NUMERIC(20, 8)  NOT NULL,
    average_cost           NUMERIC(20, 8)  NOT NULL,
    currency               CHAR(3)         NOT NULL,
    transact_time          TIMESTAMP       NOT NULL,
    trade_date             DATE            NOT NULL,
    close_price            NUMERIC(20, 8),
    market_currency        CHAR(3),
    fx_rate_to_usd         NUMERIC(20, 10),
    realized_pnl_local     NUMERIC(24, 4),
    realized_pnl_usd       NUMERIC(24, 4),
    pnl_unresolvable       BOOLEAN         NOT NULL,
    loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_processed_trades
        PRIMARY KEY (order_id, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_processed_trades_date
    ON raw.processed_trades (trade_date);

CREATE INDEX IF NOT EXISTS idx_processed_trades_portfolio
    ON raw.processed_trades (portfolio_id, trade_date);

CREATE INDEX IF NOT EXISTS idx_processed_trades_symbol
    ON raw.processed_trades (symbol, trade_date);


-- ============================================================
-- Portfolio-level P&L output from Spark
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.portfolio_pnl (
    portfolio_id                       VARCHAR(50)    NOT NULL,
    portfolio_name                     VARCHAR(255),
    pnl_date                           DATE           NOT NULL,
    position_count                     INTEGER        NOT NULL,
    total_market_value_usd             NUMERIC(24, 4),
    total_unrealized_pnl_usd           NUMERIC(24, 4),
    max_market_value_usd               NUMERIC(24, 4),
    max_daily_loss_usd                 NUMERIC(24, 4),
    max_position_concentration_pct     NUMERIC(10, 4),
    market_value_limit_breached        BOOLEAN,
    daily_loss_limit_breached          BOOLEAN,
    loaded_at                          TIMESTAMPTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_portfolio_pnl
        PRIMARY KEY (portfolio_id, pnl_date)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_pnl_date
    ON raw.portfolio_pnl (pnl_date);

CREATE INDEX IF NOT EXISTS idx_portfolio_pnl_limit_breaches
    ON raw.portfolio_pnl (
        pnl_date,
        market_value_limit_breached,
        daily_loss_limit_breached
    );


-- ============================================================
-- Rejected records from all source systems
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.rejected_records (
    rejection_id           BIGSERIAL      PRIMARY KEY,
    source_name            VARCHAR(50)    NOT NULL,
    run_date               DATE           NOT NULL,
    record_identifier      VARCHAR(255),
    raw_record             TEXT,
    rejection_rule         VARCHAR(100)   NOT NULL,
    rejection_reason       TEXT           NOT NULL,
    rejected_at            TIMESTAMPTZ    NOT NULL,
    loaded_at              TIMESTAMPTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_rejected_records_run_date
    ON raw.rejected_records (run_date);

CREATE INDEX IF NOT EXISTS idx_rejected_records_source_rule
    ON raw.rejected_records (
        source_name,
        rejection_rule,
        run_date
    );


-- ============================================================
-- Unresolvable trade P&L
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.unresolvable_pnl (
    order_id               VARCHAR(100)    NOT NULL,
    portfolio_id           VARCHAR(50)     NOT NULL,
    symbol                 VARCHAR(50)     NOT NULL,
    side                   VARCHAR(10)     NOT NULL,
    quantity               NUMERIC(20, 4)  NOT NULL,
    trade_price            NUMERIC(20, 8)  NOT NULL,
    average_cost           NUMERIC(20, 8)  NOT NULL,
    currency               CHAR(3)         NOT NULL,
    transact_time          TIMESTAMP       NOT NULL,
    trade_date             DATE            NOT NULL,
    unresolvable_reason    TEXT            NOT NULL,
    loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_unresolvable_pnl
        PRIMARY KEY (order_id, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_unresolvable_pnl_date
    ON raw.unresolvable_pnl (trade_date);


-- ============================================================
-- Pipeline run audit
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    dag_id                       VARCHAR(100)    NOT NULL,
    airflow_run_id               VARCHAR(255)   NOT NULL,
    run_date                     DATE           NOT NULL,
    status                       VARCHAR(30)     NOT NULL,
    trade_row_count              BIGINT         DEFAULT 0,
    portfolio_row_count          BIGINT         DEFAULT 0,
    rejected_row_count           BIGINT         DEFAULT 0,
    unresolvable_row_count       BIGINT         DEFAULT 0,
    reject_rate                  NUMERIC(12, 8),
    quality_gate_passed          BOOLEAN,
    started_at                   TIMESTAMPTZ,
    completed_at                 TIMESTAMPTZ,
    error_message                TEXT,
    created_at                   TIMESTAMPTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                   TIMESTAMPTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_pipeline_runs
        PRIMARY KEY (dag_id, airflow_run_id)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_run_date
    ON audit.pipeline_runs (run_date);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON audit.pipeline_runs (status, run_date);


-- ============================================================
-- Per-table warehouse load audit
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.pipeline_load_log (
    load_id                 BIGSERIAL      PRIMARY KEY,
    dag_id                  VARCHAR(100)   NOT NULL,
    airflow_run_id          VARCHAR(255)   NOT NULL,
    run_date                DATE           NOT NULL,
    target_table            VARCHAR(150)   NOT NULL,
    source_path             TEXT           NOT NULL,
    rows_deleted            BIGINT         NOT NULL DEFAULT 0,
    rows_inserted           BIGINT         NOT NULL DEFAULT 0,
    load_status             VARCHAR(30)    NOT NULL,
    started_at              TIMESTAMPTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at            TIMESTAMPTZ,
    error_message           TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_load_log_run
    ON audit.pipeline_load_log (
        dag_id,
        airflow_run_id
    );