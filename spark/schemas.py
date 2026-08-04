from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    BooleanType,
    TimestampType,
)

PRICE_TYPE = DecimalType(20, 8)
QUANTITY_TYPE = DecimalType(20, 4)
FX_RATE_TYPE = DecimalType(20, 10)
MONEY_TYPE = DecimalType(24, 4)
PERCENTAGE_TYPE = DecimalType(10, 4)


FIX_TRADE_SCHEMA = StructType(
    [
        StructField("begin_string", StringType(), True),
        StructField("message_type", StringType(), True),
        StructField("sender_comp_id", StringType(), True),
        StructField("target_comp_id", StringType(), True),
        StructField("message_sequence_number", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("portfolio_id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("side_code", StringType(), True),
        StructField("side", StringType(), True),
        StructField("quantity", QUANTITY_TYPE, True),
        StructField("trade_price", PRICE_TYPE, True),
        StructField("average_cost", PRICE_TYPE, True),
        StructField("currency", StringType(), True),
        StructField("transact_time", TimestampType(), True),
        StructField("trade_date", DateType(), True),
        StructField("raw_record", StringType(), False),
    ]
)

MARKET_DATA_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("asset_class", StringType(), True),
        StructField("close_price", PRICE_TYPE, True),
        StructField("currency", StringType(), True),
        StructField("fx_rate_to_usd", FX_RATE_TYPE, True),
        StructField("price_date", DateType(), True),
        StructField("loaded_at", TimestampType(), True),
    ]
)

RISK_LIMIT_SCHEMA = StructType(
    [
        StructField("max_market_value_usd", MONEY_TYPE, True),
        StructField("max_daily_loss_usd", MONEY_TYPE, True),
        StructField(
            "max_position_concentration_pct",
            PERCENTAGE_TYPE,
            True,
        ),
    ]
)

PORTFOLIO_METADATA_SCHEMA = StructType(
    [
        StructField("risk_limits", RISK_LIMIT_SCHEMA, True),
        StructField("source_system", StringType(), True),
        StructField("loaded_at", TimestampType(), True),
    ]
)

POSITION_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("position_quantity", QUANTITY_TYPE, True),
        StructField("currency", StringType(), True),
        StructField("average_cost", PRICE_TYPE, True),
    ]
)


PORTFOLIO_SCHEMA = StructType(
    [
        StructField("portfolio_id", StringType(), True),
        StructField("portfolio_name", StringType(), True),
        StructField("base_currency", StringType(), True),
        StructField(
            "positions",
            ArrayType(POSITION_SCHEMA, containsNull=True),
            True,
        ),
        StructField("metadata", PORTFOLIO_METADATA_SCHEMA, True),
    ]
)

PORTFOLIO_EXPORT_SCHEMA = StructType(
    [
        StructField("run_date", DateType(), True),
        StructField(
            "portfolios",
            ArrayType(PORTFOLIO_SCHEMA, containsNull=True),
            True,
        ),
    ]
)


REJECTED_RECORD_SCHEMA = StructType(
    [
        StructField("source_name", StringType(), False),
        StructField("run_date", DateType(), False),
        StructField("record_identifier", StringType(), True),
        StructField("raw_record", StringType(), True),
        StructField("rejection_rule", StringType(), False),
        StructField("rejection_reason", StringType(), False),
        StructField("rejected_at", TimestampType(), False),
    ]
)


CLEAN_TRADE_OUTPUT_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("portfolio_id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("side", StringType(), False),
        StructField("quantity", QUANTITY_TYPE, False),
        StructField("trade_price", PRICE_TYPE, False),
        StructField("average_cost", PRICE_TYPE, False),
        StructField("currency", StringType(), False),
        StructField("transact_time", TimestampType(), False),
        StructField("trade_date", DateType(), False),
        StructField("close_price", PRICE_TYPE, True),
        StructField("market_currency", StringType(), True),
        StructField("fx_rate_to_usd", FX_RATE_TYPE, True),
        StructField("realized_pnl_local", MONEY_TYPE, True),
        StructField("realized_pnl_usd", MONEY_TYPE, True),
        StructField("pnl_unresolvable", BooleanType(), False),
    ]
)


PORTFOLIO_PNL_OUTPUT_SCHEMA = StructType(
    [
        StructField("portfolio_id", StringType(), False),
        StructField("portfolio_name", StringType(), True),
        StructField("symbol", StringType(), False),
        StructField("position_quantity", QUANTITY_TYPE, False),
        StructField("position_currency", StringType(), False),
        StructField("average_cost", PRICE_TYPE, True),
        StructField("close_price", PRICE_TYPE, True),
        StructField("fx_rate_to_usd", FX_RATE_TYPE, True),
        StructField("market_value_local", MONEY_TYPE, True),
        StructField("market_value_usd", MONEY_TYPE, True),
        StructField("unrealized_pnl_local", MONEY_TYPE, True),
        StructField("unrealized_pnl_usd", MONEY_TYPE, True),
        StructField("max_market_value_usd", MONEY_TYPE, True),
        StructField("max_daily_loss_usd", MONEY_TYPE, True),
        StructField(
            "max_position_concentration_pct",
            PERCENTAGE_TYPE,
            True,
        ),
        StructField("pnl_date", DateType(), False),
    ]
)


FIX_TAG_MAPPING = {
    "8": "begin_string",
    "35": "message_type",
    "49": "sender_comp_id",
    "56": "target_comp_id",
    "34": "message_sequence_number",
    "11": "order_id",
    "1": "portfolio_id",
    "55": "symbol",
    "54": "side_code",
    "38": "quantity",
    "44": "trade_price",
    "6": "average_cost",
    "15": "currency",
    "60": "transact_time",
}


SIDE_MAPPING = {
    "1": "BUY",
    "2": "SELL",
}