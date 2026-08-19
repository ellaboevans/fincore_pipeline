from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from schemas import (
    FIX_TAG_MAPPING,
    FX_RATE_TYPE,
    MARKET_DATA_SCHEMA,
    MONEY_TYPE,
    PORTFOLIO_EXPORT_SCHEMA,
    PRICE_TYPE,
    QUANTITY_TYPE,
)


LOGGER = logging.getLogger("fincore-etl")

RUN_DATE_FORMAT = "%Y-%m-%d"
FIX_TIMESTAMP_FORMAT = "yyyyMMdd-HH:mm:ss"

SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

SOURCE_TRADES = "trades"
SOURCE_MARKET_DATA = "market_data"
SOURCE_PORTFOLIO = "portfolio"

# Quality-gate thresholds. The trade reject rate is defined as
# trade rejections / total inbound trades, so it is comparable with
# monitoring.v_daily_quality_metrics, which computes the same ratio
# from the warehouse.
TRADE_REJECT_RATE_THRESHOLD = 0.02
PNL_UNRESOLVABLE_THRESHOLD = 100

DECIMAL_ZERO = Decimal("0")
DECIMAL_ONE = Decimal("1")


# ---------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FinCore multi-source financial PySpark ETL job."
    )

    parser.add_argument(
        "--run-date",
        required=True,
        help="Logical trading date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--s3-endpoint",
        default=os.getenv("MINIO_ENDPOINT"),
    )
    parser.add_argument(
        "--s3-access-key",
        default=os.getenv("MINIO_ROOT_USER"),
    )
    parser.add_argument(
        "--s3-secret-key",
        default=os.getenv("MINIO_ROOT_PASSWORD"),
    )
    parser.add_argument(
        "--s3-region",
        default=os.getenv("MINIO_REGION", "us-east-1"),
    )

    parser.add_argument(
        "--raw-bucket",
        default="fincore-raw",
    )
    parser.add_argument(
        "--processed-bucket",
        default="fincore-processed",
    )

    return parser.parse_args()


def validate_run_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"Invalid run date {value!r}; expected YYYY-MM-DD."
        ) from exc


# ---------------------------------------------------------------------
# Spark configuration
# ---------------------------------------------------------------------


def create_spark_session(args: argparse.Namespace) -> SparkSession:
    if not args.s3_access_key or not args.s3_secret_key:
        raise ValueError(
            "MinIO access key and secret key must be provided through "
            "--s3-access-key/--s3-secret-key or environment variables."
        )

    spark = (
        SparkSession.builder
        .appName(f"fincore-daily-etl-{args.run_date}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint)
        .config("spark.hadoop.fs.s3a.endpoint.region", args.s3_region)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            args.s3_access_key,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            args.s3_secret_key,
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------


def normalize_symbol(column: F.Column) -> F.Column:
    return F.upper(
        F.regexp_replace(
            F.trim(column),
            r"\s+",
            "",
        )
    )


def empty_to_null(column: F.Column) -> F.Column:
    return F.when(
        F.trim(column) == "",
        F.lit(None),
    ).otherwise(F.trim(column))


def extract_fix_tag(
    raw_record: F.Column,
    tag: str,
) -> F.Column:
    pattern = rf"(?:^|\|){tag}=([^|]*)"
    return empty_to_null(F.regexp_extract(raw_record, pattern, 1))


def rejected_record(
    dataframe: DataFrame,
    *,
    source_name: str,
    run_date: str,
    identifier_column: F.Column,
    raw_record_column: F.Column,
    rejection_rule_column: F.Column,
    rejection_reason_column: F.Column,
) -> DataFrame:
    return dataframe.select(
        F.lit(source_name).alias("source_name"),
        F.to_date(F.lit(run_date)).alias("run_date"),
        identifier_column.cast(StringType()).alias("record_identifier"),
        raw_record_column.cast(StringType()).alias("raw_record"),
        rejection_rule_column.cast(StringType()).alias("rejection_rule"),
        rejection_reason_column.cast(StringType()).alias("rejection_reason"),
        F.current_timestamp().alias("rejected_at"),
    )


def union_dataframes(dataframes: list[DataFrame]) -> DataFrame:
    if not dataframes:
        raise ValueError("At least one DataFrame is required.")

    result = dataframes[0]

    for dataframe in dataframes[1:]:
        result = result.unionByName(
            dataframe,
            allowMissingColumns=True,
        )

    return result


# ---------------------------------------------------------------------
# FIX trade processing
# ---------------------------------------------------------------------


def parse_fix_trades(
    spark: SparkSession,
    input_path: str,
    run_date: str,
) -> tuple[DataFrame, DataFrame]:
    LOGGER.info("Reading FIX trades from %s", input_path)

    raw = (
        spark.read
        .option("mode", "PERMISSIVE")
        .text(input_path)
        .select(F.col("value").alias("raw_record"))
    )

    parsed = raw

    for tag, column_name in FIX_TAG_MAPPING.items():
        parsed = parsed.withColumn(
            column_name,
            extract_fix_tag(F.col("raw_record"), tag),
        )

    parsed = (
        parsed
        .withColumn(
            "symbol",
            normalize_symbol(F.col("symbol")),
        )
        .withColumn(
            "side",
            F.when(F.col("side_code") == "1", F.lit(SIDE_BUY))
            .when(F.col("side_code") == "2", F.lit(SIDE_SELL)),
        )
        .withColumn(
            "quantity",
            F.col("quantity").cast(QUANTITY_TYPE),
        )
        .withColumn(
            "trade_price",
            F.col("trade_price").cast(PRICE_TYPE),
        )
        .withColumn(
            "average_cost",
            F.col("average_cost").cast(PRICE_TYPE),
        )
        .withColumn(
            "currency",
            F.upper(F.trim(F.col("currency"))),
        )
        .withColumn(
            "transact_time",
            F.try_to_timestamp(
                F.col("transact_time"),
                F.lit(FIX_TIMESTAMP_FORMAT),
            ),
        )
        .withColumn(
            "trade_date",
            F.to_date(F.col("transact_time")),
        )
    )

    parsed = parsed.withColumn(
        "rejection_rule",
        F.when(
            F.col("order_id").isNull(),
            F.lit("TRADE_MISSING_ORDER_ID"),
        )
        .when(
            F.col("portfolio_id").isNull(),
            F.lit("TRADE_MISSING_PORTFOLIO_ID"),
        )
        .when(
            F.col("symbol").isNull() | (F.col("symbol") == ""),
            F.lit("TRADE_MISSING_SYMBOL"),
        )
        .when(
            ~F.col("side_code").isin("1", "2"),
            F.lit("TRADE_INVALID_SIDE"),
        )
        .when(
            F.col("quantity").isNull() | (F.col("quantity") <= 0),
            F.lit("TRADE_INVALID_QUANTITY"),
        )
        .when(
            F.col("trade_price").isNull() | (F.col("trade_price") <= 0),
            F.lit("TRADE_INVALID_PRICE"),
        )
        .when(
            F.col("average_cost").isNull() | (F.col("average_cost") < 0),
            F.lit("TRADE_INVALID_AVERAGE_COST"),
        )
        .when(
            F.col("currency").isNull() | (F.length("currency") != 3),
            F.lit("TRADE_INVALID_CURRENCY"),
        )
        .when(
            F.col("transact_time").isNull(),
            F.lit("TRADE_INVALID_TIMESTAMP"),
        )
        .when(
            F.col("trade_date") != F.to_date(F.lit(run_date)),
            F.lit("TRADE_DATE_MISMATCH"),
        )
        .otherwise(F.lit(None)),
    )

    parsed = parsed.withColumn(
        "rejection_reason",
        F.when(
            F.col("rejection_rule") == "TRADE_MISSING_ORDER_ID",
            F.lit("FIX tag 11 is missing."),
        )
        .when(
            F.col("rejection_rule") == "TRADE_MISSING_PORTFOLIO_ID",
            F.lit("FIX tag 1 is missing."),
        )
        .when(
            F.col("rejection_rule") == "TRADE_MISSING_SYMBOL",
            F.lit("FIX tag 55 is missing or empty."),
        )
        .when(
            F.col("rejection_rule") == "TRADE_INVALID_SIDE",
            F.lit("FIX tag 54 must be 1 for BUY or 2 for SELL."),
        )
        .when(
            F.col("rejection_rule") == "TRADE_INVALID_QUANTITY",
            F.lit("FIX tag 38 must contain a positive quantity."),
        )
        .when(
            F.col("rejection_rule") == "TRADE_INVALID_PRICE",
            F.lit("FIX tag 44 must contain a positive trade price."),
        )
        .when(
            F.col("rejection_rule") == "TRADE_INVALID_AVERAGE_COST",
            F.lit("FIX tag 6 must contain a non-negative average cost."),
        )
        .when(
            F.col("rejection_rule") == "TRADE_INVALID_CURRENCY",
            F.lit("FIX tag 15 must contain a three-character currency."),
        )
        .when(
            F.col("rejection_rule") == "TRADE_INVALID_TIMESTAMP",
            F.lit(
                "FIX tag 60 must use the format "
                "YYYYMMDD-HH:MM:SS."
            ),
        )
        .when(
            F.col("rejection_rule") == "TRADE_DATE_MISMATCH",
            F.lit("Trade date does not match the pipeline run date."),
        ),
    )

    valid = parsed.filter(F.col("rejection_rule").isNull())

    rejected_source = parsed.filter(F.col("rejection_rule").isNotNull())

    rejected = rejected_record(
        rejected_source,
        source_name=SOURCE_TRADES,
        run_date=run_date,
        identifier_column=F.col("order_id"),
        raw_record_column=F.col("raw_record"),
        rejection_rule_column=F.col("rejection_rule"),
        rejection_reason_column=F.col("rejection_reason"),
    )

    return valid, rejected


# ---------------------------------------------------------------------
# Market data processing
# ---------------------------------------------------------------------


def process_market_data(
    spark: SparkSession,
    input_path: str,
    run_date: str,
) -> tuple[DataFrame, DataFrame]:
    LOGGER.info("Reading market data from %s", input_path)

    market = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(MARKET_DATA_SCHEMA)
        .csv(input_path)
        .withColumn(
            "normalized_symbol",
            normalize_symbol(F.col("symbol")),
        )
        .withColumn(
            "currency",
            F.upper(F.trim(F.col("currency"))),
        )
    )

    market = market.withColumn(
        "rejection_rule",
        F.when(
            F.col("normalized_symbol").isNull()
            | (F.col("normalized_symbol") == ""),
            F.lit("MARKET_MISSING_SYMBOL"),
        )
        .when(
            F.col("price_date").isNull(),
            F.lit("MARKET_INVALID_PRICE_DATE"),
        )
        .when(
            F.col("price_date") != F.to_date(F.lit(run_date)),
            F.lit("MARKET_DATE_MISMATCH"),
        )
        .when(
            F.col("close_price").isNull(),
            F.lit("MARKET_NULL_CLOSE_PRICE"),
        )
        .when(
            F.col("close_price") <= 0,
            F.lit("MARKET_NON_POSITIVE_CLOSE_PRICE"),
        )
        .when(
            F.col("currency").isNull() | (F.length("currency") != 3),
            F.lit("MARKET_INVALID_CURRENCY"),
        )
        .when(
            (F.col("currency") != "USD")
            & (
                F.col("fx_rate_to_usd").isNull()
                | (F.col("fx_rate_to_usd") <= 0)
            ),
            F.lit("MARKET_INVALID_FX_RATE"),
        )
        .otherwise(F.lit(None)),
    )

    market = market.withColumn(
        "rejection_reason",
        F.when(
            F.col("rejection_rule") == "MARKET_MISSING_SYMBOL",
            F.lit("Market-data symbol is missing."),
        )
        .when(
            F.col("rejection_rule") == "MARKET_INVALID_PRICE_DATE",
            F.lit("Market-data price date is invalid."),
        )
        .when(
            F.col("rejection_rule") == "MARKET_DATE_MISMATCH",
            F.lit("Market-data price date does not match the run date."),
        )
        .when(
            F.col("rejection_rule") == "MARKET_NULL_CLOSE_PRICE",
            F.lit(
                "Close price is NULL; stale prices are not permitted."
            ),
        )
        .when(
            F.col("rejection_rule")
            == "MARKET_NON_POSITIVE_CLOSE_PRICE",
            F.lit("Close price must be greater than zero."),
        )
        .when(
            F.col("rejection_rule") == "MARKET_INVALID_CURRENCY",
            F.lit("Market-data currency is invalid."),
        )
        .when(
            F.col("rejection_rule") == "MARKET_INVALID_FX_RATE",
            F.lit(
                "A positive FX rate is required for non-USD prices."
            ),
        ),
    )

    valid = (
        market
        .filter(F.col("rejection_rule").isNull())
        .withColumn(
            "effective_fx_rate_to_usd",
            F.when(
                F.col("currency") == "USD",
                F.lit(DECIMAL_ONE).cast(FX_RATE_TYPE),
            ).otherwise(F.col("fx_rate_to_usd")),
        )
        .drop("rejection_rule", "rejection_reason")
    )

    rejected_source = market.filter(
        F.col("rejection_rule").isNotNull()
    )

    rejected = rejected_record(
        rejected_source,
        source_name=SOURCE_MARKET_DATA,
        run_date=run_date,
        identifier_column=F.col("symbol"),
        raw_record_column=F.to_json(
            F.struct(
                "symbol",
                "asset_class",
                "close_price",
                "currency",
                "fx_rate_to_usd",
                "price_date",
                "loaded_at",
            )
        ),
        rejection_rule_column=F.col("rejection_rule"),
        rejection_reason_column=F.col("rejection_reason"),
    )

    return valid, rejected


# ---------------------------------------------------------------------
# Portfolio processing
# ---------------------------------------------------------------------


def process_portfolios(
    spark: SparkSession,
    input_path: str,
    run_date: str,
    market_data: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    LOGGER.info("Reading portfolio data from %s", input_path)

    # The generated source is a regular multi-line JSON document.
    source = (
        spark.read
        .schema(PORTFOLIO_EXPORT_SCHEMA)
        .option("multiLine", "true")
        .json(input_path)
    )

    portfolios = (
        source
        .select(
            F.col("run_date").alias("source_run_date"),
            F.explode_outer("portfolios").alias("portfolio"),
        )
        .select(
            "source_run_date",
            F.col("portfolio.portfolio_id").alias("portfolio_id"),
            F.col("portfolio.portfolio_name").alias("portfolio_name"),
            F.col("portfolio.base_currency").alias("base_currency"),
            F.col("portfolio.positions").alias("positions"),
            F.col(
                "portfolio.metadata.risk_limits.max_market_value_usd"
            ).alias("max_market_value_usd"),
            F.col(
                "portfolio.metadata.risk_limits.max_daily_loss_usd"
            ).alias("max_daily_loss_usd"),
            F.col(
                "portfolio.metadata.risk_limits."
                "max_position_concentration_pct"
            ).alias("max_position_concentration_pct"),
            F.col("portfolio.metadata.loaded_at").alias("loaded_at"),
        )
        .withColumn(
            "position",
            F.explode_outer("positions"),
        )
        .select(
            "source_run_date",
            "portfolio_id",
            "portfolio_name",
            "base_currency",
            F.col("position.symbol").alias("symbol"),
            F.col("position.position_quantity").alias(
                "position_quantity"
            ),
            F.col("position.currency").alias("position_currency"),
            F.col("position.average_cost").alias("average_cost"),
            "max_market_value_usd",
            "max_daily_loss_usd",
            "max_position_concentration_pct",
            "loaded_at",
        )
        .withColumn(
            "normalized_symbol",
            normalize_symbol(F.col("symbol")),
        )
        .withColumn(
            "position_currency",
            F.upper(F.trim(F.col("position_currency"))),
        )
    )

    portfolios = portfolios.withColumn(
        "rejection_rule",
        F.when(
            F.col("source_run_date") != F.to_date(F.lit(run_date)),
            F.lit("PORTFOLIO_DATE_MISMATCH"),
        )
        .when(
            F.col("portfolio_id").isNull(),
            F.lit("PORTFOLIO_MISSING_ID"),
        )
        .when(
            F.col("normalized_symbol").isNull()
            | (F.col("normalized_symbol") == ""),
            F.lit("PORTFOLIO_MISSING_SYMBOL"),
        )
        .when(
            F.col("position_quantity").isNull(),
            F.lit("PORTFOLIO_INVALID_QUANTITY"),
        )
        .when(
            F.col("position_currency").isNull()
            | (F.length("position_currency") != 3),
            F.lit("PORTFOLIO_INVALID_CURRENCY"),
        )
        .otherwise(F.lit(None)),
    )

    base_valid = portfolios.filter(F.col("rejection_rule").isNull())

    base_rejected = portfolios.filter(
        F.col("rejection_rule").isNotNull()
    ).withColumn(
        "rejection_reason",
        F.when(
            F.col("rejection_rule") == "PORTFOLIO_DATE_MISMATCH",
            F.lit("Portfolio export date does not match the run date."),
        )
        .when(
            F.col("rejection_rule") == "PORTFOLIO_MISSING_ID",
            F.lit("Portfolio ID is missing."),
        )
        .when(
            F.col("rejection_rule") == "PORTFOLIO_MISSING_SYMBOL",
            F.lit("Portfolio position symbol is missing."),
        )
        .when(
            F.col("rejection_rule") == "PORTFOLIO_INVALID_QUANTITY",
            F.lit("Portfolio position quantity is invalid."),
        )
        .when(
            F.col("rejection_rule") == "PORTFOLIO_INVALID_CURRENCY",
            F.lit("Portfolio position currency is invalid."),
        ),
    )

    market_lookup = market_data.select(
        F.col("normalized_symbol").alias("market_symbol"),
        F.col("close_price"),
        F.col("currency").alias("market_currency"),
        F.col("effective_fx_rate_to_usd").alias("fx_rate_to_usd"),
        F.col("price_date"),
    )

    enriched = base_valid.join(
        market_lookup,
        (
            F.col("normalized_symbol") == F.col("market_symbol")
        )
        & (
            F.col("source_run_date") == F.col("price_date")
        ),
        "left",
    )

    enriched = enriched.withColumn(
        "market_rejection_rule",
        F.when(
            F.col("close_price").isNull(),
            F.lit("PORTFOLIO_MARKET_PRICE_UNAVAILABLE"),
        )
        .when(
            F.col("fx_rate_to_usd").isNull()
            | (F.col("fx_rate_to_usd") <= 0),
            F.lit("PORTFOLIO_FX_RATE_UNAVAILABLE"),
        )
        .otherwise(F.lit(None)),
    )

    market_rejected_source = (
        enriched
        .filter(F.col("market_rejection_rule").isNotNull())
        .withColumn(
            "rejection_reason",
            F.when(
                F.col("market_rejection_rule")
                == "PORTFOLIO_MARKET_PRICE_UNAVAILABLE",
                F.lit(
                    "No valid same-date market price is available "
                    "for the position."
                ),
            ).otherwise(
                F.lit(
                    "No valid USD FX conversion rate is available "
                    "for the position."
                )
            ),
        )
    )

    valid_positions = enriched.filter(
        F.col("market_rejection_rule").isNull()
    )

    valid_positions = (
        valid_positions
        .withColumn(
            "market_value_local",
            (
                F.col("position_quantity")
                * F.col("close_price")
            ).cast(MONEY_TYPE),
        )
        .withColumn(
            "market_value_usd",
            (
                F.col("position_quantity")
                * F.col("close_price")
                * F.col("fx_rate_to_usd")
            ).cast(MONEY_TYPE),
        )
        .withColumn(
            "unrealized_pnl_local",
            (
                (
                    F.col("close_price")
                    - F.coalesce(
                        F.col("average_cost"),
                        F.col("close_price"),
                    )
                )
                * F.col("position_quantity")
            ).cast(MONEY_TYPE),
        )
        .withColumn(
            "unrealized_pnl_usd",
            (
                (
                    F.col("close_price")
                    - F.coalesce(
                        F.col("average_cost"),
                        F.col("close_price"),
                    )
                )
                * F.col("position_quantity")
                * F.col("fx_rate_to_usd")
            ).cast(MONEY_TYPE),
        )
    )

    portfolio_pnl = (
        valid_positions
        .groupBy(
            "portfolio_id",
            "portfolio_name",
            "source_run_date",
            "max_market_value_usd",
            "max_daily_loss_usd",
            "max_position_concentration_pct",
        )
        .agg(
            F.count("*").alias("position_count"),
            F.sum("market_value_usd")
            .cast(MONEY_TYPE)
            .alias("total_market_value_usd"),
            F.sum("unrealized_pnl_usd")
            .cast(MONEY_TYPE)
            .alias("total_unrealized_pnl_usd"),
        )
        .withColumnRenamed("source_run_date", "pnl_date")
        .withColumn(
            "market_value_limit_breached",
            F.abs(F.col("total_market_value_usd"))
            > F.col("max_market_value_usd"),
        )
        .withColumn(
            "daily_loss_limit_breached",
            F.col("total_unrealized_pnl_usd")
            < -F.col("max_daily_loss_usd"),
        )
    )

    base_rejections = rejected_record(
        base_rejected,
        source_name=SOURCE_PORTFOLIO,
        run_date=run_date,
        identifier_column=F.col("portfolio_id"),
        raw_record_column=F.to_json(
            F.struct(
                "portfolio_id",
                "symbol",
                "position_quantity",
                "position_currency",
                "average_cost",
            )
        ),
        rejection_rule_column=F.col("rejection_rule"),
        rejection_reason_column=F.col("rejection_reason"),
    )

    market_rejections = rejected_record(
        market_rejected_source,
        source_name=SOURCE_PORTFOLIO,
        run_date=run_date,
        identifier_column=F.concat_ws(
            ":",
            F.col("portfolio_id"),
            F.col("normalized_symbol"),
        ),
        raw_record_column=F.to_json(
            F.struct(
                "portfolio_id",
                "symbol",
                "position_quantity",
                "position_currency",
                "average_cost",
            )
        ),
        rejection_rule_column=F.col("market_rejection_rule"),
        rejection_reason_column=F.col("rejection_reason"),
    )

    rejected = base_rejections.unionByName(
        market_rejections,
        allowMissingColumns=True,
    )

    return portfolio_pnl, rejected


# ---------------------------------------------------------------------
# Trade P&L
# ---------------------------------------------------------------------


def calculate_trade_pnl(
    trades: DataFrame,
    market_data: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    market_lookup = market_data.select(
        F.col("normalized_symbol").alias("market_symbol"),
        F.col("price_date"),
        F.col("close_price"),
        F.col("currency").alias("market_currency"),
        F.col("effective_fx_rate_to_usd").alias("fx_rate_to_usd"),
    )

    joined = trades.join(
        market_lookup,
        (
            F.col("symbol") == F.col("market_symbol")
        )
        & (
            F.col("trade_date") == F.col("price_date")
        ),
        "left",
    )

    enriched = (
        joined
        .withColumn(
            "side_multiplier",
            F.when(
                F.col("side") == SIDE_BUY,
                F.lit(DECIMAL_ONE),
            )
            .when(
                F.col("side") == SIDE_SELL,
                F.lit(Decimal("-1")),
            )
            .cast(DecimalType(2, 0)),
        )
        .withColumn(
            "pnl_unresolvable",
            F.col("close_price").isNull()
            | F.col("fx_rate_to_usd").isNull()
            | (F.col("fx_rate_to_usd") <= 0),
        )
        .withColumn(
            "realized_pnl_local",
            F.when(
                ~F.col("pnl_unresolvable"),
                (
                    (
                        F.col("close_price")
                        - F.col("average_cost")
                    )
                    * F.col("quantity")
                    * F.col("side_multiplier")
                ).cast(MONEY_TYPE),
            ),
        )
        .withColumn(
            "realized_pnl_usd",
            F.when(
                ~F.col("pnl_unresolvable"),
                (
                    (
                        F.col("close_price")
                        - F.col("average_cost")
                    )
                    * F.col("quantity")
                    * F.col("side_multiplier")
                    * F.col("fx_rate_to_usd")
                ).cast(MONEY_TYPE),
            ),
        )
    )

    clean_output = enriched.select(
        "order_id",
        "portfolio_id",
        "symbol",
        "side",
        "quantity",
        "trade_price",
        "average_cost",
        "currency",
        "transact_time",
        "trade_date",
        "close_price",
        "market_currency",
        "fx_rate_to_usd",
        "realized_pnl_local",
        "realized_pnl_usd",
        "pnl_unresolvable",
    )

    unresolvable = (
        clean_output
        .filter(F.col("pnl_unresolvable"))
        .withColumn(
            "unresolvable_reason",
            F.lit(
                "No valid same-date market price or FX rate "
                "was available."
            ),
        )
    )

    return clean_output, unresolvable


# ---------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------


def collect_rejection_counts(
    rejected: DataFrame,
) -> list[tuple[str, str, int]]:
    """
    Count rejections once, grouped by source and rule.

    Returning the raw (source, rule, count) triples lets the caller
    derive both the per-rule and the per-source breakdown from a
    single Spark action.
    """
    rows = (
        rejected
        .groupBy("source_name", "rejection_rule")
        .count()
        .collect()
    )

    return [
        (
            row["source_name"],
            row["rejection_rule"],
            int(row["count"]),
        )
        for row in rows
    ]


def build_metrics(
    *,
    run_date: str,
    total_trade_count: int,
    clean_trade_count: int,
    rejected: DataFrame,
    unresolvable_count: int,
    total_realized_pnl: Decimal | None,
    portfolio_count: int,
) -> dict[str, Any]:
    rejection_counts = collect_rejection_counts(rejected)

    rejection_count_by_rule: dict[str, int] = {}
    rejection_count_by_source: dict[str, int] = {}

    for source_name, rejection_rule, count in rejection_counts:
        rejection_count_by_rule[rejection_rule] = (
            rejection_count_by_rule.get(rejection_rule, 0) + count
        )
        rejection_count_by_source[source_name] = (
            rejection_count_by_source.get(source_name, 0) + count
        )

    total_rejection_count = sum(rejection_count_by_rule.values())

    trade_rejection_count = rejection_count_by_source.get(
        SOURCE_TRADES,
        0,
    )

    # The gate measures trade quality, so only trade rejections may
    # appear in the numerator. Mixing in market-data and portfolio
    # rejections would inflate the rate against a trade-only
    # denominator and would not match the equivalent warehouse view.
    trade_reject_rate = (
        trade_rejection_count / total_trade_count
        if total_trade_count
        else 0.0
    )

    # Reported separately because it is a useful operational signal,
    # but it is deliberately not what the gate is evaluated against.
    overall_reject_rate = (
        total_rejection_count / total_trade_count
        if total_trade_count
        else 0.0
    )

    gate_passed = (
        trade_reject_rate < TRADE_REJECT_RATE_THRESHOLD
        and unresolvable_count < PNL_UNRESOLVABLE_THRESHOLD
    )

    return {
        "pipeline_name": "fincore_daily_pipeline",
        "run_date": run_date,
        "total_trade_count": total_trade_count,
        "clean_trade_count": clean_trade_count,
        "trade_rejection_count": trade_rejection_count,
        "total_rejection_count": total_rejection_count,
        "rejection_count_by_rule": rejection_count_by_rule,
        "rejection_count_by_source": rejection_count_by_source,
        "trade_reject_rate": round(trade_reject_rate, 6),
        "overall_reject_rate": round(overall_reject_rate, 6),
        "pnl_unresolvable_count": unresolvable_count,
        "total_realized_pnl_usd": str(
            total_realized_pnl or DECIMAL_ZERO
        ),
        "portfolio_pnl_count": portfolio_count,
        "quality_gate": {
            "trade_reject_rate": round(trade_reject_rate, 6),
            "trade_reject_rate_threshold": (
                TRADE_REJECT_RATE_THRESHOLD
            ),
            "pnl_unresolvable_count": unresolvable_count,
            "pnl_unresolvable_threshold": (
                PNL_UNRESOLVABLE_THRESHOLD
            ),
            "passed": gate_passed,
        },
    }


def write_metrics_json(
    spark: SparkSession,
    metrics: dict[str, Any],
    output_path: str,
) -> None:
    payload = json.dumps(
        metrics,
        sort_keys=True,
    )

    (
        spark.createDataFrame(
            [(payload,)],
            StructType(
                [
                    StructField(
                        "value",
                        StringType(),
                        False,
                    )
                ]
            ),
        )
        .coalesce(1)
        .write
        .mode("overwrite")
        .text(output_path)
    )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s - %(message)s"
        ),
    )

    args = parse_args()
    run_date = validate_run_date(args.run_date)

    spark = create_spark_session(args)

    partition = f"dt={run_date.isoformat()}"

    trades_input = (
        f"s3a://{args.raw_bucket}/trades/{partition}/"
    )
    market_input = (
        f"s3a://{args.raw_bucket}/market_data/{partition}/"
    )
    portfolio_input = (
        f"s3a://{args.raw_bucket}/portfolio/{partition}/"
    )

    trades_output = (
        f"s3a://{args.processed_bucket}/"
        f"processed/trades/{partition}/"
    )
    portfolio_output = (
        f"s3a://{args.processed_bucket}/"
        f"processed/portfolio_pnl/{partition}/"
    )
    unresolvable_output = (
        f"s3a://{args.processed_bucket}/"
        f"processed/unresolvable_pnl/{partition}/"
    )
    rejected_output = (
        f"s3a://{args.processed_bucket}/"
        f"rejected/{partition}/"
    )
    metrics_output = (
        f"s3a://{args.processed_bucket}/"
        f"metrics/{partition}/"
    )

    try:
        LOGGER.info("Starting FinCore ETL for %s", run_date)

        valid_trades, trade_rejections = parse_fix_trades(
            spark,
            trades_input,
            run_date.isoformat(),
        )

        market_data, market_rejections = process_market_data(
            spark,
            market_input,
            run_date.isoformat(),
        )

        portfolio_pnl, portfolio_rejections = process_portfolios(
            spark,
            portfolio_input,
            run_date.isoformat(),
            market_data,
        )

        clean_trades, unresolvable_trades = calculate_trade_pnl(
            valid_trades,
            market_data,
        )

        all_rejections = union_dataframes(
            [
                trade_rejections,
                market_rejections,
                portfolio_rejections,
            ]
        ).cache()

        clean_trades = clean_trades.cache()
        unresolvable_trades = unresolvable_trades.cache()
        portfolio_pnl = portfolio_pnl.cache()

        total_trade_count = (
            valid_trades.count()
            + trade_rejections.count()
        )
        clean_trade_count = clean_trades.count()
        unresolvable_count = unresolvable_trades.count()
        portfolio_count = portfolio_pnl.count()

        total_realized_row = (
            clean_trades
            .filter(~F.col("pnl_unresolvable"))
            .agg(
                F.sum("realized_pnl_usd").alias(
                    "total_realized_pnl_usd"
                )
            )
            .first()
        )

        total_realized_pnl = (
            total_realized_row["total_realized_pnl_usd"]
            if total_realized_row
            else DECIMAL_ZERO
        )

        metrics = build_metrics(
            run_date=run_date.isoformat(),
            total_trade_count=total_trade_count,
            clean_trade_count=clean_trade_count,
            rejected=all_rejections,
            unresolvable_count=unresolvable_count,
            total_realized_pnl=total_realized_pnl,
            portfolio_count=portfolio_count,
        )

        LOGGER.info("Writing clean trades to %s", trades_output)
        (
            clean_trades
            .write
            .mode("overwrite")
            .parquet(trades_output)
        )

        LOGGER.info(
            "Writing portfolio P&L to %s",
            portfolio_output,
        )
        (
            portfolio_pnl
            .write
            .mode("overwrite")
            .parquet(portfolio_output)
        )

        LOGGER.info(
            "Writing unresolvable P&L to %s",
            unresolvable_output,
        )
        (
            unresolvable_trades
            .write
            .mode("overwrite")
            .parquet(unresolvable_output)
        )

        LOGGER.info(
            "Writing rejected records to %s",
            rejected_output,
        )
        (
            all_rejections
            .write
            .mode("overwrite")
            .parquet(rejected_output)
        )

        LOGGER.info("Writing metrics to %s", metrics_output)
        write_metrics_json(
            spark,
            metrics,
            metrics_output,
        )

        LOGGER.info(
            "FinCore ETL completed successfully: %s",
            json.dumps(metrics, indent=2),
        )

    except Exception:
        LOGGER.exception(
            "FinCore ETL failed for run date %s",
            run_date,
        )
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()