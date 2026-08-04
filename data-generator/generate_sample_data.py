from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any


MONEY_SCALE = Decimal("0.0001")
PRICE_SCALE = Decimal("0.00000001")
QUANTITY_SCALE = Decimal("0.0001")


INSTRUMENTS = [
    {
        "symbol": "AAPL",
        "asset_class": "EQUITY",
        "currency": "USD",
        "close_price": Decimal("225.75000000"),
        "fx_rate_to_usd": Decimal("1.0000000000"),
    },
    {
        "symbol": "MSFT",
        "asset_class": "EQUITY",
        "currency": "USD",
        "close_price": Decimal("418.26000000"),
        "fx_rate_to_usd": Decimal("1.0000000000"),
    },
    {
        "symbol": "NVDA",
        "asset_class": "EQUITY",
        "currency": "USD",
        "close_price": Decimal("117.93000000"),
        "fx_rate_to_usd": Decimal("1.0000000000"),
    },
    {
        "symbol": "VOD.L",
        "asset_class": "EQUITY",
        "currency": "GBP",
        "close_price": Decimal("0.74500000"),
        "fx_rate_to_usd": Decimal("1.2850000000"),
    },
    {
        "symbol": "SAP.DE",
        "asset_class": "EQUITY",
        "currency": "EUR",
        "close_price": Decimal("197.80000000"),
        "fx_rate_to_usd": Decimal("1.0900000000"),
    },
    {
        "symbol": "US10Y",
        "asset_class": "BOND",
        "currency": "USD",
        "close_price": Decimal("98.37500000"),
        "fx_rate_to_usd": Decimal("1.0000000000"),
    },
    {
        "symbol": "UK10Y",
        "asset_class": "BOND",
        "currency": "GBP",
        "close_price": Decimal("96.62500000"),
        "fx_rate_to_usd": Decimal("1.2850000000"),
    },
    {
        "symbol": "DE10Y",
        "asset_class": "BOND",
        "currency": "EUR",
        "close_price": Decimal("101.12500000"),
        "fx_rate_to_usd": Decimal("1.0900000000"),
    },
]


def decimal_string(value: Decimal, scale: Decimal) -> str:
    return str(value.quantize(scale, rounding=ROUND_HALF_UP))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate deterministic FinCore sample source feeds."
    )
    parser.add_argument(
        "--run-date",
        required=True,
        help="Trading date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/generated",
        help="Base output directory.",
    )
    parser.add_argument(
        "--trade-count",
        type=int,
        default=1000,
        help="Number of FIX trade records to generate.",
    )
    parser.add_argument(
        "--portfolio-count",
        type=int,
        default=20,
        help="Number of portfolios to generate.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for deterministic output.",
    )
    return parser.parse_args()


def validate_run_date(raw_value: str) -> date:
    try:
        return date.fromisoformat(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"Invalid run date {raw_value!r}; expected YYYY-MM-DD."
        ) from exc


def build_output_paths(base_dir: Path, run_date: date) -> dict[str, Path]:
    partition = f"dt={run_date.isoformat()}"

    paths = {
        "trades": base_dir / "trades" / partition,
        "market_data": base_dir / "market_data" / partition,
        "portfolio": base_dir / "portfolio" / partition,
    }

    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)

    return paths


def random_decimal(
    rng: random.Random,
    minimum: Decimal,
    maximum: Decimal,
    scale: Decimal,
) -> Decimal:
    factor = Decimal(str(rng.random()))
    value = minimum + ((maximum - minimum) * factor)
    return value.quantize(scale, rounding=ROUND_HALF_UP)


def build_fix_line(fields: list[tuple[str, str]]) -> str:
    return "|".join(f"{tag}={value}" for tag, value in fields) + "|"


def generate_trades(
    output_path: Path,
    run_date: date,
    trade_count: int,
    rng: random.Random,
) -> dict[str, int]:
    output_file = output_path / "trades.fix"

    malformed_count = max(1, int(trade_count * 0.009))
    unresolvable_count = min(20, max(1, int(trade_count * 0.015)))

    clean_count = trade_count - malformed_count
    missing_symbol_start = clean_count - unresolvable_count

    malformed_lines = [
        "8=FIX.4.4|35=D|11=BAD-MISSING-SYMBOL|54=1|38=100|",
        "8=FIX.4.4|35=D|11=BAD-SIDE|55=AAPL|54=9|38=100|44=220.00|",
        "8=FIX.4.4|35=D|11=BAD-TIME|55=MSFT|54=2|38=25|60=INVALID|",
        "THIS IS NOT A FIX MESSAGE",
        "8=FIX.4.4|35=D|11=BAD-QUANTITY|55=NVDA|54=1|38=-10|44=110|",
        "8=FIX.4.4|35=D|11=BAD-PRICE|55=AAPL|54=1|38=10|44=-2|",
        "8=FIX.4.4|35=D|11=BAD-NO-ORDER|55=AAPL|54=1|38=10|44=220|",
        "8=FIX.4.4|35=D|11=BAD-CURRENCY|55=VOD.L|54=1|38=10|15=XYZ|",
        "8=FIX.4.4|35=D|11=BAD-EMPTY||55=AAPL|54=1|",
    ]

    with output_file.open("w", encoding="utf-8", newline="\n") as handle:
        for index in range(clean_count):
            portfolio_id = f"PORT{(index % 20) + 1:04d}"

            if index >= missing_symbol_start:
                instrument = {
                    "symbol": f"MISSING{index - missing_symbol_start + 1:03d}",
                    "currency": "USD",
                    "close_price": Decimal("100.00000000"),
                }
            else:
                instrument = rng.choice(INSTRUMENTS)

            side = rng.choice(["1", "2"])
            quantity = random_decimal(
                rng,
                Decimal("1"),
                Decimal("1000"),
                QUANTITY_SCALE,
            )

            average_cost = (
                instrument["close_price"]
                * random_decimal(
                    rng,
                    Decimal("0.90"),
                    Decimal("1.10"),
                    PRICE_SCALE,
                )
            ).quantize(PRICE_SCALE)

            trade_price = (
                average_cost
                * random_decimal(
                    rng,
                    Decimal("0.995"),
                    Decimal("1.005"),
                    PRICE_SCALE,
                )
            ).quantize(PRICE_SCALE)

            seconds_after_open = rng.randint(0, (8 * 60 * 60) - 1)
            trade_time = datetime.combine(
                run_date,
                time(9, 30),
            ) + timedelta(seconds=seconds_after_open)

            fields = [
                ("8", "FIX.4.4"),
                ("35", "D"),
                ("49", "FINCORE"),
                ("56", "BROKER"),
                ("34", str(index + 1)),
                ("11", f"ORD-{run_date:%Y%m%d}-{index + 1:07d}"),
                ("1", portfolio_id),
                ("55", instrument["symbol"]),
                ("54", side),
                ("38", decimal_string(quantity, QUANTITY_SCALE)),
                ("44", decimal_string(trade_price, PRICE_SCALE)),
                ("6", decimal_string(average_cost, PRICE_SCALE)),
                ("15", instrument["currency"]),
                ("60", trade_time.strftime("%Y%m%d-%H:%M:%S")),
            ]

            handle.write(build_fix_line(fields) + "\n")

        for index in range(malformed_count):
            handle.write(
                malformed_lines[index % len(malformed_lines)] + "\n"
            )

    return {
        "total_trades": trade_count,
        "expected_malformed_trades": malformed_count,
        "expected_pnl_unresolvable": unresolvable_count,
    }


def market_row(
    instrument: dict[str, Any],
    run_date: date,
    loaded_at: str,
) -> dict[str, str]:
    return {
        "symbol": instrument["symbol"],
        "asset_class": instrument["asset_class"],
        "close_price": decimal_string(
            instrument["close_price"],
            PRICE_SCALE,
        ),
        "currency": instrument["currency"],
        "fx_rate_to_usd": decimal_string(
            instrument["fx_rate_to_usd"],
            Decimal("0.0000000001"),
        ),
        "price_date": run_date.isoformat(),
        "loaded_at": loaded_at,
    }


def generate_market_data(
    output_path: Path,
    run_date: date,
) -> dict[str, int]:
    loaded_at = datetime.combine(
        run_date,
        time(17, 45),
        tzinfo=timezone.utc,
    ).isoformat()

    fieldnames = [
        "symbol",
        "asset_class",
        "close_price",
        "currency",
        "fx_rate_to_usd",
        "price_date",
        "loaded_at",
    ]

    grouped: dict[str, list[dict[str, str]]] = {
        "equities": [],
        "bonds": [],
        "fx": [],
    }

    for instrument in INSTRUMENTS:
        row = market_row(instrument, run_date, loaded_at)

        if instrument["asset_class"] == "EQUITY":
            grouped["equities"].append(row)
        else:
            grouped["bonds"].append(row)

    grouped["fx"].extend(
        [
            {
                "symbol": "GBPUSD",
                "asset_class": "FX",
                "close_price": "1.28500000",
                "currency": "USD",
                "fx_rate_to_usd": "1.0000000000",
                "price_date": run_date.isoformat(),
                "loaded_at": loaded_at,
            },
            {
                "symbol": "EURUSD",
                "asset_class": "FX",
                "close_price": "1.09000000",
                "currency": "USD",
                "fx_rate_to_usd": "1.0000000000",
                "price_date": run_date.isoformat(),
                "loaded_at": loaded_at,
            },
            {
                "symbol": "NULLPX",
                "asset_class": "FX",
                "close_price": "",
                "currency": "USD",
                "fx_rate_to_usd": "1.0000000000",
                "price_date": run_date.isoformat(),
                "loaded_at": loaded_at,
            },
            {
                "symbol": "NEGPX",
                "asset_class": "FX",
                "close_price": "-10.00000000",
                "currency": "USD",
                "fx_rate_to_usd": "1.0000000000",
                "price_date": run_date.isoformat(),
                "loaded_at": loaded_at,
            },
        ]
    )

    for filename, rows in grouped.items():
        with (output_path / f"{filename}.csv").open(
            "w",
            encoding="utf-8",
            newline="",
        ) as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    return {
        "market_rows": sum(len(rows) for rows in grouped.values()),
        "expected_null_prices": 1,
        "expected_non_positive_prices": 1,
    }


def generate_portfolios(
    output_path: Path,
    run_date: date,
    portfolio_count: int,
    rng: random.Random,
) -> dict[str, int]:
    portfolios: list[dict[str, Any]] = []
    total_positions = 0

    loaded_at = datetime.combine(
        run_date,
        time(18, 0),
        tzinfo=timezone.utc,
    ).isoformat()

    for portfolio_number in range(1, portfolio_count + 1):
        position_count = rng.randint(4, 7)
        positions: list[dict[str, Any]] = []

        selected = rng.sample(
            INSTRUMENTS,
            k=min(position_count, len(INSTRUMENTS)),
        )

        for instrument in selected:
            quantity = random_decimal(
                rng,
                Decimal("-1500"),
                Decimal("2000"),
                QUANTITY_SCALE,
            )

            if quantity == 0:
                quantity = Decimal("10.0000")

            average_cost = (
                instrument["close_price"]
                * random_decimal(
                    rng,
                    Decimal("0.85"),
                    Decimal("1.15"),
                    PRICE_SCALE,
                )
            ).quantize(PRICE_SCALE)

            positions.append(
                {
                    "symbol": instrument["symbol"],
                    "position_quantity": decimal_string(
                        quantity,
                        QUANTITY_SCALE,
                    ),
                    "currency": instrument["currency"],
                    "average_cost": decimal_string(
                        average_cost,
                        PRICE_SCALE,
                    ),
                }
            )
            total_positions += 1

        portfolios.append(
            {
                "portfolio_id": f"PORT{portfolio_number:04d}",
                "portfolio_name": f"Institutional Portfolio {portfolio_number}",
                "base_currency": "USD",
                "positions": positions,
                "metadata": {
                    "risk_limits": {
                        "max_market_value_usd": decimal_string(
                            Decimal("5000000")
                            + Decimal(portfolio_number * 100000),
                            MONEY_SCALE,
                        ),
                        "max_daily_loss_usd": decimal_string(
                            Decimal("250000")
                            + Decimal(portfolio_number * 5000),
                            MONEY_SCALE,
                        ),
                        "max_position_concentration_pct": "20.0000",
                    },
                    "source_system": "portfolio_state_export",
                    "loaded_at": loaded_at,
                },
            }
        )

    output_file = output_path / "portfolio_state.json"

    with output_file.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "run_date": run_date.isoformat(),
                "portfolios": portfolios,
            },
            handle,
            indent=2,
        )

    return {
        "portfolio_count": portfolio_count,
        "position_count": total_positions,
    }


def write_manifest(
    base_dir: Path,
    run_date: date,
    metrics: dict[str, int],
) -> None:
    manifest_path = base_dir / f"manifest_{run_date.isoformat()}.json"

    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "run_date": run_date.isoformat(),
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "expected_metrics": metrics,
            },
            handle,
            indent=2,
        )


def main() -> None:
    args = parse_args()
    run_date = validate_run_date(args.run_date)
    rng = random.Random(args.seed)

    base_dir = Path(args.output_dir).resolve()
    paths = build_output_paths(base_dir, run_date)

    metrics: dict[str, int] = {}
    metrics.update(
        generate_trades(
            paths["trades"],
            run_date,
            args.trade_count,
            rng,
        )
    )
    metrics.update(generate_market_data(paths["market_data"], run_date))
    metrics.update(
        generate_portfolios(
            paths["portfolio"],
            run_date,
            args.portfolio_count,
            rng,
        )
    )

    write_manifest(base_dir, run_date, metrics)

    print("FinCore sample data generated successfully.")
    print(f"Run date: {run_date}")
    print(f"Output directory: {base_dir}")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()