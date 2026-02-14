#!/usr/bin/env python3
"""
Post sample trades to the Trade Store API.

Usage:
    python scripts/post_sample_trades.py
    python scripts/post_sample_trades.py --base-url http://localhost:8000 --count 10
    python scripts/post_sample_trades.py --prefix DEMO --start 101 --book-id BOOK-2
"""

import argparse
import json
from datetime import date, timedelta
from urllib import error, request


def build_trade(index: int, prefix: str, book_id: str, counter_party_prefix: str) -> dict:
    trade_id = f"{prefix}{index:04d}"
    today = date.today()

    return {
        "trade_id": trade_id,
        "version": 4,
        "counter_party_id": f"{counter_party_prefix}-{index:04d}",
        "book_id": book_id,
        "maturity_date": str(today + timedelta(days=30 + index)),
        "created_date": str(today),
        "expired": False,
    }


def post_trade(base_url: str, trade: dict, timeout: float) -> tuple[int, str]:
    url = f"{base_url.rstrip('/')}/trades"
    payload = json.dumps(trade).encode("utf-8")

    req = request.Request(
        url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )

    try:
        with request.urlopen(req, timeout=timeout) as response:
            status_code = response.status
            body = response.read().decode("utf-8")
            return status_code, body
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, body


def main() -> None:
    parser = argparse.ArgumentParser(description="Post sample trades to Trade Store API")
    parser.add_argument("--base-url", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--count", type=int, default=1, help="Number of trades to post")
    parser.add_argument("--start", type=int, default=1, help="Starting numeric index for trade IDs")
    parser.add_argument("--prefix", default="T", help="Trade ID prefix")
    parser.add_argument("--book-id", default="BOOK-1", help="Book ID value")
    parser.add_argument(
        "--counter-party-prefix",
        default="CP",
        help="Counter-party prefix (result: <prefix>-NNNN)",
    )
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP request timeout in seconds")
    args = parser.parse_args()

    if args.count < 1:
        raise SystemExit("--count must be >= 1")
    if args.start < 1:
        raise SystemExit("--start must be >= 1")

    success_count = 0
    failed_count = 0

    for index in range(args.start, args.start + args.count):
        trade = build_trade(index, args.prefix, args.book_id, args.counter_party_prefix)
        status_code, body = post_trade(args.base_url, trade, args.timeout)

        is_success = status_code == 202
        if is_success:
            success_count += 1
        else:
            failed_count += 1

        print(f"[{status_code}] {trade['trade_id']} v{trade['version']}")
        print(body)

    print("\nSummary")
    print(f"  Sent:     {args.count}")
    print(f"  Accepted: {success_count}")
    print(f"  Failed:   {failed_count}")


if __name__ == "__main__":
    main()
