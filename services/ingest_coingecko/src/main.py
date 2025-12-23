import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import requests
from kafka import KafkaProducer


def env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing env var: {name}")
    return value


def build_headers(api_key: str) -> Dict[str, str]:
    # As per CoinGecko docs: required header x-cg-demo-api-key
    return {
        "accept": "application/json",
        "x-cg-demo-api-key": api_key,
        "user-agent": "crypto-streaming-platform/1.0",
    }


def get_coins_markets(
    base_url: str,
    api_key: str,
    vs_currency: str,
    order: str,
    per_page: int,
    page: int,
    sparkline: bool,
    price_change_percentage: str,
    coin_ids: Optional[str] = None,
) -> List[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order": order,
        "per_page": per_page,
        "page": page,
        "sparkline": str(sparkline).lower(),
        "price_change_percentage": price_change_percentage,  # e.g. "24h"
    }
    if coin_ids:
        params["ids"] = coin_ids

    resp = requests.get(url, params=params, headers=build_headers(api_key), timeout=20)
    resp.raise_for_status()
    return resp.json()


def make_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[s.strip() for s in bootstrap_servers.split(",") if s.strip()],
        acks="all",
        retries=3,
        linger_ms=10,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
    )


def main() -> None:
    # Kafka config
    kafka_bootstrap = env("KAFKA_BOOTSTRAP_SERVERS")
    topic = env("KAFKA_TOPIC", "crypto_quotes_raw")
    poll_seconds = int(env("POLL_SECONDS", "60"))

    # CoinGecko config
    base_url = env("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
    api_key = env("COINGECKO_API_KEY")  # required by docs

    vs_currency = env("VS_CURRENCY", "usd")
    order = env("ORDER", "market_cap_desc")
    per_page = int(env("PER_PAGE", "50"))
    page = int(env("PAGE", "1"))
    price_change_percentage = env("PRICE_CHANGE_PERCENTAGE", "24h")
    coin_ids = os.getenv("COIN_IDS", "").strip() or None

    producer = make_kafka_producer(kafka_bootstrap)

    print(f"[START] Polling CoinGecko /coins/markets every {poll_seconds}s, vs_currency={vs_currency}, topic={topic}")

    while True:
        ingestion_run_id = str(uuid.uuid4())
        event_time = datetime.now(timezone.utc).isoformat()

        try:
            coins = get_coins_markets(
                base_url=base_url,
                api_key=api_key,
                vs_currency=vs_currency,
                order=order,
                per_page=per_page,
                page=page,
                sparkline=False,
                price_change_percentage=price_change_percentage,
                coin_ids=coin_ids,
            )

            sent = 0
            for coin in coins:
                # Align with documented response keys
                event = {
                    "event_time": event_time,
                    "ingestion_run_id": ingestion_run_id,
                    "source": "coingecko",
                    "vs_currency": vs_currency,

                    "coin_id": coin.get("id"),
                    "symbol": coin.get("symbol"),
                    "name": coin.get("name"),

                    "price_usd": coin.get("current_price"),
                    "market_cap_usd": coin.get("market_cap"),
                    "total_volume_usd": coin.get("total_volume"),
                    "price_change_percentage_24h": coin.get("price_change_percentage_24h"),

                    # optional metadata from API (useful later)
                    "market_cap_rank": coin.get("market_cap_rank"),
                    "last_updated": coin.get("last_updated"),
                }

                key = (event["coin_id"] or event["symbol"] or "unknown")
                producer.send(topic, key=key, value=event)
                sent += 1

            producer.flush()
            print(f"[OK] {event_time} run_id={ingestion_run_id} sent={sent} topic={topic}")

        except Exception as e:
            print(f"[ERROR] run_id={ingestion_run_id} {type(e).__name__}: {e}")

        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
