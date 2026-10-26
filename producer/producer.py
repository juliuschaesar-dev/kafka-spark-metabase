"""Polls CoinGecko /coins/markets and publishes one Avro record per coin to Kafka."""
import json
import logging
import os
import random
import signal
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

API_PARAMS = {"vs_currency": "usd", "per_page": 250, "page": 1}
SCHEMA = Path(__file__).with_name("coingecko_market.avsc").read_text()
FIELDS = tuple(field["name"] for field in json.loads(SCHEMA)["fields"])

MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 5
BACKOFF_MAX_SECONDS = 120
REQUEST_TIMEOUT_SECONDS = 30

log = logging.getLogger("coingecko-producer")
stop = threading.Event()


def require_env(name):
    value = os.environ.get(name)
    if not value:
        log.error("Environment variable %s is required", name)
        sys.exit(1)
    return value


def backoff_delay(attempt):
    delay = min(BACKOFF_BASE_SECONDS * 2 ** attempt, BACKOFF_MAX_SECONDS)
    return delay + random.uniform(0, delay / 4)


def fetch_markets(session, api_url, api_key):
    """Returns the list of coins, or None if every retry failed.

    Retries count toward the CoinGecko quota, so the attempt count is kept small
    and a Retry-After header is honoured when the API sends one.
    """
    headers = {"accept": "application/json"}
    if api_key:
        headers["x-cg-demo-api-key"] = api_key
    for attempt in range(MAX_RETRIES + 1):
        delay = None
        try:
            resp = session.get(
                api_url,
                params=API_PARAMS,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429 or resp.status_code >= 500:
                retry_after = resp.headers.get("Retry-After", "")
                delay = float(retry_after) if retry_after.isdigit() else backoff_delay(attempt)
                log.warning("HTTP %s from CoinGecko (attempt %d/%d)", resp.status_code, attempt + 1, MAX_RETRIES + 1)
            else:
                # 4xx other than 429 (bad/missing key, etc.): retrying will not help.
                log.error("HTTP %s from CoinGecko, not retrying: %s", resp.status_code, resp.text[:200])
                return None
        except requests.RequestException as exc:
            delay = backoff_delay(attempt)
            log.warning("Network error: %s (attempt %d/%d)", exc, attempt + 1, MAX_RETRIES + 1)

        if attempt == MAX_RETRIES:
            break
        log.info("Retrying in %.0fs", delay)
        if stop.wait(delay):
            return None
    log.error("Giving up on this poll after %d attempts", MAX_RETRIES + 1)
    return None


def to_record(coin):
    record = {field: coin.get(field) for field in FIELDS}
    record["last_updated"] = record["last_updated"] or datetime.now(timezone.utc).isoformat()
    record["currency"] = API_PARAMS["vs_currency"].upper()
    return record


def delivery_report(err, msg):
    if err is not None:
        log.error("Delivery failed for key %s: %s", msg.key(), err)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    api_key = os.environ.get("COINGECKO_API_KEY", "")
    if not api_key:
        log.warning("COINGECKO_API_KEY not set; calling CoinGecko without a key (stricter rate limits)")
    api_url = require_env("COINGECKO_API_URL")
    bootstrap = require_env("KAFKA_BOOTSTRAP_SERVERS")
    registry_url = require_env("SCHEMA_REGISTRY_URL")
    topic = require_env("TOPIC")
    interval = int(require_env("POLL_INTERVAL_SECONDS"))

    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    registry = SchemaRegistryClient({"url": registry_url})
    producer = SerializingProducer({
        "bootstrap.servers": bootstrap,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": AvroSerializer(registry, SCHEMA),
        "enable.idempotence": True,
    })

    session = requests.Session()
    last_seen = {}  # coin id -> last_updated already published
    log.info("Polling %s every %ds -> topic %s", api_url, interval, topic)

    while not stop.is_set():
        coins = fetch_markets(session, api_url, api_key)
        if coins is not None:
            sent = 0
            for coin in coins:
                record = to_record(coin)
                if last_seen.get(record["id"]) == record["last_updated"]:
                    continue  # unchanged since last poll; avoids duplicate PKs in history mode
                producer.produce(topic=topic, key=record["id"], value=record, on_delivery=delivery_report)
                producer.poll(0)
                last_seen[record["id"]] = record["last_updated"]
                sent += 1
            producer.flush(30)
            log.info("Fetched %d coins, published %d changed", len(coins), sent)
        stop.wait(interval)

    log.info("Shutting down, flushing pending messages")
    producer.flush(30)


if __name__ == "__main__":
    main()
