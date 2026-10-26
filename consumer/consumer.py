"""Structured Streaming job: Kafka (Confluent Avro) -> PostgreSQL.

Each micro-batch is written with Spark's JDBC writer to a staging table, then merged into the
target tables in one transaction:
  coingecko_markets          upsert, one row per coin (latest last_updated wins)
  coingecko_markets_history  insert, one row per (id, last_updated); duplicates are ignored
"""
import json
import logging
import os
import sys
import time
import urllib.request

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.avro.functions import from_avro

log = logging.getLogger("consumer")

COLUMNS = [
    "id", "symbol", "name", "currency",
    "current_price", "market_cap", "total_volume",
    "market_cap_rank", "fully_diluted_valuation", "high_24h", "low_24h",
    "price_change_24h", "price_change_percentage_24h",
    "market_cap_change_24h", "market_cap_change_percentage_24h",
    "last_updated",
]
UPSERT_TABLE = "coingecko_markets"
HISTORY_TABLE = "coingecko_markets_history"
STAGING_TABLE = "coingecko_markets_staging"

DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    id            text NOT NULL,
    symbol        text NOT NULL,
    name          text NOT NULL,
    currency      text,
    current_price double precision,
    market_cap    double precision,
    total_volume  double precision,
    market_cap_rank                   integer,
    fully_diluted_valuation           double precision,
    high_24h                          double precision,
    low_24h                           double precision,
    price_change_24h                  double precision,
    price_change_percentage_24h       double precision,
    market_cap_change_24h             double precision,
    market_cap_change_percentage_24h  double precision,
    last_updated  text NOT NULL,
    PRIMARY KEY ({pk})
)
"""

COLS = ", ".join(COLUMNS)

UPSERT_SQL = f"""
INSERT INTO {UPSERT_TABLE} ({COLS})
SELECT DISTINCT ON (id) {COLS} FROM {STAGING_TABLE} ORDER BY id, last_updated DESC
ON CONFLICT (id) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS if c != "id")}
WHERE EXCLUDED.last_updated >= {UPSERT_TABLE}.last_updated
"""

HISTORY_SQL = f"""
INSERT INTO {HISTORY_TABLE} ({COLS})
SELECT {COLS} FROM {STAGING_TABLE}
ON CONFLICT (id, last_updated) DO NOTHING
"""


def require_env(name):
    value = os.environ.get(name)
    if not value:
        log.error("Environment variable %s is required", name)
        sys.exit(1)
    return value


def fetch_schema(registry_url, subject):
    """Latest value schema; the producer registers it on its first poll, so wait for it."""
    url = f"{registry_url}/subjects/{subject}/versions/latest"
    for _ in range(60):
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                return json.load(resp)["schema"]
        except OSError:
            log.info("Schema %s not available yet, retrying", subject)
            time.sleep(5)
    log.error("Schema %s never appeared in Schema Registry", subject)
    sys.exit(1)


def run_sql(spark, db, statements):
    """Runs the statements in one transaction over the JVM's JDBC driver."""
    conn = spark._jvm.java.sql.DriverManager.getConnection(db["url"], db["user"], db["password"])
    try:
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        for sql in statements:
            stmt.execute(sql)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bootstrap = require_env("KAFKA_BOOTSTRAP_SERVERS")
    registry_url = require_env("SCHEMA_REGISTRY_URL")
    topic = require_env("TOPIC")
    db = {
        "url": f"jdbc:postgresql://{require_env('POSTGRES_HOST')}:5432/{require_env('POSTGRES_DB')}",
        "user": require_env("POSTGRES_USER"),
        "password": require_env("POSTGRES_PASSWORD"),
    }
    checkpoint = os.environ.get("CHECKPOINT_DIR", "/checkpoint")

    schema = fetch_schema(registry_url, f"{topic}-value")
    spark = SparkSession.builder.appName("coingecko-consumer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    run_sql(spark, db, [
        DDL.format(table=UPSERT_TABLE, pk="id"),
        DDL.format(table=HISTORY_TABLE, pk="id, last_updated"),
    ])

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")  # first run only; afterwards the checkpoint wins
        .option("failOnDataLoss", "true")
        .load()
    )
    # Confluent wire format: 1 magic byte + 4 byte schema id, then the Avro payload.
    records = raw.select(
        from_avro(F.expr("substring(value, 6, length(value))"), schema).alias("r")
    ).select([F.col(f"r.{c}") for c in COLUMNS])

    def write_batch(batch_df, batch_id):
        df = batch_df.dropDuplicates(["id", "last_updated"]).cache()
        try:
            count = df.count()
            if count == 0:
                return
            (
                df.write.format("jdbc")
                .options(**db, dbtable=STAGING_TABLE, driver="org.postgresql.Driver", truncate="true")
                .mode("overwrite")
                .save()
            )
            run_sql(spark, db, [UPSERT_SQL, HISTORY_SQL])
            log.info("Batch %d: wrote %d records to %s and %s", batch_id, count, UPSERT_TABLE, HISTORY_TABLE)
        finally:
            df.unpersist()

    # No trigger set: the next micro-batch starts as soon as the previous one finishes and Kafka has new offsets.
    query = (
        records.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
