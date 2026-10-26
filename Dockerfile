FROM python:3.14-slim AS base

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM base AS producer

COPY producer/producer.py producer/coingecko_market.avsc ./

RUN useradd --system --no-create-home producer
USER producer

CMD ["python", "producer.py"]

FROM base AS consumer

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/usr/local/lib/python3.14/site-packages/pyspark \
    PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.spark:spark-avro_2.13:4.2.0"
ENV PATH="${SPARK_HOME}/bin:${PATH}"

ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar ${SPARK_HOME}/jars/

RUN chmod 644 ${SPARK_HOME}/jars/postgresql-42.7.14.jar && \
    mkdir -p /opt/ivy /checkpoint

COPY consumer/consumer.py ./

RUN useradd --system --uid 185 --create-home consumer && chown -R consumer /opt/ivy /checkpoint /app
USER consumer

CMD ["sh", "-c", "exec spark-submit --master local[2] --driver-memory 1g --conf spark.jars.ivy=/opt/ivy --packages \"$PACKAGES\" /app/consumer.py"]
