"""Spark Structured Streaming: Kafka (crypto_ticks) -> agregasi 1 menit -> PostgreSQL, raw -> MinIO (Parquet).

Query 1: window 1 menit per symbol (watermark 10 detik, mode append) -> tabel crypto_1m_metrics
Query 2: data mentah -> s3a://crypto-lake/raw/ (Parquet, partisi trade_date & symbol)
Keduanya memakai foreachBatch.
"""
import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

load_dotenv()

# Pastikan worker Spark memakai Python yang sama (penting di Windows/venv)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Windows: Hadoop butuh winutils.exe + hadoop.dll di %HADOOP_HOME%\bin
if sys.platform == "win32" and "HADOOP_HOME" not in os.environ:
    _hh = os.path.join(os.path.expanduser("~"), "hadoop")
    if os.path.exists(os.path.join(_hh, "bin", "winutils.exe")):
        os.environ["HADOOP_HOME"] = _hh
        os.environ["PATH"] = os.path.join(_hh, "bin") + os.pathsep + os.environ["PATH"]

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto_ticks")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")

PG_URL = os.getenv("PG_URL", "jdbc:postgresql://localhost:5432/crypto")
PG_USER = os.getenv("POSTGRES_USER", "crypto")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto")
PG_TABLE = "crypto_1m_metrics"

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
S3_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "crypto-lake")
RAW_PATH = f"s3a://{S3_BUCKET}/raw/"
CHECKPOINT_BASE = f"s3a://{S3_BUCKET}/checkpoints"

PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.postgresql:postgresql:42.7.3",
    "org.apache.hadoop:hadoop-aws:3.3.4",
])

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
log = logging.getLogger("spark_streaming")

SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("event_time", StringType()),  # ISO-8601
])


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("crypto-streaming")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_ticks(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .dropna(subset=["symbol", "price", "quantity", "event_time"])
    )


def aggregate_1m(ticks: DataFrame) -> DataFrame:
    return (
        ticks.withWatermark("event_time", "10 seconds")
        .groupBy(F.window("event_time", "1 minute"), "symbol")
        .agg(
            F.avg("price").alias("avg_price"),
            F.max("price").alias("max_price"),
            F.min("price").alias("min_price"),
            F.sum("quantity").alias("total_volume"),
            F.count("*").alias("trade_count"),
        )
        .select(
            "symbol",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "avg_price", "max_price", "min_price", "total_volume", "trade_count",
        )
    )


def write_metrics_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    n = batch_df.count()
    if n == 0:
        return
    (
        batch_df.write.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", PG_TABLE)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    log.info("batch %d: %d baris agregasi -> PostgreSQL.%s", batch_id, n, PG_TABLE)


def write_raw_to_minio(batch_df: DataFrame, batch_id: int) -> None:
    n = batch_df.count()
    if n == 0:
        return
    (
        batch_df.withColumn("trade_date", F.to_date("event_time"))
        .write.mode("append")
        .partitionBy("trade_date", "symbol")
        .parquet(RAW_PATH)
    )
    log.info("batch %d: %d baris mentah -> %s", batch_id, n, RAW_PATH)


def main() -> None:
    spark = build_spark()
    ticks = read_ticks(spark)
    log.info("Membaca topic=%s broker=%s offsets=%s", TOPIC, BROKER, STARTING_OFFSETS)

    metrics_query = (
        aggregate_1m(ticks).writeStream
        .outputMode("append")  # window dikeluarkan setelah ditutup oleh watermark
        .foreachBatch(write_metrics_to_postgres)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/metrics")
        .trigger(processingTime="10 seconds")
        .queryName("metrics_to_postgres")
        .start()
    )

    raw_query = (
        ticks.writeStream
        .foreachBatch(write_raw_to_minio)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw")
        .trigger(processingTime="10 seconds")
        .queryName("raw_to_minio")
        .start()
    )

    log.info("Streaming berjalan: %s, %s", metrics_query.name, raw_query.name)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
