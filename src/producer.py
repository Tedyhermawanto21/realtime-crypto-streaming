"""Binance WebSocket trade stream -> Kafka/Redpanda (topic crypto_ticks).

Pesan JSON: {"symbol", "price", "quantity", "event_time"}.
Auto-reconnect dengan exponential backoff jika koneksi terputus.
"""
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone

import websocket
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto_ticks")
SYMBOLS = [s.strip().lower() for s in
           os.getenv("SYMBOLS", "btcusdt,ethusdt,solusdt,bnbusdt").split(",") if s.strip()]
# data-stream.binance.vision = endpoint publik market-data (port 443).
# Alternatif jika jaringan mengizinkan: wss://stream.binance.com:9443
WS_BASE = os.getenv("WS_BASE", "wss://data-stream.binance.vision")
WS_URL = f"{WS_BASE}/stream?streams=" + "/".join(f"{s}@trade" for s in SYMBOLS)

# Batas demo: berhenti otomatis setelah N menit atau N pesan (0 = tanpa batas)
MAX_RUNTIME_MINUTES = float(os.getenv("MAX_RUNTIME_MINUTES", "15"))
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "0"))

RECONNECT_MIN = 1
RECONNECT_MAX = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
log = logging.getLogger("producer")

producer = Producer({
    "bootstrap.servers": BROKER,
    "client.id": "binance-trade-producer",
    "linger.ms": 50,
    "retries": 5,
})

_ws = None
_stop = False
_sent = 0


def ensure_topic():
    """Buat topic jika belum ada (Redpanda default tidak auto-create)."""
    admin = AdminClient({"bootstrap.servers": BROKER})
    try:
        md = admin.list_topics(timeout=10)
    except KafkaException as e:
        log.error("Tidak bisa terhubung ke broker %s: %s", BROKER, e)
        sys.exit(1)
    if TOPIC in md.topics:
        return
    futures = admin.create_topics([NewTopic(
        TOPIC, num_partitions=3, replication_factor=1,
        # retensi: simpan maksimal 1 jam atau ~200 MB per partisi
        config={"retention.ms": "3600000", "retention.bytes": "209715200"},
    )])
    for topic, fut in futures.items():
        try:
            fut.result()
            log.info("Topic dibuat: %s", topic)
        except KafkaException as e:
            log.warning("Create topic %s: %s", topic, e)


def delivery_report(err, msg):
    if err is not None:
        log.error("Gagal kirim ke Kafka: %s", err)


def on_message(ws, raw):
    global _sent
    try:
        data = json.loads(raw)["data"]
        record = {
            "symbol": data["s"],
            "price": float(data["p"]),
            "quantity": float(data["q"]),
            "event_time": datetime.fromtimestamp(data["E"] / 1000, tz=timezone.utc).isoformat(),
        }
    except (KeyError, ValueError, TypeError) as e:
        log.warning("Pesan tidak valid dilewati (%s): %s", e, raw[:200])
        return

    try:
        producer.produce(
            TOPIC,
            key=record["symbol"].encode(),
            value=json.dumps(record).encode(),
            callback=delivery_report,
        )
    except BufferError:
        producer.poll(0.5)
        log.warning("Buffer producer penuh, pesan dilewati")
        return
    producer.poll(0)

    _sent += 1
    if MAX_MESSAGES and _sent >= MAX_MESSAGES:
        log.info("Batas %d pesan tercapai, berhenti.", MAX_MESSAGES)
        shutdown(None, None)
        return
    if _sent % 500 == 0:
        log.info("Terkirim %d pesan (terakhir: %s)", _sent, record)


def on_open(ws):
    log.info("Terhubung ke Binance: %s", ", ".join(SYMBOLS))


def on_error(ws, error):
    log.error("WebSocket error: %s", error)


def on_close(ws, code, reason):
    log.warning("WebSocket ditutup (code=%s reason=%s)", code, reason)


def shutdown(signum, frame):
    global _stop
    log.info("Shutdown diminta...")
    _stop = True
    if _ws is not None:
        _ws.close()


def main():
    global _ws
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    ensure_topic()
    log.info("Broker=%s topic=%s", BROKER, TOPIC)

    if MAX_RUNTIME_MINUTES:
        # timer daemon: menutup WebSocket saat batas waktu habis
        timer = threading.Timer(MAX_RUNTIME_MINUTES * 60, lambda: (
            log.info("Batas waktu %.0f menit tercapai, berhenti.", MAX_RUNTIME_MINUTES),
            shutdown(None, None),
        ))
        timer.daemon = True
        timer.start()

    delay = RECONNECT_MIN
    while not _stop:
        started = time.time()
        _ws = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        # ping berkala menjaga koneksi & mendeteksi koneksi mati
        _ws.run_forever(ping_interval=20, ping_timeout=10)
        if _stop:
            break
        # reset backoff jika koneksi sempat stabil > 60 detik
        delay = RECONNECT_MIN if time.time() - started > 60 else min(delay * 2, RECONNECT_MAX)
        log.info("Reconnect dalam %ds...", delay)
        time.sleep(delay)

    log.info("Flush pesan tersisa...")
    producer.flush(10)
    log.info("Selesai. Total terkirim: %d", _sent)


if __name__ == "__main__":
    main()
