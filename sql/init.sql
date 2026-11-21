CREATE TABLE IF NOT EXISTS crypto_1m_metrics (
    symbol        TEXT             NOT NULL,
    window_start  TIMESTAMPTZ      NOT NULL,
    window_end    TIMESTAMPTZ      NOT NULL,
    avg_price     DOUBLE PRECISION,
    max_price     DOUBLE PRECISION,
    min_price     DOUBLE PRECISION,
    total_volume  DOUBLE PRECISION,
    trade_count   BIGINT
);
CREATE INDEX IF NOT EXISTS idx_crypto_1m_metrics_symbol_window
    ON crypto_1m_metrics (symbol, window_start);
