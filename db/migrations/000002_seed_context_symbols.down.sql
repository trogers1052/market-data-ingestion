-- Rollback: Disable context symbols (don't delete — they may have OHLCV data)
UPDATE monitored_symbols
SET enabled = false, updated_at = NOW()
WHERE symbol IN (
    'SPY', 'QQQ', 'GDX', 'GLD', 'XLB', 'XME', 'URA', 'SIL', 'REMX',
    'XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLY', 'XLP', 'XLU'
);
