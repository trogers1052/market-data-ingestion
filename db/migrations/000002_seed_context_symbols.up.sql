-- Migration: Seed context symbols for regime detection and sector analysis
-- These symbols are used by context-service for market regime, sector strength,
-- and commodity/mining context. They must be ingested alongside trade symbols.

INSERT INTO monitored_symbols (symbol, name, enabled) VALUES
    ('SPY', 'SPDR S&P 500 ETF Trust', true),
    ('QQQ', 'Invesco QQQ Trust', true),
    ('GDX', 'VanEck Gold Miners ETF', true),
    ('GLD', 'SPDR Gold Shares', true),
    ('XLB', 'Materials Select Sector SPDR Fund', true),
    ('XME', 'SPDR S&P Metals & Mining ETF', true),
    ('URA', 'Global X Uranium ETF', true),
    ('SIL', 'Global X Silver Miners ETF', true),
    ('REMX', 'VanEck Rare Earth/Strategic Metals ETF', true),
    ('XLK', 'Technology Select Sector SPDR Fund', true),
    ('XLF', 'Financial Select Sector SPDR Fund', true),
    ('XLE', 'Energy Select Sector SPDR Fund', true),
    ('XLV', 'Health Care Select Sector SPDR Fund', true),
    ('XLI', 'Industrial Select Sector SPDR Fund', true),
    ('XLY', 'Consumer Discretionary Select Sector SPDR Fund', true),
    ('XLP', 'Consumer Staples Select Sector SPDR Fund', true),
    ('XLU', 'Utilities Select Sector SPDR Fund', true)
ON CONFLICT (symbol) DO UPDATE SET
    enabled = true,
    updated_at = NOW();
