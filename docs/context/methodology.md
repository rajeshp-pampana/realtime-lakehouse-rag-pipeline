---
type: methodology_notes
title: Data methodology and governance notes
---

# Data methodology and governance notes

## What's real vs. simulated

- Daily OHLCV bars (`ohlcv_raw`/`ohlcv_curated`) are **real** historical data
  pulled from Yahoo Finance.
- The intraday tick stream (`ticks_raw`) is **entirely simulated**: a random
  walk seeded from each ticker's last known daily Close, generated locally by
  `tick_producer.py`. It is not a paid real-time market data subscription and
  should never be presented as one.
- AI-generated briefings are model output, not verified analyst research;
  they should read as a starting point for review, not a trading signal.

## Governance, if this were a real production deployment

- API keys and Kafka credentials would come from a secret manager, not the
  `.env` file this project uses for local dev.
- Market data licensing terms (Yahoo Finance's own terms restrict commercial
  redistribution) would need review before any real deployment beyond
  personal/educational use.
- Retrieved context and generated briefings could contain PII if real analyst
  notes were ever indexed; access to the vector store would need the same
  controls as the rest of the data platform.

## Known limitations to disclose in any briefing or write-up

- Historical OHLCV is limited to whatever window was ingested (currently
  ~1 month per run); briefings should not claim longer-term trend context
  than the data actually supports.
- The simulated tick stream's volatility is a fixed +/-30bps per tick and does
  not reflect real intraday market microstructure (no order book, no real
  news-driven jumps).
