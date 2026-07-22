---
license: other
task_categories:
- time-series-forecasting
- reinforcement-learning
tags:
- polymarket
- prediction-markets
- order-book
- market-microstructure
---

# Polymarket rewarded-market L2 history

Append-only archives of public Polymarket CLOB WebSocket messages for markets
eligible for liquidity rewards. The dataset is intended for market
microstructure research, backtests and machine-learning experiments.

No wallet data, private keys or authenticated trading information is collected.
Files are gzip-compressed JSON Lines and grouped by source and UTC date.
The source data comes from Polymarket's public CLOB interfaces and remains
subject to the applicable source-platform terms.
