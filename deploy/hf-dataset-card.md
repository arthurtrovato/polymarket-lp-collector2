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

Append-only archives of public Polymarket market data: CLOB WebSocket L2
messages for selected liquidity-reward markets, periodic REST order-book
checkpoints, the 500 highest-reward rich market records plus every currently
sponsored configuration, public sports scores, and Polymarket RTDS crypto
reference prices. Each universe snapshot includes explicit coverage metadata.
Connection and subscription lifecycle records make interruptions measurable.

The dataset is intended for market microstructure research, conservative
backtests and machine-learning experiments. REST checkpoints can restore a
book after a disconnect, but cannot recreate unobserved intermediate messages.

No wallet data, private keys or authenticated trading information is collected.
Files are gzip-compressed JSON Lines and grouped by source and UTC date.
The source data comes from Polymarket's public CLOB interfaces and remains
subject to the applicable source-platform terms.
