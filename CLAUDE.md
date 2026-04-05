# 260 Sample Sale — Technical Documentation

## Overview
Internal AI Agent for 260 Sample Sale.
Interface: Google Chat (via Pub/Sub) → Python 3.11 on GCP (e2-standard-2).
**Status: LIVE** — running as a systemd service (`chat-agent.service`).

## Key Files
| File | Purpose |
|---|---|
| `chat_agent.py` | Single entry point (~1150 lines, 13 sections) |
| `basecamp_auth.py` | Manual Basecamp OAuth token refresh (run when needed) |
| `CLAUDE.md` | This file — technical reference |
| `.env` | All API keys and tokens — **never commit** |
| `credentials.json` | Google service account — **never commit** |
| `chroma_db/` | ChromaDB persistent storage — **never commit** |
| `requirements.txt` | Python dependencies |

## Tech Stack
- **Models**: Claude Haiku (fast/routine queries) → Claude Sonnet (analysis/reports)
- **Routing**: `COMPLEX_SIGNALS` list in `chat_agent.py` (~line 67) determines Haiku vs Sonnet
- **Memory**: ChromaDB (`chroma_db/`) stores Basecamp project IDs, conversation history, and business facts
- **Safety**: `MAX_TOOL_ROUNDS = 10`, sessions expire after `SESSION_TTL_SECONDS = 3600`
- **Rate limit**: Anthropic plan is 10k input tokens/minute — not an issue in production (one query at a time), only shows up when hammering the API during testing

## Integrations
| Platform | Key Detail | Auth |
|---|---|---|
| Google Chat + Pub/Sub | Topic: `chat-agent-topic`, Sub: `chat-agent-sub` | `credentials.json` (service account) |
| Shopify (ShopifyQL) | **Primary source for vendor sales** | `SHOPIFY_TOKEN` in `.env` |
| BigQuery | POS only (`pos_data.teamwork_transactions`) — see note below | `credentials.json` |
| Google Analytics 4 | Traffic, engagement, conversions | `credentials.json`, Property `329727471` |
| Basecamp | 15 active projects, read/write | OAuth — `BC_LIVE_ACCESS_TOKEN` in `.env` |
| SmartSuite | Tasks, Requests, Inventory, Projects, Brands tables | `SS_API_KEY` in `.env` |
| Tavily | Web search | `TAVILY_API_KEY` in `.env` |
| Anthropic | Claude Haiku + Sonnet | `ANTHROPIC_API_KEY` in `.env` |

## BigQuery — Important Note
`shopify_data.vendor_performance` exists but all data columns (`vendor_name`, `net_sales`, etc.)
are NULL — the pipeline that populates it has never run. **Do not route vendor sales queries here.**
Use ShopifyQL instead. Only `pos_data.teamwork_transactions` has real data.

## ShopifyQL — Correct Syntax
```
FROM sales SHOW net_sales, gross_sales, orders
GROUP BY product_vendor
SINCE 2026-04-01 UNTIL 2026-04-05
WITH TIMEZONE 'America/New_York'
```
- Use `GROUP BY` not `BY`
- Use `SINCE/UNTIL` not `WHERE` for dates
- Always exclude `ShipInsure` (shipping add-on) and `Inner Circle` (loyalty program) from vendor reports

## Basecamp — Token Management
- **Access token** (`BC_LIVE_ACCESS_TOKEN`): expires every **14 days** — auto-refreshed on startup via `BC_REFRESH_TOKEN`
- **Refresh token** (`BC_REFRESH_TOKEN`): lasts ~10 years — populated by `basecamp_auth.py`
- **Project sync**: Basecamp project IDs cached in ChromaDB, refreshes every `BC_SYNC_DAYS=3` days
- If auto-refresh ever fails: `source venv/bin/activate && python basecamp_auth.py`

## Managing the Live Agent
```bash
# Check status
sudo systemctl status chat-agent

# Watch live logs
tail -f /home/ariel/my-ai-agent/agent.log

# Restart after code changes
sudo systemctl restart chat-agent

# Stop / Start
sudo systemctl stop chat-agent
sudo systemctl start chat-agent
```

## Deploying Code Changes
```bash
git add [files]
git commit -m "description"
git push origin main
sudo systemctl restart chat-agent
```

## Security Rules
- **Never commit**: `.env`, `credentials.json`, `chroma_db/`
- All secrets loaded via `python-dotenv` at startup
- `.gitignore` already covers the above — verify before any `git add .`

## Intentional Decisions (Do Not Revert)
- **No daily report scheduler** — removed by design; reports are generated on-demand
- **No Ollama / local LLM** — ruled out; GCP e2-standard-2 is CPU-only, inference too slow
- **Dual-Claude routing** — Haiku handles simple queries, Sonnet handles reports/analysis
- **Basecamp token stored in `.env`** — `set_key()` writes it automatically on refresh
- **ShipInsure excluded from all vendor reports** — it's a shipping add-on, not a brand
- **Inner Circle excluded from sales reports** — it's the loyalty program (line/VIP passes); relevant for event ops context only
