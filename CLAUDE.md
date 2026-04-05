# 260 Sample Sale — Technical Documentation

## Overview
Internal AI Agent for 260 Sample Sale.
Interface: Google Chat (via Pub/Sub) → Python 3.11 on GCP (e2-standard-2).

## Key Files
| File | Purpose |
|---|---|
| `chat_agent.py` | Single entry point (~1100 lines, 13 sections) |
| `basecamp_auth.py` | Manual Basecamp OAuth token refresh (run when needed) |
| `.env` | All API keys and tokens — **never commit** |
| `credentials.json` | Google service account — **never commit** |
| `chroma_db/` | ChromaDB persistent storage — **never commit** |
| `requirements.txt` | Python dependencies |

## Tech Stack
- **Models**: Claude Haiku (fast/routine queries) → Claude Sonnet (analysis/reports)
- **Routing**: `COMPLEX_SIGNALS` list in `chat_agent.py` (~line 67) determines Haiku vs Sonnet
- **Memory**: ChromaDB (`chroma_db/`) stores Basecamp project IDs, conversation history, and business facts
- **Safety**: `MAX_TOOL_ROUNDS = 10`, sessions expire after `SESSION_TTL_SECONDS = 3600`

## Integrations
| Platform | Key Detail | Auth |
|---|---|---|
| Google Chat + Pub/Sub | Messaging interface | `credentials.json` (service account) |
| Shopify (ShopifyQL) | Net Sales, UPT, Conversion | `SHOPIFY_TOKEN` in `.env` |
| BigQuery | Vendor performance + Teamwork POS | `credentials.json` |
| Google Analytics 4 | Traffic, engagement, conversions | `credentials.json`, Property `329727471` |
| Basecamp | 15 active projects, read/write | OAuth — `BC_LIVE_ACCESS_TOKEN` in `.env` |
| SmartSuite | Tasks, Requests, Inventory, Projects, Brands tables | `SS_API_KEY` in `.env` |
| Tavily | Web search | `TAVILY_API_KEY` in `.env` |
| Anthropic | Claude Haiku + Sonnet | `ANTHROPIC_API_KEY` in `.env` |

## Basecamp — Token Management
- **Access token** (`BC_LIVE_ACCESS_TOKEN`): expires every **14 days** — auto-refreshed on startup via `BC_REFRESH_TOKEN`
- **Refresh token** (`BC_REFRESH_TOKEN`): lasts ~10 years — populated by `basecamp_auth.py`
- **Project sync**: Basecamp project IDs cached in ChromaDB, refreshes every `BC_SYNC_DAYS=3` days
- If auto-refresh ever fails: `source venv/bin/activate && python basecamp_auth.py`

## Running the Agent
```bash
source venv/bin/activate
python chat_agent.py
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
