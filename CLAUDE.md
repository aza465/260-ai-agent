# 260 Sample Sale — Technical Documentation

## Overview
Internal AI Agent for 260 Sample Sale.
Interface: Google Chat (via Pub/Sub) → Python 3.11 on GCP (e2-standard-2).
**Status: LIVE** — running as a systemd service (`chat-agent.service`).

## Key Files
| File | Purpose |
|---|---|
| `chat_agent.py` | Single entry point (~1650 lines, 13 sections + pipeline) |
| `basecamp_auth.py` | Manual Basecamp OAuth token refresh (run when needed) |
| `CLAUDE.md` | This file — technical reference |
| `.env` | All API keys and tokens — **never commit** |
| `credentials.json` | Google service account — **never commit** |
| `chroma_db/` | ChromaDB persistent storage — **never commit** |
| `requirements.txt` | Python dependencies |

## Tech Stack
- **Models**: Claude Haiku → Sonnet (2k) → Sonnet (4k) — three-tier routing via `classify_complexity()`
- **Routing tiers**: Tier 1 = Haiku (greetings, quick lookups) | Tier 2 = Sonnet (single-tool queries) | Tier 3 = Sonnet high-cap (reports, multi-source, analysis)
- **Memory**: ChromaDB (`chroma_db/`) stores Basecamp project IDs, conversation history, and business facts — namespaced by domain (`shopify`, `basecamp`, `smartsuite`, `general`)
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

## Scheduled Jobs
| Job | Schedule | Function | Status |
|---|---|---|---|
| Session cleanup | Every 30 min | `cleanup_sessions()` | Live |
| Daily report pipeline | 8 AM ET | `run_daily_report_pipeline()` | Live — console log only |

### Daily Report Pipeline — Stage Map
```
Stage 1: fetch_shopify_data()       → verify: non-empty vendor list
Stage 2: fetch_bigquery_data()      → verify: non-empty transaction rows
Stage 3: merge_and_format_report()  → verify: output length > 100 chars
Stage 4: log_report_output()        → writes to agent.log (console only)
```
**Basecamp posting is intentionally deferred.** Stage 4 currently logs to console only.
Once Stage 1–3 are confirmed working in production, wire `post_to_basecamp()` into Stage 4.
Do not add Basecamp posting silently — it must be a deliberate future task.

## Intentional Decisions (Do Not Revert)
- **No Ollama / local LLM** — ruled out; GCP e2-standard-2 is CPU-only, inference too slow
- **Three-tier Claude routing** — Haiku (Tier 1), Sonnet 2k (Tier 2), Sonnet 4k (Tier 3); all within Claude API
- **Basecamp token stored in `.env`** — `set_key()` writes it automatically on refresh
- **ShipInsure excluded from all vendor reports** — it's a shipping add-on, not a brand
- **Inner Circle excluded from sales reports** — it's the loyalty program (line/VIP passes); relevant for event ops context only
- **Daily report posts to log only** — Basecamp posting deferred until data output is validated in production

## Coding Principles (Karpathy-Inspired)

These principles apply to all future work on this codebase. Read them before writing any code.

**Think Before Coding** — State your assumptions explicitly before writing. If something is ambiguous, ask rather than guess. If a simpler approach exists than what was asked, say so. Stop and name what's unclear rather than running with a wrong interpretation.

**Simplicity First** — Write the minimum code that solves the problem. No speculative features, no abstractions for single-use code, no error handling for impossible scenarios. If the solution could be 50 lines instead of 200, write 50.

**Surgical Changes** — When editing existing code, touch only what's necessary for the task. Don't improve adjacent code, reformat unrelated sections, or silently remove pre-existing dead code. If you notice something broken but unrelated, mention it — don't fix it silently.

**Goal-Driven Execution** — Transform tasks into verifiable success criteria before implementing. Instead of "fix the routing bug", define "write a test that reproduces the routing failure, then make it pass." For multi-step tasks, produce a brief numbered plan with a verification check for each step.
