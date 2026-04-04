# 260 Sample Sale — AI Business Agent

A Google Chat bot that serves as an AI-powered business assistant for 260 Sample Sale. It answers questions, pulls reports, manages tasks, and posts daily sales summaries — all from within Google Chat.

---

## What It Does

- **Answers questions** via Google Chat (responds to messages in real time)
- **Sales reports** — pulls data from Shopify and BigQuery, posts daily summaries to Basecamp at 8 AM ET
- **Shopify analytics** — vendor performance, sales totals, order counts via ShopifyQL
- **BigQuery reports** — Shopify vendor performance + Teamwork POS transactions
- **Basecamp** — reads/posts messages, chat, and to-dos across multiple projects
- **SmartSuite** — reads and creates records in Tasks, Requests, Projects, Inventory, Brands tables
- **Web search** — live internet search via Tavily
- **Long-term memory** — remembers business facts and past conversations via ChromaDB

---

## Architecture

```
Google Chat → Pub/Sub → chat_agent.py → Claude API (complex) or Ollama (simple)
                                       ↓
                          Tools: Shopify, BigQuery, Basecamp, SmartSuite, Web Search
                                       ↓
                          ChromaDB (persistent memory) + APScheduler (daily reports)
```

**AI Routing:**
- Simple/conversational queries → local Ollama model (`qwen3:14b`) — fast, no API cost
- Complex queries (reports, analysis, writing) → Claude API (`claude-sonnet-4-6`)
- If local model fails → automatically falls back to Claude

---

## Setup

### 1. Prerequisites

- Python 3.10+
- Google Cloud project with Pub/Sub + Chat API enabled
- Service account with appropriate permissions (`credentials.json`)
- Ollama installed (optional, for hybrid mode): `curl -fsSL https://ollama.com/install.sh | sh`

### 2. Install dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure environment

Copy `.env.example` to `.env` and fill in all values:

```bash
cp .env.example .env
```

Required keys:
- `ANTHROPIC_API_KEY` — Claude API key
- `SHOPIFY_TOKEN` + `SHOP_URL` — Shopify Admin API
- `BC_ACCESS_TOKEN` + `BC_ACCOUNT_ID` — Basecamp OAuth token
- `SS_API_KEY` + `SS_WORKSPACE_ID` — SmartSuite API
- `TAVILY_API_KEY` — Tavily search
- `GOOGLE_APPLICATION_CREDENTIALS` — path to service account JSON
- `BC_REPORT_PROJECT_ID` + `BC_REPORT_BOARD_ID` — Basecamp target for daily reports

### 4. (Optional) Enable local LLM

```bash
# Install Ollama
curl -fsSL https://ollama.com/install.sh | sh
# Pull the model
ollama pull qwen3:14b
# Enable in .env
USE_LOCAL_LLM=true
```

### 5. Run

```bash
source venv/bin/activate
python chat_agent.py
```

---

## Basecamp Project IDs

The agent can look up Basecamp IDs dynamically at runtime — just ask it in Google Chat:
- *"List my Basecamp projects"* → returns all project IDs
- *"Get tools for project 12345678"* → returns message board, chat, and todo IDs

For the **automated daily report**, set the target once in `.env`:
```
BC_REPORT_PROJECT_ID=<your project id>
BC_REPORT_BOARD_ID=<your message board id>
```

---

## File Structure

```
my-ai-agent/
├── chat_agent.py        # Main agent (all logic in one file)
├── requirements.txt     # Python dependencies
├── .env                 # Secrets — never commit this
├── .env.example         # Template for .env (safe to commit)
├── credentials.json     # Google service account — never commit this
├── .gitignore           # Excludes .env and credentials.json
└── chroma_db/           # ChromaDB persistent storage (auto-created)
```

---

## Environment Variables Reference

| Variable | Description |
|----------|-------------|
| `ANTHROPIC_API_KEY` | Claude API key |
| `SHOPIFY_TOKEN` | Shopify Admin API access token |
| `SHOP_URL` | Shopify store URL (e.g. `your-store.myshopify.com`) |
| `BC_ACCESS_TOKEN` | Basecamp OAuth access token |
| `BC_ACCOUNT_ID` | Basecamp account ID |
| `BC_REPORT_PROJECT_ID` | Basecamp project ID for daily reports |
| `BC_REPORT_BOARD_ID` | Basecamp message board ID for daily reports |
| `SS_API_KEY` | SmartSuite API key |
| `SS_WORKSPACE_ID` | SmartSuite workspace ID |
| `TAVILY_API_KEY` | Tavily search API key |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to Google service account JSON |
| `USE_LOCAL_LLM` | `true` to enable Ollama hybrid mode (default: `false`) |
| `OLLAMA_MODEL` | Ollama model name (default: `qwen3:14b`) |
| `OLLAMA_HOST` | Ollama server URL (default: `http://localhost:11434`) |
