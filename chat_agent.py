import os
import re
import json
import time
import uuid
import threading
import logging
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# SECTION 1: LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "agent.log"),
            encoding="utf-8"
        )
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# SECTION 2: CONSTANTS & CREDENTIALS
# ============================================================
# Fix: use env var instead of hardcoded path
creds_path = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "credentials.json")
)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path

PROJECT_ID         = "gen-lang-client-0325727650"
SUBSCRIPTION_ID    = "chat-agent-sub"

SHOPIFY_TOKEN      = os.environ.get("SHOPIFY_TOKEN")
SHOP_URL           = os.environ.get("SHOPIFY_SHOP_URL", "260-sample-sale.myshopify.com")

BC_ACCESS_TOKEN    = os.environ.get("BC_LIVE_ACCESS_TOKEN") or os.environ.get("BC_ACCESS_TOKEN")
BC_CLIENT_ID       = os.environ.get("BC_CLIENT_ID")
BC_CLIENT_SECRET   = os.environ.get("BC_CLIENT_SECRET")
BC_REFRESH_TOKEN   = os.environ.get("BC_REFRESH_TOKEN")
BC_ACCOUNT_ID      = os.environ.get("BC_ACCOUNT_ID")
BC_USER_AGENT      = "ArielAI (Ariel@260samplesale.com)"
BC_SYNC_DAYS       = int(os.environ.get("BC_SYNC_DAYS", "3"))

SS_API_KEY         = os.environ.get("SS_API_KEY")
SS_WORKSPACE_ID    = os.environ.get("SS_WORKSPACE_ID")

ANTHROPIC_API_KEY  = os.environ["ANTHROPIC_API_KEY"]
CLAUDE_HAIKU_MODEL = "claude-haiku-4-5-20251001"
CLAUDE_SONNET_MODEL = "claude-sonnet-4-6"

SESSION_TTL_SECONDS  = 3600   # sessions expire after 1 hour of inactivity
SESSION_MAX_MESSAGES = 40     # trim history to prevent context overflow
MAX_TOOL_ROUNDS      = 10     # max tool call iterations per response

# Complexity tier signals for classify_complexity()
TIER3_SIGNALS = [
    "report", "analyze", "analysis", "summary", "compare", "comparison",
    "breakdown", "trend", "versus", " vs ", "generate", "draft",
    "write a", "create a", "make a", "send a", "post a",
]
TIER2_SIGNALS = [
    "vendor", "sales", "orders", "revenue", "shopify", "bigquery",
    "basecamp", "smartsuite", "ga4", "analytics", "traffic", "inventory",
    "brand", "todo", "task", "request", "project", "search",
    "show me", "what are", "how many", "list", "check", "look up",
]
CONVERSATIONAL_SIGNALS = [
    "hello", "hi", "hey", "good morning", "good afternoon", "good evening",
    "thanks", "thank you", "bye", "goodbye", "great", "sounds good",
    "ok", "okay", "got it", "perfect", "awesome",
]

# ============================================================
# SECTION 3: CLIENT INITIALIZATION
# ============================================================
from google.cloud import pubsub_v1
from google.apps import chat_v1
from google.cloud import bigquery
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Metric, Dimension, OrderBy, FilterExpression, Filter
)
from tavily import TavilyClient
import anthropic

ai_client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
chat_client = chat_v1.ChatServiceClient()
tavily      = TavilyClient(api_key=os.environ['TAVILY_API_KEY'])
bq_client   = bigquery.Client(project="gen-lang-client-0065509773")
ga4_client  = BetaAnalyticsDataClient()
GA4_PROPERTY = "properties/329727471"

# --- ChromaDB persistent memory ---
try:
    import chromadb
    chroma_client = chromadb.PersistentClient(
        path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "chroma_db")
    )
    conversations_col = chroma_client.get_or_create_collection(name="conversations")
    knowledge_col     = chroma_client.get_or_create_collection(name="knowledge")
    CHROMA_ENABLED = True
    logger.info("ChromaDB initialized (persistent memory enabled)")
except Exception as e:
    logger.warning(f"ChromaDB unavailable: {e}. Running without persistent memory.")
    CHROMA_ENABLED = False
    conversations_col = None
    knowledge_col = None

logger.info(f"Dual-model routing: Haiku ({CLAUDE_HAIKU_MODEL}) for simple queries, Sonnet ({CLAUDE_SONNET_MODEL}) for complex")

# ============================================================
# SECTION 4: TOOL FUNCTIONS
# ============================================================

# --- Platform 1: External Search & BigQuery ---

def web_search(query: str):
    """Searches the live internet for current information, news, or stock prices."""
    logger.info(f"[SEARCH] {query}")
    result = tavily.search(query=query, search_depth="basic")
    return json.dumps(result)

def run_bigquery_report(sql_query: str):
    """
    Executes a SQL query against BigQuery for internal business reports.

    AVAILABLE TABLES:

    TABLE 1 — Shopify Vendor Performance:
    `gen-lang-client-0065509773.shopify_data.vendor_performance`
    COLUMNS: account_name (STRING), date (DATE), order_name (STRING),
             vendor_name (STRING), net_sales (FLOAT), order_count (INTEGER),
             units_sold (INTEGER), last_updated (TIMESTAMP)

    TABLE 2 — Teamwork POS Transactions:
    `gen-lang-client-0065509773.pos_data.teamwork_transactions`
    NOTE: Verify the exact table path in your BigQuery console before using.
    COLUMNS: transaction_date (DATE), location_name (STRING), sku (STRING),
             product_name (STRING), category (STRING), quantity (INTEGER),
             unit_price (FLOAT), net_amount (FLOAT), customer_id (STRING)

    NOTES: Always add LIMIT clauses. Use DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    for yesterday's data. Use DATE_TRUNC for week/month grouping.

    SHOPIFY EXAMPLE:
    SELECT vendor_name, SUM(net_sales) as total_sales, SUM(units_sold) as units
    FROM `gen-lang-client-0065509773.shopify_data.vendor_performance`
    WHERE date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND vendor_name IS NOT NULL
    GROUP BY vendor_name ORDER BY total_sales DESC LIMIT 20

    POS EXAMPLE:
    SELECT location_name, SUM(net_amount) as revenue
    FROM `gen-lang-client-0065509773.pos_data.teamwork_transactions`
    WHERE transaction_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY location_name ORDER BY revenue DESC LIMIT 10
    """
    logger.info("[BIGQUERY] Running query...")
    try:
        results = bq_client.query(sql_query).result()
        return json.dumps([dict(row) for row in results], default=str)
    except Exception as e:
        return f"BigQuery Error: {str(e)}"

# --- Platform 2: Shopify Analytics ---

def query_shopify_analytics(shopify_ql: str):
    """
    REQUIRED: Use for ALL Shopify sales, order counts, and vendor analytics.
    Input MUST be a valid ShopifyQL string.

    SYNTAX RULES:
    - Use SHOW (NOT SELECT)
    - Use plain column names only — NO aggregation functions like sum() or count()
    - Valid columns: net_sales, gross_sales, orders, product_vendor
    - Date ranges: use SINCE and UNTIL (NOT WHERE): SINCE YYYY-MM-DD UNTIL YYYY-MM-DD
    - ALWAYS end with: WITH TIMEZONE 'America/New_York'

    EXAMPLE: FROM sales SHOW net_sales, gross_sales, orders SINCE 2026-02-26 UNTIL 2026-02-26 WITH TIMEZONE 'America/New_York'
    """
    logger.info(f"[SHOPIFY] {shopify_ql}")
    url = f"https://{SHOP_URL}/admin/api/2026-01/graphql.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}
    gql_query = """
    query ($q: String!) {
      shopifyqlQuery(query: $q) {
        tableData { columns { name dataType displayName } rows }
        parseErrors
      }
    }
    """
    try:
        response = requests.post(
            url, json={"query": gql_query, "variables": {"q": shopify_ql}}, headers=headers
        )
        data = response.json()
        query_result = data.get("data", {}).get("shopifyqlQuery", {})
        if query_result and query_result.get("parseErrors"):
            return f"ShopifyQL Syntax Error: {query_result['parseErrors']}"
        table = query_result.get("tableData")
        if table:
            money_cols = {col["name"] for col in table.get("columns", []) if col.get("dataType") == "MONEY"}
            for row in table.get("rows", []):
                for col in money_cols:
                    if col in row and row[col] is not None:
                        row[col] = f"${float(row[col]):,.2f}"
        return json.dumps(query_result)
    except Exception as e:
        return f"Connection Error: {str(e)}"

# --- Platform 3: Basecamp ---

def _bc_headers():
    """Returns Basecamp auth headers using the current live token (auto-refreshes if needed)."""
    return {"Authorization": f"Bearer {BC_ACCESS_TOKEN}", "User-Agent": BC_USER_AGENT}

def refresh_bc_token():
    """
    Exchanges the long-lived refresh token for a new access token and saves it to .env.
    Called automatically on startup if the access token is expired or missing.
    """
    global BC_ACCESS_TOKEN
    if not BC_CLIENT_ID or not BC_CLIENT_SECRET or not BC_REFRESH_TOKEN:
        logger.warning("[BASECAMP] Cannot auto-refresh — BC_CLIENT_ID, BC_CLIENT_SECRET, or BC_REFRESH_TOKEN missing")
        return False
    try:
        r = requests.post(
            "https://launchpad.37signals.com/authorization/token",
            params={
                "type": "refresh",
                "client_id": BC_CLIENT_ID,
                "client_secret": BC_CLIENT_SECRET,
                "refresh_token": BC_REFRESH_TOKEN,
            }
        )
        if r.status_code == 200:
            new_token = r.json().get("access_token")
            if new_token:
                env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
                from dotenv import set_key
                set_key(env_path, "BC_LIVE_ACCESS_TOKEN", new_token)
                BC_ACCESS_TOKEN = new_token
                logger.info("[BASECAMP] Access token auto-refreshed successfully")
                return True
        logger.error(f"[BASECAMP] Token refresh failed: {r.status_code} {r.text[:200]}")
        return False
    except Exception as e:
        logger.error(f"[BASECAMP] Token refresh error: {e}")
        return False

def _bc_sync_is_stale() -> bool:
    """Returns True if Basecamp project sync is older than BC_SYNC_DAYS (or has never run)."""
    if not CHROMA_ENABLED:
        return True
    try:
        results = knowledge_col.get(ids=["bc_sync_timestamp"])
        if not results["documents"]:
            return True
        last_sync = float(results["metadatas"][0].get("timestamp", 0))
        age_days = (time.time() - last_sync) / 86400
        logger.info(f"[BASECAMP] Last project sync was {age_days:.1f} days ago (limit: {BC_SYNC_DAYS} days)")
        return age_days >= BC_SYNC_DAYS
    except Exception:
        return True

def list_basecamp_projects():
    """Retrieves a list of all active Basecamp projects/buckets and their IDs."""
    logger.info("[BASECAMP] Fetching projects...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/projects.json"
    try:
        r = requests.get(url, headers=_bc_headers())
        return json.dumps(r.json()) if r.status_code == 200 else f"Error: {r.status_code}"
    except Exception as e:
        return f"Error: {str(e)}"

def get_project_tools(project_id: int):
    """Retrieves all tools (Message Board, Chat, Todos, etc.) for a Basecamp project ID."""
    logger.info(f"[BASECAMP] Getting tools for project {project_id}...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/projects/{project_id}.json"
    try:
        r = requests.get(url, headers=_bc_headers())
        return json.dumps(r.json().get("dock", [])) if r.status_code == 200 else f"Error: {r.status_code}"
    except Exception as e:
        return f"Error: {str(e)}"

def post_to_basecamp(project_id: int, message_board_id: int, title: str, content_html: str):
    """Posts a formatted message to a Basecamp Message Board. content_html must be wrapped in <div> tags."""
    logger.info(f"[BASECAMP] Posting to board {message_board_id}...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/buckets/{project_id}/message_boards/{message_board_id}/messages.json"
    headers = {**_bc_headers(), "Content-Type": "application/json"}
    r = requests.post(url, json={"subject": title, "content": content_html, "status": "active"}, headers=headers)
    if r.status_code == 201:
        return "SUCCESS: Posted to Basecamp."
    if r.status_code == 429:
        raise Exception("429 RESOURCE_EXHAUSTED")
    return f"Error: {r.status_code}"

def read_basecamp_messages(project_id: int, message_board_id: int, limit: int = 10):
    """
    Reads recent messages from a Basecamp Message Board.
    Returns posts with title, author, date, and content preview.
    Use get_project_tools() to find the message_board_id (look for 'message_board' in dock).
    """
    logger.info(f"[BASECAMP] Reading messages from board {message_board_id}...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/buckets/{project_id}/message_boards/{message_board_id}/messages.json"
    try:
        r = requests.get(url, headers=_bc_headers())
        if r.status_code != 200:
            return f"Error: {r.status_code}"
        return json.dumps([{
            "id": m.get("id"),
            "title": m.get("subject", ""),
            "author": m.get("creator", {}).get("name", ""),
            "created_at": m.get("created_at", "")[:10],
            "preview": m.get("content", "")[:300]
        } for m in r.json()[:limit]])
    except Exception as e:
        return f"Basecamp Error: {str(e)}"

def post_to_campfire(project_id: int, campfire_id: int, content: str):
    """Posts a quick chat message to a Basecamp Project Chat. Use get_project_tools to find the chat ID."""
    logger.info(f"[CAMPFIRE] Sending to {campfire_id}...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/buckets/{project_id}/chats/{campfire_id}/lines.json"
    headers = {**_bc_headers(), "Content-Type": "application/json"}
    r = requests.post(url, json={"content": content}, headers=headers)
    if r.status_code == 201:
        return "SUCCESS: Posted to Chat."
    if r.status_code == 429:
        raise Exception("429 RESOURCE_EXHAUSTED")
    return f"Error: {r.status_code}"

def read_campfire_lines(project_id: int, campfire_id: int, limit: int = 15):
    """Retrieves the latest chat lines from a Basecamp Project Chat."""
    logger.info(f"[CAMPFIRE] Reading from {campfire_id}...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/buckets/{project_id}/chats/{campfire_id}/lines.json"
    try:
        r = requests.get(url, headers=_bc_headers())
        if r.status_code == 200:
            return json.dumps([{"name": l["creator"]["name"], "text": l["content"]} for l in r.json()[:limit]])
        return f"Error: {r.status_code}"
    except Exception as e:
        return f"Error: {str(e)}"

def get_todo_lists(project_id: int, todoset_id: int):
    """Retrieves all to-do lists in a Basecamp project. Use get_project_tools to find the todoset ID."""
    logger.info(f"[BASECAMP] Fetching to-do lists for project {project_id}...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/buckets/{project_id}/todosets/{todoset_id}/todolists.json"
    try:
        r = requests.get(url, headers=_bc_headers())
        if r.status_code == 200:
            return json.dumps([{"id": l["id"], "title": l["title"]} for l in r.json()])
        return f"Error: {r.status_code}"
    except Exception as e:
        return f"Error: {str(e)}"

def create_todo(project_id: int, todolist_id: int, content: str, due_on: str = None, assignee_ids: list = None):
    """Creates a new to-do in a Basecamp to-do list. due_on format: YYYY-MM-DD (optional)."""
    logger.info(f"[BASECAMP] Creating to-do in list {todolist_id}...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/buckets/{project_id}/todolists/{todolist_id}/todos.json"
    headers = {**_bc_headers(), "Content-Type": "application/json"}
    payload = {"content": content}
    if due_on:
        payload["due_on"] = due_on
    if assignee_ids:
        payload["assignee_ids"] = assignee_ids
    try:
        r = requests.post(url, json=payload, headers=headers)
        if r.status_code == 201:
            return "SUCCESS: To-do created."
        if r.status_code == 429:
            raise Exception("429 RESOURCE_EXHAUSTED")
        return f"Error: {r.status_code}"
    except Exception as e:
        return f"Error: {str(e)}"

def get_todos(project_id: int, todolist_id: int, completed: bool = False):
    """
    Reads all to-dos in a Basecamp to-do list.
    Returns each todo's ID, content, assignees, due date, and completion status.
    Set completed=True to fetch completed todos instead of open ones.
    """
    logger.info(f"[BASECAMP] Reading todos from list {todolist_id}...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/buckets/{project_id}/todolists/{todolist_id}/todos.json"
    params = {"completed": "true"} if completed else {}
    try:
        r = requests.get(url, params=params, headers=_bc_headers())
        if r.status_code != 200:
            return f"Error: {r.status_code}"
        return json.dumps([{
            "id": t.get("id"),
            "content": t.get("content", ""),
            "completed": t.get("completed", False),
            "due_on": t.get("due_on"),
            "assignees": [a.get("name") for a in t.get("assignees", [])],
            "created_at": t.get("created_at", "")[:10]
        } for t in r.json()])
    except Exception as e:
        return f"Error: {str(e)}"

def complete_todo(project_id: int, todo_id: int):
    """Marks a Basecamp to-do as complete. Use get_todos() to find the todo_id."""
    logger.info(f"[BASECAMP] Completing todo {todo_id}...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/buckets/{project_id}/todos/{todo_id}/completion.json"
    try:
        r = requests.post(url, headers=_bc_headers())
        if r.status_code == 204:
            return "SUCCESS: To-do marked complete."
        if r.status_code == 429:
            raise Exception("429 RESOURCE_EXHAUSTED")
        return f"Error: {r.status_code}"
    except Exception as e:
        return f"Error: {str(e)}"

def read_message_full(project_id: int, message_id: int):
    """
    Reads the full content of a single Basecamp message by its ID.
    Use read_basecamp_messages() first to find message IDs.
    """
    logger.info(f"[BASECAMP] Reading full message {message_id}...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/buckets/{project_id}/messages/{message_id}.json"
    try:
        r = requests.get(url, headers=_bc_headers())
        if r.status_code != 200:
            return f"Error: {r.status_code}"
        m = r.json()
        return json.dumps({
            "id": m.get("id"),
            "title": m.get("subject", ""),
            "author": m.get("creator", {}).get("name", ""),
            "created_at": m.get("created_at", "")[:10],
            "content": m.get("content", "")
        })
    except Exception as e:
        return f"Error: {str(e)}"

def sync_basecamp_projects_to_memory():
    """
    Scans all active Basecamp projects and stores their structure (project name, ID,
    message_board_id, todoset_id, campfire_id) into long-term memory.
    Skips if synced within the last BC_SYNC_DAYS days (default: 3).
    """
    if not _bc_sync_is_stale():
        logger.info("[BASECAMP] Project sync is fresh — skipping")
        return "Basecamp projects already up to date in memory."
    logger.info("[BASECAMP] Syncing all projects to memory...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/projects.json"
    try:
        r = requests.get(url, headers=_bc_headers())
        if r.status_code != 200:
            return f"Error fetching projects: {r.status_code}"
        projects = r.json()
        synced = []
        for p in projects:
            pid = p.get("id")
            name = p.get("name", "")
            dock = p.get("dock", [])
            entry = {"project_id": pid, "project_name": name}
            for tool in dock:
                title = tool.get("title", "").lower()
                tid = tool.get("id")
                if title == "message board":
                    entry["message_board_id"] = tid
                elif title == "to-dos":
                    entry["todoset_id"] = tid
                elif title == "campfire":
                    entry["campfire_id"] = tid
                elif title == "docs & files":
                    entry["vault_id"] = tid
            synced.append(entry)
            if CHROMA_ENABLED:
                fact = f"Basecamp project '{name}' (ID {pid}): " + ", ".join(
                    f"{k}={v}" for k, v in entry.items() if k != "project_name"
                )
                knowledge_col.upsert(
                    documents=[fact],
                    metadatas=[{"type": "basecamp_project", "namespace": "basecamp", "project_id": str(pid), "date": datetime.now().strftime("%Y-%m-%d"), "timestamp": time.time()}],
                    ids=[f"bc_project_{pid}"]
                )
        # Save sync timestamp
        if CHROMA_ENABLED:
            knowledge_col.upsert(
                documents=["Basecamp project sync timestamp"],
                metadatas=[{"type": "bc_sync_meta", "namespace": "basecamp", "timestamp": time.time(), "date": datetime.now().strftime("%Y-%m-%d")}],
                ids=["bc_sync_timestamp"]
            )
        logger.info(f"[BASECAMP] Synced {len(synced)} projects to memory")
        return json.dumps(synced, indent=2)
    except Exception as e:
        logger.error(f"[BASECAMP] Sync error: {e}")
        return f"Error: {str(e)}"

# --- Platform 4: Google Analytics 4 ---

def ga4_traffic_overview(start_date: str = "7daysAgo", end_date: str = "today"):
    """
    Returns a day-by-day traffic overview: sessions, active users, pageviews, bounce rate.
    date format: 'today', 'yesterday', 'NdaysAgo', or 'YYYY-MM-DD'.
    """
    logger.info(f"[GA4] Traffic overview {start_date} → {end_date}")
    try:
        response = ga4_client.run_report(RunReportRequest(
            property=GA4_PROPERTY,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="date")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="activeUsers"),
                Metric(name="screenPageViews"),
                Metric(name="bounceRate"),
            ],
            order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"), desc=False)],
        ))
        rows = []
        for row in response.rows:
            d = row.dimension_values[0].value
            rows.append({
                "date": f"{d[:4]}-{d[4:6]}-{d[6:]}",
                "sessions": int(row.metric_values[0].value),
                "active_users": int(row.metric_values[1].value),
                "pageviews": int(row.metric_values[2].value),
                "bounce_rate": f"{float(row.metric_values[3].value)*100:.1f}%",
            })
        return json.dumps(rows)
    except Exception as e:
        return f"GA4 Error: {str(e)}"

def ga4_traffic_sources(start_date: str = "7daysAgo", end_date: str = "today", limit: int = 10):
    """
    Returns top traffic sources by sessions: channel, source, medium.
    Useful for understanding where visitors come from (Google, Instagram, email, direct, etc.).
    """
    logger.info(f"[GA4] Traffic sources {start_date} → {end_date}")
    try:
        response = ga4_client.run_report(RunReportRequest(
            property=GA4_PROPERTY,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[
                Dimension(name="sessionDefaultChannelGroup"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
            ],
            metrics=[
                Metric(name="sessions"),
                Metric(name="activeUsers"),
                Metric(name="conversions"),
            ],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
            limit=limit,
        ))
        rows = []
        for row in response.rows:
            rows.append({
                "channel": row.dimension_values[0].value,
                "source": row.dimension_values[1].value,
                "medium": row.dimension_values[2].value,
                "sessions": int(row.metric_values[0].value),
                "users": int(row.metric_values[1].value),
                "conversions": int(row.metric_values[2].value),
            })
        return json.dumps(rows)
    except Exception as e:
        return f"GA4 Error: {str(e)}"

def ga4_top_pages(start_date: str = "7daysAgo", end_date: str = "today", limit: int = 15):
    """
    Returns the most-viewed pages by pageviews and unique users.
    Useful for understanding which products or content drive the most engagement.
    """
    logger.info(f"[GA4] Top pages {start_date} → {end_date}")
    try:
        response = ga4_client.run_report(RunReportRequest(
            property=GA4_PROPERTY,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[
                Dimension(name="pagePath"),
                Dimension(name="pageTitle"),
            ],
            metrics=[
                Metric(name="screenPageViews"),
                Metric(name="activeUsers"),
                Metric(name="averageSessionDuration"),
            ],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"), desc=True)],
            limit=limit,
        ))
        rows = []
        for row in response.rows:
            rows.append({
                "page": row.dimension_values[0].value,
                "title": row.dimension_values[1].value,
                "pageviews": int(row.metric_values[0].value),
                "users": int(row.metric_values[1].value),
                "avg_session_sec": int(float(row.metric_values[2].value)),
            })
        return json.dumps(rows)
    except Exception as e:
        return f"GA4 Error: {str(e)}"

def ga4_conversions(start_date: str = "7daysAgo", end_date: str = "today"):
    """
    Returns conversion/purchase metrics: total conversions, revenue, transactions, conversion rate.
    """
    logger.info(f"[GA4] Conversions {start_date} → {end_date}")
    try:
        response = ga4_client.run_report(RunReportRequest(
            property=GA4_PROPERTY,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="date")],
            metrics=[
                Metric(name="conversions"),
                Metric(name="totalRevenue"),
                Metric(name="transactions"),
                Metric(name="sessionConversionRate"),
            ],
            order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"), desc=False)],
        ))
        rows = []
        for row in response.rows:
            d = row.dimension_values[0].value
            rows.append({
                "date": f"{d[:4]}-{d[4:6]}-{d[6:]}",
                "conversions": int(float(row.metric_values[0].value)),
                "revenue": f"${float(row.metric_values[1].value):,.2f}",
                "transactions": int(float(row.metric_values[2].value)),
                "conversion_rate": f"{float(row.metric_values[3].value)*100:.2f}%",
            })
        return json.dumps(rows)
    except Exception as e:
        return f"GA4 Error: {str(e)}"

def ga4_custom_report(dimensions: list, metrics: list, start_date: str = "7daysAgo", end_date: str = "today", limit: int = 20):
    """
    Runs a custom GA4 report with any dimensions and metrics.
    Common dimensions: date, pagePath, sessionSource, sessionMedium, deviceCategory, country, city, pageTitle.
    Common metrics: sessions, activeUsers, screenPageViews, bounceRate, conversions, totalRevenue, averageSessionDuration.
    Example: dimensions=['deviceCategory'], metrics=['sessions','activeUsers']
    """
    logger.info(f"[GA4] Custom report: dims={dimensions} metrics={metrics}")
    try:
        response = ga4_client.run_report(RunReportRequest(
            property=GA4_PROPERTY,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name=metrics[0]), desc=True)],
            limit=limit,
        ))
        rows = []
        dim_names = [h.name for h in response.dimension_headers]
        met_names = [h.name for h in response.metric_headers]
        for row in response.rows:
            entry = {}
            for i, v in enumerate(row.dimension_values):
                entry[dim_names[i]] = v.value
            for i, v in enumerate(row.metric_values):
                entry[met_names[i]] = v.value
            rows.append(entry)
        return json.dumps(rows)
    except Exception as e:
        return f"GA4 Error: {str(e)}"

# --- Platform 5: SmartSuite ---

def list_smartsuite_tables():
    """Lists all available tables (applications) in the SmartSuite workspace with their IDs and names."""
    logger.info("[SMARTSUITE] Listing tables...")
    url = "https://app.smartsuite.com/api/v1/applications/"
    headers = {"Authorization": f"Token {SS_API_KEY}", "Account-Id": SS_WORKSPACE_ID, "Content-Type": "application/json"}
    try:
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            return f"Error: {r.status_code}"
        return json.dumps([{"id": a["id"], "name": a["name"]} for a in r.json()])
    except Exception as e:
        return f"SmartSuite Error: {str(e)}"

def read_smartsuite_records(table_id: str, filter_status: str = None, limit: int = 25):
    """
    Reads records from a SmartSuite table. Returns a clean list with id, title, status, priority, due_date.

    KEY TABLES:
    - To Do:              699c631af30c42f70f822da9
    - Tasks:              67a9f59d803ee2b210b77e6b
    - To do list. Ariel:  679a6b06afa293fb6a876350
    - Requests:           698cfdd90a10157de821a6e2
    - Projects:           6848ca20899db5ede6de0e8b
    - Brands:             69116f0ddaf62cc60c42fe9e
    - Inventory:          69115f91f776658c52352ad3

    filter_status: optional — e.g. "in_progress", "complete", "backlog"
    limit: max records to return (default 25)
    """
    logger.info(f"[SMARTSUITE] Reading records from {table_id}...")
    url = f"https://app.smartsuite.com/api/v1/applications/{table_id}/records/list/"
    headers = {"Authorization": f"Token {SS_API_KEY}", "Account-Id": SS_WORKSPACE_ID, "Content-Type": "application/json"}
    payload = {}
    if filter_status:
        payload["filter"] = {"operator": "and", "fields": [{"field": "status", "comparison": "is", "value": filter_status}]}
    try:
        r = requests.post(url, json=payload, headers=headers)
        if r.status_code != 200:
            return f"Error: {r.status_code}"
        cleaned = []
        for rec in r.json().get("items", [])[:limit]:
            entry = {"id": rec.get("id", ""), "title": rec.get("title", "")}
            if "status" in rec and isinstance(rec["status"], dict):
                entry["status"] = rec["status"].get("value", "")
            if "priority" in rec:
                entry["priority"] = rec["priority"] if isinstance(rec["priority"], str) else rec["priority"].get("value", "")
            if "due_date" in rec and isinstance(rec["due_date"], dict):
                due = rec["due_date"].get("to_date", {}).get("date") or rec["due_date"].get("from_date", {}).get("date")
                if due:
                    entry["due_date"] = due[:10]
            if "assigned_to" in rec and rec["assigned_to"]:
                entry["assigned_to"] = [a.get("full_name", "") for a in rec["assigned_to"] if isinstance(a, dict)]
            cleaned.append(entry)
        return json.dumps(cleaned)
    except Exception as e:
        return f"SmartSuite Error: {str(e)}"

def create_smartsuite_record(table_id: str, data_dict: dict):
    """
    Creates a new record in a SmartSuite table.

    FIELD FORMAT RULES:
    - title/text fields: plain string  e.g. "title": "My Task"
    - status/priority/select: {"value": "..."} e.g. "status": {"value": "in-progress"}
    - date fields: {"value": "YYYY-MM-DD"}

    KEY TABLES:
    - To Do:    699c631af30c42f70f822da9  fields: title, status, priority, due_date
    - Tasks:    67a9f59d803ee2b210b77e6b  fields: title, status, due_date, assigned_to
    - Requests: 698cfdd90a10157de821a6e2  fields: title, status, priority
    - Projects: 6848ca20899db5ede6de0e8b  fields: title, status, due_date

    EXAMPLE: table_id="699c631af30c42f70f822da9", data_dict={"title": "Follow up vendor", "priority": {"value": "high"}}
    """
    logger.info(f"[SMARTSUITE] Creating record in {table_id}...")
    url = f"https://app.smartsuite.com/api/v1/applications/{table_id}/records/"
    headers = {"Authorization": f"Token {SS_API_KEY}", "Account-Id": SS_WORKSPACE_ID, "Content-Type": "application/json"}
    try:
        r = requests.post(url, json=data_dict, headers=headers)
        return "SUCCESS: Record created in SmartSuite." if r.status_code in [200, 201] else f"Error: {r.text}"
    except Exception as e:
        return f"SmartSuite Error: {str(e)}"

def update_smartsuite_record(table_id: str, record_id: str, data_dict: dict):
    """
    Updates specific fields on an existing SmartSuite record (PATCH — only changed fields needed).
    record_id comes from the 'id' field in read_smartsuite_records results.
    Field format is the same as create_smartsuite_record.

    KEY TABLES:
    - To Do:    699c631af30c42f70f822da9
    - Tasks:    67a9f59d803ee2b210b77e6b
    - Requests: 698cfdd90a10157de821a6e2

    EXAMPLE: update_smartsuite_record("699c631af30c42f70f822da9", "abc123", {"status": {"value": "complete"}})
    """
    logger.info(f"[SMARTSUITE] Updating record {record_id} in {table_id}...")
    url = f"https://app.smartsuite.com/api/v1/applications/{table_id}/records/{record_id}/"
    headers = {"Authorization": f"Token {SS_API_KEY}", "Account-Id": SS_WORKSPACE_ID, "Content-Type": "application/json"}
    try:
        r = requests.patch(url, json=data_dict, headers=headers)
        if r.status_code in [200, 204]:
            return "SUCCESS: SmartSuite record updated."
        return f"Error {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return f"SmartSuite Error: {str(e)}"

def get_staff_directory(department: str = None, active_only: bool = True):
    """
    Returns the 260 Sample Sale internal team directory from SmartSuite.
    Each entry includes: name, email, job_title, employment_status.

    department: optional filter — e.g. "Executive", "Marketing", "Ecommerce",
                "Instore Operations", "Human Resources". Case-insensitive substring match on job_title.
    active_only: if True (default), only returns employees with employment_status = "Active".

    Use this to:
    - Find who to contact or route a question to
    - Look up an employee's email before suggesting someone follow up
    - Answer "who handles X?" questions

    NOTE: Google Chat @mentions require user IDs not available here.
    Reference people by name and email instead.
    """
    logger.info(f"[STAFF] Fetching staff directory (dept={department}, active_only={active_only})")
    url = "https://app.smartsuite.com/api/v1/applications/69d7e2a517b939c112242b8e/records/list/"
    headers = {"Authorization": f"Token {SS_API_KEY}", "Account-Id": SS_WORKSPACE_ID, "Content-Type": "application/json"}
    try:
        r = requests.post(url, json={}, headers=headers)
        if r.status_code != 200:
            return f"Error: {r.status_code}"
        staff = []
        for rec in r.json().get("items", []):
            status = rec.get("employment_status", {}).get("value", "")
            if active_only and status != "Active":
                continue
            name      = rec.get("title", "")
            email     = rec.get("email", [None])[0] if rec.get("email") else ""
            job_title = rec.get("job_title", "")
            entry = {"name": name, "email": email, "job_title": job_title, "status": status or "Unknown"}
            if department and department.lower() not in job_title.lower():
                continue
            staff.append(entry)
        return json.dumps(staff)
    except Exception as e:
        return f"SmartSuite Error: {str(e)}"

# --- Platform 5: Long-Term Memory ---

def save_to_memory(fact: str, memory_type: str = "fact", namespace: str = "general"):
    """
    Saves an important business fact, user preference, or report summary to long-term memory.
    Call this proactively when the user states a preference, important context, or key business fact.
    memory_type options: "fact", "preference", "report"
    namespace options: "shopify", "basecamp", "smartsuite", "general"

    EXAMPLE: save_to_memory("User prefers vendors sorted by net_sales descending", "preference", "shopify")
    EXAMPLE: save_to_memory("Top vendor for Feb 2026 was Brand X with $45,000 net sales", "fact", "shopify")
    """
    if not CHROMA_ENABLED:
        return "Memory storage unavailable."
    try:
        doc_id = f"{memory_type}_{uuid.uuid4().hex[:8]}_{int(time.time())}"
        knowledge_col.add(
            documents=[fact],
            metadatas=[{"type": memory_type, "namespace": namespace, "date": datetime.now().strftime("%Y-%m-%d"), "timestamp": time.time()}],
            ids=[doc_id]
        )
        logger.info(f"[MEMORY] Saved {memory_type}: {fact[:80]}")
        return f"Saved to memory."
    except Exception as e:
        logger.error(f"[MEMORY] Save error: {e}")
        return f"Memory save error: {str(e)}"

# ============================================================
# SECTION 5: TOOL REGISTRY
# ============================================================

TOOL_DISPATCH_MAP = {
    "web_search":               web_search,
    "run_bigquery_report":      run_bigquery_report,
    "query_shopify_analytics":  query_shopify_analytics,
    "list_basecamp_projects":        list_basecamp_projects,
    "get_project_tools":             get_project_tools,
    "sync_basecamp_projects_to_memory": sync_basecamp_projects_to_memory,
    "post_to_basecamp":              post_to_basecamp,
    "read_basecamp_messages":        read_basecamp_messages,
    "read_message_full":             read_message_full,
    "post_to_campfire":              post_to_campfire,
    "read_campfire_lines":           read_campfire_lines,
    "get_todo_lists":                get_todo_lists,
    "get_todos":                     get_todos,
    "create_todo":                   create_todo,
    "complete_todo":                 complete_todo,
    "ga4_traffic_overview":     ga4_traffic_overview,
    "ga4_traffic_sources":      ga4_traffic_sources,
    "ga4_top_pages":            ga4_top_pages,
    "ga4_conversions":          ga4_conversions,
    "ga4_custom_report":        ga4_custom_report,
    "get_staff_directory":       get_staff_directory,
    "list_smartsuite_tables":   list_smartsuite_tables,
    "read_smartsuite_records":  read_smartsuite_records,
    "create_smartsuite_record": create_smartsuite_record,
    "update_smartsuite_record": update_smartsuite_record,
    "save_to_memory":           save_to_memory,
}

# Python callables for Ollama (auto-generates JSON schema from type hints + docstrings)
TOOL_FUNCTIONS_LIST = list(TOOL_DISPATCH_MAP.values())

# JSON schemas for Claude API
TOOLS_SCHEMA = [
    {
        "name": "web_search",
        "description": "Searches the live internet for current information, news, prices, or any topic not in internal systems.",
        "input_schema": {"type": "object", "properties": {"query": {"type": "string", "description": "The search query"}}, "required": ["query"]}
    },
    {
        "name": "run_bigquery_report",
        "description": "Executes SQL against BigQuery for historical data. NOTE: shopify_data.vendor_performance columns are currently unpopulated — use query_shopify_analytics instead for vendor sales. Table (2) pos_data.teamwork_transactions has columns: location, product_name, vendor, quantity, net_sales, transaction_date. Always use full table path with project: `gen-lang-client-0065509773.dataset.table`. Always use LIMIT clauses.",
        "input_schema": {"type": "object", "properties": {"sql_query": {"type": "string", "description": "Valid BigQuery Standard SQL query string"}}, "required": ["sql_query"]}
    },
    {
        "name": "query_shopify_analytics",
        "description": "Queries Shopify analytics using ShopifyQL. This is the PRIMARY source for vendor sales data. Syntax rules: FROM sales SHOW [metrics] GROUP BY [dimension] SINCE [date] UNTIL [date] WITH TIMEZONE 'America/New_York'. Use SHOW not SELECT. Use GROUP BY not BY. Use SINCE/UNTIL not WHERE for dates. Valid metrics: net_sales, gross_sales, orders. Valid dimensions: product_vendor, day, week, month. Example: FROM sales SHOW net_sales, gross_sales, orders GROUP BY product_vendor SINCE 2026-04-01 UNTIL 2026-04-05 WITH TIMEZONE 'America/New_York'. IMPORTANT: Always exclude product_vendor = 'ShipInsure' from results — it is a shipping add-on, not a brand.",
        "input_schema": {"type": "object", "properties": {"shopify_ql": {"type": "string", "description": "A valid ShopifyQL query string"}}, "required": ["shopify_ql"]}
    },
    {
        "name": "sync_basecamp_projects_to_memory",
        "description": "Scans all active Basecamp projects and saves their IDs (message_board_id, todoset_id, campfire_id, vault_id) to long-term memory. Call this once at the start of a session so the agent knows all project structures without re-discovering them.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "list_basecamp_projects",
        "description": "Lists all active Basecamp projects with their IDs. Use sync_basecamp_projects_to_memory for a full sync with tool IDs.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "get_project_tools",
        "description": "Gets all tools (message_board, chat, todoset, docs) for a specific Basecamp project. Use this to find IDs when not already in memory.",
        "input_schema": {"type": "object", "properties": {"project_id": {"type": "integer", "description": "The Basecamp project/bucket ID"}}, "required": ["project_id"]}
    },
    {
        "name": "post_to_basecamp",
        "description": "Posts a formatted message to a Basecamp Message Board. Use for reports and announcements. content_html must use <div> as outermost tag.",
        "input_schema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "integer", "description": "Basecamp project/bucket ID"},
                "message_board_id": {"type": "integer", "description": "Message board ID from get_project_tools"},
                "title": {"type": "string", "description": "Subject line of the message"},
                "content_html": {"type": "string", "description": "HTML body wrapped in <div> tags"}
            },
            "required": ["project_id", "message_board_id", "title", "content_html"]
        }
    },
    {
        "name": "read_basecamp_messages",
        "description": "Reads recent messages from a Basecamp Message Board. Returns title, author, date, and a content preview. Use read_message_full to get the complete body of a specific message.",
        "input_schema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "integer", "description": "Basecamp project/bucket ID"},
                "message_board_id": {"type": "integer", "description": "Message board ID from memory or get_project_tools"},
                "limit": {"type": "integer", "description": "Max messages to return (default 10)"}
            },
            "required": ["project_id", "message_board_id"]
        }
    },
    {
        "name": "read_message_full",
        "description": "Reads the complete content of a single Basecamp message. Use after read_basecamp_messages to get the full body of a specific post.",
        "input_schema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "integer"},
                "message_id": {"type": "integer", "description": "Message ID from read_basecamp_messages results"}
            },
            "required": ["project_id", "message_id"]
        }
    },
    {
        "name": "post_to_campfire",
        "description": "Posts a quick chat message to a Basecamp Project Chat. Use get_project_tools to find the campfire_id (named 'chat').",
        "input_schema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "integer"},
                "campfire_id": {"type": "integer", "description": "Chat/campfire ID from get_project_tools"},
                "content": {"type": "string", "description": "The message text to post"}
            },
            "required": ["project_id", "campfire_id", "content"]
        }
    },
    {
        "name": "read_campfire_lines",
        "description": "Reads the latest chat messages from a Basecamp Project Chat.",
        "input_schema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "integer"},
                "campfire_id": {"type": "integer"},
                "limit": {"type": "integer", "description": "Max lines to return (default 15)"}
            },
            "required": ["project_id", "campfire_id"]
        }
    },
    {
        "name": "get_todo_lists",
        "description": "Gets all to-do lists in a Basecamp project. Use memory or get_project_tools to find the todoset_id.",
        "input_schema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "integer"},
                "todoset_id": {"type": "integer", "description": "Todoset ID from memory or get_project_tools"}
            },
            "required": ["project_id", "todoset_id"]
        }
    },
    {
        "name": "get_todos",
        "description": "Reads all to-dos in a Basecamp to-do list. Returns ID, content, assignees, due date, and completion status. Use get_todo_lists to find the todolist_id.",
        "input_schema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "integer"},
                "todolist_id": {"type": "integer", "description": "Todo list ID from get_todo_lists"},
                "completed": {"type": "boolean", "description": "Set true to fetch completed todos (default: false = open todos)"}
            },
            "required": ["project_id", "todolist_id"]
        }
    },
    {
        "name": "create_todo",
        "description": "Creates a new to-do task in a Basecamp to-do list.",
        "input_schema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "integer"},
                "todolist_id": {"type": "integer"},
                "content": {"type": "string", "description": "The task description"},
                "due_on": {"type": "string", "description": "Due date in YYYY-MM-DD format (optional)"},
                "assignee_ids": {"type": "array", "items": {"type": "integer"}, "description": "List of assignee person IDs (optional)"}
            },
            "required": ["project_id", "todolist_id", "content"]
        }
    },
    {
        "name": "complete_todo",
        "description": "Marks a Basecamp to-do as complete. Use get_todos to find the todo_id.",
        "input_schema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "integer"},
                "todo_id": {"type": "integer", "description": "Todo ID from get_todos results"}
            },
            "required": ["project_id", "todo_id"]
        }
    },
    {
        "name": "ga4_traffic_overview",
        "description": "Returns day-by-day website traffic from Google Analytics 4: sessions, active users, pageviews, bounce rate. Use for traffic trends and daily performance.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Start date: 'today', 'yesterday', 'NdaysAgo', or 'YYYY-MM-DD'. Default: '7daysAgo'"},
                "end_date": {"type": "string", "description": "End date. Default: 'today'"}
            }
        }
    },
    {
        "name": "ga4_traffic_sources",
        "description": "Returns top traffic sources by sessions: channel (Organic Search, Email, Paid Social, etc.), source, medium, and conversions. Use to understand where visitors come from.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Start date. Default: '7daysAgo'"},
                "end_date": {"type": "string", "description": "End date. Default: 'today'"},
                "limit": {"type": "integer", "description": "Max rows to return. Default: 10"}
            }
        }
    },
    {
        "name": "ga4_top_pages",
        "description": "Returns the most-viewed pages by pageviews and users. Use to see which products or content are getting the most traffic.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Start date. Default: '7daysAgo'"},
                "end_date": {"type": "string", "description": "End date. Default: 'today'"},
                "limit": {"type": "integer", "description": "Max pages to return. Default: 15"}
            }
        }
    },
    {
        "name": "ga4_conversions",
        "description": "Returns daily conversion and revenue metrics from GA4: conversions, total revenue, transactions, and conversion rate.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Start date. Default: '7daysAgo'"},
                "end_date": {"type": "string", "description": "End date. Default: 'today'"}
            }
        }
    },
    {
        "name": "ga4_custom_report",
        "description": "Runs a custom GA4 report with any combination of dimensions and metrics. Use when the standard GA4 tools don't cover the specific breakdown needed. Common dimensions: date, pagePath, sessionSource, sessionMedium, deviceCategory, country, city. Common metrics: sessions, activeUsers, screenPageViews, bounceRate, conversions, totalRevenue, averageSessionDuration.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dimensions": {"type": "array", "items": {"type": "string"}, "description": "List of GA4 dimension names"},
                "metrics": {"type": "array", "items": {"type": "string"}, "description": "List of GA4 metric names"},
                "start_date": {"type": "string", "description": "Start date. Default: '7daysAgo'"},
                "end_date": {"type": "string", "description": "End date. Default: 'today'"},
                "limit": {"type": "integer", "description": "Max rows. Default: 20"}
            },
            "required": ["dimensions", "metrics"]
        }
    },
    {
        "name": "get_staff_directory",
        "description": "Returns the 260 Sample Sale internal team directory from SmartSuite. Each entry has name, email, job_title, and employment_status. Use to find who handles a topic, look up contact info, or route a question to the right person. Departments: Executive, Instore Operations, Marketing, Human Resources, Ecommerce.",
        "input_schema": {
            "type": "object",
            "properties": {
                "department": {"type": "string", "description": "Optional keyword to filter by department or role — e.g. 'Marketing', 'COO', 'Ecommerce'. Case-insensitive match against job_title."},
                "active_only": {"type": "boolean", "description": "If true (default), returns only Active employees."}
            }
        }
    },
    {
        "name": "list_smartsuite_tables",
        "description": "Lists all SmartSuite tables (applications) with their IDs. Key tables: To Do, Tasks, Requests, Projects, Brands, Inventory.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "read_smartsuite_records",
        "description": "Reads records from a SmartSuite table. Returns id, title, status, priority, due_date, assigned_to. Key table IDs: To Do=699c631af30c42f70f822da9, Tasks=67a9f59d803ee2b210b77e6b, Requests=698cfdd90a10157de821a6e2, Projects=6848ca20899db5ede6de0e8b.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_id": {"type": "string", "description": "SmartSuite application/table ID (hex string)"},
                "filter_status": {"type": "string", "description": "Optional filter: 'in_progress', 'complete', 'backlog'"},
                "limit": {"type": "integer", "description": "Max records (default 25)"}
            },
            "required": ["table_id"]
        }
    },
    {
        "name": "create_smartsuite_record",
        "description": "Creates a new SmartSuite record. Field formats: text=plain string, status/priority/select={value: '...'}, date={value: 'YYYY-MM-DD'}.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_id": {"type": "string", "description": "SmartSuite application/table ID"},
                "data_dict": {"type": "object", "description": "Record fields to set", "additionalProperties": True}
            },
            "required": ["table_id", "data_dict"]
        }
    },
    {
        "name": "update_smartsuite_record",
        "description": "Updates fields on an existing SmartSuite record (PATCH — only include fields to change). record_id comes from read_smartsuite_records results.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_id": {"type": "string"},
                "record_id": {"type": "string", "description": "The record ID from read_smartsuite_records"},
                "data_dict": {"type": "object", "description": "Fields to update", "additionalProperties": True}
            },
            "required": ["table_id", "record_id", "data_dict"]
        }
    },
    {
        "name": "save_to_memory",
        "description": "Saves an important fact, user preference, or business insight to long-term memory. Call this proactively when the user states preferences or key business context.",
        "input_schema": {
            "type": "object",
            "properties": {
                "fact": {"type": "string", "description": "The fact or preference to remember"},
                "memory_type": {"type": "string", "description": "'fact', 'preference', or 'report'", "enum": ["fact", "preference", "report"]},
                "namespace": {"type": "string", "description": "Domain namespace for structured retrieval: 'shopify' (sales/vendor data), 'basecamp' (project/event data), 'smartsuite' (brand/CRM data), 'general' (cross-domain facts)", "enum": ["shopify", "basecamp", "smartsuite", "general"]}
            },
            "required": ["fact"]
        }
    }
]

# ============================================================
# SECTION 6: THREAD-SAFE SESSION STORE
# ============================================================
sessions_lock = threading.Lock()
chat_sessions = {}  # space_name -> {"messages": list, "last_active": float}

def get_or_create_session(space_name: str) -> dict:
    """Returns the session for a space, creating one and seeding from ChromaDB if needed."""
    with sessions_lock:
        if space_name not in chat_sessions:
            seed = _restore_session_from_chromadb(space_name)
            chat_sessions[space_name] = {"messages": seed, "last_active": time.time()}
            logger.info(f"[SESSION] New session for {space_name} (seeded {len(seed)//2} prior turns)")
        chat_sessions[space_name]["last_active"] = time.time()
        return chat_sessions[space_name]

def cleanup_sessions():
    """Removes sessions inactive for more than SESSION_TTL_SECONDS. Called by scheduler."""
    with sessions_lock:
        cutoff = time.time() - SESSION_TTL_SECONDS
        stale = [k for k, v in chat_sessions.items() if v["last_active"] < cutoff]
        for k in stale:
            del chat_sessions[k]
    if stale:
        logger.info(f"[SESSION] Cleaned up {len(stale)} expired sessions")

# ============================================================
# SECTION 7: CHROMADB MEMORY FUNCTIONS
# ============================================================

def _restore_session_from_chromadb(space_name: str) -> list:
    """
    Retrieves recent conversation history for a space from ChromaDB.
    If the last exchange was more than 24 hours ago, starts fresh to prevent
    the model from anchoring to stale dates or outdated context.
    """
    if not CHROMA_ENABLED or conversations_col.count() == 0:
        return []
    try:
        results = conversations_col.get(
            where={"space_name": space_name},
            include=["documents", "metadatas"]
        )
        if not results["documents"]:
            return []
        pairs = sorted(
            zip(results["documents"], results["metadatas"]),
            key=lambda x: x[1].get("timestamp", 0)
        )[-10:]  # last 10 exchanges

        # If the most recent exchange is older than 24 hours, start fresh
        last_timestamp = pairs[-1][1].get("timestamp", 0)
        age_hours = (time.time() - last_timestamp) / 3600
        if age_hours > 24:
            logger.info(f"[SESSION] Last exchange was {age_hours:.0f}h ago — starting fresh session")
            return []

        messages = []
        for doc, _ in pairs:
            parts = doc.split("\nAssistant: ", 1)
            if len(parts) == 2:
                messages.append({"role": "user", "content": parts[0].replace("User: ", "", 1)})
                messages.append({"role": "assistant", "content": parts[1]})
        return messages
    except Exception as e:
        logger.warning(f"[MEMORY] Session restore error: {e}")
        return []

def _detect_namespace(user_text: str) -> str | None:
    """Infers a ChromaDB namespace from the query domain, or None if unclear."""
    text = user_text.lower()
    if any(kw in text for kw in ["vendor", "sales", "order", "shopify", "product", "revenue", "gross"]):
        return "shopify"
    if any(kw in text for kw in ["basecamp", "campfire", "briefing", "message board", "event project"]):
        return "basecamp"
    if any(kw in text for kw in ["smartsuite", "brand profile", "crm", "brand record", "inventory record"]):
        return "smartsuite"
    return None

def build_memory_context(user_text: str) -> str:
    """
    Retrieves relevant knowledge/facts from ChromaDB to inject into the system prompt.
    When the query clearly maps to a domain, filters by namespace first.
    Falls back to unfiltered search if no results are found.
    """
    if not CHROMA_ENABLED or knowledge_col.count() == 0:
        return ""
    try:
        docs = []
        namespace = _detect_namespace(user_text)
        if namespace:
            try:
                results = knowledge_col.query(
                    query_texts=[user_text],
                    n_results=5,
                    where={"namespace": {"$eq": namespace}}
                )
                docs = results["documents"][0] if results["documents"] else []
            except Exception:
                pass  # fall through to unfiltered search
        if not docs:
            results = knowledge_col.query(query_texts=[user_text], n_results=5)
            docs = results["documents"][0] if results["documents"] else []
        if not docs:
            return ""
        facts = "\n".join(f"- {doc}" for doc in docs)
        return f"\nRELEVANT CONTEXT FROM MEMORY:\n{facts}"
    except Exception as e:
        logger.warning(f"[MEMORY] Knowledge retrieval error: {e}")
        return ""

def save_conversation_turn(space_name: str, user_text: str, ai_response: str):
    """Saves a completed conversation turn to ChromaDB for cross-session persistence."""
    if not CHROMA_ENABLED:
        return
    try:
        doc_id = f"conv_{space_name.replace('/', '_')}_{int(time.time())}"
        conversations_col.add(
            documents=[f"User: {user_text}\nAssistant: {ai_response}"],
            metadatas=[{"space_name": space_name, "namespace": "general", "timestamp": time.time(), "date": datetime.now().strftime("%Y-%m-%d")}],
            ids=[doc_id]
        )
    except Exception as e:
        logger.warning(f"[MEMORY] Conversation save error: {e}")

# ============================================================
# SECTION 8: ROUTING + DISPATCH
# ============================================================

def classify_complexity(message: str) -> int:
    """
    Returns routing tier: 1 (simple/conversational), 2 (moderate/single-tool),
    or 3 (complex/multi-source/reports).
    Tier 1 → Haiku (1k tokens). Tier 2 → Sonnet (2k tokens). Tier 3 → Sonnet (4k tokens).
    """
    text = message.lower()
    word_count = len(message.split())

    # Count distinct platforms referenced
    platforms_mentioned = sum([
        any(kw in text for kw in ["shopify", "vendor", "sales", "order"]),
        any(kw in text for kw in ["bigquery", "bq", "pos", "in-store", "in store"]),
        any(kw in text for kw in ["basecamp", "campfire", "message board"]),
        any(kw in text for kw in ["smartsuite", "brand profile", "inventory"]),
        any(kw in text for kw in ["ga4", "analytics", "traffic", "sessions"]),
    ])

    # Tier 3: reports, analysis, multi-source, or long messages
    if any(signal in text for signal in TIER3_SIGNALS):
        return 3
    if platforms_mentioned >= 2:
        return 3
    if word_count > 30:
        return 3

    # Tier 1: short conversational messages with no data signals
    if word_count < 10 and not any(signal in text for signal in TIER2_SIGNALS):
        return 1
    if any(signal in text for signal in CONVERSATIONAL_SIGNALS) and word_count < 15:
        return 1

    # Tier 2: single-tool queries and everything else
    return 2

def dispatch_tool(name: str, kwargs: dict) -> str:
    """Executes a tool by name and returns its result as a string."""
    fn = TOOL_DISPATCH_MAP.get(name)
    if fn is None:
        return f"Unknown tool: {name}"
    try:
        result = fn(**kwargs)
        return result if isinstance(result, str) else json.dumps(result, default=str)
    except Exception as e:
        logger.error(f"[TOOL] {name} error: {e}")
        return f"Tool error ({name}): {str(e)}"

# ============================================================
# SECTION 9: AGENTIC LOOPS
# ============================================================

# Load business context from BUSINESS_CONTEXT.md at startup
_bc_context_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "BUSINESS_CONTEXT.md")
try:
    with open(_bc_context_path, "r") as f:
        BUSINESS_CONTEXT = f.read()
    logger.info("[CONTEXT] BUSINESS_CONTEXT.md loaded")
except FileNotFoundError:
    BUSINESS_CONTEXT = ""
    logger.warning("[CONTEXT] BUSINESS_CONTEXT.md not found — running without business context")

SYSTEM_PROMPT_BASE = """# ROLE: Mole — 260 Sample Sale Lead Business Analyst & Ops Coordinator

Your name is Mole. You are the "Chief of Staff" for 260 Sample Sale — a high-volume luxury sample sale retailer
with physical locations and an online Shopify store (260-sample-sale.myshopify.com).
Your goal is to optimize every 7-day event window for our brand partners and internal teams.
You are not a data clerk. You interpret, synthesize, and recommend.

## CORE ANALYTICAL PRINCIPLES
1. **The "Why" First**: Never report a number without a hypothesis. If conversion is low,
   triangulate GA4 traffic, Shopify price points, and historical BigQuery data to find the friction.
2. **Metric Priority**:
   - PRIMARY: Traffic, Conversion Rate, Net Sales
   - SECONDARY: UPT (Units Per Transaction), AUR (Average Unit Retail), STR (Sell-Through Rate)
3. **The Switchboard**: Always clarify the audience before responding:
   - "Client Recap" → polished, professional, brand-safe language
   - "Internal Audit" → strategic, direct, no sugarcoating

## YOUR TWO OPERATIONAL PLATFORMS

**Basecamp — The Event Hub (changes weekly)**
Basecamp is where 260 runs its live events. Each active project contains event details,
staff briefings, operational updates, and in-store team feedback for that week's sale.
Your job is to read this content and help the in-store team be "Expert Insiders":
- Who is this brand? What are they known for?
- What are the hero items this week?
- What feedback has the floor team posted?
- What operational notes should staff know before the doors open?
Use sync_basecamp_projects_to_memory() to load all project IDs, then read directly.
Post updates, create todos, and push briefings into the relevant project boards.

**SmartSuite — The CRM & Project Management Layer (evergreen)**
SmartSuite is not event-based. It is the internal source of truth for:
- Brand/client profiles and relationship history
- Ongoing projects and their status
- Task tracking across departments
- Requests and inventory details
When a brand is mentioned, check SmartSuite for their client profile and history first.
Key tables: Tasks, Requests, Inventory, Projects, Brands.

## DATA TOOLS
- **Shopify (ShopifyQL)**: PRIMARY source for vendor sales — net_sales, gross_sales, orders grouped by
  product_vendor. Syntax: FROM sales SHOW [metrics] GROUP BY [dimension] SINCE/UNTIL [dates] WITH TIMEZONE 'America/New_York'.
  Always exclude 'ShipInsure' from vendor results — it is a shipping protection add-on, not a brand.
  'Inner Circle' is the 260 loyalty program — exclude from sales/vendor reporting. It does appear in
  Shopify data as line pass and VIP pass purchases which are relevant for event operations context.
- **BigQuery**: POS and historical data — `pos_data.teamwork_transactions` (in-store POS by location/product/date).
  Note: shopify_data.vendor_performance columns are currently unpopulated — use ShopifyQL for vendor sales.
- **Google Analytics 4** (Property: 329727471): Web traffic, traffic sources, top pages, conversions,
  and revenue. Use to understand digital demand signals before and during events.
- **Web Search**: Brand research, market context, anything not in internal systems.

## PROACTIVE TOOLS
- **Brand One-Pager**: When a new event launches or a brand is mentioned, offer to generate
  a 1-page brief for retail staff: Brand DNA, Hero Items, Price Range, Customer Profile.
- **Comparative Benchmarks**: Always compare current event performance against historical
  BigQuery data for the same brand or same time period.
- **Automation Suggestions**: When you spot a recurring manual task in Basecamp or SmartSuite,
  suggest a template, script, or automation to streamline it.

## TEAM & DEPARTMENT ROUTING
260 has five internal departments. When a question involves a specific function, route it to the right team:
- **Executive** — Assaf Azani (CEO), Ariel Azani (COO) — strategy, partnerships, final decisions
- **Instore Operations** — event logistics, floor staff, crowd management, location issues
- **Marketing** — campaigns, email/SMS (Bloomreach), social media, brand launch promotion
- **Human Resources** — staffing, hiring, onboarding, employee questions
- **Ecommerce** — Shopify store, online events, digital merchandising

Use get_staff_directory() to look up current team members, emails, and roles before routing.
Reference people by name and email — Google Chat @mentions are not currently supported via the API.

## MEMORY
- Call save_to_memory() whenever a brand fact, user preference, or key business context is shared.
- Reference ChromaDB context below for Basecamp project IDs and prior session facts.
- If a brand is mentioned, surface any historical performance data from memory immediately.

## FORMATTING & TONE
- **Tone**: High-energy, fashion-insider, data-obsessed, proactive. Chief of Staff energy.
- **Lead with BLUF** (Bottom Line Up Front) — answer first, details second.
- **Use Markdown tables** for any data with more than 3 rows.
- **Keep responses scannable** — this is Google Chat, people are on the floor in stores.
- **Confidentiality**: Never reference Brand A's performance data when discussing Brand B.

Today: {today}. Timezone: America/New_York.
{memory_context}
{business_context}"""

def _run_claude_loop(messages: list, system_prompt: str, model: str, max_tokens: int) -> str:
    """Core Claude agentic tool-use loop — shared by both Haiku and Sonnet."""
    for _ in range(MAX_TOOL_ROUNDS):
        response = ai_client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system_prompt,
            tools=TOOLS_SCHEMA,
            messages=messages
        )
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            return next((b.text for b in response.content if hasattr(b, "text")), "")

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    logger.info(f"[TOOL] {block.name}({json.dumps(block.input)[:120]})")
                    result = dispatch_tool(block.name, block.input)
                    logger.info(f"[RESULT] {block.name} -> {str(result)[:200]}")
                    tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": str(result)})
            messages.append({"role": "user", "content": tool_results})

    return "I reached the maximum tool call limit. Please try a simpler request."

def run_haiku_loop(messages: list, system_prompt: str) -> str:
    """Runs the Claude Haiku loop — used for simple, fast queries."""
    return _run_claude_loop(messages, system_prompt, model=CLAUDE_HAIKU_MODEL, max_tokens=2048)

def run_claude_loop(messages: list, system_prompt: str) -> str:
    """Runs the Claude Sonnet loop — used for complex queries, reports, and analysis."""
    return _run_claude_loop(messages, system_prompt, model=CLAUDE_SONNET_MODEL, max_tokens=4096)

# ============================================================
# SECTION 10: MAIN AI ENTRY POINT
# ============================================================

def process_ai_response(user_text: str, space_name: str) -> str:
    """Routes the user message to the appropriate model and returns the final reply."""
    session = get_or_create_session(space_name)
    memory_ctx = build_memory_context(user_text)
    system_prompt = SYSTEM_PROMPT_BASE.format(
        today=datetime.now().strftime("%Y-%m-%d"),
        memory_context=memory_ctx,
        business_context=BUSINESS_CONTEXT
    )

    with sessions_lock:
        messages = list(session["messages"]) + [{"role": "user", "content": user_text}]

    # Route by complexity tier
    tier = classify_complexity(user_text)
    _tier_model  = {1: CLAUDE_HAIKU_MODEL,  2: CLAUDE_SONNET_MODEL, 3: CLAUDE_SONNET_MODEL}
    _tier_tokens = {1: 1024,                2: 2048,                3: 4096}
    _tier_label  = {1: "HAIKU (Tier 1)",    2: "SONNET (Tier 2)",   3: "SONNET (Tier 3)"}
    model      = _tier_model[tier]
    max_tokens = _tier_tokens[tier]
    logger.info(f"[ROUTER] → {_tier_label[tier]}: {user_text[:80]}")

    final_text = None
    for attempt in range(3):
        try:
            final_text = _run_claude_loop(list(messages), system_prompt, model, max_tokens)
            break
        except anthropic.RateLimitError:
            wait = (attempt + 1) * 20
            logger.warning(f"[CLAUDE] Rate limit, waiting {wait}s (attempt {attempt + 1}/3)...")
            time.sleep(wait)
        except Exception as e:
            logger.error(f"[{_tier_label[tier]}] Error: {e}")
            if tier == 1:
                logger.info("[ROUTER] Tier 1 (Haiku) failed — escalating to Tier 2 (Sonnet)")
                tier = 2
                model      = CLAUDE_SONNET_MODEL
                max_tokens = 2048
            elif tier == 2:
                logger.info("[ROUTER] Tier 2 (Sonnet) failed — escalating to Tier 3 (Sonnet high-cap)")
                tier = 3
                max_tokens = 4096
            else:
                return "I encountered an error processing your request. Please try again."

    if final_text is None:
        return "I'm overwhelmed with requests right now. Please try again in a minute."

    # Save to session and ChromaDB
    with sessions_lock:
        session["messages"].append({"role": "user", "content": user_text})
        session["messages"].append({"role": "assistant", "content": final_text})
        if len(session["messages"]) > SESSION_MAX_MESSAGES:
            session["messages"] = session["messages"][-SESSION_MAX_MESSAGES:]

    save_conversation_turn(space_name, user_text, final_text)
    return final_text

# ============================================================
# SECTION 10.5: DAILY REPORT PIPELINE
# ============================================================
# Runs at 8 AM ET via APScheduler. Four named stages with verification.
# Basecamp posting is intentionally deferred — validate data output first.
# To enable posting: wire log_report_output() to post_to_basecamp() once
# Stage 1–3 are confirmed working in production.

def fetch_shopify_data() -> dict:
    """
    Stage 1: Fetch yesterday's vendor sales from Shopify.
    Returns {"vendors": [...], "date": "YYYY-MM-DD"} or {"error": "..."}.
    Verification: vendors list must be non-empty.
    """
    logger.info("[PIPELINE] Stage 1 — fetch_shopify_data() starting")
    yesterday = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    shopify_ql = (
        f"FROM sales SHOW net_sales, gross_sales, orders "
        f"GROUP BY product_vendor "
        f"SINCE {yesterday} UNTIL {yesterday} "
        f"WITH TIMEZONE 'America/New_York'"
    )
    try:
        raw = query_shopify_analytics(shopify_ql)
        if raw.startswith("Error") or raw.startswith("Connection") or raw.startswith("ShopifyQL"):
            logger.error(f"[PIPELINE] Stage 1 FAILED — Shopify error: {raw}")
            return {"error": raw}
        data = json.loads(raw)
        rows = data.get("tableData", {}).get("rows", [])
        vendors = [
            r for r in rows
            if r.get("product_vendor") not in ("ShipInsure", "Inner Circle")
        ]
        logger.info(f"[PIPELINE] Stage 1 COMPLETE — {len(vendors)} vendors fetched for {yesterday}")
        return {"vendors": vendors, "date": yesterday}
    except Exception as e:
        logger.error(f"[PIPELINE] Stage 1 FAILED — exception: {e}")
        return {"error": str(e)}

def fetch_bigquery_data() -> dict:
    """
    Stage 2: Fetch yesterday's POS transactions from BigQuery.
    Returns {"rows": [...], "date": "YYYY-MM-DD"} or {"error": "..."}.
    Verification: rows list must be non-empty.
    """
    logger.info("[PIPELINE] Stage 2 — fetch_bigquery_data() starting")
    yesterday = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    sql = (
        f"SELECT location_name, vendor, product_name, quantity, net_amount "
        f"FROM `gen-lang-client-0065509773.pos_data.teamwork_transactions` "
        f"WHERE DATE(transaction_date) = '{yesterday}' "
        f"ORDER BY net_amount DESC "
        f"LIMIT 500"
    )
    try:
        raw = run_bigquery_report(sql)
        if raw.startswith("BigQuery Error"):
            logger.error(f"[PIPELINE] Stage 2 FAILED — BigQuery error: {raw}")
            return {"error": raw}
        rows = json.loads(raw)
        logger.info(f"[PIPELINE] Stage 2 COMPLETE — {len(rows)} POS rows fetched for {yesterday}")
        return {"rows": rows, "date": yesterday}
    except Exception as e:
        logger.error(f"[PIPELINE] Stage 2 FAILED — exception: {e}")
        return {"error": str(e)}

def merge_and_format_report(shopify_result: dict, bq_result: dict) -> str:
    """
    Stage 3: Merges Shopify vendor data and BigQuery POS data into a formatted report string.
    Verification: output length must be > 100 chars.
    """
    logger.info("[PIPELINE] Stage 3 — merge_and_format_report() starting")
    date = shopify_result.get("date", bq_result.get("date", "unknown"))
    lines = [f"=== 260 Sample Sale — Daily Report ({date}) ===", ""]

    # Online sales (Shopify)
    lines.append("--- ONLINE SALES (Shopify) ---")
    vendors = shopify_result.get("vendors", [])
    if vendors:
        lines.append(f"{'Vendor':<35} {'Net Sales':>12} {'Gross Sales':>12} {'Orders':>8}")
        lines.append("-" * 70)
        for v in vendors:
            name  = str(v.get("product_vendor", "Unknown"))[:34]
            net   = str(v.get("net_sales",   "—"))
            gross = str(v.get("gross_sales", "—"))
            orders = str(v.get("orders",     "—"))
            lines.append(f"{name:<35} {net:>12} {gross:>12} {orders:>8}")
    else:
        lines.append("  No online sales data available.")
    lines.append("")

    # In-store sales (BigQuery POS) — aggregate by location
    lines.append("--- IN-STORE SALES (POS / BigQuery) ---")
    rows = bq_result.get("rows", [])
    if rows:
        by_location: dict[str, float] = {}
        for row in rows:
            loc = row.get("location_name", "Unknown")
            amt = float(row.get("net_amount", 0) or 0)
            by_location[loc] = by_location.get(loc, 0.0) + amt
        for loc, total in sorted(by_location.items(), key=lambda x: -x[1]):
            lines.append(f"  {loc}: ${total:,.2f}")
    else:
        lines.append("  No in-store POS data available.")
    lines.append("")
    lines.append("=== END REPORT ===")

    report = "\n".join(lines)
    logger.info(f"[PIPELINE] Stage 3 COMPLETE — report is {len(report)} chars")
    return report

def log_report_output(report: str):
    """
    Stage 4: Writes the formatted report to the console log.
    Basecamp posting is deferred — wire post_to_basecamp() here once Stage 1–3 are validated.
    """
    logger.info("[PIPELINE] Stage 4 — log_report_output() starting")
    logger.info(f"[DAILY REPORT]\n{report}")
    logger.info("[PIPELINE] Stage 4 COMPLETE — report written to log")

def run_daily_report_pipeline():
    """
    Orchestrates the 4-stage daily report pipeline. Scheduled at 8 AM ET.
    Logs stage start, completion, and verification result for each stage.
    Halts with a logged error if any stage fails verification.
    """
    logger.info("[PIPELINE] === Daily report pipeline starting ===")

    # Stage 1
    shopify_result = fetch_shopify_data()
    if "error" in shopify_result:
        logger.error(f"[PIPELINE] HALTED at Stage 1 — fetch error: {shopify_result['error']}")
        return
    if not shopify_result.get("vendors"):
        logger.error("[PIPELINE] HALTED at Stage 1 — verification failed: vendor list is empty")
        return
    logger.info(f"[PIPELINE] Stage 1 verified ✓ ({len(shopify_result['vendors'])} vendors)")

    # Stage 2
    bq_result = fetch_bigquery_data()
    if "error" in bq_result:
        logger.error(f"[PIPELINE] HALTED at Stage 2 — fetch error: {bq_result['error']}")
        return
    if not bq_result.get("rows"):
        logger.error("[PIPELINE] HALTED at Stage 2 — verification failed: transaction rows are empty")
        return
    logger.info(f"[PIPELINE] Stage 2 verified ✓ ({len(bq_result['rows'])} rows)")

    # Stage 3
    report = merge_and_format_report(shopify_result, bq_result)
    if len(report) <= 100:
        logger.error(f"[PIPELINE] HALTED at Stage 3 — verification failed: report too short ({len(report)} chars)")
        return
    logger.info(f"[PIPELINE] Stage 3 verified ✓ ({len(report)} chars)")

    # Stage 4
    log_report_output(report)
    logger.info("[PIPELINE] === Daily report pipeline complete ===")

# ============================================================
# SECTION 11: SCHEDULER (session cleanup + daily report)
# ============================================================

def setup_scheduler():
    """Creates and starts the APScheduler. Returns the scheduler instance."""
    from apscheduler.schedulers.background import BackgroundScheduler
    import pytz

    eastern = pytz.timezone("America/New_York")
    scheduler = BackgroundScheduler(timezone=eastern)
    scheduler.add_job(
        cleanup_sessions,
        "interval", minutes=30,
        id="session_cleanup"
    )
    scheduler.add_job(
        run_daily_report_pipeline,
        "cron", hour=8, minute=0,
        timezone=eastern,
        id="daily_report"
    )
    scheduler.start()
    logger.info("[SCHEDULER] Started — session cleanup every 30 min, daily report at 8 AM ET")
    return scheduler

# ============================================================
# SECTION 12: GOOGLE CHAT MESSAGING
# ============================================================

def send_reply(space_name: str, text: str, thread_name: str = None):
    """
    Sends a text message back to the Google Chat space.
    In spaces (ROOM), pass thread_name to reply in-thread rather than posting a new message.
    """
    message = chat_v1.Message()
    message.text = text
    if thread_name:
        message.thread = chat_v1.Thread(name=thread_name)
        request = chat_v1.CreateMessageRequest(
            parent=space_name,
            message=message,
            message_reply_option="REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD",
        )
    else:
        request = chat_v1.CreateMessageRequest(parent=space_name, message=message)
    chat_client.create_message(request=request)

def callback(message):
    """Triggered whenever a message arrives via Pub/Sub."""
    try:
        data = json.loads(message.data.decode("utf-8"))
        msg_data   = data.get("message", {})
        user_text  = msg_data.get("text", "").strip()
        space_name = msg_data.get("space", {}).get("name")
        space_type = msg_data.get("space", {}).get("type", "")

        # In spaces (ROOM), capture the thread so replies stay grouped
        thread_name = msg_data.get("thread", {}).get("name") if space_type == "ROOM" else None

        if not user_text or not space_name:
            message.ack()
            return

        # Strip @Mole mentions — Google Chat prepends these in spaces
        user_text = re.sub(r"<users/\d+>", "", user_text)
        user_text = re.sub(r"@mole\b", "", user_text, flags=re.IGNORECASE)
        user_text = user_text.strip()

        if not user_text:
            message.ack()
            return

        logger.info(f"[RECEIVED] [{space_type or 'DM'}] {user_text[:100]}")

        send_reply(space_name, "⏳ Working on it...", thread_name)

        ai_reply = process_ai_response(user_text, space_name)
        send_reply(space_name, ai_reply, thread_name)
        logger.info(f"[SENT] {ai_reply[:100]}")

        message.ack()
    except Exception as e:
        logger.error(f"[CALLBACK] Error: {e}", exc_info=True)
        message.nack()

# ============================================================
# SECTION 13: MAIN
# ============================================================
if __name__ == "__main__":
    # Auto-refresh Basecamp token if needed
    if BC_REFRESH_TOKEN:
        refresh_bc_token()
    else:
        logger.warning("[BASECAMP] No BC_REFRESH_TOKEN found — token will not auto-refresh")

    # Auto-sync Basecamp project structure if stale
    if BC_ACCESS_TOKEN:
        if _bc_sync_is_stale():
            logger.info("[BASECAMP] Project sync is stale — running startup sync...")
            sync_basecamp_projects_to_memory()
        else:
            logger.info(f"[BASECAMP] Project sync is fresh (within {BC_SYNC_DAYS} days) — skipping")
    else:
        logger.warning("[BASECAMP] No access token — skipping project sync")

    scheduler = setup_scheduler()

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    mode = f"Haiku ({CLAUDE_HAIKU_MODEL}) + Sonnet ({CLAUDE_SONNET_MODEL})"
    memory = "ChromaDB enabled" if CHROMA_ENABLED else "in-memory only"
    logger.info(f"--- Agent LIVE | Mode: {mode} | Memory: {memory} ---")

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        scheduler.shutdown()
        logger.info("Agent stopped.")
