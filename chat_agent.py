import os
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

BC_ACCESS_TOKEN    = os.environ.get("BC_ACCESS_TOKEN")
BC_ACCOUNT_ID      = os.environ.get("BC_ACCOUNT_ID")
BC_USER_AGENT      = "ArielAI (Ariel@260samplesale.com)"
BC_REPORT_PROJECT_ID = int(os.environ.get("BC_REPORT_PROJECT_ID", "0"))
BC_REPORT_BOARD_ID   = int(os.environ.get("BC_REPORT_BOARD_ID", "0"))

SS_API_KEY         = os.environ.get("SS_API_KEY")
SS_WORKSPACE_ID    = os.environ.get("SS_WORKSPACE_ID")

ANTHROPIC_API_KEY  = os.environ["ANTHROPIC_API_KEY"]
USE_LOCAL_LLM      = os.environ.get("USE_LOCAL_LLM", "false").lower() == "true"
OLLAMA_MODEL       = os.environ.get("OLLAMA_MODEL", "qwen3:14b")
OLLAMA_HOST        = os.environ.get("OLLAMA_HOST", "http://localhost:11434")

SESSION_TTL_SECONDS  = 3600   # sessions expire after 1 hour of inactivity
SESSION_MAX_MESSAGES = 40     # trim history to prevent context overflow
MAX_TOOL_ROUNDS      = 10     # max tool call iterations per response

# Queries containing these words are routed to Claude (more powerful)
COMPLEX_SIGNALS = [
    "report", "compare", "analyze", "summary", "post to", "post a",
    "generate", "schedule", "breakdown", "trend", "vs", "versus",
    "create a", "make a", "write a", "send a", "draft"
]

# ============================================================
# SECTION 3: CLIENT INITIALIZATION
# ============================================================
from google.cloud import pubsub_v1
from google.apps import chat_v1
from google.cloud import bigquery
from tavily import TavilyClient
import anthropic

ai_client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
chat_client = chat_v1.ChatServiceClient()
tavily      = TavilyClient(api_key=os.environ['TAVILY_API_KEY'])
bq_client   = bigquery.Client(project="gen-lang-client-0065509773")

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

# --- Optional: Ollama local LLM ---
ollama_client = None
if USE_LOCAL_LLM:
    try:
        import ollama as ollama_module
        ollama_client = ollama_module.Client(host=OLLAMA_HOST)
        logger.info(f"Ollama client ready at {OLLAMA_HOST} (model: {OLLAMA_MODEL})")
    except ImportError:
        logger.warning("ollama package not installed — falling back to Claude-only mode")
        USE_LOCAL_LLM = False

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

def list_basecamp_projects():
    """Retrieves a list of all active Basecamp projects/buckets and their IDs."""
    logger.info("[BASECAMP] Fetching projects...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/projects.json"
    headers = {"Authorization": f"Bearer {BC_ACCESS_TOKEN}", "User-Agent": BC_USER_AGENT}
    try:
        r = requests.get(url, headers=headers)
        return json.dumps(r.json()) if r.status_code == 200 else f"Error: {r.status_code}"
    except Exception as e:
        return f"Error: {str(e)}"

def get_project_tools(project_id: int):
    """Retrieves all tools (Message Board, Chat, Todos, etc.) for a Basecamp project ID."""
    logger.info(f"[BASECAMP] Getting tools for project {project_id}...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/projects/{project_id}.json"
    headers = {"Authorization": f"Bearer {BC_ACCESS_TOKEN}", "User-Agent": BC_USER_AGENT}
    try:
        r = requests.get(url, headers=headers)
        return json.dumps(r.json().get("dock", [])) if r.status_code == 200 else f"Error: {r.status_code}"
    except Exception as e:
        return f"Error: {str(e)}"

def post_to_basecamp(project_id: int, message_board_id: int, title: str, content_html: str):
    """Posts a formatted message to a Basecamp Message Board. content_html must be wrapped in <div> tags."""
    logger.info(f"[BASECAMP] Posting to board {message_board_id}...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/buckets/{project_id}/message_boards/{message_board_id}/messages.json"
    headers = {"Authorization": f"Bearer {BC_ACCESS_TOKEN}", "Content-Type": "application/json", "User-Agent": BC_USER_AGENT}
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
    headers = {"Authorization": f"Bearer {BC_ACCESS_TOKEN}", "User-Agent": BC_USER_AGENT}
    try:
        r = requests.get(url, headers=headers)
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
    headers = {"Authorization": f"Bearer {BC_ACCESS_TOKEN}", "Content-Type": "application/json", "User-Agent": BC_USER_AGENT}
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
    headers = {"Authorization": f"Bearer {BC_ACCESS_TOKEN}", "User-Agent": BC_USER_AGENT}
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            return json.dumps([{"name": l["creator"]["name"], "text": l["content"]} for l in r.json()[:limit]])
        return f"Error: {r.status_code}"
    except Exception as e:
        return f"Error: {str(e)}"

def get_todo_lists(project_id: int, todoset_id: int):
    """Retrieves all to-do lists in a Basecamp project. Use get_project_tools to find the todoset ID."""
    logger.info(f"[BASECAMP] Fetching to-do lists for project {project_id}...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/buckets/{project_id}/todosets/{todoset_id}/todolists.json"
    headers = {"Authorization": f"Bearer {BC_ACCESS_TOKEN}", "User-Agent": BC_USER_AGENT}
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            return json.dumps([{"id": l["id"], "title": l["title"]} for l in r.json()])
        return f"Error: {r.status_code}"
    except Exception as e:
        return f"Error: {str(e)}"

def create_todo(project_id: int, todolist_id: int, content: str, due_on: str = None, assignee_ids: list = None):
    """Creates a new to-do in a Basecamp to-do list. due_on format: YYYY-MM-DD (optional)."""
    logger.info(f"[BASECAMP] Creating to-do in list {todolist_id}...")
    url = f"https://3.basecampapi.com/{BC_ACCOUNT_ID}/buckets/{project_id}/todolists/{todolist_id}/todos.json"
    headers = {"Authorization": f"Bearer {BC_ACCESS_TOKEN}", "Content-Type": "application/json", "User-Agent": BC_USER_AGENT}
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

# --- Platform 4: SmartSuite ---

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

# --- Platform 5: Long-Term Memory ---

def save_to_memory(fact: str, memory_type: str = "fact"):
    """
    Saves an important business fact, user preference, or report summary to long-term memory.
    Call this proactively when the user states a preference, important context, or key business fact.
    memory_type options: "fact", "preference", "report"

    EXAMPLE: save_to_memory("User prefers vendors sorted by net_sales descending", "preference")
    EXAMPLE: save_to_memory("Top vendor for Feb 2026 was Brand X with $45,000 net sales", "fact")
    """
    if not CHROMA_ENABLED:
        return "Memory storage unavailable."
    try:
        doc_id = f"{memory_type}_{uuid.uuid4().hex[:8]}_{int(time.time())}"
        knowledge_col.add(
            documents=[fact],
            metadatas=[{"type": memory_type, "date": datetime.now().strftime("%Y-%m-%d"), "timestamp": time.time()}],
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
    "list_basecamp_projects":   list_basecamp_projects,
    "get_project_tools":        get_project_tools,
    "post_to_basecamp":         post_to_basecamp,
    "read_basecamp_messages":   read_basecamp_messages,
    "post_to_campfire":         post_to_campfire,
    "read_campfire_lines":      read_campfire_lines,
    "get_todo_lists":           get_todo_lists,
    "create_todo":              create_todo,
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
        "description": "Executes SQL against BigQuery. Tables: (1) shopify_data.vendor_performance — vendor/date/net_sales/units_sold/order_count. (2) pos_data.teamwork_transactions — Teamwork POS transactions by location/product/date. Always use LIMIT clauses.",
        "input_schema": {"type": "object", "properties": {"sql_query": {"type": "string", "description": "Valid BigQuery Standard SQL query string"}}, "required": ["sql_query"]}
    },
    {
        "name": "query_shopify_analytics",
        "description": "Queries Shopify analytics using ShopifyQL. Use SHOW (not SELECT), SINCE/UNTIL for dates (not WHERE), and always end with WITH TIMEZONE 'America/New_York'. Valid columns: net_sales, gross_sales, orders, product_vendor.",
        "input_schema": {"type": "object", "properties": {"shopify_ql": {"type": "string", "description": "A valid ShopifyQL query string"}}, "required": ["shopify_ql"]}
    },
    {
        "name": "list_basecamp_projects",
        "description": "Lists all active Basecamp projects with their IDs. Call this first before using any other Basecamp tool to get project IDs.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "get_project_tools",
        "description": "Gets all tools (message_board, chat, todoset, etc.) for a Basecamp project. Use this to find IDs for message boards, chat rooms, and todo lists.",
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
        "description": "Reads recent messages from a Basecamp Message Board. Returns title, author, date, and content preview.",
        "input_schema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "integer", "description": "Basecamp project/bucket ID"},
                "message_board_id": {"type": "integer", "description": "Message board ID from get_project_tools"},
                "limit": {"type": "integer", "description": "Max messages to return (default 10)"}
            },
            "required": ["project_id", "message_board_id"]
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
        "description": "Gets all to-do lists in a Basecamp project. Use get_project_tools to find the todoset_id (named 'todoset').",
        "input_schema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "integer"},
                "todoset_id": {"type": "integer", "description": "Todoset ID from get_project_tools"}
            },
            "required": ["project_id", "todoset_id"]
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
                "memory_type": {"type": "string", "description": "'fact', 'preference', or 'report'", "enum": ["fact", "preference", "report"]}
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
    """Retrieves recent conversation history for a space from ChromaDB (internal use)."""
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

def build_memory_context(user_text: str) -> str:
    """Retrieves relevant knowledge/facts from ChromaDB to inject into the system prompt."""
    if not CHROMA_ENABLED or knowledge_col.count() == 0:
        return ""
    try:
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
            metadatas=[{"space_name": space_name, "timestamp": time.time(), "date": datetime.now().strftime("%Y-%m-%d")}],
            ids=[doc_id]
        )
    except Exception as e:
        logger.warning(f"[MEMORY] Conversation save error: {e}")

# ============================================================
# SECTION 8: ROUTING + DISPATCH
# ============================================================

def route_query(user_text: str) -> str:
    """Routes query to 'local' (Ollama) or 'claude' based on complexity signals."""
    text = user_text.lower()
    if any(signal in text for signal in COMPLEX_SIGNALS):
        return "claude"
    if len(user_text.split()) > 20:
        return "claude"
    return "local"

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

SYSTEM_PROMPT_BASE = """You are a business assistant for 260 Sample Sale.
You have tools for: web search, BigQuery (Shopify vendor performance + Teamwork POS),
Shopify analytics (ShopifyQL), Basecamp (projects, messages, chat, todos),
SmartSuite (tasks, requests, inventory, projects, brands), and long-term memory.

Use save_to_memory() whenever the user states a preference or important business fact.
Be concise and professional. Format data in tables when helpful.
Today: {today}. Timezone: America/New_York.
{memory_context}"""

def run_claude_loop(messages: list, system_prompt: str) -> str:
    """Runs the Claude agentic tool-use loop until a final answer is produced."""
    for _ in range(MAX_TOOL_ROUNDS):
        response = ai_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
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

def run_ollama_loop(messages: list, system_prompt: str) -> str:
    """Runs the Ollama agentic tool-use loop until a final answer is produced."""
    for _ in range(MAX_TOOL_ROUNDS):
        messages_with_sys = [{"role": "system", "content": system_prompt}] + messages
        response = ollama_client.chat(
            model=OLLAMA_MODEL,
            messages=messages_with_sys,
            tools=TOOL_FUNCTIONS_LIST,
            options={"temperature": 0.1}
        )
        asst = response.message
        messages.append({"role": "assistant", "content": asst.content or ""})

        if not asst.tool_calls:
            return asst.content or ""

        for tc in asst.tool_calls:
            logger.info(f"[TOOL] {tc.function.name}({json.dumps(tc.function.arguments or {})[:120]})")
            result = dispatch_tool(tc.function.name, tc.function.arguments or {})
            logger.info(f"[RESULT] {tc.function.name} -> {str(result)[:200]}")
            messages.append({"role": "tool", "content": str(result)})

    return "I reached the maximum tool call limit. Please try a simpler request."

# ============================================================
# SECTION 10: MAIN AI ENTRY POINT
# ============================================================

def process_ai_response(user_text: str, space_name: str) -> str:
    """Routes the user message to the appropriate model and returns the final reply."""
    session = get_or_create_session(space_name)
    memory_ctx = build_memory_context(user_text)
    system_prompt = SYSTEM_PROMPT_BASE.format(today=datetime.now().strftime("%Y-%m-%d"), memory_context=memory_ctx)

    with sessions_lock:
        messages = list(session["messages"]) + [{"role": "user", "content": user_text}]

    # Route: local if enabled and not a complex query, otherwise Claude
    use_local = USE_LOCAL_LLM and ollama_client and route_query(user_text) == "local"
    model_name = "LOCAL" if use_local else "CLAUDE"
    logger.info(f"[ROUTER] → {model_name}: {user_text[:80]}")

    final_text = None
    for attempt in range(3):
        try:
            if use_local:
                final_text = run_ollama_loop(list(messages), system_prompt)
            else:
                final_text = run_claude_loop(list(messages), system_prompt)
            break
        except anthropic.RateLimitError:
            wait = (attempt + 1) * 5
            logger.warning(f"[CLAUDE] Rate limit, waiting {wait}s (attempt {attempt + 1}/3)...")
            time.sleep(wait)
        except Exception as e:
            logger.error(f"[{model_name}] Error: {e}")
            if use_local:
                logger.info("[ROUTER] Local model failed — falling back to Claude")
                use_local = False
                model_name = "CLAUDE (fallback)"
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
# SECTION 11: SCHEDULED REPORTS
# ============================================================

def generate_daily_sales_report():
    """Generates and posts the daily sales summary to Basecamp. Runs at 8 AM ET."""
    logger.info("[SCHEDULER] Generating daily sales report...")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        # 1. Vendor performance from BigQuery
        vendor_sql = f"""
            SELECT vendor_name,
                   SUM(net_sales) as total_sales,
                   SUM(units_sold) as total_units,
                   COUNT(DISTINCT order_name) as total_orders
            FROM `gen-lang-client-0065509773.shopify_data.vendor_performance`
            WHERE date = '{yesterday}' AND vendor_name IS NOT NULL
            GROUP BY vendor_name ORDER BY total_sales DESC LIMIT 25
        """
        vendor_raw = run_bigquery_report(vendor_sql)
        vendors = json.loads(vendor_raw) if vendor_raw and not vendor_raw.startswith("BigQuery") else []

        # 2. Shopify totals
        shopify_ql = f"FROM sales SHOW net_sales, gross_sales, orders SINCE {yesterday} UNTIL {yesterday} WITH TIMEZONE 'America/New_York'"
        shopify_raw = query_shopify_analytics(shopify_ql)

        # 3. Format HTML report
        rows = "".join(
            f"<tr><td>{v.get('vendor_name','')}</td><td>${float(v.get('total_sales',0)):,.2f}</td>"
            f"<td>{v.get('total_units',0)}</td><td>{v.get('total_orders',0)}</td></tr>"
            for v in vendors
        )
        content_html = f"""
        <div>
          <h2>Daily Sales Summary — {yesterday}</h2>
          <table>
            <thead><tr><th>Vendor</th><th>Net Sales</th><th>Units</th><th>Orders</th></tr></thead>
            <tbody>{rows if rows else '<tr><td colspan="4">No data available</td></tr>'}</tbody>
          </table>
          <p><em>Auto-generated. Shopify totals: {shopify_raw[:400]}</em></p>
        </div>
        """

        # 4. Post to Basecamp if configured
        if BC_REPORT_PROJECT_ID and BC_REPORT_BOARD_ID:
            result = post_to_basecamp(
                project_id=BC_REPORT_PROJECT_ID,
                message_board_id=BC_REPORT_BOARD_ID,
                title=f"Daily Sales Summary — {yesterday}",
                content_html=content_html
            )
            logger.info(f"[SCHEDULER] Report posted: {result}")
        else:
            logger.warning("[SCHEDULER] BC_REPORT_PROJECT_ID or BC_REPORT_BOARD_ID not set — skipping post")

    except Exception as e:
        logger.error(f"[SCHEDULER] Daily report failed: {e}", exc_info=True)

def setup_scheduler():
    """Creates and starts the APScheduler. Returns the scheduler instance."""
    from apscheduler.schedulers.background import BackgroundScheduler
    import pytz

    eastern = pytz.timezone("America/New_York")
    scheduler = BackgroundScheduler(timezone=eastern)
    scheduler.add_job(
        generate_daily_sales_report,
        "cron", hour=8, minute=0,
        id="daily_sales_report",
        misfire_grace_time=900
    )
    scheduler.add_job(
        cleanup_sessions,
        "interval", minutes=30,
        id="session_cleanup"
    )
    scheduler.start()
    logger.info("[SCHEDULER] Started — daily report at 08:00 ET, session cleanup every 30 min")
    return scheduler

# ============================================================
# SECTION 12: GOOGLE CHAT MESSAGING
# ============================================================

def send_reply(space_name: str, text: str):
    """Sends a text message back to the Google Chat space."""
    message = chat_v1.Message()
    message.text = text
    request = chat_v1.CreateMessageRequest(parent=space_name, message=message)
    chat_client.create_message(request=request)

def callback(message):
    """Triggered whenever a message arrives via Pub/Sub."""
    try:
        data = json.loads(message.data.decode("utf-8"))
        user_text  = data.get("message", {}).get("text", "").strip()
        space_name = data.get("message", {}).get("space", {}).get("name")

        if not user_text or not space_name:
            message.ack()
            return

        logger.info(f"[RECEIVED] {user_text[:100]}")

        # Acknowledge immediately so the user knows the agent is working
        send_reply(space_name, "⏳ Working on it...")

        ai_reply = process_ai_response(user_text, space_name)
        send_reply(space_name, ai_reply)
        logger.info(f"[SENT] {ai_reply[:100]}")

        message.ack()
    except Exception as e:
        logger.error(f"[CALLBACK] Error: {e}", exc_info=True)
        message.nack()

# ============================================================
# SECTION 13: MAIN
# ============================================================
if __name__ == "__main__":
    scheduler = setup_scheduler()

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    mode = "Hybrid (Local + Claude)" if USE_LOCAL_LLM else "Claude API only"
    memory = "ChromaDB enabled" if CHROMA_ENABLED else "in-memory only"
    logger.info(f"--- Agent LIVE | Mode: {mode} | Memory: {memory} ---")

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        scheduler.shutdown()
        logger.info("Agent stopped.")
