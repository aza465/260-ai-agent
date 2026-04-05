"""
Basecamp OAuth Token Refresher
Run this whenever your Basecamp access token expires.
It will open an auth URL, then exchange the code for fresh tokens.
"""
import os
import urllib.parse
import requests
from dotenv import load_dotenv, set_key

load_dotenv()

CLIENT_ID     = os.environ.get("BC_CLIENT_ID")
CLIENT_SECRET = os.environ.get("BC_CLIENT_SECRET")
REDIRECT_URI  = "https://httpbin.org/get"
ENV_FILE      = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

if __name__ == "__main__":
    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERROR: BC_CLIENT_ID or BC_CLIENT_SECRET missing from .env")
        exit(1)

    auth_url = (
        f"https://launchpad.37signals.com/authorization/new"
        f"?type=web_server"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
    )

    print("\n--- Basecamp Token Refresh ---")
    print("\nStep 1: Open this URL in your browser:\n")
    print(f"  {auth_url}\n")
    print("Step 2: Log in and click 'Allow access'")
    print("Step 3: The browser will redirect to httpbin.org and show a JSON page.")
    print("         Look for the 'code' field in the JSON, it will look like:")
    print('         "code": "XXXXXXXXXXXXXXXXXX"')
    print("         Copy just that code value.\n")

    auth_code = input("Paste the code here: ").strip()

    if not auth_code:
        print("No code received. Exiting.")
        exit(1)

    print(f"\nGot auth code. Exchanging for tokens...")

    # Exchange code for access + refresh tokens
    r = requests.post(
        "https://launchpad.37signals.com/authorization/token",
        params={
            "type": "web_server",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri": REDIRECT_URI,
            "code": auth_code,
        }
    )

    if r.status_code != 200:
        print(f"ERROR: {r.status_code} — {r.text}")
        exit(1)

    tokens = r.json()
    access_token  = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    expires_in    = tokens.get("expires_in", "unknown")

    if not access_token:
        print(f"ERROR: No access_token in response: {tokens}")
        exit(1)

    print(f"\nSuccess! Token expires in: {expires_in} seconds")

    # Save to .env
    set_key(ENV_FILE, "BC_LIVE_ACCESS_TOKEN", access_token)
    if refresh_token:
        set_key(ENV_FILE, "BC_REFRESH_TOKEN", refresh_token)

    print(f"\nTokens saved to .env:")
    print(f"  BC_LIVE_ACCESS_TOKEN = {access_token[:12]}...")
    if refresh_token:
        print(f"  BC_REFRESH_TOKEN     = {refresh_token[:12]}...")

    print("\nDone! Update chat_agent.py to use BC_LIVE_ACCESS_TOKEN instead of BC_ACCESS_TOKEN.")
