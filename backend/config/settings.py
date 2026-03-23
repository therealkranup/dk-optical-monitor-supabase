"""
Application settings and API configuration.
Loads from .env file automatically, or from environment variables.
"""
import os

# Auto-load .env file if it exists (no need to 'export' variables manually)
_env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _key, _, _val = _line.partition("=")
                _key = _key.strip()
                _val = _val.strip().strip('"').strip("'")
                if _key and _val and _val != "your_apify_token_here":
                    os.environ.setdefault(_key, _val)

# --- Apify Web Scraping ---
# Get token from: https://console.apify.com → Settings → Integrations
# Put it in .env file as: APIFY_API_TOKEN=your_token_here
APIFY_API_TOKEN = os.environ.get("APIFY_API_TOKEN", "")
APIFY_API_BASE = "https://api.apify.com/v2"

# Apify actor IDs (use ~ separator, not / — required by Apify API URL format)
APIFY_FACEBOOK_ACTOR = "apify~facebook-posts-scraper"
APIFY_INSTAGRAM_ACTOR = "apify~instagram-scraper"
APIFY_TIKTOK_ACTOR = "clockworks~tiktok-scraper"

# --- Meta (Facebook + Instagram) Graph API ---
# To get these:
# 1. Create a Meta Developer App at https://developers.facebook.com
# 2. Generate a Page Access Token with pages_read_engagement permission
# 3. For Instagram, link the IG Business account to the Facebook Page
META_APP_ID = os.environ.get("META_APP_ID", "")
META_APP_SECRET = os.environ.get("META_APP_SECRET", "")
META_ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN", "")
META_GRAPH_API_VERSION = "v19.0"
META_GRAPH_API_BASE = f"https://graph.facebook.com/{META_GRAPH_API_VERSION}"

# --- LinkedIn ---
# Requires LinkedIn Marketing API Partner access
LINKEDIN_ACCESS_TOKEN = os.environ.get("LINKEDIN_ACCESS_TOKEN", "")
LINKEDIN_API_BASE = "https://api.linkedin.com/v2"

# --- Scraping settings ---
REQUEST_TIMEOUT = 60  # seconds
REQUEST_DELAY = 2.0   # seconds between requests (polite scraping)
MAX_RETRIES = 3
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# --- Data storage (Supabase via REST/HTTPS) ---
# Uses supabase-py client to avoid IPv6 issues with GitHub Actions runners.
# Set SUPABASE_URL and SUPABASE_SERVICE_KEY in .env or as environment variables.
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kwbmzwgxancdkjkxtmgc.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

# --- Server ---
API_HOST = "0.0.0.0"
API_PORT = 8000
