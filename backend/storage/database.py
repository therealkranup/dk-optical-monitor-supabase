"""
Supabase-based storage for scraped social media posts.
Uses supabase-py (HTTPS/REST) to avoid IPv6 issues with GitHub Actions runners.
"""
import json
from datetime import datetime, timedelta

from supabase import create_client, Client

from ..config.settings import SUPABASE_URL, SUPABASE_SERVICE_KEY


class PostDatabase:
    def __init__(self, url: str = None, key: str = None):
        _url = url or SUPABASE_URL
        _key = key or SUPABASE_SERVICE_KEY
        if not _url or not _key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in environment."
            )
        self.client: Client = create_client(_url, _key)

    def upsert_posts(self, posts: list[dict]) -> int:
        """Insert or update posts. Returns count of records submitted."""
        if not posts:
            return 0

        rows = []
        for post in posts:
            rows.append({
                "company_id":   post["company_id"],
                "company_name": post["company_name"],
                "platform":     post["platform"],
                "post_id":      post["post_id"],
                "post_url":     post.get("post_url", ""),
                "date":         post["date"],
                "text":         post.get("text", ""),
                "likes":        post.get("likes", 0),
                "comments":     post.get("comments", 0),
                "shares":       post.get("shares", 0),
                "media_urls":   json.dumps(post.get("media_urls", [])),
                "media_type":   post.get("media_type", ""),
                "scraped_at":   post["scraped_at"],
            })

        try:
            self.client.table("posts").upsert(
                rows, on_conflict="platform,post_id"
            ).execute()
            return len(rows)
        except Exception as e:
            print(f"Error upserting {len(rows)} posts: {e}")
            return 0

    def query_posts(self, company_id: str = None, platform: str = None,
                    since_days: int = None, limit: int = 500) -> list[dict]:
        """Query posts with optional filters."""
        query = (
            self.client.table("posts")
            .select("*")
            .order("date", desc=True)
            .limit(limit)
        )

        if company_id:
            query = query.eq("company_id", company_id)
        if platform:
            query = query.eq("platform", platform)
        if since_days:
            cutoff = (datetime.utcnow() - timedelta(days=since_days)).isoformat()
            query = query.gte("date", cutoff)

        result = query.execute()
        return [self._row_to_dict(r) for r in (result.data or [])]

    def get_stats(self, since_days: int = None) -> dict:
        """Get aggregated statistics per company and platform."""
        # Fetch all relevant posts and aggregate client-side
        # (supabase-py REST doesn't support GROUP BY natively)
        posts = self.query_posts(since_days=since_days, limit=10000)

        stats: dict = {}
        for post in posts:
            cid = post["company_id"]
            if cid not in stats:
                stats[cid] = {
                    "company_id":   cid,
                    "company_name": post["company_name"],
                    "platforms":    {},
                    "totals":       {"posts": 0, "likes": 0, "comments": 0, "shares": 0},
                }
            plat = post["platform"]
            if plat not in stats[cid]["platforms"]:
                stats[cid]["platforms"][plat] = {
                    "post_count":     0,
                    "total_likes":    0,
                    "total_comments": 0,
                    "total_shares":   0,
                    "avg_likes":      0.0,
                    "avg_comments":   0.0,
                    "latest_post":    None,
                    "_likes_sum":     0,
                    "_comments_sum":  0,
                }
            p = stats[cid]["platforms"][plat]
            p["post_count"]     += 1
            p["total_likes"]    += post.get("likes", 0)
            p["total_comments"] += post.get("comments", 0)
            p["total_shares"]   += post.get("shares", 0)
            if p["latest_post"] is None or post["date"] > p["latest_post"]:
                p["latest_post"] = post["date"]

            # Update company totals
            stats[cid]["totals"]["posts"]    += 1
            stats[cid]["totals"]["likes"]    += post.get("likes", 0)
            stats[cid]["totals"]["comments"] += post.get("comments", 0)
            stats[cid]["totals"]["shares"]   += post.get("shares", 0)

        # Compute averages and clean up temp keys
        for cid, company in stats.items():
            for plat, p in company["platforms"].items():
                n = p["post_count"] or 1
                p["avg_likes"]    = round(p["total_likes"]    / n, 1)
                p["avg_comments"] = round(p["total_comments"] / n, 1)
                p.pop("_likes_sum", None)
                p.pop("_comments_sum", None)

        return stats

    def clear_all(self):
        """Clear all posts (for testing)."""
        self.client.table("posts").delete().neq("id", 0).execute()

    def _row_to_dict(self, row: dict) -> dict:
        # media_urls is stored as a JSON string; decode it back to a list
        raw = row.get("media_urls", "[]")
        if isinstance(raw, str):
            try:
                row["media_urls"] = json.loads(raw)
            except (ValueError, TypeError):
                row["media_urls"] = []
        return row
