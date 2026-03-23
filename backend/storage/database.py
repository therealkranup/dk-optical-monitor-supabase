"""
PostgreSQL-based storage for scraped social media posts.
Uses Supabase (or any PostgreSQL DB) for persistent, server-independent storage.
"""
import json
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor

from ..config.settings import DATABASE_URL


class PostDatabase:
    def __init__(self, db_url: str = None):
        self.db_url = db_url or DATABASE_URL
        if not self.db_url:
            raise ValueError("DATABASE_URL is not set. Add it to your .env or environment.")
        self._init_db()

    def _conn(self):
        return psycopg2.connect(self.db_url)

    def _init_db(self):
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS posts (
                        id SERIAL PRIMARY KEY,
                        company_id TEXT NOT NULL,
                        company_name TEXT NOT NULL,
                        platform TEXT NOT NULL,
                        post_id TEXT NOT NULL,
                        post_url TEXT,
                        date TEXT NOT NULL,
                        text TEXT,
                        likes INTEGER DEFAULT 0,
                        comments INTEGER DEFAULT 0,
                        shares INTEGER DEFAULT 0,
                        media_urls TEXT DEFAULT '[]',
                        media_type TEXT DEFAULT '',
                        scraped_at TEXT NOT NULL,
                        UNIQUE(platform, post_id)
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_company_platform
                    ON posts (company_id, platform)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_date
                    ON posts (date)
                """)

    def upsert_posts(self, posts: list[dict]) -> int:
        """Insert or update posts. Returns count of new/updated posts."""
        count = 0
        with self._conn() as conn:
            with conn.cursor() as cur:
                for post in posts:
                    media_urls = json.dumps(post.get("media_urls", []))
                    try:
                        cur.execute("""
                            INSERT INTO posts
                                (company_id, company_name, platform, post_id, post_url,
                                 date, text, likes, comments, shares, media_urls, media_type, scraped_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT(platform, post_id) DO UPDATE SET
                                likes = EXCLUDED.likes,
                                comments = EXCLUDED.comments,
                                shares = EXCLUDED.shares,
                                text = EXCLUDED.text,
                                scraped_at = EXCLUDED.scraped_at
                        """, (
                            post["company_id"], post["company_name"], post["platform"],
                            post["post_id"], post["post_url"], post["date"], post["text"],
                            post["likes"], post["comments"], post["shares"],
                            media_urls, post["media_type"], post["scraped_at"],
                        ))
                        count += 1
                    except Exception as e:
                        print(f"Error upserting post {post.get('post_id')}: {e}")
        return count

    def query_posts(self, company_id: str = None, platform: str = None,
                    since_days: int = None, limit: int = 500) -> list[dict]:
        """Query posts with optional filters."""
        conditions = ["1=1"]
        params = []

        if company_id:
            conditions.append("company_id = %s")
            params.append(company_id)
        if platform:
            conditions.append("platform = %s")
            params.append(platform)
        if since_days:
            cutoff = (datetime.utcnow() - timedelta(days=since_days)).isoformat()
            conditions.append("date >= %s")
            params.append(cutoff)

        params.append(limit)

        with self._conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"SELECT * FROM posts WHERE {' AND '.join(conditions)} ORDER BY date DESC LIMIT %s",
                    params,
                )
                rows = cur.fetchall()

        return [self._row_to_dict(dict(r)) for r in rows]

    def get_stats(self, since_days: int = None) -> dict:
        """Get aggregated statistics per company and platform."""
        cutoff_clause = ""
        params = []
        if since_days:
            cutoff = (datetime.utcnow() - timedelta(days=since_days)).isoformat()
            cutoff_clause = "WHERE date >= %s"
            params.append(cutoff)

        with self._conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"""
                    SELECT
                        company_id, company_name, platform,
                        COUNT(*) as post_count,
                        SUM(likes) as total_likes,
                        SUM(comments) as total_comments,
                        SUM(shares) as total_shares,
                        AVG(likes) as avg_likes,
                        AVG(comments) as avg_comments,
                        MAX(date) as latest_post
                    FROM posts
                    {cutoff_clause}
                    GROUP BY company_id, company_name, platform
                    ORDER BY company_name, platform
                """, params)
                rows = cur.fetchall()

        stats = {}
        for r in [dict(r) for r in rows]:
            cid = r["company_id"]
            if cid not in stats:
                stats[cid] = {
                    "company_id": r["company_id"],
                    "company_name": r["company_name"],
                    "platforms": {},
                    "totals": {"posts": 0, "likes": 0, "comments": 0, "shares": 0},
                }
            stats[cid]["platforms"][r["platform"]] = {
                "post_count": r["post_count"],
                "total_likes": int(r["total_likes"] or 0),
                "total_comments": int(r["total_comments"] or 0),
                "total_shares": int(r["total_shares"] or 0),
                "avg_likes": round(float(r["avg_likes"] or 0), 1),
                "avg_comments": round(float(r["avg_comments"] or 0), 1),
                "latest_post": r["latest_post"],
            }
            stats[cid]["totals"]["posts"] += r["post_count"]
            stats[cid]["totals"]["likes"] += int(r["total_likes"] or 0)
            stats[cid]["totals"]["comments"] += int(r["total_comments"] or 0)
            stats[cid]["totals"]["shares"] += int(r["total_shares"] or 0)

        return stats

    def clear_all(self):
        """Clear all posts (for testing)."""
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM posts")

    def _row_to_dict(self, row: dict) -> dict:
        row["media_urls"] = json.loads(row.get("media_urls", "[]"))
        return row
