"""
Company Agent — one agent per company, running all platform scrapers in parallel.
Each agent is an async task that:
  1. Spins up scrapers for Facebook, Instagram, TikTok concurrently
  2. Collects and normalizes the results
  3. Stores them in the database
"""
import asyncio
import time
from datetime import datetime

from ..scrapers.facebook_scraper import FacebookScraper
from ..scrapers.instagram_scraper import InstagramScraper
from ..scrapers.tiktok_scraper import TikTokScraper
from ..storage.database import PostDatabase


class CompanyAgent:
    """
    Autonomous agent for monitoring a single company across all social platforms.
    """

    def __init__(self, company: dict, db: PostDatabase):
        self.company = company
        self.db = db
        self.scrapers = [
            FacebookScraper(),
            InstagramScraper(),
            TikTokScraper(),
        ]
        self.results = {}

    async def run(self, since_days: int) -> dict:
        """
        Run all scrapers for this company concurrently.
        Returns a summary dict.
        """
        company_name = self.company["name"]
        start_time = time.time()
        print(f"[Agent:{company_name}] Starting scrape (last {since_days} days)...")

        # Run platform scrapers sequentially to stay within Apify's 8192MB memory limit.
        # Facebook alone uses 4096MB, so running all 3 in parallel risks exceeding the cap.
        results = []
        for scraper in self.scrapers:
            try:
                result = await scraper.scrape(self.company, since_days)
                results.append(result)
            except Exception as e:
                results.append(e)

        all_posts = []
        platform_summaries = {}

        for scraper, result in zip(self.scrapers, results):
            platform = scraper.platform
            if isinstance(result, Exception):
                print(f"[Agent:{company_name}] {platform} error: {result}")
                platform_summaries[platform] = {"status": "error", "error": str(result), "posts": 0}
            else:
                post_dicts = [p.to_dict() for p in result]
                all_posts.extend(post_dicts)
                platform_summaries[platform] = {
                    "status": "ok",
                    "posts": len(result),
                    "total_likes": sum(p.likes for p in result),
                    "total_comments": sum(p.comments for p in result),
                }
                print(f"[Agent:{company_name}] {platform}: {len(result)} posts collected")

        # Store all posts
        stored_count = self.db.upsert_posts(all_posts)

        elapsed = round(time.time() - start_time, 2)
        summary = {
            "company_id": self.company["id"],
            "company_name": company_name,
            "since_days": since_days,
            "total_posts": len(all_posts),
            "stored": stored_count,
            "platforms": platform_summaries,
            "elapsed_seconds": elapsed,
            "completed_at": datetime.utcnow().isoformat(),
        }

        print(f"[Agent:{company_name}] Done — {len(all_posts)} posts in {elapsed}s")
        self.results = summary
        return summary
