"""
Orchestrator — launches one CompanyAgent per company, all running in parallel.
Aggregates results and provides the entry point for scraping runs.
"""
import asyncio
import time
from datetime import datetime

from ..config.companies import COMPANIES, TIME_FILTERS
from ..storage.database import PostDatabase
from .company_agent import CompanyAgent


class Orchestrator:
    """
    Multi-agent orchestrator that spins up parallel agents for each company.
    """

    def __init__(self, db: PostDatabase = None):
        self.db = db or PostDatabase()
        self.last_run = None

    async def run_all(self, since_days: int = 30) -> dict:
        """
        Launch agents for all companies concurrently.
        Each agent scrapes all platforms for its assigned company.
        """
        start_time = time.time()
        print(f"\n{'='*60}")
        print(f"  ORCHESTRATOR: Starting sequential scrape (1 company at a time)")
        print(f"  Companies: {len(COMPANIES)} | Time window: {since_days} days")
        print(f"{'='*60}\n")

        # Create one agent per company
        agents = [CompanyAgent(company, self.db) for company in COMPANIES]

        # Run companies ONE AT A TIME to stay within Apify's free plan memory limit.
        # (Free plan = 8192MB total. Facebook alone uses 4096MB, so only 1 company
        #  can run at a time. Within each company, platforms still run in parallel.)
        results = []
        for i, agent in enumerate(agents):
            result = await agent.run(since_days)
            results.append(result)
            # Wait for Apify to release memory before starting the next company.
            # The free plan allows 8192MB total; without a pause the next company
            # sees the previous jobs still counted against the limit.
            if i < len(agents) - 1:
                print(f"\n  ⏳ Waiting 60s for Apify memory to release before next company...\n")
                await asyncio.sleep(60)

        # Aggregate results
        agent_summaries = []
        total_posts = 0
        for company, result in zip(COMPANIES, results):
            if isinstance(result, Exception):
                agent_summaries.append({
                    "company_id": company["id"],
                    "company_name": company["name"],
                    "status": "error",
                    "error": str(result),
                })
            else:
                agent_summaries.append(result)
                total_posts += result.get("total_posts", 0)

        elapsed = round(time.time() - start_time, 2)

        run_summary = {
            "status": "completed",
            "since_days": since_days,
            "companies_scraped": len(COMPANIES),
            "total_posts_collected": total_posts,
            "elapsed_seconds": elapsed,
            "completed_at": datetime.utcnow().isoformat(),
            "agents": agent_summaries,
        }

        self.last_run = run_summary

        print(f"\n{'='*60}")
        print(f"  ORCHESTRATOR: Complete")
        print(f"  Total posts: {total_posts} | Time: {elapsed}s")
        print(f"{'='*60}\n")

        return run_summary

    async def run_all_time_filters(self) -> dict:
        """Run scraping for all time filter presets (1 day, 1 week, 1 month)."""
        self.db.clear_all()  # Fresh start for comprehensive scrape

        all_results = {}
        for filter_name, days in TIME_FILTERS.items():
            result = await self.run_all(since_days=days)
            all_results[filter_name] = result

        return all_results
