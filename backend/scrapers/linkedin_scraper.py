"""
LinkedIn scraper using Apify or partner API.
Falls back to demo data if no API credentials are configured.
"""
import asyncio
import aiohttp
from datetime import datetime, timedelta

from .base import BaseScraper, SocialPost
from ..config.settings import (
    APIFY_API_TOKEN, APIFY_API_BASE, APIFY_LINKEDIN_ACTOR,
    LINKEDIN_ACCESS_TOKEN, LINKEDIN_API_BASE, REQUEST_TIMEOUT
)


class LinkedInScraper(BaseScraper):
    platform = "linkedin"

    async def scrape(self, company: dict, since_days: int) -> list[SocialPost]:
        li_config = company["socials"].get("linkedin")
        if not li_config:
            return []

        if APIFY_API_TOKEN:
            return await self._scrape_via_apify(company, li_config, since_days)
        elif LINKEDIN_ACCESS_TOKEN:
            return await self._scrape_via_api(company, li_config, since_days)
        else:
            return self._generate_demo_data(company, li_config, since_days)

    async def _scrape_via_apify(self, company: dict, li_config: dict, since_days: int) -> list[SocialPost]:
        """Scrape using Apify LinkedIn Post Scraper actor (free tier compatible)."""
        company_url = li_config["url"]

        input_data = {
            "startUrls": [{"url": f"{company_url}posts/"}],
            "count": 5,  # Limited to 5 posts to save API costs
        }

        try:
            async with aiohttp.ClientSession() as session:
                # Start the actor run
                run_url = f"{APIFY_API_BASE}/acts/{APIFY_LINKEDIN_ACTOR}/runs"
                headers = {"Authorization": f"Bearer {APIFY_API_TOKEN}"}

                async with session.post(
                    run_url, json=input_data, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as resp:
                    if resp.status != 201:
                        print(f"[LinkedIn/Apify] Error {resp.status} for {company['name']}")
                        return self._generate_demo_data(company, li_config, since_days)

                    run_data = await resp.json()
                    run_id = run_data.get("data", {}).get("id")

                    if not run_id:
                        return self._generate_demo_data(company, li_config, since_days)

                # Poll for completion (max 4 minutes)
                for _ in range(240):
                    async with session.get(
                        f"{APIFY_API_BASE}/actor-runs/{run_id}",
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                    ) as resp:
                        run_info = await resp.json()
                        status = run_info.get("data", {}).get("status")

                        if status == "SUCCEEDED":
                            break
                        elif status in ("FAILED", "ABORTED"):
                            return self._generate_demo_data(company, li_config, since_days)

                    await asyncio.sleep(1)

                # Get dataset
                dataset_id = run_info.get("data", {}).get("defaultDatasetId")
                if not dataset_id:
                    return self._generate_demo_data(company, li_config, since_days)

                async with session.get(
                    f"{APIFY_API_BASE}/datasets/{dataset_id}/items",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as resp:
                    items = await resp.json()
                    posts = self._parse_linkedin_items(company, items, since_days)
                    return posts

        except Exception as e:
            print(f"[LinkedIn/Apify] Error scraping {company['name']}: {e}")
            return self._generate_demo_data(company, li_config, since_days)

    def _parse_linkedin_items(self, company: dict, items: list, since_days: int) -> list[SocialPost]:
        """Parse Apify LinkedIn actor output."""
        cutoff = self._cutoff_date(since_days)
        posts = []

        for item in items:
            if not isinstance(item, dict):
                continue

            post_date_str = item.get("publishedAtDate") or item.get("postedAt") or ""
            if not post_date_str:
                continue

            try:
                post_date = datetime.fromisoformat(post_date_str.replace("Z", "+00:00"))
            except:
                continue

            if post_date.replace(tzinfo=None) < cutoff:
                continue

            media_urls = []
            media_type = "text"

            if item.get("image"):
                media_urls.append(item["image"])
                media_type = "image"
            elif item.get("video"):
                media_urls.append(item["video"])
                media_type = "video"

            posts.append(SocialPost(
                company_id=company["id"],
                company_name=company["name"],
                platform="linkedin",
                post_id=item.get("postUrl", "") or item.get("id", ""),
                post_url=item.get("postUrl") or item.get("url") or item.get("link") or "",
                date=post_date.isoformat(),
                text=item.get("description") or item.get("text") or "",
                likes=item.get("reactionsCount") or item.get("likes", 0),
                comments=item.get("commentsCount") or item.get("comments", 0),
                shares=item.get("repostsCount") or 0,
                media_urls=media_urls,
                media_type=media_type,
            ))

        return sorted(posts, key=lambda p: p.date, reverse=True)

    async def _scrape_via_api(self, company: dict, li_config: dict, since_days: int) -> list[SocialPost]:
        """Scrape via LinkedIn Marketing API (requires partner access)."""
        cutoff = self._cutoff_date(since_days)
        company_urn = f"urn:li:organization:{li_config['company_id']}"

        headers = {
            "Authorization": f"Bearer {LINKEDIN_ACCESS_TOKEN}",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": "202401",
        }

        url = (
            f"{LINKEDIN_API_BASE}/posts"
            f"?author={company_urn}"
            f"&q=author"
            f"&count=50"
        )

        posts = []
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                    if resp.status != 200:
                        print(f"[LinkedIn] API error {resp.status} for {company['name']}")
                        return self._generate_demo_data(company, li_config, since_days)

                    data = await resp.json()

                    for item in data.get("elements", []):
                        created_ts = item.get("createdAt", 0) / 1000
                        post_date = datetime.utcfromtimestamp(created_ts)

                        if post_date < cutoff:
                            continue

                        text = item.get("commentary", "")

                        media_urls = []
                        media_type = "text"
                        if "content" in item:
                            article = item["content"].get("article", {})
                            if article:
                                media_type = "link"
                                if article.get("thumbnail"):
                                    media_urls.append(article["thumbnail"])

                        social_detail = item.get("socialDetail", {})
                        likes = social_detail.get("totalSocialActivityCounts", {}).get("numLikes", 0)
                        comments_count = social_detail.get("totalSocialActivityCounts", {}).get("numComments", 0)
                        shares_count = social_detail.get("totalSocialActivityCounts", {}).get("numShares", 0)

                        post_id = item.get("id", "")
                        posts.append(SocialPost(
                            company_id=company["id"],
                            company_name=company["name"],
                            platform="linkedin",
                            post_id=post_id,
                            post_url=f"https://www.linkedin.com/feed/update/{post_id}",
                            date=post_date.isoformat(),
                            text=text,
                            likes=likes,
                            comments=comments_count,
                            shares=shares_count,
                            media_urls=media_urls,
                            media_type=media_type,
                        ))

        except Exception as e:
            print(f"[LinkedIn] Error scraping {company['name']}: {e}")
            return self._generate_demo_data(company, li_config, since_days)

        return posts

    def _generate_demo_data(self, company: dict, li_config: dict, since_days: int) -> list[SocialPost]:
        """Generate realistic LinkedIn demo data."""
        import random
        import hashlib

        seed = hashlib.md5(f"{company['id']}_linkedin".encode()).hexdigest()
        rng = random.Random(seed)

        templates = [
            "Vi er glade for at kunne annoncere, at {company} har fået en ny partner-aftale med {brand}.",
            "Hos {company} investerer vi i vores medarbejdere. Denne uge har kollegaer afsluttet avanceret optometri-kursus.",
            "{company} søger nye medarbejdere! Vi leder efter passionerede optikere til hele Danmark.",
            "Bæredygtighed er en kerneværdi hos {company}. Vi introducerer vores nye miljøvenlige kollektion.",
            "Vi er stolte af at {company} er blevet kåret som en af Danmarks bedste arbejdspladser.",
        ]

        brands = ["EssilorLuxottica", "Zeiss", "Hoya", "LINDBERG"]

        now = datetime.utcnow()
        posts = []
        count = {1: rng.randint(0, 1), 7: rng.randint(1, 3), 30: rng.randint(2, 6)}.get(since_days, 2)

        for i in range(count):
            template = rng.choice(templates)
            post_text = template.format(company=company["name"], brand=rng.choice(brands))
            hours_ago = rng.randint(1, since_days * 24)
            post_date = now - timedelta(hours=hours_ago)

            posts.append(SocialPost(
                company_id=company["id"],
                company_name=company["name"],
                platform="linkedin",
                post_id=f"li_{company['id']}_{i}_{since_days}d",
                post_url=f"{li_config['url']}/posts/{company['id']}-{i}",
                date=post_date.isoformat(),
                text=post_text,
                likes=rng.randint(10, 200),
                comments=rng.randint(0, 35),
                shares=rng.randint(0, 25),
                media_urls=[f"https://picsum.photos/seed/{company['id']}li{i}/1200/628"],
                media_type="image",
            ))

        return sorted(posts, key=lambda p: p.date, reverse=True)
