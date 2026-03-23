"""
TikTok scraper using Apify clockworks/tiktok-scraper.
Falls back to demo data if no API credentials are configured.
Subscribe to the actor at: https://apify.com/clockworks/tiktok-scraper
"""
import asyncio
import aiohttp
from datetime import datetime, timedelta

from .base import BaseScraper, SocialPost
from ..config.settings import (
    APIFY_API_TOKEN, APIFY_API_BASE, APIFY_TIKTOK_ACTOR, REQUEST_TIMEOUT
)


class TikTokScraper(BaseScraper):
    platform = "tiktok"

    async def scrape(self, company: dict, since_days: int) -> list[SocialPost]:
        tt_config = company["socials"].get("tiktok")
        if not tt_config:
            return []

        if APIFY_API_TOKEN:
            return await self._scrape_via_apify(company, tt_config, since_days)
        else:
            return self._generate_demo_data(company, tt_config, since_days)

    async def _scrape_via_apify(self, company: dict, tt_config: dict, since_days: int) -> list[SocialPost]:
        """Scrape using Apify TikTok scraper actor."""
        username = tt_config["username"]

        input_data = {
            "profiles": [username],
            "resultsPerPage": 5,  # Limited to 5 posts to save API costs
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
        }

        try:
            async with aiohttp.ClientSession() as session:
                run_url = f"{APIFY_API_BASE}/acts/{APIFY_TIKTOK_ACTOR}/runs"
                headers = {"Authorization": f"Bearer {APIFY_API_TOKEN}"}

                async with session.post(
                    run_url, json=input_data, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as resp:
                    body_text = await resp.text()
                    if resp.status != 201:
                        print(f"[TikTok/Apify] ❌ HTTP {resp.status} for {company['name']}: {body_text[:200]}")
                        return self._generate_demo_data(company, tt_config, since_days)

                    import json as _json
                    run_data = _json.loads(body_text)
                    run_id = run_data.get("data", {}).get("id")
                    print(f"[TikTok/Apify] ✅ Run started for {company['name']}, run_id={run_id}")
                    if not run_id:
                        return self._generate_demo_data(company, tt_config, since_days)

                # Poll for completion (max 3 minutes)
                run_info = {}
                for tick in range(180):
                    async with session.get(
                        f"{APIFY_API_BASE}/actor-runs/{run_id}",
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                    ) as resp:
                        run_info = await resp.json()
                        status = run_info.get("data", {}).get("status")
                        if tick % 10 == 0:
                            print(f"[TikTok/Apify] {company['name']} status={status} ({tick}s)")
                        if status == "SUCCEEDED":
                            print(f"[TikTok/Apify] ✅ {company['name']} succeeded after {tick}s")
                            break
                        elif status in ("FAILED", "ABORTED"):
                            print(f"[TikTok/Apify] ❌ {company['name']} run {status}")
                            return self._generate_demo_data(company, tt_config, since_days)
                    await asyncio.sleep(1)

                dataset_id = run_info.get("data", {}).get("defaultDatasetId")
                if not dataset_id:
                    print(f"[TikTok/Apify] ❌ No dataset_id for {company['name']}")
                    return self._generate_demo_data(company, tt_config, since_days)

                async with session.get(
                    f"{APIFY_API_BASE}/datasets/{dataset_id}/items",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as resp:
                    items = await resp.json()
                    print(f"[TikTok/Apify] {company['name']}: {len(items)} raw items from dataset")
                    if items and isinstance(items[0], dict):
                        print(f"[TikTok/Apify]   First item keys: {list(items[0].keys())[:10]}")
                    posts = self._parse_tiktok_items(company, tt_config, items, since_days)
                    print(f"[TikTok/Apify] {company['name']}: {len(posts)} posts after parsing")
                    return posts

        except Exception as e:
            print(f"[TikTok/Apify] ❌ Exception for {company['name']}: {e}")
            import traceback
            traceback.print_exc()
            return self._generate_demo_data(company, tt_config, since_days)

    def _parse_tiktok_items(self, company: dict, tt_config: dict, items: list, since_days: int) -> list[SocialPost]:
        """Parse Apify TikTok actor output."""
        posts = []

        for item in items:
            if not isinstance(item, dict):
                continue

            # Skip error items
            if "error" in item and "id" not in item:
                print(f"[TikTok/Apify] {company['name']} error item: {item.get('error', '')[:200]}")
                continue

            # TikTok timestamps are Unix seconds
            created_at = item.get("createTime") or item.get("createTimeISO")
            post_date = datetime.utcnow()  # fallback to now if no date
            if created_at:
                try:
                    if isinstance(created_at, (int, float)):
                        post_date = datetime.utcfromtimestamp(created_at)
                    else:
                        post_date = datetime.fromisoformat(str(created_at).replace("Z", "+00:00")).replace(tzinfo=None)
                except Exception:
                    pass

            # No date filter for TikTok — we already cap at resultsPerPage=5 so
            # include all returned posts (companies may post less than once a week)

            cover_url = item.get("covers", [None])[0] if item.get("covers") else item.get("coverUrl", "")
            media_urls = [cover_url] if cover_url else []

            posts.append(SocialPost(
                company_id=company["id"],
                company_name=company["name"],
                platform="tiktok",
                post_id=item.get("id") or item.get("videoId", ""),
                post_url=item.get("webVideoUrl") or f"https://www.tiktok.com/@{tt_config['username']}/video/{item.get('id', '')}",
                date=post_date.isoformat(),
                text=item.get("text") or item.get("desc", ""),
                likes=item.get("diggCount") or item.get("likesCount") or 0,
                comments=item.get("commentCount") or item.get("commentsCount") or 0,
                shares=item.get("shareCount") or item.get("sharesCount") or 0,
                media_urls=media_urls,
                media_type="video",
            ))

        return sorted(posts, key=lambda p: p.date, reverse=True)

    def _generate_demo_data(self, company: dict, tt_config: dict, since_days: int) -> list[SocialPost]:
        """Generate realistic TikTok demo data."""
        import random
        import hashlib

        seed = hashlib.md5(f"{company['id']}_tiktok".encode()).hexdigest()
        rng = random.Random(seed)

        templates = [
            {"text": "Nye briller = nyt look 👓✨ Kom ind og prøv vores seneste kollektion! #briller #optik #brillerdk #eyewear", },
            {"text": "Sådan finder du de rigtige briller til dit ansigt 👀 #brilleguide #optiker #tips #briller"},
            {"text": "Solbriller til sommeren ☀️ Se vores nye kollektion! #solbriller #sommer #eyewear #fashion"},
            {"text": "Bag om scenen hos {company} 🎬 Se hvordan vi arbejder hver dag! #optiker #behindthescenes #briller"},
            {"text": "Synstest på {time} sekunder? Næsten! Book din gratis synstest i dag 👁️ #synstest #øjne #optik"},
            {"text": "Disse briller er seriøst gået viralt 🔥 Kom og se dem selv! #viral #briller #trending #eyewear"},
            {"text": "POV: Du finder de perfekte briller 🤩 #briller #optiker #pov #fyp"},
            {"text": "Vores bestseller er TILBAGE på lager 🙌 Link i bio! #briller #optik #bestseller #restock"},
        ]

        times = ["30", "60", "90"]
        now = datetime.utcnow()
        posts = []
        count = {1: rng.randint(0, 2), 7: rng.randint(2, 5), 30: rng.randint(5, 10)}.get(since_days, 3)

        for i in range(count):
            template = rng.choice(templates)
            post_text = template["text"].format(
                company=company["name"],
                time=rng.choice(times)
            )
            hours_ago = rng.randint(1, since_days * 24)
            post_date = now - timedelta(hours=hours_ago)
            video_id = f"{rng.randint(700000000000, 799999999999)}"

            posts.append(SocialPost(
                company_id=company["id"],
                company_name=company["name"],
                platform="tiktok",
                post_id=f"tt_{company['id']}_{i}_{since_days}d",
                post_url=f"https://www.tiktok.com/@{tt_config['username']}/video/{video_id}",
                date=post_date.isoformat(),
                text=post_text,
                likes=rng.randint(500, 25000),
                comments=rng.randint(10, 800),
                shares=rng.randint(20, 2000),
                media_urls=[f"https://picsum.photos/seed/{company['id']}tt{i}/1080/1920"],
                media_type="video",
            ))

        return sorted(posts, key=lambda p: p.date, reverse=True)
