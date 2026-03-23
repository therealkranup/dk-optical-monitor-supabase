"""
Facebook scraper using Apify or Meta Graph API.
Falls back to demo data if no API credentials are configured.
"""
import asyncio
import aiohttp
from datetime import datetime, timedelta

from .base import BaseScraper, SocialPost
from ..config.settings import (
    APIFY_API_TOKEN, APIFY_API_BASE, APIFY_FACEBOOK_ACTOR,
    META_ACCESS_TOKEN, META_GRAPH_API_BASE, REQUEST_TIMEOUT
)


class FacebookScraper(BaseScraper):
    platform = "facebook"

    async def scrape(self, company: dict, since_days: int) -> list[SocialPost]:
        fb_config = company["socials"].get("facebook")
        if not fb_config:
            return []

        if APIFY_API_TOKEN:
            return await self._scrape_via_apify(company, fb_config, since_days)
        elif META_ACCESS_TOKEN:
            return await self._scrape_via_api(company, fb_config, since_days)
        else:
            return self._generate_demo_data(company, fb_config, since_days)

    async def _scrape_via_apify(self, company: dict, fb_config: dict, since_days: int) -> list[SocialPost]:
        """Scrape using Apify Facebook Posts Scraper actor (free, uses compute credits)."""
        page_url = fb_config["url"]

        input_data = {
            "startUrls": [{"url": page_url}],
            "maxPosts": 50,  # Up to 50 posts (covers ~30 days)
            "maxPostComments": 0,  # Skip comments to save credits
        }

        try:
            async with aiohttp.ClientSession() as session:
                # Start the actor run
                run_url = f"{APIFY_API_BASE}/acts/{APIFY_FACEBOOK_ACTOR}/runs"
                headers = {"Authorization": f"Bearer {APIFY_API_TOKEN}"}

                async with session.post(
                    run_url, json=input_data, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as resp:
                    body_text = await resp.text()
                    if resp.status != 201:
                        print(f"[Facebook/Apify] ❌ HTTP {resp.status} starting run for {company['name']}")
                        print(f"[Facebook/Apify]    Response: {body_text[:300]}")
                        return self._generate_demo_data(company, fb_config, since_days)

                    import json as _json
                    run_data = _json.loads(body_text)
                    run_id = run_data.get("data", {}).get("id")
                    print(f"[Facebook/Apify] ✅ Run started for {company['name']}, run_id={run_id}")

                    if not run_id:
                        print(f"[Facebook/Apify] ❌ No run_id in response")
                        return self._generate_demo_data(company, fb_config, since_days)

                # Poll for completion (max 2 minutes)
                run_info = {}
                for tick in range(120):
                    async with session.get(
                        f"{APIFY_API_BASE}/actor-runs/{run_id}",
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                    ) as resp:
                        run_info = await resp.json()
                        status = run_info.get("data", {}).get("status")
                        if tick % 10 == 0:
                            print(f"[Facebook/Apify] {company['name']} status={status} ({tick}s)")

                        if status == "SUCCEEDED":
                            print(f"[Facebook/Apify] ✅ {company['name']} succeeded after {tick}s")
                            break
                        elif status in ("FAILED", "ABORTED"):
                            print(f"[Facebook/Apify] ❌ {company['name']} run {status}")
                            return self._generate_demo_data(company, fb_config, since_days)

                    await asyncio.sleep(1)

                # Get dataset
                dataset_id = run_info.get("data", {}).get("defaultDatasetId")
                if not dataset_id:
                    print(f"[Facebook/Apify] ❌ No dataset_id for {company['name']}")
                    return self._generate_demo_data(company, fb_config, since_days)

                async with session.get(
                    f"{APIFY_API_BASE}/datasets/{dataset_id}/items",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as resp:
                    items = await resp.json()
                    print(f"[Facebook/Apify] {company['name']}: {len(items)} raw items from dataset")
                    posts = self._parse_facebook_items(company, items, since_days)
                    print(f"[Facebook/Apify] {company['name']}: {len(posts)} posts after parsing")
                    return posts

        except Exception as e:
            print(f"[Facebook/Apify] ❌ Exception for {company['name']}: {e}")
            import traceback
            traceback.print_exc()
            return self._generate_demo_data(company, fb_config, since_days)

    def _parse_facebook_items(self, company: dict, items: list, since_days: int) -> list[SocialPost]:
        """Parse Apify Facebook actor output."""
        cutoff = self._cutoff_date(since_days)
        posts = []

        for item in items:
            if not isinstance(item, dict):
                continue

            # Skip error items
            if "error" in item and "postText" not in item and "url" not in item:
                print(f"[Facebook/Apify] {company['name']} error item: {item.get('errorDescription', item.get('error', ''))[:200]}")
                continue

            # Apify returns posts directly — also handle pagePosts nesting if present
            page_posts = item.get("pagePosts") or [item]

            for post in page_posts:
                if not isinstance(post, dict):
                    continue

                # Try all known date field names
                post_date_str = (
                    post.get("postDate") or
                    post.get("date") or
                    post.get("createdTime") or
                    post.get("time") or
                    ""
                )

                post_date = None
                if post_date_str:
                    for fmt in [
                        lambda s: datetime.fromisoformat(s.replace("Z", "+00:00")),
                        lambda s: datetime.fromisoformat(s),
                    ]:
                        try:
                            post_date = fmt(post_date_str)
                            break
                        except:
                            continue

                # If no date or can't parse, still include the post (don't filter)
                if post_date and post_date.replace(tzinfo=None) < cutoff:
                    continue

                post_id = (
                    post.get("postId") or
                    post.get("id") or
                    post.get("url") or
                    ""
                )
                if not post_id:
                    continue

                posts.append(SocialPost(
                    company_id=company["id"],
                    company_name=company["name"],
                    platform="facebook",
                    post_id=post_id,
                    post_url=post.get("url") or post.get("postUrl") or post.get("link", ""),
                    date=post_date.isoformat() if post_date else datetime.utcnow().isoformat(),
                    text=post.get("postText") or post.get("text") or post.get("message") or "",
                    likes=post.get("reactionsCount") or post.get("likesCount") or post.get("likes") or 0,
                    comments=post.get("commentsCount") or post.get("comments") or 0,
                    shares=post.get("sharesCount") or post.get("shares") or 0,
                    media_urls=post.get("images", []) if post.get("images") else (
                        [post["image"]] if post.get("image") else []
                    ),
                    media_type="image" if post.get("images") or post.get("image") else "text",
                ))

        return sorted(posts, key=lambda p: p.date, reverse=True)

    async def _scrape_via_api(self, company: dict, fb_config: dict, since_days: int) -> list[SocialPost]:
        """Scrape using Facebook Graph API (fallback)."""
        cutoff = self._cutoff_date(since_days)
        page_id = fb_config["page_id"]
        since_ts = int(cutoff.timestamp())

        url = (
            f"{META_GRAPH_API_BASE}/{page_id}/posts"
            f"?fields=id,message,created_time,full_picture,attachments{{media,media_type,url}},"
            f"likes.summary(true),comments.summary(true),shares"
            f"&since={since_ts}"
            f"&limit=100"
            f"&access_token={META_ACCESS_TOKEN}"
        )

        posts = []
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                    if resp.status != 200:
                        print(f"[Facebook] API error {resp.status} for {company['name']}")
                        return self._generate_demo_data(company, fb_config, since_days)

                    data = await resp.json()

                    for item in data.get("data", []):
                        post_date = item.get("created_time", "")
                        media_urls = []
                        media_type = "text"

                        if "attachments" in item:
                            for att in item["attachments"].get("data", []):
                                mt = att.get("media_type", "")
                                if mt == "photo":
                                    media_type = "image"
                                elif mt == "video":
                                    media_type = "video"
                                elif mt == "album":
                                    media_type = "carousel"
                                if "media" in att and "image" in att["media"]:
                                    media_urls.append(att["media"]["image"]["src"])
                        elif item.get("full_picture"):
                            media_urls.append(item["full_picture"])
                            media_type = "image"

                        posts.append(SocialPost(
                            company_id=company["id"],
                            company_name=company["name"],
                            platform="facebook",
                            post_id=item.get("id", ""),
                            post_url=f"https://www.facebook.com/{item.get('id', '')}",
                            date=post_date,
                            text=item.get("message", ""),
                            likes=item.get("likes", {}).get("summary", {}).get("total_count", 0),
                            comments=item.get("comments", {}).get("summary", {}).get("total_count", 0),
                            shares=item.get("shares", {}).get("count", 0),
                            media_urls=media_urls,
                            media_type=media_type,
                        ))
        except Exception as e:
            print(f"[Facebook] Error scraping {company['name']}: {e}")
            return self._generate_demo_data(company, fb_config, since_days)

        return posts

    def _generate_demo_data(self, company: dict, fb_config: dict, since_days: int) -> list[SocialPost]:
        """Generate realistic demo data for development/demo purposes."""
        import random
        import hashlib

        seed = hashlib.md5(f"{company['id']}_facebook".encode()).hexdigest()
        rng = random.Random(seed)

        templates = [
            {"text": "Nye briller til foråret! 🌸 Kom ind og se vores seneste kollektion af stel fra {brand}. Book en tid i dag.", "type": "image"},
            {"text": "Vidste du, at en synstest hvert andet år kan forebygge øjenproblemer? Book din gratis synstest hos os i dag! 👀", "type": "image"},
            {"text": "Vi er stolte af at præsentere vores nye {brand} kollektion. Moderne design møder dansk kvalitet. Se mere på vores hjemmeside.", "type": "carousel"},
            {"text": "Tillykke til vores medarbejder {name} med 10 års jubilæum! 🎉 Tak for din dedikation til vores kunder.", "type": "image"},
            {"text": "Solbriller til sommeren ☀️ Vores nye kollektion er landet. Kom forbi og find dit nye par.", "type": "image"},
        ]

        brands = ["Ray-Ban", "Tom Ford", "Prada", "Gucci", "Oakley"]
        names = ["Maria", "Lars", "Anna", "Peter"]

        now = datetime.utcnow()
        posts = []
        count = {1: rng.randint(0, 2), 7: rng.randint(2, 5), 30: rng.randint(5, 12)}.get(since_days, 3)

        for i in range(count):
            template = rng.choice(templates)
            post_text = template["text"].format(brand=rng.choice(brands), name=rng.choice(names))
            hours_ago = rng.randint(1, since_days * 24)
            post_date = now - timedelta(hours=hours_ago)

            posts.append(SocialPost(
                company_id=company["id"],
                company_name=company["name"],
                platform="facebook",
                post_id=f"fb_{company['id']}_{i}_{since_days}d",
                post_url=f"{fb_config['url']}/posts/{i}",
                date=post_date.isoformat(),
                text=post_text,
                likes=rng.randint(15, 450),
                comments=rng.randint(2, 85),
                shares=rng.randint(0, 45),
                media_urls=[f"https://picsum.photos/seed/{company['id']}fb{i}/800/600"] if template["type"] != "text" else [],
                media_type=template["type"],
            ))

        return sorted(posts, key=lambda p: p.date, reverse=True)
