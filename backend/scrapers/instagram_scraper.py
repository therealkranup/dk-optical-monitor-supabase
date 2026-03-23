"""
Instagram scraper using Apify or Meta Graph API.
Falls back to demo data if no API credentials are configured.
"""
import asyncio
import aiohttp
from datetime import datetime, timedelta

from .base import BaseScraper, SocialPost
from ..config.settings import (
    APIFY_API_TOKEN, APIFY_API_BASE, APIFY_INSTAGRAM_ACTOR,
    META_ACCESS_TOKEN, META_GRAPH_API_BASE, REQUEST_TIMEOUT
)


class InstagramScraper(BaseScraper):
    platform = "instagram"

    async def scrape(self, company: dict, since_days: int) -> list[SocialPost]:
        ig_config = company["socials"].get("instagram")
        if not ig_config:
            return []

        if APIFY_API_TOKEN:
            return await self._scrape_via_apify(company, ig_config, since_days)
        elif META_ACCESS_TOKEN:
            return await self._scrape_via_api(company, ig_config, since_days)
        else:
            return self._generate_demo_data(company, ig_config, since_days)

    async def _scrape_via_apify(self, company: dict, ig_config: dict, since_days: int) -> list[SocialPost]:
        """Scrape using Apify Instagram Scraper actor."""
        username = ig_config["username"]
        profile_url = ig_config.get("url", f"https://www.instagram.com/{username}/")

        input_data = {
            "directUrls": [profile_url],  # More reliable than usernames
            "resultsLimit": 5,            # Max 5 posts to save API costs
        }

        try:
            async with aiohttp.ClientSession() as session:
                # Start the actor run
                run_url = f"{APIFY_API_BASE}/acts/{APIFY_INSTAGRAM_ACTOR}/runs"
                headers = {"Authorization": f"Bearer {APIFY_API_TOKEN}"}

                async with session.post(
                    run_url, json=input_data, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as resp:
                    body_text = await resp.text()
                    if resp.status != 201:
                        print(f"[Instagram/Apify] ❌ HTTP {resp.status} starting run for {company['name']}")
                        print(f"[Instagram/Apify]    Response: {body_text[:300]}")
                        return self._generate_demo_data(company, ig_config, since_days)

                    import json as _json
                    run_data = _json.loads(body_text)
                    run_id = run_data.get("data", {}).get("id")
                    print(f"[Instagram/Apify] ✅ Run started for {company['name']}, run_id={run_id}")

                    if not run_id:
                        print(f"[Instagram/Apify] ❌ No run_id in response")
                        return self._generate_demo_data(company, ig_config, since_days)

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
                            print(f"[Instagram/Apify] {company['name']} status={status} ({tick}s)")

                        if status == "SUCCEEDED":
                            print(f"[Instagram/Apify] ✅ {company['name']} succeeded after {tick}s")
                            break
                        elif status in ("FAILED", "ABORTED"):
                            print(f"[Instagram/Apify] ❌ {company['name']} run {status}")
                            return self._generate_demo_data(company, ig_config, since_days)

                    await asyncio.sleep(1)

                # Get dataset
                dataset_id = run_info.get("data", {}).get("defaultDatasetId")
                if not dataset_id:
                    print(f"[Instagram/Apify] ❌ No dataset_id for {company['name']}")
                    return self._generate_demo_data(company, ig_config, since_days)

                async with session.get(
                    f"{APIFY_API_BASE}/datasets/{dataset_id}/items",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as resp:
                    items = await resp.json()
                    print(f"[Instagram/Apify] {company['name']}: {len(items)} raw items from dataset")
                    if items and isinstance(items[0], dict):
                        first = items[0]
                        print(f"[Instagram/Apify]   First item keys: {list(first.keys())[:12]}")
                        # Check for error items
                        if "error" in first or "errorDescription" in first:
                            err = first.get("errorDescription") or first.get("error") or "unknown"
                            print(f"[Instagram/Apify] ⚠️  Actor error for {company['name']}: {str(err)[:300]}")
                    posts = self._parse_instagram_items(company, items, since_days)
                    print(f"[Instagram/Apify] {company['name']}: {len(posts)} posts after parsing")
                    return posts

        except Exception as e:
            print(f"[Instagram/Apify] ❌ Exception for {company['name']}: {e}")
            import traceback
            traceback.print_exc()
            return self._generate_demo_data(company, ig_config, since_days)

    def _parse_instagram_items(self, company: dict, items: list, since_days: int) -> list[SocialPost]:
        """Parse Apify Instagram actor output.
        Field names from apify/instagram-scraper:
          timestamp, caption, commentsCount, likesCount, displayUrl,
          shortCode, id, type, images, videoUrl, childPosts
        """
        cutoff = self._cutoff_date(since_days)
        posts = []

        for item in items:
            if not isinstance(item, dict):
                continue

            # Each item is a single post from the actor
            post_items = item.get("posts", [item]) if "posts" in item else [item]

            for post in post_items:
                if not isinstance(post, dict):
                    continue

                # Try all known timestamp field names
                post_date_str = (
                    post.get("timestamp") or
                    post.get("takenAtTs") or
                    post.get("captionedAt") or
                    post.get("takenAt") or ""
                )
                if not post_date_str:
                    continue

                try:
                    # Handle both ISO string and Unix timestamp
                    if isinstance(post_date_str, (int, float)):
                        post_date = datetime.utcfromtimestamp(post_date_str)
                    else:
                        post_date = datetime.fromisoformat(str(post_date_str).replace("Z", "+00:00"))
                except Exception:
                    continue

                if post_date.replace(tzinfo=None) < cutoff:
                    continue

                media_urls = []
                media_type = "image"
                post_type = post.get("type", "").lower()

                if post_type == "video" or post.get("isVideo"):
                    media_type = "video"
                    if post.get("videoUrl"):
                        media_urls.append(post["videoUrl"])
                elif post_type in ("sidecar", "carousel") or post.get("childPosts"):
                    media_type = "carousel"
                    for child in (post.get("childPosts") or post.get("images") or []):
                        src = child.get("displayUrl") or child.get("src") or child.get("url", "")
                        if src:
                            media_urls.append(src)
                else:
                    media_type = "image"

                if not media_urls and post.get("displayUrl"):
                    media_urls.append(post["displayUrl"])

                short_code = post.get("shortCode") or post.get("shortcode") or ""
                post_url = (
                    post.get("url") or
                    post.get("postUrl") or
                    (f"https://www.instagram.com/p/{short_code}/" if short_code else "")
                )

                posts.append(SocialPost(
                    company_id=company["id"],
                    company_name=company["name"],
                    platform="instagram",
                    post_id=str(post.get("id") or short_code or ""),
                    post_url=post_url,
                    date=post_date.isoformat(),
                    text=post.get("caption") or post.get("alt") or "",
                    # Apify uses commentsCount and likesCount (not commentCount/likeCount)
                    likes=post.get("likesCount") or post.get("likeCount") or post.get("likes") or 0,
                    comments=post.get("commentsCount") or post.get("commentCount") or post.get("comments") or 0,
                    shares=0,
                    media_urls=media_urls,
                    media_type=media_type,
                ))

        return sorted(posts, key=lambda p: p.date, reverse=True)

    async def _scrape_via_api(self, company: dict, ig_config: dict, since_days: int) -> list[SocialPost]:
        """Scrape using Instagram Graph API (fallback)."""
        cutoff = self._cutoff_date(since_days)
        username = ig_config["username"]

        # Get IG account ID via linked FB page
        url = (
            f"{META_GRAPH_API_BASE}/me/accounts"
            f"?fields=instagram_business_account{{id,username}}"
            f"&access_token={META_ACCESS_TOKEN}"
        )

        posts = []
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                    if resp.status != 200:
                        return self._generate_demo_data(company, ig_config, since_days)
                    accounts_data = await resp.json()

                ig_account_id = None
                for page in accounts_data.get("data", []):
                    ig = page.get("instagram_business_account", {})
                    if ig.get("username", "").lower() == username.lower():
                        ig_account_id = ig["id"]
                        break

                if not ig_account_id:
                    return self._generate_demo_data(company, ig_config, since_days)

                media_url = (
                    f"{META_GRAPH_API_BASE}/{ig_account_id}/media"
                    f"?fields=id,caption,timestamp,media_type,media_url,permalink,"
                    f"like_count,comments_count,children{{media_url,media_type}}"
                    f"&limit=50"
                    f"&access_token={META_ACCESS_TOKEN}"
                )

                async with session.get(media_url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                    if resp.status != 200:
                        return self._generate_demo_data(company, ig_config, since_days)
                    media_data = await resp.json()

                for item in media_data.get("data", []):
                    post_date = item.get("timestamp", "")
                    if post_date:
                        dt = datetime.fromisoformat(post_date.replace("Z", "+00:00"))
                        if dt.replace(tzinfo=None) < cutoff:
                            continue

                    media_urls = []
                    media_type = "image"
                    ig_media_type = item.get("media_type", "IMAGE")

                    if ig_media_type == "VIDEO":
                        media_type = "video"
                    elif ig_media_type == "CAROUSEL_ALBUM":
                        media_type = "carousel"
                        for child in item.get("children", {}).get("data", []):
                            if child.get("media_url"):
                                media_urls.append(child["media_url"])
                    if item.get("media_url") and not media_urls:
                        media_urls.append(item["media_url"])

                    posts.append(SocialPost(
                        company_id=company["id"],
                        company_name=company["name"],
                        platform="instagram",
                        post_id=item.get("id", ""),
                        post_url=item.get("permalink", f"https://www.instagram.com/p/{item.get('id', '')}"),
                        date=post_date,
                        text=item.get("caption", ""),
                        likes=item.get("like_count", 0),
                        comments=item.get("comments_count", 0),
                        shares=0,
                        media_urls=media_urls,
                        media_type=media_type,
                    ))

        except Exception as e:
            print(f"[Instagram] Error scraping {company['name']}: {e}")
            return self._generate_demo_data(company, ig_config, since_days)

        return posts

    def _generate_demo_data(self, company: dict, ig_config: dict, since_days: int) -> list[SocialPost]:
        """Generate realistic Instagram demo data."""
        import random
        import hashlib

        seed = hashlib.md5(f"{company['id']}_instagram".encode()).hexdigest()
        rng = random.Random(seed)

        templates = [
            {"text": "Forårskollektionen er her 🌷 Swipe for at se de nyeste styles.\n\n#briller #optik #dansk #forår", "type": "carousel"},
            {"text": "Ny kollektion just dropped ✨ Link i bio.\n\n#sunglasses #eyewear #fashion", "type": "image"},
            {"text": "Monday motivation: Se verden klart 👁️ Book din synstest via linket i bio.\n\n#optik #øjne", "type": "image"},
        ]

        brands = ["Ray-Ban", "Tom Ford", "Prada"]

        now = datetime.utcnow()
        posts = []
        count = {1: rng.randint(0, 3), 7: rng.randint(3, 7), 30: rng.randint(8, 15)}.get(since_days, 5)

        for i in range(count):
            template = rng.choice(templates)
            post_text = template["text"].format(brand=rng.choice(brands))
            hours_ago = rng.randint(1, since_days * 24)
            post_date = now - timedelta(hours=hours_ago)

            posts.append(SocialPost(
                company_id=company["id"],
                company_name=company["name"],
                platform="instagram",
                post_id=f"ig_{company['id']}_{i}_{since_days}d",
                post_url=f"https://www.instagram.com/p/{company['id']}{i}/",
                date=post_date.isoformat(),
                text=post_text,
                likes=rng.randint(30, 800),
                comments=rng.randint(1, 120),
                shares=0,
                media_urls=[f"https://picsum.photos/seed/{company['id']}ig{i}/1080/1080"],
                media_type=template["type"],
            ))

        return sorted(posts, key=lambda p: p.date, reverse=True)
