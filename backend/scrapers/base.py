"""
Base scraper class defining the interface all platform scrapers must implement.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional


@dataclass
class SocialPost:
    """Standardized social media post across all platforms."""
    company_id: str
    company_name: str
    platform: str  # facebook | instagram | linkedin
    post_id: str
    post_url: str
    date: str  # ISO 8601
    text: str
    likes: int = 0
    comments: int = 0
    shares: int = 0
    media_urls: list = field(default_factory=list)
    media_type: str = ""  # image | video | carousel | link | text
    scraped_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self):
        return asdict(self)


class BaseScraper(ABC):
    """Abstract base for all platform scrapers."""

    platform: str = ""

    @abstractmethod
    async def scrape(self, company: dict, since_days: int) -> list[SocialPost]:
        """
        Scrape posts for a given company within the time window.

        Args:
            company: Company config dict from companies.py
            since_days: Number of days to look back (1, 7, or 30)

        Returns:
            List of SocialPost objects
        """
        ...

    def _cutoff_date(self, since_days: int) -> datetime:
        from datetime import timedelta
        return datetime.utcnow() - timedelta(days=since_days)
