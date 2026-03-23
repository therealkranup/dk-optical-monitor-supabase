"""
FastAPI server providing REST endpoints for the dashboard.
Endpoints:
  GET  /api/scrape?days=30          — trigger a scrape run
  GET  /api/posts?company=X&platform=Y&days=30  — query posts
  GET  /api/stats?days=30           — aggregated stats
  GET  /api/companies               — list configured companies
  GET  /                            — serve the dashboard
"""
import asyncio
import os
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from .agents.orchestrator import Orchestrator
from .storage.database import PostDatabase
from .config.companies import COMPANIES, TIME_FILTERS

app = FastAPI(title="DK Optical Social Monitor", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

db = PostDatabase()
orchestrator = Orchestrator(db=db)

# Track scrape status
scrape_status = {"running": False, "last_result": None}


@app.get("/api/companies")
async def get_companies():
    """Return the list of monitored companies."""
    return {"companies": COMPANIES, "time_filters": TIME_FILTERS}


@app.get("/api/scrape")
async def trigger_scrape(days: int = Query(default=30, ge=1, le=90)):
    """Trigger a scrape run for all companies."""
    if scrape_status["running"]:
        return JSONResponse(
            status_code=409,
            content={"status": "already_running", "message": "A scrape is already in progress"},
        )

    scrape_status["running"] = True
    try:
        result = await orchestrator.run_all(since_days=days)
        scrape_status["last_result"] = result
        return result
    finally:
        scrape_status["running"] = False


@app.get("/api/scrape/all")
async def trigger_full_scrape():
    """Run scraping for all time filter presets."""
    if scrape_status["running"]:
        return JSONResponse(
            status_code=409,
            content={"status": "already_running"},
        )

    scrape_status["running"] = True
    try:
        result = await orchestrator.run_all_time_filters()
        scrape_status["last_result"] = result
        return result
    finally:
        scrape_status["running"] = False


@app.get("/api/posts")
async def get_posts(
    company: str = Query(default=None),
    platform: str = Query(default=None),
    days: int = Query(default=None, ge=1, le=90),
    limit: int = Query(default=200, ge=1, le=1000),
):
    """Query stored posts with optional filters."""
    posts = db.query_posts(
        company_id=company,
        platform=platform,
        since_days=days,
        limit=limit,
    )
    return {"count": len(posts), "posts": posts}


@app.get("/api/stats")
async def get_stats(days: int = Query(default=None, ge=1, le=90)):
    """Get aggregated statistics per company and platform."""
    stats = db.get_stats(since_days=days)
    return {"time_filter_days": days, "companies": stats}


@app.get("/api/status")
async def get_status():
    """Get scrape status."""
    return scrape_status


# Serve frontend
frontend_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")


@app.get("/")
async def serve_dashboard():
    index_path = os.path.join(frontend_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "Dashboard not found. Place index.html in frontend/"}


# Mount static files for frontend assets
if os.path.exists(frontend_dir):
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")
