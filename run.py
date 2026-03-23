#!/usr/bin/env python3
"""
Entry point for the DK Optical Social Monitor.

Usage:
    python run.py serve     — Start the API server + dashboard
    python run.py scrape    — Run a one-off scrape (30 days)
    python run.py scrape 7  — Run a one-off scrape (7 days)
"""
import sys
import os
import asyncio


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "serve":
        import uvicorn
        from backend.server import app
        port = int(os.environ.get("PORT", 8000))
        print("\n🔭 DK Optical Social Monitor")
        print(f"   Dashboard: http://localhost:{port}")
        print(f"   API docs:  http://localhost:{port}/docs\n")
        uvicorn.run(app, host="0.0.0.0", port=port)

    elif command == "scrape":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        from backend.agents.orchestrator import Orchestrator
        orch = Orchestrator()
        result = asyncio.run(orch.run_all(since_days=days))
        import json
        print(json.dumps(result, indent=2))

    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
