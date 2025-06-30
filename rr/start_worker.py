#!/usr/bin/env python3
"""
Startup script for CrateDB Temporal worker.
"""

import asyncio
import sys
from pathlib import Path

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from worker import run_worker

if __name__ == "__main__":
    asyncio.run(run_worker())