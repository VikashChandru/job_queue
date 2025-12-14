#!/usr/bin/env python3
"""Demo script: enqueues sample jobs and starts workers for a short demo run."""
import time
from pathlib import Path

from config import ConfigManager
from queue_manager import QueueManager
from worker import WorkerManager


def main():
    base = Path(__file__).resolve().parent
    cfg = ConfigManager(path=str(base / "config.json"))
    qm = QueueManager(cfg, storage_path=str(base / "jobs.json"))
    wm = WorkerManager(qm, cfg)

    # Enqueue a few jobs
    print("Enqueuing demo jobs...")
    for i in range(3):
        jid = f"demo-{i+1}"
        cmd = "ping -n 2 127.0.0.1 >nul & echo demo" if (os.name=="nt") else "sleep 1 && echo demo"
        qm.enqueue(jid, cmd)
        print("  enqueued", jid)

    print("Starting 2 workers...")
    wm.start_workers(2)

    # Run demo for 10 seconds
    try:
        for _ in range(10):
            time.sleep(1)
    finally:
        print("Stopping workers...")
        wm.stop_workers()


if __name__ == "__main__":
    import os
    main()
