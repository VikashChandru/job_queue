#!/usr/bin/env python3
"""Debug helper: enqueue a job using QueueManager and print jobs.json contents.

Run from project root. Prints helpful debug info on failure.
"""
import sys
import uuid
import json
from pathlib import Path

# Ensure we operate relative to the project directory (file location)
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = str(BASE_DIR / "config.json")
JOBS_PATH = str(BASE_DIR / "jobs.json")

sys.path.insert(0, str(BASE_DIR))

try:
    from config import ConfigManager
    from queue_manager import QueueManager
except Exception as e:
    print("Import error:", e)
    raise


def main():
    cfg = ConfigManager(path=CONFIG_PATH)
    qm = QueueManager(cfg, storage_path=JOBS_PATH)

    job_id = f"debug-{uuid.uuid4().hex[:8]}"
    command = "echo debug-job"

    print(f"Enqueuing job {job_id} to {JOBS_PATH} (config: {CONFIG_PATH})...")
    try:
        job = qm.enqueue(job_id=job_id, command=command)
        print("Enqueued:", job.to_dict())
    except Exception as e:
        print("Failed to enqueue:", e)
        raise

    # print jobs.json
    jobs_path = Path(JOBS_PATH)
    if jobs_path.exists():
        print("\njobs.json contents:")
        print(jobs_path.read_text())
    else:
        print("jobs.json not found at:", jobs_path)


if __name__ == "__main__":
    main()
from config import ConfigManager
from queue_manager import QueueManager

if __name__ == '__main__':
    cfg = ConfigManager()
    qm = QueueManager(cfg)
    job = qm.enqueue('debug_job_1', 'echo hello from debug', max_retries=2)
    print('ENQUEUED:', job.to_dict())
    with open('jobs.json','r',encoding='utf-8') as f:
        print('JOBS.JSON:')
        print(f.read())
