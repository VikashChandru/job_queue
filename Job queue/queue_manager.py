"""
Queue manager: job lifecycle, retry/backoff, DLQ and statistics.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import List, Optional

from storage import Storage
from config import ConfigManager
from models import Job


class QueueManager:
    def __init__(self, config: ConfigManager, storage_path: str = None):
        self.config = config
        self.storage = Storage(storage_path)

    def enqueue(self, job_id: str, command: str, max_retries: Optional[int] = None, run_at: Optional[float] = None) -> Job:
        if max_retries is None:
            max_retries = int(self.config.get("max_retries", 3))

        state = "pending" if run_at is None else "scheduled"
        job = Job(
            id=job_id,
            command=command,
            state=state,
            attempts=0,
            max_retries=max_retries,
        )

        # If a run_at timestamp (seconds since epoch) is provided, set next_retry_at
        if run_at is not None:
            job.next_retry_at = datetime.fromtimestamp(run_at, tz=timezone.utc).isoformat()

        stored = self.storage.create_job(job.to_dict())
        return Job.from_dict(stored)

    def claim_next_job(self) -> Optional[Job]:
        """Find the next runnable job and atomically mark it processing.

        Returns Job or None.
        """
        now_ts = datetime.now(timezone.utc).timestamp()
        candidates = self.storage.list_jobs()
        # simple FIFO: iterate in file order
        for j in candidates:
            state = j.get("state")
            if state not in ("pending", "scheduled"):
                continue
            next_retry = j.get("next_retry_at")
            if next_retry:
                try:
                    # parse ISO-ish timestamp
                    t = datetime.fromisoformat(next_retry.replace("Z", "+00:00")).timestamp()
                except Exception:
                    t = 0
                if t > now_ts:
                    continue
            # try to atomically transition
            ok = self.storage.atomic_state_transition(j.get("id"), state, "processing", {})
            if ok:
                updated = self.storage.get_job(j.get("id"))
                return Job.from_dict(updated)
        return None

    def complete_job(self, job_id: str) -> Job:
        # Mark job completed and optionally store outputs
        updated = self.storage.get_job(job_id)
        if not updated:
            raise KeyError(f"Job not found: {job_id}")
        # ensure we preserve any existing fields
        data = {**updated, "state": "completed", "updated_at": datetime.utcnow().isoformat() + "Z"}
        # allow caller to pass stdout/stderr in updated if provided
        updated = self.storage.update_job(job_id, data)
        return Job.from_dict(updated)

    def fail_job(self, job_id: str, error_message: Optional[str] = None, stdout: Optional[str] = None, stderr: Optional[str] = None) -> Job:
        job = self.storage.get_job(job_id)
        if not job:
            raise KeyError(f"Job not found: {job_id}")

        attempts = int(job.get("attempts", 0)) + 1
        max_retries = int(job.get("max_retries", self.config.get("max_retries", 3)))
        backoff_base = int(self.config.get("backoff_base", 2))

        if attempts > max_retries:
            # move to dead letter
            payload = {"state": "dead", "attempts": attempts, "error_message": str(error_message or ""), "updated_at": datetime.utcnow().isoformat() + "Z"}
            if stdout is not None:
                payload["stdout"] = stdout
            if stderr is not None:
                payload["stderr"] = stderr
            updated = self.storage.update_job(job_id, payload)
            return Job.from_dict(updated)

        # schedule retry
        delay = backoff_base ** (attempts - 1)
        next_ts = datetime.utcnow().timestamp() + delay
        next_iso = datetime.fromtimestamp(next_ts, tz=timezone.utc).isoformat()
        payload = {
            "state": "scheduled",
            "attempts": attempts,
            "next_retry_at": next_iso,
            "error_message": str(error_message or ""),
            "updated_at": datetime.utcnow().isoformat() + "Z",
        }
        if stdout is not None:
            payload["stdout"] = stdout
        if stderr is not None:
            payload["stderr"] = stderr
        updated = self.storage.update_job(job_id, payload)
        return Job.from_dict(updated)

    def move_to_dead_letter(self, job_id: str, reason: Optional[str] = None) -> Job:
        updated = self.storage.update_job(job_id, {"state": "dead", "error_message": str(reason or ""), "updated_at": datetime.utcnow().isoformat() + "Z"})
        return Job.from_dict(updated)

    def get_stats(self) -> dict:
        jobs = self.storage.list_jobs()
        stats = {"pending": 0, "processing": 0, "completed": 0, "failed": 0, "dead": 0, "total": len(jobs)}
        for j in jobs:
            s = j.get("state")
            if s in stats:
                stats[s] += 1
            else:
                # treat unknown as failed
                stats["failed"] += 1
        return stats

    def list_jobs(self, state: Optional[str] = None, limit: int = 20) -> List[Job]:
        jobs = self.storage.list_jobs(limit=limit, state=state)
        return [Job.from_dict(j) for j in jobs]

    def list_dlq_jobs(self, limit: int = 20) -> List[Job]:
        return self.list_jobs(state="dead", limit=limit)

    def retry_dlq_job(self, job_id: str) -> Job:
        job = self.storage.get_job(job_id)
        if not job:
            raise ValueError("Job not found")
        if job.get("state") != "dead":
            raise ValueError("Job is not in Dead Letter Queue")
        updated = self.storage.update_job(job_id, {"state": "pending", "attempts": 0, "error_message": None, "next_retry_at": None, "updated_at": datetime.utcnow().isoformat() + "Z"})
        return Job.from_dict(updated)
