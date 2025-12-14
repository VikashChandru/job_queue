"""
File-based JSON storage with cross-platform file locking and atomic writes.

Provides basic CRUD for job dicts and an atomic state transition helper.
"""
from __future__ import annotations

import json
import os
import tempfile
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import sys

if os.name == "nt":
    import msvcrt
else:
    import fcntl


@contextmanager
def file_lock(f):
    """Cross-platform exclusive lock for an open file object.

    On POSIX uses fcntl.flock, on Windows uses msvcrt.locking.
    The caller should open the file in r+ or a+ mode before calling.
    """
    if os.name == "nt":
        try:
            # Lock the whole file (0 to 0x7fffffff)
            msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 0x7FFFFFFF)
            yield
        finally:
            try:
                f.seek(0)
                msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 0x7FFFFFFF)
            except Exception:
                pass
    else:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass


class Storage:
    """Simple file-backed JSON storage for jobs.

    Data schema in file: { "jobs": [ {job dict}, ... ] }
    Operations are protected with file locks and atomic replace on write.
    """

    def __init__(self, path: Optional[str] = None):
        if path:
            self.path = os.path.abspath(path)
        else:
            self.path = os.path.abspath(os.path.join(os.getcwd(), "jobs.json"))

        # Ensure file exists
        if not os.path.exists(self.path):
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump({"jobs": []}, f)

    def _read(self) -> Dict[str, Any]:
        with open(self.path, "r", encoding="utf-8") as f:
            # Lock for reading to coordinate with writers
            with file_lock(f):
                f.seek(0)
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    return {"jobs": []}

    def _write_atomic(self, data: Dict[str, Any]) -> None:
        dirpath = os.path.dirname(self.path) or "."
        fd, tmp_path = tempfile.mkstemp(dir=dirpath)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as tmp:
                json.dump(data, tmp, indent=2)
                tmp.flush()
                os.fsync(tmp.fileno())
            # Replace atomically
            os.replace(tmp_path, self.path)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

    # CRUD operations
    def list_jobs(self, *, limit: Optional[int] = None, state: Optional[str] = None) -> List[Dict[str, Any]]:
        data = self._read()
        jobs = data.get("jobs", [])
        if state:
            jobs = [j for j in jobs if j.get("state") == state]
        if limit:
            return jobs[:limit]
        return jobs

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        data = self._read()
        for j in data.get("jobs", []):
            if j.get("id") == job_id:
                return j
        return None

    def create_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        # append and write
        for _ in range(3):
            data = self._read()
            jobs = data.setdefault("jobs", [])
            jobs.append(job)
            try:
                self._write_atomic(data)
                return job
            except Exception:
                time.sleep(0.05)
        raise RuntimeError("Failed to create job after retries")

    def update_job(self, job_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        for _ in range(3):
            data = self._read()
            jobs = data.setdefault("jobs", [])
            modified = False
            for idx, j in enumerate(jobs):
                if j.get("id") == job_id:
                    jobs[idx] = {**j, **updates}
                    modified = True
                    break
            if not modified:
                raise KeyError(f"Job not found: {job_id}")
            try:
                self._write_atomic(data)
                return jobs[idx]
            except Exception:
                time.sleep(0.05)
        raise RuntimeError("Failed to update job after retries")

    def delete_job(self, job_id: str) -> None:
        for _ in range(3):
            data = self._read()
            jobs = data.setdefault("jobs", [])
            new_jobs = [j for j in jobs if j.get("id") != job_id]
            if len(new_jobs) == len(jobs):
                raise KeyError(f"Job not found: {job_id}")
            data["jobs"] = new_jobs
            try:
                self._write_atomic(data)
                return
            except Exception:
                time.sleep(0.05)
        raise RuntimeError("Failed to delete job after retries")

    def atomic_state_transition(self, job_id: str, from_state: str, to_state: str, extra: Optional[Dict[str, Any]] = None) -> bool:
        """Atomically change job state only if current state == from_state.

        Returns True if transition occurred, False otherwise.
        """
        extra = extra or {}
        for _ in range(5):
            data = self._read()
            jobs = data.setdefault("jobs", [])
            changed = False
            for idx, j in enumerate(jobs):
                if j.get("id") == job_id:
                    if j.get("state") != from_state:
                        return False
                    new_job = dict(j)
                    new_job.update({"state": to_state})
                    new_job.update(extra)
                    new_job["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    jobs[idx] = new_job
                    changed = True
                    break
            if not changed:
                return False
            try:
                self._write_atomic(data)
                return True
            except Exception:
                time.sleep(0.02)
        return False
