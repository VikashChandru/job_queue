"""
Worker infrastructure: spawn/manage worker processes and worker loop.

Provides WorkerManager with start/stop/get_active_workers used by `queuectl.py`.
Each worker process runs `worker_main` which polls the queue, claims jobs
and executes commands via subprocess.
"""
from __future__ import annotations

import multiprocessing
import signal
import time
import os
import json
import tempfile
from datetime import datetime, timezone
from typing import Dict, List, Optional

import subprocess

from config import ConfigManager
from queue_manager import QueueManager


# Registry and per-worker stop-file helpers
def _registry_path() -> str:
    return os.path.abspath(os.path.join(os.getcwd(), "workers.json"))


def _stop_file_for(pid: int) -> str:
    return os.path.abspath(os.path.join(os.getcwd(), f"worker_{pid}.stop"))


def _read_registry() -> List[Dict]:
    p = _registry_path()
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def _write_registry(entries: List[Dict]) -> None:
    p = _registry_path()
    dirpath = os.path.dirname(p) or "."
    fd, tmp = tempfile.mkstemp(dir=dirpath)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(entries, f, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp, p)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass


def _add_registry_entry(pid: int, start_time: float) -> None:
    entries = _read_registry()
    # avoid duplicates
    entries = [e for e in entries if e.get("pid") != pid]
    entries.append({"pid": pid, "start_time": start_time})
    _write_registry(entries)


def _remove_registry_entry(pid: int) -> None:
    entries = _read_registry()
    entries = [e for e in entries if e.get("pid") != pid]
    _write_registry(entries)


def worker_main(storage_path: Optional[str], config_path: Optional[str], poll_interval: float = 1.0):
    """Main loop for a single worker process."""
    cfg = ConfigManager(config_path)
    qm = QueueManager(cfg, storage_path)
    shutting_down = False

    def _sigterm_handler(signum, frame):
        nonlocal shutting_down
        shutting_down = True

    # Register signal handlers (POSIX). Windows will still handle KeyboardInterrupt.
    try:
        signal.signal(signal.SIGTERM, _sigterm_handler)
    except Exception:
        # Some platforms (Windows) may not support SIGTERM setting in the same way
        pass
    signal.signal(signal.SIGINT, _sigterm_handler)

    pid = os.getpid()
    # ensure our registry entry exists if someone started this worker directly
    try:
        _add_registry_entry(pid, time.time())
    except Exception:
        pass

    stop_file = _stop_file_for(pid)

    while not shutting_down:
        try:
            # Cooperative shutdown check: if a stop file exists, mark for shutdown
            if os.path.exists(stop_file):
                shutting_down = True
                break

            job = qm.claim_next_job()
            if not job:
                time.sleep(poll_interval)
                continue

            # Execute the job command
            cmd = job.command
            try:
                # Run command in shell so user can pass string commands
                proc = subprocess.run(str(cmd), shell=True, capture_output=True, text=True)
                stdout = proc.stdout
                stderr = proc.stderr
                if proc.returncode == 0:
                    try:
                        qm.complete_job(job.id)
                        # also store outputs if present
                        if stdout or stderr:
                            qm.storage.update_job(job.id, {"stdout": stdout, "stderr": stderr})
                    except Exception:
                        # fallback: update directly
                        qm.storage.update_job(job.id, {"state": "completed", "stdout": stdout, "stderr": stderr, "updated_at": datetime.utcnow().isoformat() + "Z"})
                else:
                    err = stderr or stdout or f"exit:{proc.returncode}"
                    try:
                        qm.fail_job(job.id, error_message=err, stdout=stdout, stderr=stderr)
                    except Exception:
                        qm.storage.update_job(job.id, {"state": "scheduled", "error_message": str(err), "stdout": stdout, "stderr": stderr, "updated_at": datetime.utcnow().isoformat() + "Z"})
            except Exception as e:
                try:
                    qm.fail_job(job.id, error_message=str(e))
                except Exception:
                    qm.storage.update_job(job.id, {"state": "scheduled", "error_message": str(e), "updated_at": datetime.utcnow().isoformat() + "Z"})

        except Exception:
            # Avoid crashing worker on transient errors; sleep briefly
            time.sleep(0.5)

    # Clean up registry and stop file on exit
    try:
        _remove_registry_entry(pid)
    except Exception:
        pass
    try:
        if os.path.exists(stop_file):
            os.remove(stop_file)
    except Exception:
        pass


class WorkerManager:
    def __init__(self, queue_manager: QueueManager, config_manager: ConfigManager):
        self.queue_manager = queue_manager
        self.config_manager = config_manager
        self._procs: List[Dict] = []

    def start_workers(self, count: int = 1) -> None:
        """Start `count` background worker processes."""
        for _ in range(count):
            p = multiprocessing.Process(target=worker_main, args=(self.queue_manager.storage.path, self.config_manager.path))
            p.daemon = False
            p.start()
            self._procs.append({"proc": p, "start_time": time.time()})
            try:
                _add_registry_entry(p.pid, time.time())
            except Exception:
                pass

    def stop_workers(self, timeout: float = 5.0) -> int:
        """Stop all workers, return number stopped."""
        stopped = 0

        # First, attempt cooperative shutdown via registry for all known pids
        entries = _read_registry()
        now = time.time()
        for e in entries:
            pid = int(e.get("pid"))
            stop_file = _stop_file_for(pid)
            try:
                # create stop file to request cooperative shutdown
                with open(stop_file, "w", encoding="utf-8") as f:
                    f.write(str(now))
            except Exception:
                pass

        # Give workers some time to exit gracefully
        deadline = time.time() + timeout
        for e in list(entries):
            pid = int(e.get("pid"))
            alive = True
            try:
                while time.time() < deadline and alive:
                    try:
                        # os.kill(pid, 0) checks for existence on POSIX; on Windows will raise OSError if not present
                        os.kill(pid, 0)
                        alive = True
                    except Exception:
                        alive = False
                        break
                    time.sleep(0.1)
            except KeyboardInterrupt:
                # User interrupted shutdown; proceed to force termination cleanup
                alive = True

            if alive:
                # Force terminate
                try:
                    if os.name == "nt":
                        # Use taskkill to terminate the external process
                        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    else:
                        try:
                            os.kill(pid, signal.SIGTERM)
                        except Exception:
                            pass
                        time.sleep(0.5)
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except Exception:
                            pass
                except Exception:
                    pass

            # cleanup registry and stop file
            try:
                _remove_registry_entry(pid)
            except Exception:
                pass
            try:
                sf = _stop_file_for(pid)
                if os.path.exists(sf):
                    os.remove(sf)
            except Exception:
                pass

            stopped += 1

        # Also handle any procs started in this manager instance
        for info in list(self._procs):
            p = info.get("proc")
            if p.is_alive():
                try:
                    p.terminate()
                    p.join(0.5)
                except KeyboardInterrupt:
                    # Ignore further interrupts while forcing termination
                    pass
                except Exception:
                    pass
            if not p.is_alive():
                try:
                    _remove_registry_entry(p.pid)
                except Exception:
                    pass
                self._procs.remove(info)

        return stopped

    def get_active_workers(self) -> List[Dict]:
        out = []
        for info in list(self._procs):
            p = info.get("proc")
            if not p.is_alive():
                # cleanup
                self._procs.remove(info)
                continue
            uptime = int(time.time() - info.get("start_time", time.time()))
            out.append({"pid": p.pid, "uptime": uptime})
        return out


if __name__ == "__main__":
    # Simple CLI runner for manual testing: start a single worker
    cfg = ConfigManager()
    qm = QueueManager(cfg)
    wm = WorkerManager(qm, cfg)
    print("Starting 1 worker (Ctrl-C to stop)")
    wm.start_workers(1)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # Ignore further SIGINTs while we attempt a graceful shutdown
        try:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
        except Exception:
            pass
        print("Stopping workers...")
        try:
            stopped = wm.stop_workers()
        except KeyboardInterrupt:
            # If user insists on interrupting shutdown, proceed to force-exit
            print("Interrupted during shutdown. Exiting immediately.")
            stopped = 0
        print(f"Stopped {stopped} worker(s)")
