# QueueCTL — Background Job Queue (Python)

This repository is a small CLI-based background job queue system used for a take-home assignment.

Summary
- Enqueue jobs, run multiple workers, retry failed jobs with exponential backoff, and move permanently failed jobs to a Dead Letter Queue.
- File-based persistent storage (`jobs.json`, `config.json`).
- Worker registry (`workers.json`) and cooperative shutdown.

Quick setup (Windows / PowerShell)

1. Create & activate venv (recommended):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

2. Basic commands (run from project folder or use full path to `queuectl.py`)

Enqueue a job:
```powershell
python .\queuectl.py enqueue '{ "id":"job1","command":"echo hello" }'
```

Start workers (spawn background worker processes):
```powershell
python .\queuectl.py worker start --count 2
```

Stop workers (requests graceful shutdown, forces remaining after timeout):
```powershell
python .\queuectl.py worker stop
```

Check status:
```powershell
python .\queuectl.py status
```

List jobs:
```powershell
python .\queuectl.py list --state pending
python .\queuectl.py dlq list
```

Configuration
- `config.json` holds runtime configuration: `max_retries` and `backoff_base`.

Logs & job output
- Workers capture stdout/stderr and persist them into each job record (`stdout` and `stderr`) so you can inspect outputs and errors.

Testing
- Run pytest from project root:

```powershell
pytest -q
```

Notes & assumptions
- Storage is file-based JSON for simplicity. It uses atomic writes and a simple file lock.
- Cooperative shutdown uses per-worker stop files on Windows (and POSIX signal on Unix). Forced termination uses OS facilities.
- This is an educational/demo implementation and not intended for production scale; consider SQLite/Redis for production.

Files
- `queuectl.py` — CLI entrypoint
- `models.py` — Job dataclass
- `storage.py` — File-backed storage layer
- `queue_manager.py` — Job lifecycle and retry logic
- `worker.py` — Worker loop and process management
- `debug_enqueue.py` — helper to enqueue a test job
- `run_demo.py` — demo script (creates jobs and runs workers)
