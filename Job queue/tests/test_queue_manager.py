import os
import tempfile
import time

from config import ConfigManager
from queue_manager import QueueManager


def test_enqueue_and_claim_complete():
    with tempfile.TemporaryDirectory() as td:
        cfg = ConfigManager(path=os.path.join(td, "config.json"))
        qm = QueueManager(cfg, storage_path=os.path.join(td, "jobs.json"))

        job = qm.enqueue("t1", "echo hi", max_retries=1)
        assert job.id == "t1"

        claimed = qm.claim_next_job()
        assert claimed is not None
        assert claimed.id == "t1"

        qm.complete_job(claimed.id)
        stored = qm.storage.get_job(claimed.id)
        assert stored["state"] == "completed"


def test_fail_and_retry_then_dead():
    with tempfile.TemporaryDirectory() as td:
        cfg = ConfigManager(path=os.path.join(td, "config.json"))
        qm = QueueManager(cfg, storage_path=os.path.join(td, "jobs.json"))

        job = qm.enqueue("t2", "false", max_retries=1)
        claimed = qm.claim_next_job()
        assert claimed is not None
        # simulate failure
        qm.fail_job(claimed.id, error_message="err1")
        stored = qm.storage.get_job(claimed.id)
        assert stored["state"] in ("scheduled", "dead")

        # force attempts past max_retries
        qm.fail_job(claimed.id, error_message="err2")
        stored2 = qm.storage.get_job(claimed.id)
        assert stored2["state"] == "dead"
