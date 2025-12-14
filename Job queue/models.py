"""
Data models for the queue system
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional
import json


@dataclass
class Job:
    """Job data model"""
    id: str
    command: str
    state: str = "pending"  # pending, processing, completed, failed, dead
    attempts: int = 0
    max_retries: int = 3
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    error_message: Optional[str] = None
    next_retry_at: Optional[str] = None

    def __post_init__(self):
        """Initialize timestamps if not provided"""
        now = datetime.utcnow().isoformat() + "Z"
        if self.created_at is None:
            self.created_at = now
        if self.updated_at is None:
            self.updated_at = now

    def to_dict(self) -> dict:
        """Convert job to dictionary"""
        return asdict(self)

    def to_json(self) -> str:
        """Convert job to JSON string"""
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: dict) -> "Job":
        """Create job from dictionary"""
        # Ensure missing optional fields are present with defaults
        defaults = {
            "state": "pending",
            "attempts": 0,
            "max_retries": 3,
            "created_at": None,
            "updated_at": None,
            "error_message": None,
            "next_retry_at": None,
        }
        merged = {**defaults, **data}
        return cls(**merged)

    @classmethod
    def from_json(cls, json_str: str) -> "Job":
        """Create job from JSON string"""
        return cls.from_dict(json.loads(json_str))

    def update_state(self, new_state: str, error_message: Optional[str] = None):
        """Update job state and timestamp"""
        self.state = new_state
        self.updated_at = datetime.utcnow().isoformat() + "Z"
        if error_message:
            self.error_message = error_message

    def increment_attempts(self):
        """Increment attempt counter"""
        self.attempts += 1
        self.updated_at = datetime.utcnow().isoformat() + "Z"

    def should_retry(self) -> bool:
        """Check if job should be retried"""
        return self.attempts < self.max_retries

    def calculate_retry_delay(self, backoff_base: int = 2) -> int:
        """Calculate exponential backoff delay in seconds"""
        return backoff_base ** self.attempts

    def set_next_retry(self, backoff_base: int = 2):
        """Set next retry timestamp"""
        delay = self.calculate_retry_delay(backoff_base)
        next_time = datetime.utcnow().timestamp() + delay
        self.next_retry_at = datetime.fromtimestamp(next_time).isoformat() + "Z"