"""
SchemaIQ — Agent Monitor
Tracks query execution metrics, failures, and system health.
"""

import time
import threading
from collections import deque
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class QueryRecord:
    question: str
    sql: Optional[str]
    success: bool
    latency_ms: float
    error: Optional[str]
    timestamp: float  # unix epoch


class AgentMonitor:
    """In-memory monitoring agent that tracks query metrics."""

    def __init__(self, max_history: int = 50):
        self._lock = threading.Lock()
        self._history: deque[QueryRecord] = deque(maxlen=max_history)
        self._total = 0
        self._success = 0
        self._fail = 0
        self._total_latency = 0.0
        self._started_at = time.time()

    def record_query(
        self,
        question: str,
        sql: Optional[str],
        success: bool,
        latency_ms: float,
        error: Optional[str] = None,
    ):
        rec = QueryRecord(
            question=question,
            sql=sql,
            success=success,
            latency_ms=round(latency_ms, 1),
            error=error,
            timestamp=time.time(),
        )
        with self._lock:
            self._history.append(rec)
            self._total += 1
            self._total_latency += latency_ms
            if success:
                self._success += 1
            else:
                self._fail += 1

    def get_stats(self) -> dict:
        with self._lock:
            avg = (self._total_latency / self._total) if self._total else 0
            recent = [asdict(r) for r in reversed(self._history)]
            return {
                "total_queries": self._total,
                "success_count": self._success,
                "fail_count": self._fail,
                "success_rate": round(self._success / self._total * 100, 1) if self._total else 0,
                "avg_latency_ms": round(avg, 1),
                "uptime_seconds": round(time.time() - self._started_at),
                "recent_queries": recent[:20],
            }


# Singleton instance
monitor = AgentMonitor()
