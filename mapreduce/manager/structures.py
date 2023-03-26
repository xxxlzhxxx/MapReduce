"""Structures used by the manager."""
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional


class WorkerStatus(Enum):
    """Worker status."""

    READY = 1
    BUSY = 2
    DEAD = 3


@dataclass
class PartitionInfo:
    """Partition class."""

    task_id: int  # task id
    files: List[str]  # list of files in the partition


@dataclass
class WorkerInfo:
    """Worker class."""

    host: Any  # socket host
    port: Any  # socket port
    status: WorkerStatus  # worker status
    last_heartbeat: float  # time.time() when last heartbeat was received
    job_id: Optional[
        int
    ] = None  # the job id that the worker is currently working on
    task: Optional[
        PartitionInfo
    ] = None  # the partition worker is currently working on
