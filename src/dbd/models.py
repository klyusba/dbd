"""Shared data models for manager <-> worker communication."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Optional

JobState = Literal["running", "failed", "done"]


@dataclass
class JobSpec:
    """Description of a dbt run job, sent from client to manager and forwarded to worker."""

    job_id: str
    select: Optional[list[str]] = field(default_factory=list)
    exclude: Optional[list[str]] = field(default_factory=list)
    full_refresh: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobSpec":
        return cls(
            job_id=data["job_id"],
            select=data.get("select"),
            exclude=data.get("exclude"),
            full_refresh=bool(data.get("full_refresh", False)),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_dbt_args(self) -> list[str]:
        args: list[str] = ["run"]
        if self.select:
            args += ["--select", *self.select]
        if self.exclude:
            args += ["--exclude", *self.exclude]
        if self.full_refresh:
            args.append("--full-refresh")
        return args


@dataclass
class JobStatus:
    job_id: str
    state: JobState
    error: str | None = None
    started_at: float | None = None
    finished_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
