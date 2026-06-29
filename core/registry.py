"""Plugin registry. Entity modules call orchestrator.add_phase() to register."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import duckdb

from core.credentials import Credentials
from core.sync_run import PhaseResult


@dataclass
class RunContext:
    run_id: str
    db: duckdb.DuckDBPyConnection
    credentials: Credentials
    # logger is injected by the orchestrator using ingest_name as the child logger name


PhaseFn = Callable[[RunContext], PhaseResult]


@dataclass
class Registration:
    phase_name: str
    ingest_name: str
    fn: PhaseFn


@dataclass
class Registry:
    items: list[Registration] = field(default_factory=list)

    def add_phase(self, phase_name: str, ingest_name: str, fn: PhaseFn) -> None:
        self.items.append(Registration(phase_name, ingest_name, fn))

    def by_phase(self, phase_name: str) -> list[Registration]:
        return [r for r in self.items if r.phase_name == phase_name]

# gate-coverage probe 20260629T002119Z (throwaway; revert)
