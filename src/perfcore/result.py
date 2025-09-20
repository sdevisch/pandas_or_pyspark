from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, List
import json
import platform


@dataclass
class Result:
    timestamp: str
    frontend: str
    backend: str
    operation: str
    dataset_rows: Optional[int]
    input_rows: Optional[int]
    read_seconds: Optional[float]
    compute_seconds: Optional[float]
    groups: Optional[int]
    used_cores: Optional[int]
    python: str
    platform: str
    ok: bool
    notes: Optional[str] = None

    @staticmethod
    def now(frontend: str, backend: str, operation: str) -> "Result":
        return Result(
            timestamp=datetime.utcnow().isoformat(timespec="seconds") + "Z",
            frontend=frontend,
            backend=backend,
            operation=operation,
            dataset_rows=None,
            input_rows=None,
            read_seconds=None,
            compute_seconds=None,
            groups=None,
            used_cores=None,
            python=platform.python_version(),
            platform=platform.platform(),
            ok=False,
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @staticmethod
    def from_json(line: str) -> "Result":
        obj = json.loads(line)
        return Result(**obj)


