from __future__ import annotations

from typing import List


def echo_command(parts: List[str]) -> None:
    print("$", " ".join(parts))


def echo_timeout(parts: List[str], timeout_s: float) -> None:
    print(f"[timeout] {' '.join(parts)} after {timeout_s}s")


def note(msg: str) -> None:
    print(msg)


