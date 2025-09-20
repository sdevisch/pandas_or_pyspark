"""Pytest configuration for test session.

Sets environment flags early to avoid noisy runtime warnings from
third-party libraries (e.g., pandas-on-Spark) and to ensure
consistent behavior across test runs.
"""

from __future__ import annotations

import os
import warnings


# Ensure pandas-on-Spark does not emit timezone-related UserWarning on import.
# This must be set before Spark/PyArrow are initialized.
os.environ.setdefault("PYARROW_IGNORE_TIMEZONE", "1")

# Be defensive: silence noisy third-party warnings that do not indicate test issues.
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    module=r"^pyspark\.pandas(\..*)?$",
)
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message=r"'PYARROW_IGNORE_TIMEZONE' environment variable was not set",
)


