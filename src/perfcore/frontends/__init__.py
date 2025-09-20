from __future__ import annotations

# Registry of available front-ends explored in this repo. Front-ends represent
# user-facing APIs that can drive different computational backends.
# - pandas: native pandas API
# - pyspark: pandas API on Spark
# - narwhals: thin compatibility layer (experimental)

AVAILABLE_FRONTENDS = ["pandas", "pyspark", "narwhals"]
