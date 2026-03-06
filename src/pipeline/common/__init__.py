"""Common infrastructure shared across all pipeline domains.

This package provides domain-agnostic infrastructure including:
- EventHub consumers and producers (via transport factory)
- Logging utilities
- Exception classes and error classification
- Resilience patterns (circuit breaker, retry)

Import classes directly from submodules to avoid loading heavy dependencies:
    from pipeline.common.transport import create_consumer, create_producer
    import logging
    from core.types import ErrorCategory
"""

# Don't import concrete implementations here to avoid loading
# heavy dependencies (aiohttp, etc.) at package import time.
# Users should import directly from submodules.

__all__: list[str] = []
