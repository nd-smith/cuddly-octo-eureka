"""Base types for Verisk handler side-effects.

The FileHandlerSideEffect dataclass is the only type retained from the
original handler base module. All handler classes, registries, and runners
have been replaced by the unified rule engine (rules.py, rule_runner.py).
"""

from dataclasses import dataclass

from pydantic import BaseModel


@dataclass
class FileHandlerSideEffect:
    """A message a handler wants to produce to a side-effect topic."""

    topic_key: str          # e.g. "reinspections" -> resolved to verisk.reinspections
    message: BaseModel      # Pydantic model to serialize and send
