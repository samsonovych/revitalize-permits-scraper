"""Auto-discovery registry for post-processors.

The registry scans ``permits_post_processing/processors/<STATE>/<CITY>/post_processor.py``
modules and registers classes that subclass :class:`BasePostProcessor`.
"""

from __future__ import annotations

import importlib
import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Type

from .base import BasePostProcessor


@dataclass(frozen=True)
class ProcessorKey:
    state: str
    city: str

    def display(self) -> str:
        return f"{self.state.upper()} / {self.city.replace('_', ' ').title()}"


class ProcessorRegistry:
    """Registry that discovers and provides post-processor classes."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._entries: Dict[ProcessorKey, Type[BasePostProcessor]] = {}

    def discover(self) -> None:
        base_dir = self._root / "processors"
        if not base_dir.exists():
            return
        # Expect layout processors/<STATE>/<CITY>/post_processor.py
        for state_dir in base_dir.iterdir():
            if not state_dir.is_dir():
                continue
            for city_dir in state_dir.iterdir():
                if not city_dir.is_dir():
                    continue
                # Support both legacy name and alternative filename
                module_path = city_dir / "post_processor.py"
                if not module_path.exists():
                    alt = city_dir / "processor.py"
                    if alt.exists():
                        module_path = alt
                    else:
                        continue
                key = ProcessorKey(state=state_dir.name, city=city_dir.name)
                try:
                    module = self._import_module_from_path(module_path)
                    cls = self._first_processor_class(module)
                    if cls is not None:
                        self._entries[key] = cls
                except Exception as e:
                    print(e)
                    continue

    def _import_module_from_path(self, file_path: Path):
        # Convert path to a module import path relative to package root
        # Example: permits_post_processing/processors/tx/austin/post_processor.py ->
        #          permits_post_processing.processors.tx.austin.post_processor
        rel = file_path.relative_to(self._root.parent)
        module_name = ".".join(rel.with_suffix("").parts)
        return importlib.import_module(module_name)

    def _first_processor_class(self, module) -> Optional[Type[BasePostProcessor]]:
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, BasePostProcessor) and obj is not BasePostProcessor:
                return obj
        return None

    def list(self) -> List[Tuple[ProcessorKey, Type[BasePostProcessor]]]:
        return sorted(self._entries.items(), key=lambda kv: (kv[0].state, kv[0].city))

    def get(self, state: str, city: str) -> Optional[Type[BasePostProcessor]]:
        return self._entries.get(ProcessorKey(state=state, city=city))


