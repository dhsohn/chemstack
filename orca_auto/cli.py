from __future__ import annotations

import sys
from importlib import import_module


_MODULE = import_module("core.cli")
main = _MODULE.main
sys.modules[__name__] = _MODULE
