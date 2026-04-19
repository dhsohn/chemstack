from __future__ import annotations

import sys
from importlib import import_module


_MODULE = import_module("orca_auto.job_locations")
sys.modules[__name__] = _MODULE
