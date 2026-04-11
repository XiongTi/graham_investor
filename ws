#!/usr/bin/env python3
from __future__ import annotations

import runpy
import sys
from pathlib import Path

# Make the package importable when this script is executed from the repo root.
PACKAGE_PARENT = Path(__file__).resolve().parent.parent
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

runpy.run_module("wealthkeeper", run_name="__main__")
