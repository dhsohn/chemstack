from __future__ import annotations

from importlib import metadata


def package_version() -> str:
    try:
        return metadata.version("chemstack")
    except metadata.PackageNotFoundError:
        return "0.0.0+unknown"


__version__ = package_version()

__all__ = ["__version__", "package_version"]
