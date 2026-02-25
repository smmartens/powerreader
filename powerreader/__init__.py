from importlib.metadata import PackageNotFoundError, version
from pathlib import Path


def _read_pyproject_version() -> str:
    """Read version from pyproject.toml as fallback when package is not installed."""
    try:
        pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
        for line in pyproject.read_text().splitlines():
            if line.startswith("version"):
                return line.split("=", 1)[1].strip().strip('"')
    except OSError:
        pass
    return "0.0.0-dev"


# importlib.metadata only works when the package is installed.
# In Docker the app runs with `uv run --no-sync`, which skips installation,
# so the package metadata is absent and we fall back to reading pyproject.toml.
try:
    __version__ = version("powerreader")
except PackageNotFoundError:
    __version__ = _read_pyproject_version()
