"""Reject locale-encoded source and release metadata before publication."""
import pathlib
import subprocess

paths = subprocess.check_output(["git", "ls-files", "-z"]).decode("utf-8").split("\0")
for name in filter(None, paths):
    path = pathlib.Path(name)
    if path.suffix in {".rs", ".md", ".toml", ".yml", ".yaml", ".py", ".ps1"}:
        path.read_text(encoding="utf-8")
print("Tracked source and release text is valid UTF-8")
