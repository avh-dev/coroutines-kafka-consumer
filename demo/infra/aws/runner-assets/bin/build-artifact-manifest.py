#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a manifest for an AWS experiment result directory.")
    parser.add_argument("root", type=Path)
    parser.add_argument("--run-id")
    args = parser.parse_args()

    root = args.root.resolve()
    files = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if (
            not path.is_file()
            or path.name in {"artifact-manifest.json", "COMPLETE"}
            or relative.parts[0] == ".restore"
        ):
            continue
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        files.append({
            "path": relative.as_posix(),
            "size": path.stat().st_size,
            "sha256": digest.hexdigest(),
        })

    manifest = {
        "schema_version": 1,
        "run_id": args.run_id or root.name,
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "files": files,
    }
    (root / "artifact-manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
