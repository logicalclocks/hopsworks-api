# ruff: noqa: INP001
"""Put three.js 0.186.1, which the game renders with, under static/vendor/three.

Run once in the Terminal from the app directory, before the app is uploaded:

    python fetch_three.py

The app serves three.js itself, as it serves everything the page loads, so the page loads
no script from a CDN. The npm tarball is checked against the integrity hash npm publishes
for it before anything is extracted. three.js is MIT licensed; its LICENSE goes alongside.
"""

from __future__ import annotations

import base64
import hashlib
import io
import tarfile
import urllib.request
from pathlib import Path


VERSION = "0.186.1"
TARBALL = f"https://registry.npmjs.org/three/-/three-{VERSION}.tgz"
INTEGRITY = "sha512-blFeqb49wRCSGUGj7gtpfnSGHy2lwDk94RhUmS1c/hTby70kvChbWpkJ4Pm1390LqzzvTmzgXKHPEafJwCb8jA=="
# three.module.js imports three.core.js beside it; the page imports nothing else.
FILES = {
    "package/build/three.module.js",
    "package/build/three.core.js",
    "package/LICENSE",
}
TARGET = Path(__file__).resolve().parent / "static" / "vendor" / "three"


def main() -> int:
    with urllib.request.urlopen(TARBALL, timeout=60) as response:  # noqa: S310 - a fixed https URL
        data = response.read()
    digest = "sha512-" + base64.b64encode(hashlib.sha512(data).digest()).decode()
    if digest != INTEGRITY:
        raise SystemExit(f"{TARBALL} does not match its published integrity hash")
    TARGET.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        for member in archive.getmembers():
            if member.name in FILES:
                (TARGET / Path(member.name).name).write_bytes(
                    archive.extractfile(member).read()
                )
    print(f"three.js {VERSION} in {TARGET}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
