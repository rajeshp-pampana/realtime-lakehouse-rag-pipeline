"""Assert a built image contains what it should - and nothing it shouldn't.

Milestone 5's dependency split is an architectural boundary, not just a size
optimisation: the UI must be unable to read Delta or run inference, and the API
must carry no JVM. Those claims were verified once by hand, by running
containers and inspecting them. This runs the same checks on every push.

Usage:  python scripts/ci_verify_image.py <service> <image-tag>
"""

from __future__ import annotations

import subprocess
import sys

# (must be importable, must NOT be importable)
EXPECTATIONS: dict[str, tuple[list[str], list[str]]] = {
    "api": (
        ["fastapi", "deltalake", "chromadb", "ollama", "prometheus_client"],
        # pyspark/streamlit/yfinance: wrong component. kubernetes/onnxruntime:
        # chromadb transitive deps this project never uses, pruned to save
        # ~190MB (docs/METRICS.md).
        ["pyspark", "streamlit", "yfinance", "kubernetes", "onnxruntime"],
    ),
    "ui": (
        ["streamlit", "plotly", "httpx"],
        ["deltalake", "pyspark", "chromadb", "ollama"],
    ),
    "streaming": (
        ["pyspark", "deltalake", "kafka"],
        ["streamlit", "chromadb", "yfinance"],
    ),
}

PROBE = """
import importlib.util as u, json, sys
must_have = json.loads(sys.argv[1])
must_not = json.loads(sys.argv[2])
missing = [m for m in must_have if u.find_spec(m) is None]
unexpected = [m for m in must_not if u.find_spec(m) is not None]
print(json.dumps({"missing": missing, "unexpected": unexpected}))
"""


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    service, tag = sys.argv[1], sys.argv[2]

    if service not in EXPECTATIONS:
        print(f"no expectations defined for service {service!r}")
        return 2
    must_have, must_not = EXPECTATIONS[service]

    import json

    result = subprocess.run(
        [
            "docker", "run", "--rm", "--entrypoint", "python", tag,
            "-c", PROBE, json.dumps(must_have), json.dumps(must_not),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"probe failed for {tag}:\n{result.stderr}")
        return 1

    report = json.loads(result.stdout.strip().splitlines()[-1])
    print(f"{service}: {report}")

    ok = True
    if report["missing"]:
        print(f"FAIL {service} image is missing required packages: {report['missing']}")
        ok = False
    if report["unexpected"]:
        print(
            f"FAIL {service} image ships packages it must not: {report['unexpected']} "
            f"- this breaks the boundary the image split exists to enforce"
        )
        ok = False

    if service == "streaming" and ok:
        # Without the baked Kafka connector JARs, every cold container start
        # silently re-downloads them from Maven Central at query time.
        jars = subprocess.run(
            ["docker", "run", "--rm", "--entrypoint", "sh", tag,
             "-c", "ls /home/appuser/.ivy2/jars 2>/dev/null | grep -c spark-sql-kafka"],
            capture_output=True, text=True,
        )
        count = jars.stdout.strip() or "0"
        if count == "0":
            print("FAIL streaming image has no baked spark-sql-kafka JARs")
            ok = False
        else:
            print(f"streaming: spark-sql-kafka JARs baked into the image ({count})")

    print("OK" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
