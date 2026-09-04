"""Tests for the Milestone 5 container definitions.

Splitting dependencies per service (infra/requirements-*.txt) makes the images
much smaller, but it introduces a way for them to drift from the top-level
requirements.txt that CI and the dev venv actually install and test against.
That drift would be silent and would only show up as a container that behaves
differently from CI, so it gets a test rather than a convention.

These are all static checks - no Docker daemon needed, so they run in CI.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
INFRA = REPO_ROOT / "infra"
SERVICE_REQUIREMENTS = [
    INFRA / "requirements-api.txt",
    INFRA / "requirements-ui.txt",
    INFRA / "requirements-streaming.txt",
]


def _parse(path: Path) -> dict[str, str]:
    """Return ``{package_name: version_specifier}`` from a requirements file."""
    requirements: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # Split "uvicorn[standard]>=0.30,<0.40" into name and specifier.
        match = re.match(r"^([A-Za-z0-9._-]+)(\[[^\]]+\])?\s*(.*)$", line)
        assert match, f"could not parse requirement {line!r} in {path.name}"
        name, _extras, specifier = match.groups()
        requirements[name.lower().replace("_", "-")] = specifier.strip()
    return requirements


@pytest.mark.parametrize("service_file", SERVICE_REQUIREMENTS, ids=lambda p: p.name)
def test_service_requirements_are_a_subset_of_the_root(service_file: Path):
    """Every per-service package must exist in requirements.txt.

    requirements.txt is the source of truth for what this project runs on; the
    service files only choose which of those packages each image needs. A
    package here that isn't there would be untested by CI.
    """
    root = _parse(REPO_ROOT / "requirements.txt")
    service = _parse(service_file)

    missing = sorted(set(service) - set(root))
    assert not missing, (
        f"{service_file.name} lists packages absent from requirements.txt "
        f"(so CI never installs or tests them): {missing}"
    )


@pytest.mark.parametrize("service_file", SERVICE_REQUIREMENTS, ids=lambda p: p.name)
def test_service_requirement_versions_match_the_root(service_file: Path):
    """Pins must be identical, or an image runs a version CI never tested."""
    root = _parse(REPO_ROOT / "requirements.txt")
    service = _parse(service_file)

    mismatched = {
        name: (spec, root[name])
        for name, spec in service.items()
        if name in root and spec != root[name]
    }
    assert not mismatched, (
        f"{service_file.name} version specifiers differ from requirements.txt "
        f"{{package: (service, root)}}: {mismatched}"
    )


def test_ui_image_excludes_data_and_inference_packages():
    """The thin-client claim, enforced at the image level.

    Milestone 4 proved the UI *doesn't* import these; this makes sure its image
    can't, by asserting the packages are simply absent. If the UI ever regressed
    to reading Delta or calling a model directly, its container would fail.
    """
    ui = _parse(INFRA / "requirements-ui.txt")
    for banned in ("deltalake", "pyspark", "chromadb", "ollama", "kafka-python-ng"):
        assert banned not in ui, (
            f"UI image must not ship {banned} - it is a thin HTTP client "
            f"(see README 'Where the UI gets its data')"
        )


def test_api_image_excludes_spark_and_ui_packages():
    """The API reads Delta through delta-rs and serves no UI - no JVM, no Streamlit."""
    api = _parse(INFRA / "requirements-api.txt")
    for banned in ("pyspark", "streamlit", "plotly", "yfinance"):
        assert banned not in api, f"API image must not ship {banned}"


def test_every_service_requirements_file_is_used_by_a_dockerfile():
    """A requirements file no Dockerfile installs is dead weight that will rot."""
    dockerfiles = " ".join(
        p.read_text(encoding="utf-8") for p in INFRA.glob("Dockerfile.*")
    )
    for service_file in SERVICE_REQUIREMENTS:
        assert service_file.name in dockerfiles, (
            f"{service_file.name} is not installed by any Dockerfile"
        )


def test_base_images_are_pinned_to_a_fixed_distro():
    """Floating base tags broke this build once; don't let them do it again.

    `python:3.12-slim` moved from Debian 12 to Debian 13 upstream, which
    dropped openjdk-17 and failed the streaming build outright. Spark 3.5.x
    supports Java 8/11/17 and CI tests on Temurin 17, so the fix was to pin the
    OS rather than jump to trixie's Java 21 and leave the supported matrix.
    """
    for name in ("Dockerfile.api", "Dockerfile.ui", "Dockerfile.streaming"):
        body = (INFRA / name).read_text(encoding="utf-8")
        from_lines = [
            line.strip()
            for line in body.splitlines()
            if line.strip().upper().startswith("FROM ")
        ]
        assert from_lines, f"{name} has no FROM line"
        for line in from_lines:
            image = line.split()[1]
            assert image != "python:3.12-slim", (
                f"{name} uses the floating tag {image}; pin the distro "
                f"(e.g. python:3.12-slim-bookworm)"
            )


def test_streaming_modules_do_not_import_ingestion():
    """The streaming image ships no yfinance, so importing ingestion crashes it.

    tick_producer imported TICKERS from fetch_market_data, which imports
    yfinance at module level. In the dev venv that is invisible (everything is
    installed); in the streaming container it was a crash loop:
    ModuleNotFoundError: No module named 'yfinance'. The producer invents ticks
    and never calls Yahoo Finance, so it has no business importing that module.
    """
    import ast

    for module in ("tick_producer.py", "stream_consumer.py"):
        path = REPO_ROOT / "src" / "streaming" / module
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported = [
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        ] + [
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        ]
        offenders = [
            name
            for name in imported
            if name.startswith("src.ingestion") or name == "yfinance"
        ]
        assert not offenders, (
            f"{module} imports {offenders}, which pulls yfinance into the "
            f"streaming image (it isn't installed there)"
        )


def test_spark_checkpoints_are_not_on_the_host_bind_mount():
    """Spark chmods its checkpoint dir; that fails on a Windows bind mount.

    Symptom when this regresses: the streaming query dies at startup with
    "chmod: changing permissions of '...': Operation not permitted", which
    reads like a Spark bug rather than a mount problem.
    """
    streaming = _compose()["x-streaming"]
    checkpoint = streaming["environment"]["STREAM_CHECKPOINT_DIR"]
    assert not checkpoint.startswith("/app/data"), (
        f"checkpoints at {checkpoint} sit under the bind-mounted ../data; "
        f"use the stream-checkpoints named volume instead"
    )

    mounts = streaming["volumes"]
    assert any("stream-checkpoints:" in m for m in mounts), (
        "streaming services must mount the stream-checkpoints named volume"
    )
    assert any(checkpoint.startswith(m.split(":")[1]) for m in mounts if ":" in m), (
        f"{checkpoint} is not inside any mounted path"
    )


def test_baked_kafka_connector_matches_the_source():
    """The image pre-resolves the Spark Kafka JARs; the coordinate must match.

    If the Dockerfile bakes one version and stream_consumer.py asks for
    another, the build still succeeds and the container still starts - it just
    silently falls back to downloading the other version from Maven at query
    start, quietly reintroducing the runtime internet dependency this was meant
    to remove. That is exactly the kind of drift a test should catch.
    """
    source = (REPO_ROOT / "src" / "streaming" / "stream_consumer.py").read_text(
        encoding="utf-8"
    )
    source_match = re.search(r'_KAFKA_PACKAGE\s*=\s*"([^"]+)"', source)
    assert source_match, "could not find _KAFKA_PACKAGE in stream_consumer.py"

    dockerfile = (INFRA / "Dockerfile.streaming").read_text(encoding="utf-8")
    docker_match = re.search(r"ARG KAFKA_PACKAGE=(\S+)", dockerfile)
    assert docker_match, "Dockerfile.streaming should declare ARG KAFKA_PACKAGE"

    assert source_match.group(1) == docker_match.group(1), (
        f"Kafka connector mismatch: source wants {source_match.group(1)}, "
        f"image bakes {docker_match.group(1)}"
    )


def _compose() -> dict:
    import yaml

    return yaml.safe_load((INFRA / "docker-compose.yml").read_text(encoding="utf-8"))


def test_long_running_services_are_not_bounded_runs():
    """A restarting service must not be a bounded run.

    tick_producer/stream_consumer default to finite runs (30s/60s) because
    Milestone 2 only needed verification runs. Under `restart: unless-stopped`
    that is a restart loop wearing a service's clothes, so the compose commands
    must ask for the run-until-stopped mode explicitly.
    """
    services = _compose()["services"]
    expected_flag = {"tick-producer": "--duration", "stream-consumer": "--timeout"}

    for name, flag in expected_flag.items():
        command = services[name]["command"]
        assert flag in command, f"{name} must pass {flag} explicitly"
        value = command[command.index(flag) + 1]
        assert float(value) <= 0, (
            f"{name} runs with {flag}={value}, a bounded run - under "
            f"restart: unless-stopped that restart-loops instead of serving"
        )


def test_ui_points_at_the_api_service_not_localhost():
    """In compose the UI must dial the service name; localhost is its own container."""
    ui_env = _compose()["services"]["ui"]["environment"]
    assert ui_env["API_BASE_URL"] == "http://api:8000"


def test_default_profile_excludes_the_memory_hungry_services():
    """`docker compose up` must stay within an 8GB machine's headroom."""
    services = _compose()["services"]
    for heavy in ("tick-producer", "stream-consumer", "airflow"):
        assert services[heavy].get("profiles"), (
            f"{heavy} must be behind a profile so it doesn't start by default"
        )
    for default in ("api", "ui", "kafka"):
        assert not services[default].get("profiles"), (
            f"{default} is part of the default stack and should have no profile"
        )


def test_dockerfiles_do_not_install_the_full_requirements():
    """Guard against a Dockerfile quietly reverting to the everything-install.

    Only instruction lines are checked - the comments in these files discuss
    requirements.txt by name on purpose, and flagging that prose would make the
    test fail for explaining itself.
    """
    for name in ("Dockerfile.api", "Dockerfile.ui", "Dockerfile.streaming"):
        body = (INFRA / name).read_text(encoding="utf-8")
        instructions = [
            line
            for line in body.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        offenders = [
            line
            for line in instructions
            if re.search(r"(?<!-)\brequirements\.txt\b", line)
        ]
        assert not offenders, (
            f"{name} should install its own infra/requirements-*.txt, not the "
            f"full dev/CI requirements.txt: {offenders}"
        )


# --- Kubernetes manifests and Helm chart (Milestone 5, k8s half) -------------------
#
# Static checks only, so they run in CI with no cluster. Each guards a failure
# mode that is silent or reads as something unrelated when it happens.

K8S = INFRA / "k8s"
CHART = K8S / "helm" / "rlrp"


def _yaml_docs(path: Path) -> list[dict]:
    import yaml

    return [d for d in yaml.safe_load_all(path.read_text(encoding="utf-8")) if d]


def _by_kind(docs: list[dict], kind: str) -> list[dict]:
    return [d for d in docs if d.get("kind") == kind]


def test_nodeports_match_the_kind_port_mappings():
    """A NodePort with no matching extraPortMapping is unreachable from the host.

    Nothing errors: the Service exists, the pod is healthy, and curl just hangs
    on a closed port. Easy to misread as an app problem.
    """
    kind_cfg = _yaml_docs(K8S / "kind-cluster.yaml")[0]
    mapped = {
        m["containerPort"]
        for node in kind_cfg["nodes"]
        for m in node.get("extraPortMappings", [])
    }

    services = _by_kind(_yaml_docs(K8S / "service.yaml"), "Service")
    node_ports = {
        port["nodePort"]
        for svc in services
        if svc["spec"].get("type") == "NodePort"
        for port in svc["spec"]["ports"]
        if "nodePort" in port
    }
    assert node_ports, "expected at least one NodePort service"
    unmapped = node_ports - mapped
    assert not unmapped, (
        f"NodePort(s) {unmapped} have no extraPortMapping in kind-cluster.yaml, "
        f"so they are unreachable from the host (mapped: {mapped})"
    )


def test_images_are_never_pulled_from_a_registry():
    """These images exist only locally, loaded with `kind load docker-image`.

    Any pull policy that reaches out lands in ImagePullBackOff against a
    registry that has never heard of rlrp-api.
    """
    deployments = _by_kind(_yaml_docs(K8S / "deployment.yaml"), "Deployment")
    assert deployments, "expected Deployments in deployment.yaml"
    for dep in deployments:
        for container in dep["spec"]["template"]["spec"]["containers"]:
            assert container["imagePullPolicy"] == "IfNotPresent", (
                f"{dep['metadata']['name']} uses imagePullPolicy="
                f"{container['imagePullPolicy']}; local images require IfNotPresent"
            )

    import yaml

    values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))
    assert values["image"]["pullPolicy"] == "IfNotPresent"


def test_kind_node_image_is_pinned_by_digest():
    """kind's default node image changes with every kind release.

    Unpinned, upgrading the kind binary silently changes the Kubernetes version
    the cluster runs - the same drift that moved python:3.12-slim to Debian 13
    and broke the streaming build.
    """
    kind_cfg = _yaml_docs(K8S / "kind-cluster.yaml")[0]
    for node in kind_cfg["nodes"]:
        image = node.get("image", "")
        assert "@sha256:" in image, (
            f"kind node image {image!r} is not pinned by digest"
        )


def test_ui_gets_no_lakehouse_volume():
    """The thin-client boundary, enforced at the k8s layer too.

    The UI reaches data over HTTP and its image ships no deltalake/pyspark, so
    mounting the lakehouse into it would be meaningless at best and a false
    signal about the architecture at worst.
    """
    deployments = {
        d["metadata"]["name"]: d for d in _by_kind(_yaml_docs(K8S / "deployment.yaml"), "Deployment")
    }
    ui = deployments["rlrp-ui"]["spec"]["template"]["spec"]
    assert not ui.get("volumes"), "the UI Deployment should mount no volumes"

    api = deployments["rlrp-api"]["spec"]["template"]["spec"]
    assert any(v["name"] == "lakehouse" for v in api.get("volumes", [])), (
        "the API Deployment must mount the lakehouse"
    )


def test_pods_run_as_the_uid_the_images_create():
    """Images create uid 10001 and the lakehouse is read from a hostPath.

    Running as any other uid produces permission errors on Delta reads that
    look like corrupt-table problems rather than a securityContext mismatch.
    """
    import yaml

    for dep in _by_kind(_yaml_docs(K8S / "deployment.yaml"), "Deployment"):
        sc = dep["spec"]["template"]["spec"].get("securityContext", {})
        assert sc.get("runAsUser") == 10001, (
            f"{dep['metadata']['name']} must run as uid 10001 (the images' user)"
        )

    values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))
    assert values["securityContext"]["runAsUser"] == 10001


def test_chart_and_raw_manifests_agree_on_images_and_ports():
    """Two deployment paths must not drift apart.

    The plain manifests and the chart are both maintained; if they disagree,
    `kubectl apply` and `helm install` produce different clusters and the
    difference only shows up at runtime.
    """
    import yaml

    values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))
    raw_deployments = {
        d["metadata"]["name"]: d for d in _by_kind(_yaml_docs(K8S / "deployment.yaml"), "Deployment")
    }

    for component in ("api", "ui"):
        chart_image = (
            f"{values[component]['image']['repository']}:{values[component]['image']['tag']}"
        )
        raw_image = raw_deployments[f"rlrp-{component}"]["spec"]["template"]["spec"][
            "containers"
        ][0]["image"]
        assert chart_image == raw_image, (
            f"{component}: chart renders {chart_image}, raw manifest uses {raw_image}"
        )

        chart_port = values[component]["service"]["port"]
        raw_port = raw_deployments[f"rlrp-{component}"]["spec"]["template"]["spec"][
            "containers"
        ][0]["ports"][0]["containerPort"]
        assert chart_port == raw_port, (
            f"{component}: chart port {chart_port} != manifest containerPort {raw_port}"
        )


def test_chart_templates_namespace_from_the_release():
    """Templating a namespace value separately lets it disagree with `helm -n`."""
    for template in (CHART / "templates").glob("*.yaml"):
        body = template.read_text(encoding="utf-8")
        assert ".Values.namespace" not in body, (
            f"{template.name} templates .Values.namespace; use .Release.Namespace "
            f"so the chart cannot disagree with the -n flag"
        )
