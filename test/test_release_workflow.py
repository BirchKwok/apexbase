import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = (ROOT / ".github" / "workflows" / "build_release.yml").read_text(
    encoding="utf-8"
)


def _job_block(name):
    match = re.search(
        rf"(?ms)^  {re.escape(name)}:\n(?P<body>.*?)(?=^  [a-zA-Z0-9_-]+:\n|\Z)",
        WORKFLOW,
    )
    assert match is not None, f"workflow job {name!r} is missing"
    return match.group("body")


def _job_needs(name):
    block = _job_block(name)
    match = re.search(r"(?m)^    needs: \[(?P<jobs>[^\]]+)\]$", block)
    assert match is not None, f"workflow job {name!r} must use an explicit needs list"
    return {job.strip() for job in match.group("jobs").split(",")}


def test_package_publication_waits_for_all_tests_and_artifacts():
    assert _job_needs("publish-packages") == {
        "resolve-release",
        "test",
        "rust-test",
        "build-wheels-linux",
        "build-wheels",
        "build-sdist",
    }


def test_cargo_and_pypi_publish_from_the_same_job_step():
    block = _job_block("publish-packages")
    publish_step = re.search(
        r"(?ms)^    - name: Publish missing packages together\n"
        r"(?P<body>.*?)(?=^    - name: |\Z)",
        block,
    )

    assert publish_step is not None
    assert "cargo publish --no-default-features" in publish_step.group("body")
    assert "python -m twine upload" in publish_step.group("body")


def test_github_release_requires_the_combined_publication_job():
    assert _job_needs("create-release") == {
        "resolve-release",
        "publish-packages",
        "publish-legacy",
    }
    block = _job_block("create-release")
    assert "needs.resolve-release.outputs.package_kind == 'maturin'" in block
    assert "needs.resolve-release.outputs.package_kind == 'legacy'" in block
    assert "needs.publish-packages.result == 'success'" in block
    assert "needs.publish-legacy.result == 'success'" in block
    assert "needs.publish.result" not in block
    assert "needs.publish-crate.result" not in block


def test_publication_confirms_both_registries():
    block = _job_block("publish-packages")

    assert "Confirm both registries contain the version" in block
    assert "https://crates.io/api/v1/crates/apexbase/${VERSION}" in block
    assert "https://pypi.org/pypi/apexbase/${VERSION}/json" in block
