"""Release-readiness guards: the things that break a package at publish time
rather than at test time — exports, version consistency, wheel contents, and
documentation code rot."""

import pathlib
import re
import subprocess
import sys
import tomllib
import zipfile

import pytest

import hotdata_materialized

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


def pyproject():
    return tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())


def test_all_exports_resolve():
    for name in hotdata_materialized.__all__:
        assert getattr(hotdata_materialized, name, None) is not None, name


def test_version_matches_pyproject():
    assert hotdata_materialized.__version__ == pyproject()["project"]["version"]


def test_package_name_matches_directory_convention():
    assert pyproject()["project"]["name"] == "hotdata-materialized"


def test_readme_python_blocks_are_valid_syntax():
    readme = (REPO_ROOT / "README.md").read_text()
    blocks = re.findall(r"```python\n(.*?)```", readme, flags=re.S)
    assert blocks, "README should contain python examples"
    for i, block in enumerate(blocks):
        compile(block, f"README.md:block{i}", "exec")


@pytest.fixture(scope="module")
def wheel(tmp_path_factory):
    outdir = tmp_path_factory.mktemp("dist")
    subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--no-isolation",
         "--outdir", str(outdir), str(REPO_ROOT)],
        check=True,
        capture_output=True,
    )
    (path,) = outdir.glob("*.whl")
    return zipfile.ZipFile(path).namelist()


def test_wheel_contains_the_package_and_type_marker(wheel):
    assert "hotdata_materialized/__init__.py" in wheel
    assert "hotdata_materialized/py.typed" in wheel


def test_wheel_does_not_leak_tests_or_demo(wheel):
    leaked = [
        name for name in wheel
        if name.startswith(("tests/", "demo/")) or "/tests/" in name
    ]
    assert leaked == []
