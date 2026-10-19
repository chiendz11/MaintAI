"""Bundle canonical repository contracts into wheels and source distributions."""

from pathlib import Path
from shutil import copytree

from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.command.sdist import sdist

PROJECT = Path(__file__).resolve().parent
CONTRACTS = PROJECT / "contracts"
if not CONTRACTS.is_dir():
    CONTRACTS = PROJECT.parent / "contracts"


def copy_contracts(destination):
    destination = Path(destination)
    copytree(CONTRACTS / "kafka", destination / "kafka", dirs_exist_ok=True)
    for relative in ["v1.3/schemas/runtime.schema.json", "v1.3/events/alert-signal.schema.json"]:
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((CONTRACTS / relative).read_bytes())


class BuildPy(build_py):
    def run(self):
        super().run()
        copy_contracts(Path(self.build_lib) / "maintai_messaging/contracts")


class SourceDistribution(sdist):
    def make_release_tree(self, base_dir, files):
        super().make_release_tree(base_dir, files)
        copy_contracts(Path(base_dir) / "contracts")


setup(cmdclass={"build_py": BuildPy, "sdist": SourceDistribution})
