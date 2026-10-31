"""Keep canonical HTTP schemas self-contained when producing a source release."""

import json
import shutil
from pathlib import Path

from setuptools import setup
from setuptools.command.sdist import sdist

SCHEMAS = {"history-m6.schema.json", "raw-catalog-m6.schema.json"}
INSTALL_PATH = "share/maintai-processing/contracts"


class SchemaSdist(sdist):
    def make_release_tree(self, base_dir, files):
        sources = [
            Path(source)
            for destination, paths in self.distribution.data_files or []
            if destination == INSTALL_PATH
            for source in paths
        ]
        if {source.name for source in sources} != SCHEMAS or len(sources) != len(SCHEMAS):
            raise RuntimeError("Processing release requires both canonical M6 schemas")
        # Setuptools may list ../contracts inputs. Never write outside the release tree.
        members = [
            name for name in files if not Path(name).is_absolute() and ".." not in Path(name).parts
        ]
        super().make_release_tree(base_dir, members)
        release = Path(base_dir)
        metadata = release / "pyproject.toml"
        original = metadata.read_text()
        bundled = original
        for source in sources:
            relative = Path("contracts/v1.3/schemas") / source.name
            destination = release / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            bundled = bundled.replace(
                json.dumps(source.as_posix()), json.dumps(relative.as_posix())
            )
        if bundled != original:
            # Release files may be hard links; leave checkout metadata untouched.
            metadata.unlink()
            metadata.write_text(bundled)


setup(cmdclass={"sdist": SchemaSdist})
