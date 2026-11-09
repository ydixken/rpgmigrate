"""Compile required money locales privately before the owned PostgreSQL server starts."""

from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

I18N_ROOT = Path("/usr/share/i18n")
LOCALES = ("de_DE", "en_US")


@contextmanager
def money_locales(enabled):
    if not enabled:
        yield None
        return
    compiler = shutil.which("localedef")
    sources = [I18N_ROOT / "locales" / name for name in LOCALES]
    sources.append(I18N_ROOT / "charmaps/UTF-8.gz")
    if not compiler or not all(path.is_file() for path in sources):
        raise RuntimeError("money fixture requires installed GNU localedef, de_DE/en_US locale sources, and UTF-8 charmap")
    with tempfile.TemporaryDirectory(prefix="rpgmigrate-money-locales-") as temporary:
        root = Path(temporary)
        root.chmod(0o700)
        for name in LOCALES:
            output = root / (name + ".UTF-8")
            try:
                result = subprocess.run([compiler, "--no-archive", "-i", name, "-f", "UTF-8", str(output)],
                                        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=30)
            except (OSError, subprocess.TimeoutExpired):
                raise RuntimeError("money fixture could not run its installed locale compiler") from None
            if result.returncode or not (output / "LC_MONETARY").is_file():
                raise RuntimeError("money fixture could not compile its required private monetary locales")
        previous = os.environ.get("LOCPATH")
        os.environ["LOCPATH"] = str(root)
        try:
            yield root
        finally:
            if previous is None:
                os.environ.pop("LOCPATH", None)
            else:
                os.environ["LOCPATH"] = previous
