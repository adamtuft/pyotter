from types import MappingProxyType
from importlib.resources import read_text, files
from os.path import splitext

from . import _scripts


# Dynamically loads SQL scripts from _scripts dir so no need to change file
# when SQL scripts changed
scripts = MappingProxyType(
    {
        splitext(f.name)[0]: read_text(_scripts, f.name)
        for f in files(_scripts).iterdir()
        if f.is_file() and f.name.endswith(".sql")
    }
)
