# Have this as a separate submodule to avoid RuntimeWarnings due to other parts importing "demangle"
# when running directly with "python3 -m otter.utils.demangle"

from .demangle import demangle
