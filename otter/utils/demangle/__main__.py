import sys
from .demangle import demangle


mangled = sys.argv[1:]
max_len = max(map(len, mangled))
demangled = demangle(mangled)
for m, d in dict(zip(mangled, demangled)).items():
    print(f"{m.ljust(max_len)} {d}")
