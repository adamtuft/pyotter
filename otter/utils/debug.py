import inspect
import sys
import pdb
from contextlib import contextmanager


@contextmanager
def post_mortem(catch: bool):
    if catch:
        try:
            yield
        except Exception:
            exc_type, exc, tb = sys.exc_info()
            print(exc)
            print(f"caught {exc_type}, entering post-mortem")
            pdb.post_mortem(tb)
    else:
        yield


# Credit: https://stackoverflow.com/a/72782654
class LineNo:
    def __str__(self):
        frame = inspect.currentframe()
        if frame is not None and frame.f_back is not None:
            return str(frame.f_back.f_lineno)
        else:
            return "000"


__line__ = LineNo()
