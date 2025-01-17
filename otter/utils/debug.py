import inspect
import sys
import pdb
from contextlib import contextmanager, nullcontext


def post_mortem(catch: bool):
    return post_mortem_context() if catch else nullcontext()


@contextmanager
def post_mortem_context():
    try:
        yield
    except Exception:
        exc_type, exc, tb = sys.exc_info()
        print(exc)
        print(f"caught {exc_type}, entering post-mortem")
        pdb.post_mortem(tb)


# Credit: https://stackoverflow.com/a/72782654
class LineNo:
    def __str__(self):
        frame = inspect.currentframe()
        if frame is not None and frame.f_back is not None:
            return str(frame.f_back.f_lineno)
        else:
            return "000"


__line__ = LineNo()
