from contextlib import ExitStack, closing, contextmanager


@contextmanager
def closing_all(*things, reverse: bool = False):
    """Enter several contexts using contextlib.closing."""
    stack = ExitStack()
    for thing in things:
        stack.enter_context(closing(thing))
    try:
        yield
    finally:
        stack.close()
