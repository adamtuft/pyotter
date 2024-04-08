from contextlib import ExitStack, closing, contextmanager


@contextmanager
def closing_all(*things, reverse: bool = False):
    """Enter several contexts using contextlib.closing."""
    with ExitStack() as stack:
        for thing in things:
            stack.enter_context(closing(thing))
        yield
