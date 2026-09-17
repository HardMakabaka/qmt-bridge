"""Thread-local deadline shared by bridge queueing and native RPC calls."""

from contextlib import contextmanager
import threading


_state = threading.local()


def current_deadline():
    return getattr(_state, "deadline", None)


@contextmanager
def request_budget(deadline):
    previous = current_deadline()
    _state.deadline = min(previous, deadline) if previous is not None else deadline
    try:
        yield _state.deadline
    finally:
        _state.deadline = previous
