import sys
import os
import io
import signal

from contextlib import contextmanager

@contextmanager
def manage_input(path = None):
    # TODO: Rewrite this note
    # NOTE: We may encounter files that have an UTF-8 signature
    #       which cannot be treated as the content file. So, we
    #       must specify that we will ignore the BOM by passing
    #       the following encoding. This also applies if we
    #       pass a file through stdin.
    if path and os.path.exists(path) and os.path.isfile(path):
        with open(path, 'r', encoding = 'utf-8-sig') as file:
            yield file
    else:
        istream = io.TextIOWrapper(sys.stdin.buffer, encoding = 'utf-8-sig')
        yield istream



def attach_signals():
    def signal_handler(sig, frame):
        sig_obj = signal.Signals(sig)
        raise InterruptedError(f'rolling back changes due the presence of the signal: {sig_obj.name} ({sig})')

    for s in (signal.SIGABRT, signal.SIGILL, signal.SIGINT, signal.SIGSEGV, signal.SIGTERM):
        signal.signal(s, signal_handler)