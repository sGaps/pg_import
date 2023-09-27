"""
    Creates an unified interface to access to input streams easily.

    Also, implements some utilities to listen at system signals and
    coordinate a possible cancelation of the running process.
"""
import sys
import os
import io
import signal

from contextlib import contextmanager

@contextmanager
def manage_input(path = None):
    """
        Unified interface to access to stream contents.

        if path is not set, returns a stdin stream.
    """
    # NOTE: A file may have an UTF-8 signature and so, causing
    #       some issues when reading the file with the default
    #       python's decoder. So, we must ensure that the new
    #       input stream will be able to ignore the BOM at
    #       the begining of its bytes.
    if path and os.path.exists(path) and os.path.isfile(path):
        with open(path, 'r', encoding = 'utf-8-sig') as file:
            yield file
    else:
        istream = io.TextIOWrapper(sys.stdin.buffer, encoding = 'utf-8-sig')
        yield istream



def attach_signals():
    """
        Captures system signals to indicate when the process must be canceled/aborted.
    """
    def signal_handler(sig, frame):
        sig_obj = signal.Signals(sig)
        raise InterruptedError(f'rolling back changes due the presence of the signal: {sig_obj.name} ({sig})')

    for s in (signal.SIGABRT, signal.SIGILL, signal.SIGINT, signal.SIGSEGV, signal.SIGTERM):
        signal.signal(s, signal_handler)