"""Run the existing harness with a process-local eight-logical-CPU affinity mask."""
import ctypes
from ctypes import wintypes
import json
from pathlib import Path
import runpy
import subprocess
import sys

kernel = ctypes.WinDLL('kernel32', use_last_error=True)
kernel.GetCurrentProcess.restype = wintypes.HANDLE
kernel.SetProcessAffinityMask.argtypes = (wintypes.HANDLE, ctypes.c_size_t)
kernel.SetProcessAffinityMask.restype = wintypes.BOOL
kernel.GetProcessAffinityMask.argtypes = (wintypes.HANDLE, ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_size_t))
kernel.GetProcessAffinityMask.restype = wintypes.BOOL
process = kernel.GetCurrentProcess()
if not kernel.SetProcessAffinityMask(process, 0xff):
    raise ctypes.WinError(ctypes.get_last_error())
current, system = ctypes.c_size_t(), ctypes.c_size_t()
if not kernel.GetProcessAffinityMask(process, ctypes.byref(current), ctypes.byref(system)):
    raise ctypes.WinError(ctypes.get_last_error())
assert current.value == 0xff
print(json.dumps({'native_windows_affinity_mask': hex(current.value),
                  'node_available_parallelism': subprocess.check_output(
                      ['node', '-p', 'require("node:os").availableParallelism()'], text=True).strip()}), flush=True)
script = Path(__file__).with_name('benchmark_feed.py')
sys.argv[0] = str(script)
runpy.run_path(str(script), run_name='__main__')
