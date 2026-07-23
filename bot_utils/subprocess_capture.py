"""Bounded stdout/stderr capture for long-running child processes."""
from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from os import PathLike
from typing import Any, Sequence


DEFAULT_CAPTURE_BYTES_PER_STREAM = 64 * 1024
_READ_CHUNK_BYTES = 16 * 1024
_READER_DRAIN_TIMEOUT_SEC = 1.0
_PROCESS_TERMINATION_TIMEOUT_SEC = 10.0
_WINDOWS_GATE_WORKER = (
    "import sys\n"
    "if 'site' in sys.modules:\n"
    "    print('bounded-capture wrapper loaded site', file=sys.stderr)\n"
    "    raise SystemExit(125)\n"
    "import ctypes, subprocess, traceback\n"
    "from ctypes import wintypes\n"
    "kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)\n"
    "kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]\n"
    "kernel32.WaitForSingleObject.restype = wintypes.DWORD\n"
    "kernel32.CloseHandle.argtypes = [wintypes.HANDLE]\n"
    "kernel32.CloseHandle.restype = wintypes.BOOL\n"
    "gate = wintypes.HANDLE(int(sys.argv[1]))\n"
    "wait_result = kernel32.WaitForSingleObject(gate, 0xFFFFFFFF)\n"
    "close_ok = kernel32.CloseHandle(gate)\n"
    "if wait_result != 0 or not close_ok:\n"
    "    print('bounded-capture start gate failed', file=sys.stderr)\n"
    "    raise SystemExit(126)\n"
    "creationflags = int(sys.argv[2])\n"
    "show_window = int(sys.argv[3])\n"
    "startupinfo = None\n"
    "if show_window >= 0:\n"
    "    startupinfo = subprocess.STARTUPINFO()\n"
    "    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW\n"
    "    startupinfo.wShowWindow = show_window\n"
    "try:\n"
    "    with subprocess.Popen(\n"
    "            sys.argv[4:],\n"
    "            creationflags=creationflags,\n"
    "            startupinfo=startupinfo,\n"
    "    ) as child:\n"
    "        returncode = child.wait()\n"
    "except BaseException:\n"
    "    traceback.print_exc()\n"
    "    raise SystemExit(127)\n"
    "raise SystemExit(returncode)\n"
)


class _WindowsStartGate:
    def __init__(self) -> None:
        import ctypes
        from ctypes import wintypes

        class SecurityAttributes(ctypes.Structure):
            _fields_ = [
                ("nLength", wintypes.DWORD),
                ("lpSecurityDescriptor", wintypes.LPVOID),
                ("bInheritHandle", wintypes.BOOL),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateEventW.argtypes = [
            wintypes.LPVOID,
            wintypes.BOOL,
            wintypes.BOOL,
            wintypes.LPCWSTR,
        ]
        kernel32.CreateEventW.restype = wintypes.HANDLE
        kernel32.SetEvent.argtypes = [wintypes.HANDLE]
        kernel32.SetEvent.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL

        attributes = SecurityAttributes()
        attributes.nLength = ctypes.sizeof(attributes)
        attributes.bInheritHandle = True
        handle = kernel32.CreateEventW(
            ctypes.byref(attributes),
            True,
            False,
            None,
        )
        if not handle:
            raise ctypes.WinError(ctypes.get_last_error())
        self._ctypes = ctypes
        self._kernel32 = kernel32
        self._handle = handle

    @property
    def handle_value(self) -> int:
        return int(self._require_handle())

    def signal(self) -> None:
        handle = self._require_handle()
        if not self._kernel32.SetEvent(handle):
            raise self._ctypes.WinError(self._ctypes.get_last_error())

    def disable_inheritance(self) -> None:
        os.set_handle_inheritable(self.handle_value, False)

    def close(self) -> None:
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        if not self._kernel32.CloseHandle(handle):
            raise self._ctypes.WinError(self._ctypes.get_last_error())

    def _require_handle(self):
        if self._handle is None:
            raise RuntimeError("Windows child start gate is already closed")
        return self._handle


class _WindowsKillOnCloseJob:
    _BASIC_ACCOUNTING_INFORMATION = 1
    _EXTENDED_LIMIT_INFORMATION = 9
    _KILL_ON_JOB_CLOSE = 0x00002000

    def __init__(self) -> None:
        import ctypes
        from ctypes import wintypes

        class BasicLimitInformation(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class IoCounters(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class ExtendedLimitInformation(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", BasicLimitInformation),
                ("IoInfo", IoCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        class BasicAccountingInformation(ctypes.Structure):
            _fields_ = [
                ("TotalUserTime", ctypes.c_longlong),
                ("TotalKernelTime", ctypes.c_longlong),
                ("ThisPeriodTotalUserTime", ctypes.c_longlong),
                ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
                ("TotalPageFaultCount", wintypes.DWORD),
                ("TotalProcesses", wintypes.DWORD),
                ("ActiveProcesses", wintypes.DWORD),
                ("TotalTerminatedProcesses", wintypes.DWORD),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.argtypes = [
            wintypes.LPVOID,
            wintypes.LPCWSTR,
        ]
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            wintypes.LPVOID,
            wintypes.DWORD,
        ]
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.AssignProcessToJobObject.argtypes = [
            wintypes.HANDLE,
            wintypes.HANDLE,
        ]
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.QueryInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.LPVOID,
        ]
        kernel32.QueryInformationJobObject.restype = wintypes.BOOL
        kernel32.TerminateJobObject.argtypes = [
            wintypes.HANDLE,
            wintypes.UINT,
        ]
        kernel32.TerminateJobObject.restype = wintypes.BOOL
        kernel32.OpenProcess.argtypes = [
            wintypes.DWORD,
            wintypes.BOOL,
            wintypes.DWORD,
        ]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.WaitForSingleObject.argtypes = [
            wintypes.HANDLE,
            wintypes.DWORD,
        ]
        kernel32.WaitForSingleObject.restype = wintypes.DWORD
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL

        handle = kernel32.CreateJobObjectW(None, None)
        if not handle:
            raise ctypes.WinError(ctypes.get_last_error())
        limits = ExtendedLimitInformation()
        limits.BasicLimitInformation.LimitFlags = self._KILL_ON_JOB_CLOSE
        if not kernel32.SetInformationJobObject(
            handle,
            self._EXTENDED_LIMIT_INFORMATION,
            ctypes.byref(limits),
            ctypes.sizeof(limits),
        ):
            error = ctypes.get_last_error()
            kernel32.CloseHandle(handle)
            raise ctypes.WinError(error)
        self._ctypes = ctypes
        self._wintypes = wintypes
        self._kernel32 = kernel32
        self._accounting_type = BasicAccountingInformation
        self._handle = handle

    def assign(self, process: subprocess.Popen[bytes]) -> None:
        handle = self._require_handle()
        process_handle = self._wintypes.HANDLE(int(process._handle))
        if not self._kernel32.AssignProcessToJobObject(
            handle,
            process_handle,
        ):
            raise self._ctypes.WinError(self._ctypes.get_last_error())

    def active_processes(self) -> int:
        handle = self._require_handle()
        accounting = self._accounting_type()
        if not self._kernel32.QueryInformationJobObject(
            handle,
            self._BASIC_ACCOUNTING_INFORMATION,
            self._ctypes.byref(accounting),
            self._ctypes.sizeof(accounting),
            None,
        ):
            raise self._ctypes.WinError(self._ctypes.get_last_error())
        return int(accounting.ActiveProcesses)

    def has_live_processes(self) -> bool:
        synchronize = 0x00100000
        wait_object_0 = 0
        wait_timeout = 258
        invalid_parameter = 87
        if not self._active_process_ids():
            return False
        time.sleep(0.01)
        accounting_deadline = time.monotonic() + 0.05
        while True:
            process_ids = self._active_process_ids()
            if not process_ids:
                return False
            for pid in process_ids:
                process_handle = self._kernel32.OpenProcess(
                    synchronize,
                    False,
                    pid,
                )
                if not process_handle:
                    error = self._ctypes.get_last_error()
                    if error == invalid_parameter:
                        continue
                    raise self._ctypes.WinError(error)
                try:
                    wait_result = self._kernel32.WaitForSingleObject(
                        process_handle,
                        0,
                    )
                finally:
                    if not self._kernel32.CloseHandle(process_handle):
                        raise self._ctypes.WinError(
                            self._ctypes.get_last_error()
                        )
                if wait_result == wait_object_0:
                    continue
                if wait_result == wait_timeout:
                    return True
                raise RuntimeError(
                    "unexpected Windows process wait result: "
                    f"{wait_result}"
                )
            if time.monotonic() >= accounting_deadline:
                return False
            time.sleep(0.001)

    def _active_process_ids(self) -> list[int]:
        process_id_list_class = 3
        error_more_data = 234
        capacity = max(16, self.active_processes() + 4)
        for _attempt in range(8):
            process_ids_type = self._ctypes.c_size_t * capacity

            class ProcessIdList(self._ctypes.Structure):
                _fields_ = [
                    ("NumberOfAssignedProcesses", self._wintypes.DWORD),
                    ("NumberOfProcessIdsInList", self._wintypes.DWORD),
                    ("ProcessIdList", process_ids_type),
                ]

            process_ids = ProcessIdList()
            if self._kernel32.QueryInformationJobObject(
                self._require_handle(),
                process_id_list_class,
                self._ctypes.byref(process_ids),
                self._ctypes.sizeof(process_ids),
                None,
            ):
                count = int(process_ids.NumberOfProcessIdsInList)
                return [
                    int(process_ids.ProcessIdList[index])
                    for index in range(count)
                ]
            error = self._ctypes.get_last_error()
            if error != error_more_data:
                raise self._ctypes.WinError(error)
            capacity = max(
                capacity * 2,
                int(process_ids.NumberOfAssignedProcesses) + 4,
            )
        raise RuntimeError("Windows child job process list did not stabilize")

    def terminate(self) -> None:
        handle = self._require_handle()
        if not self._kernel32.TerminateJobObject(handle, 1):
            raise self._ctypes.WinError(self._ctypes.get_last_error())

    def close(self) -> None:
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        if not self._kernel32.CloseHandle(handle):
            raise self._ctypes.WinError(self._ctypes.get_last_error())

    def _require_handle(self):
        if self._handle is None:
            raise RuntimeError("Windows child job is already closed")
        return self._handle


def _new_process_job() -> _WindowsKillOnCloseJob | None:
    if os.name != "nt":
        return None
    return _WindowsKillOnCloseJob()


def _wrapper_python_executable() -> str:
    executable = os.fspath(sys.executable)
    if os.name == "nt" and os.path.basename(executable).lower() == "pythonw.exe":
        console_executable = os.path.join(
            os.path.dirname(executable),
            "python.exe",
        )
        if os.path.isfile(console_executable):
            return console_executable
    return executable


def _prepare_windows_gated_spawn(
    args: Sequence[str | PathLike[str]],
    popen_kwargs: dict[str, Any],
    *,
    wrapper_python: str | PathLike[str] | None,
) -> tuple[list[str], dict[str, Any], _WindowsStartGate | None]:
    if os.name != "nt":
        return list(args), popen_kwargs, None
    if popen_kwargs.get("shell"):
        raise ValueError("shell=True is incompatible with bounded capture")
    if popen_kwargs.get("close_fds") is False:
        raise ValueError(
            "close_fds=False is incompatible with Windows process containment"
        )

    gate = _WindowsStartGate()
    try:
        prepared_kwargs = dict(popen_kwargs)
        startupinfo = prepared_kwargs.get("startupinfo")
        if startupinfo is None:
            startupinfo = subprocess.STARTUPINFO()
        else:
            startupinfo = startupinfo.copy()
        attribute_list = dict(startupinfo.lpAttributeList or {})
        if attribute_list.get("handle_list"):
            raise ValueError(
                "caller-supplied handle_list cannot be forwarded safely"
            )
        attribute_list["handle_list"] = [gate.handle_value]
        startupinfo.lpAttributeList = attribute_list
        prepared_kwargs["startupinfo"] = startupinfo
        prepared_kwargs["close_fds"] = True
        creationflags = int(prepared_kwargs.get("creationflags", 0))
        show_window = -1
        if startupinfo.dwFlags & subprocess.STARTF_USESHOWWINDOW:
            show_window = int(startupinfo.wShowWindow)
        payload_args = [os.fspath(arg) for arg in args]
        wrapper_args = [
            os.fspath(wrapper_python or _wrapper_python_executable()),
            "-I",
            "-B",
            "-S",
            "-c",
            _WINDOWS_GATE_WORKER,
            str(gate.handle_value),
            str(creationflags),
            str(show_window),
            *payload_args,
        ]
        return wrapper_args, prepared_kwargs, gate
    except BaseException:
        gate.close()
        raise


class _BoundedByteTail:
    def __init__(self, max_bytes: int) -> None:
        if (
            not isinstance(max_bytes, int)
            or isinstance(max_bytes, bool)
            or max_bytes <= 0
        ):
            raise ValueError("max_output_bytes must be a positive integer")
        self._max_bytes = max_bytes
        self._data = bytearray()
        self._dropped = 0
        self._lock = threading.Lock()

    def append(self, chunk: bytes) -> None:
        if not chunk:
            return
        incoming = bytes(chunk)
        with self._lock:
            combined_size = len(self._data) + len(incoming)
            if len(incoming) >= self._max_bytes:
                self._dropped += combined_size - self._max_bytes
                self._data[:] = incoming[-self._max_bytes:]
                return
            self._data.extend(incoming)
            excess = len(self._data) - self._max_bytes
            if excess > 0:
                del self._data[:excess]
                self._dropped += excess

    def text(self) -> str:
        with self._lock:
            raw = bytes(self._data)
            dropped = self._dropped
        if dropped:
            marker = (
                f"[... {dropped} earlier output bytes omitted ...]\n"
            ).encode("ascii")
            if len(marker) >= self._max_bytes:
                raw = marker[:self._max_bytes]
            else:
                raw = marker + raw[-(self._max_bytes - len(marker)):]
        return _decode_bounded_utf8(raw, self._max_bytes)


def _decode_bounded_utf8(raw: bytes, max_bytes: int) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        marker = b"[... undecodable output bytes omitted ...]\n"
        if len(marker) >= max_bytes:
            return marker[:max_bytes].decode("ascii")
        budget = max_bytes - len(marker)
        tail = raw[-budget:].decode("utf-8", errors="ignore")
        tail_bytes = tail.encode("utf-8")
        if len(tail_bytes) > budget:
            tail = tail_bytes[-budget:].decode("utf-8", errors="ignore")
        return marker.decode("ascii") + tail


def _drain_pipe(stream, tail: _BoundedByteTail) -> None:
    try:
        read_chunk = getattr(stream, "read1", stream.read)
        while True:
            chunk = read_chunk(_READ_CHUNK_BYTES)
            if not chunk:
                return
            tail.append(chunk)
    except Exception as exc:
        tail.append(
            (
                "\n[... output reader failed: "
                f"{type(exc).__name__} ...]\n"
            ).encode("ascii")
        )
    finally:
        try:
            stream.close()
        except OSError:
            pass


def _join_readers(
    readers: list[threading.Thread],
    tails: list[_BoundedByteTail],
) -> bool:
    deadline = time.monotonic() + _READER_DRAIN_TIMEOUT_SEC
    for reader in readers:
        reader.join(timeout=max(0.0, deadline - time.monotonic()))
    drained = True
    for reader, tail in zip(readers, tails):
        if reader.is_alive():
            drained = False
            tail.append(
                b"\n[... output pipe remained open after child exit ...]\n"
            )
    return drained


def _close_stream(stream: Any) -> None:
    try:
        stream.close()
    except (OSError, ValueError):
        pass


def _close_finished_streams(
    process: subprocess.Popen[bytes],
    readers: list[threading.Thread],
    started_readers: list[threading.Thread],
) -> None:
    started_ids = {id(reader) for reader in started_readers}
    streams = [process.stdout, process.stderr]
    for index, stream in enumerate(streams):
        if stream is None:
            continue
        reader = readers[index] if index < len(readers) else None
        if (
            reader is None
            or id(reader) not in started_ids
            or not reader.is_alive()
        ):
            _close_stream(stream)


def _terminate_owned_processes(
    process: subprocess.Popen[bytes],
    job: _WindowsKillOnCloseJob | None,
) -> None:
    tree_error: BaseException | None = None
    if job is not None:
        try:
            job.terminate()
        except (OSError, RuntimeError) as exc:
            tree_error = exc
    if process.poll() is None:
        try:
            process.kill()
        except OSError as exc:
            if process.poll() is None and tree_error is None:
                tree_error = exc
    try:
        process.wait(timeout=_PROCESS_TERMINATION_TIMEOUT_SEC)
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError("child process could not be reaped") from exc
    if process.poll() is None:
        raise RuntimeError("child process remained alive after termination")
    if tree_error is not None:
        raise RuntimeError(
            "Windows child job termination was not confirmed"
        ) from tree_error


def _cleanup_failed_run(
    process: subprocess.Popen[bytes],
    readers: list[threading.Thread],
    started_readers: list[threading.Thread],
    tails: list[_BoundedByteTail],
    job: _WindowsKillOnCloseJob | None,
) -> None:
    termination_error: BaseException | None = None
    try:
        _terminate_owned_processes(process, job)
    except BaseException as exc:
        termination_error = exc
    _join_readers(started_readers, tails[:len(started_readers)])
    _close_finished_streams(process, readers, started_readers)
    if termination_error is not None:
        raise RuntimeError(
            "bounded child cleanup could not confirm process-tree termination"
        ) from termination_error


def run_bounded_capture(
    args: Sequence[str | PathLike[str]],
    *,
    cwd: str | PathLike[str] | None = None,
    timeout: float | None = None,
    max_output_bytes: int = DEFAULT_CAPTURE_BYTES_PER_STREAM,
    wrapper_python: str | PathLike[str] | None = None,
    **popen_kwargs: Any,
) -> subprocess.CompletedProcess[str]:
    """Run a child while retaining only a fixed byte tail per output stream."""
    if any(
        key in popen_kwargs
        for key in ("stdout", "stderr", "text", "encoding", "errors")
    ):
        raise ValueError("output stream options are managed internally")
    stdout_tail = _BoundedByteTail(max_output_bytes)
    stderr_tail = _BoundedByteTail(max_output_bytes)
    job = _new_process_job()
    start_gate: _WindowsStartGate | None = None
    try:
        spawn_args, prepared_kwargs, start_gate = (
            _prepare_windows_gated_spawn(
                args,
                popen_kwargs,
                wrapper_python=wrapper_python,
            )
        )
        process = subprocess.Popen(
            spawn_args,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **prepared_kwargs,
        )
    except BaseException:
        if start_gate is not None:
            start_gate.close()
        if job is not None:
            job.close()
        raise
    try:
        if start_gate is not None:
            start_gate.disable_inheritance()
        if job is not None:
            job.assign(process)
    except BaseException as assign_exc:
        try:
            _terminate_owned_processes(process, job)
        except BaseException as cleanup_exc:
            raise RuntimeError(
                "child process could not be contained or reaped"
            ) from cleanup_exc
        finally:
            if start_gate is not None:
                start_gate.close()
            if job is not None:
                job.close()
        raise RuntimeError(
            "child process could not be assigned to its Windows job"
        ) from assign_exc
    assert process.stdout is not None
    assert process.stderr is not None
    readers: list[threading.Thread] = []
    started_readers: list[threading.Thread] = []
    tails = [stdout_tail, stderr_tail]
    try:
        readers.append(
            threading.Thread(
                target=_drain_pipe,
                args=(process.stdout, stdout_tail),
                daemon=True,
                name="bounded-child-stdout",
            )
        )
        readers.append(
            threading.Thread(
                target=_drain_pipe,
                args=(process.stderr, stderr_tail),
                daemon=True,
                name="bounded-child-stderr",
            )
        )
        for reader in readers:
            reader.start()
            started_readers.append(reader)
        if start_gate is not None:
            try:
                start_gate.signal()
            finally:
                start_gate.close()
            start_gate = None
        returncode = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        try:
            try:
                _cleanup_failed_run(
                    process,
                    readers,
                    started_readers,
                    tails,
                    job,
                )
            finally:
                if start_gate is not None:
                    start_gate.close()
                if job is not None:
                    job.close()
        except BaseException as cleanup_exc:
            raise RuntimeError(
                "timed-out child process cleanup failed"
            ) from cleanup_exc
        exc.stdout = stdout_tail.text()
        exc.stderr = stderr_tail.text()
        exc.cmd = args
        raise
    except BaseException:
        try:
            try:
                _cleanup_failed_run(
                    process,
                    readers,
                    started_readers,
                    tails,
                    job,
                )
            finally:
                if start_gate is not None:
                    start_gate.close()
                if job is not None:
                    job.close()
        except BaseException as cleanup_exc:
            raise RuntimeError(
                "child process cleanup failed after capture error"
            ) from cleanup_exc
        raise
    try:
        if not _join_readers(readers, tails):
            _terminate_owned_processes(process, job)
            _join_readers(readers, tails)
            _close_finished_streams(process, readers, started_readers)
            raise RuntimeError(
                "child process exited while descendant output pipes remained open"
            )
        active_descendants = job.has_live_processes() if job is not None else False
        if active_descendants:
            _terminate_owned_processes(process, job)
            _join_readers(readers, tails)
            _close_finished_streams(process, readers, started_readers)
            raise RuntimeError(
                "child process exited while descendant processes remained active"
            )
        _close_finished_streams(process, readers, started_readers)
        return subprocess.CompletedProcess(
            args=args,
            returncode=returncode,
            stdout=stdout_tail.text(),
            stderr=stderr_tail.text(),
        )
    finally:
        if start_gate is not None:
            start_gate.close()
        if job is not None:
            job.close()
