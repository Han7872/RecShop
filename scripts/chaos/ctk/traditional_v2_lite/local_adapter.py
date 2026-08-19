"""Minimal Windows-local side-effect adapters for Lite-1.

This module is deliberately narrower than the collection runner.  It can only
execute an already frozen :class:`LegacyRunnerCommand` in the foreground and
can only create/promote attempt-local directories described by the wrapper's
closed plans.  It never builds a runner command, talks to Kubernetes/network,
deletes an attempt, or replaces an existing destination.

The implementation is Windows-only because the acceptance contract requires
Windows directory-rename and reparse-point semantics.  Other platforms fail
closed instead of claiming equivalent durability/atomicity.
"""

from __future__ import annotations

import ctypes
import math
import os
import stat
import subprocess
import time
from ctypes import wintypes
from pathlib import Path
from typing import Mapping

from .runner_wrapper import (
    AttemptPlan,
    ExecutionResult,
    LegacyRunnerCommand,
    LiteWrapperError,
    PromotionPlan,
    hash_existing_runner_file,
)


_WINDOWS = os.name == "nt"
_REPARSE_POINT_ATTRIBUTE = 0x0400
_INVALID_FILE_ATTRIBUTES = 0xFFFFFFFF
_GENERIC_WRITE = 0x40000000
_FILE_SHARE_READ = 0x00000001
_FILE_SHARE_WRITE = 0x00000002
_FILE_SHARE_DELETE = 0x00000004
_OPEN_EXISTING = 3
_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value


class LocalAdapterError(LiteWrapperError):
    """Stable failure from a local side-effect adapter."""


def _fail(code: str, message: str) -> "None":
    raise LocalAdapterError(code, message)


def _require_windows() -> None:
    if not _WINDOWS:
        _fail(
            "L1A001_WINDOWS_REQUIRED",
            "local execution and promotion adapters require Windows",
        )


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _absolute_normalized(path: Path, field: str) -> Path:
    if not isinstance(path, Path) or not path.is_absolute() or "\x00" in str(path):
        _fail("L1A002_INVALID_SCOPE", f"{field} must be an absolute Path")
    normalized = path.resolve(strict=False)
    if path != normalized:
        _fail("L1A002_INVALID_SCOPE", f"{field} must be normalized")
    return normalized


def _volume_identity(path: Path) -> str:
    drive, _ = os.path.splitdrive(str(path))
    return (drive or path.anchor).casefold()


def _get_file_attributes(path: Path) -> int:
    _require_windows()
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_attributes = kernel32.GetFileAttributesW
    get_attributes.argtypes = [wintypes.LPCWSTR]
    get_attributes.restype = wintypes.DWORD
    attributes = int(get_attributes(str(path)))
    if attributes == _INVALID_FILE_ATTRIBUTES:
        error = ctypes.get_last_error()
        _fail(
            "L1A003_PATH_INSPECTION_FAILED",
            f"cannot inspect {path.name!r} (winerror={error})",
        )
    return attributes


def _reject_reparse(path: Path, field: str) -> None:
    try:
        mode = os.lstat(path).st_mode
    except OSError as exc:
        _fail(
            "L1A003_PATH_INSPECTION_FAILED",
            f"cannot lstat {field} ({exc.__class__.__name__})",
        )
    if stat.S_ISLNK(mode):
        _fail("L1A004_REPARSE_POINT_FORBIDDEN", f"{field} is a symlink")
    if _get_file_attributes(path) & _REPARSE_POINT_ATTRIBUTE:
        _fail("L1A004_REPARSE_POINT_FORBIDDEN", f"{field} is a reparse point")


def _inspect_existing_chain(path: Path, boundary: Path, field: str) -> None:
    """Reject links/reparse points in every existing component through boundary."""

    boundary = _absolute_normalized(boundary, "scope_root")
    path = _absolute_normalized(path, field)
    if not _is_within(path, boundary):
        _fail("L1A002_INVALID_SCOPE", f"{field} escapes scope_root")
    current = path
    while True:
        if current.exists() or current.is_symlink():
            _reject_reparse(current, field)
        if current == boundary:
            break
        parent = current.parent
        if parent == current:
            _fail("L1A002_INVALID_SCOPE", f"{field} has no inspected boundary")
        current = parent


def _open_exclusive_binary(path: Path):
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
    )
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        _fail("L1A005_ATTEMPT_ARTIFACT_EXISTS", f"{path.name} already exists")
    except OSError as exc:
        _fail(
            "L1A006_LOG_CREATE_FAILED",
            f"cannot create {path.name} ({exc.__class__.__name__})",
        )
    return os.fdopen(descriptor, "wb", buffering=0)


def _sync_file(path: Path) -> None:
    try:
        descriptor = os.open(
            path,
            os.O_RDWR | getattr(os, "O_BINARY", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError as exc:
        _fail(
            "L1A007_SYNC_FAILED",
            f"cannot sync {path.name!r} ({exc.__class__.__name__})",
        )


def _sync_directory_windows(path: Path) -> None:
    """Flush a directory handle using documented Windows backup semantics."""

    _require_windows()
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    flush = kernel32.FlushFileBuffers
    flush.argtypes = [wintypes.HANDLE]
    flush.restype = wintypes.BOOL
    close = kernel32.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    close.restype = wintypes.BOOL
    handle = create_file(
        str(path),
        _GENERIC_WRITE,
        _FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,
        None,
        _OPEN_EXISTING,
        _FILE_FLAG_BACKUP_SEMANTICS,
        None,
    )
    if handle == _INVALID_HANDLE_VALUE:
        error = ctypes.get_last_error()
        _fail("L1A007_SYNC_FAILED", f"cannot open directory (winerror={error})")
    try:
        if not flush(handle):
            error = ctypes.get_last_error()
            _fail("L1A007_SYNC_FAILED", f"cannot flush directory (winerror={error})")
    finally:
        close(handle)


def _sync_tree(path: Path) -> None:
    """Sync regular files and all directories bottom-up; reject special entries."""

    _reject_reparse(path, "promotion source")
    try:
        entries = list(os.scandir(path))
    except OSError as exc:
        _fail("L1A007_SYNC_FAILED", f"cannot scan source ({exc.__class__.__name__})")
    for entry in entries:
        child = Path(entry.path)
        try:
            if entry.is_symlink():
                _fail("L1A004_REPARSE_POINT_FORBIDDEN", "source contains a symlink")
            if entry.is_dir(follow_symlinks=False):
                _sync_tree(child)
            elif entry.is_file(follow_symlinks=False):
                if _get_file_attributes(child) & _REPARSE_POINT_ATTRIBUTE:
                    _fail("L1A004_REPARSE_POINT_FORBIDDEN", "source contains a reparse file")
                _sync_file(child)
            else:
                _fail("L1A004_REPARSE_POINT_FORBIDDEN", "source contains a special entry")
        except OSError as exc:
            _fail("L1A003_PATH_INSPECTION_FAILED", f"source inspection failed ({exc.__class__.__name__})")
    _sync_directory_windows(path)


def _system_taskkill_path() -> Path:
    """Return the absolute Windows system taskkill path; never consult PATH."""

    _require_windows()
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_system_directory = kernel32.GetSystemDirectoryW
    get_system_directory.argtypes = [wintypes.LPWSTR, wintypes.UINT]
    get_system_directory.restype = wintypes.UINT
    buffer = ctypes.create_unicode_buffer(32768)
    length = int(get_system_directory(buffer, len(buffer)))
    if length == 0 or length >= len(buffer):
        _fail("L1A018_PROCESS_STOP_UNCERTAIN", "Windows system directory is unavailable")
    taskkill = Path(buffer.value) / "taskkill.exe"
    if not taskkill.is_absolute() or not taskkill.is_file():
        _fail("L1A018_PROCESS_STOP_UNCERTAIN", "system taskkill.exe is unavailable")
    _reject_reparse(taskkill, "system taskkill.exe")
    return taskkill


def _stop_process_tree_windows(
    process: subprocess.Popen[bytes],
    *,
    confirmation_timeout: float,
) -> None:
    """Force-stop a PID and every descendant before returning.

    The parent must still exist when ``taskkill /T`` takes its process-tree
    snapshot.  Therefore this deliberately does not call ``terminate()`` on
    the parent first: doing so could orphan a child and turn a timeout into a
    delayed side effect.  A successful taskkill result plus a reaped parent is
    the closed confirmation required by the local adapter.
    """

    taskkill = _system_taskkill_path()
    argv = (
        str(taskkill),
        "/PID",
        str(process.pid),
        "/T",
        "/F",
    )
    command_timeout = max(float(confirmation_timeout), 1.0)
    try:
        result = subprocess.run(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            close_fds=True,
            timeout=command_timeout,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
        raise LocalAdapterError(
            "L1A018_PROCESS_STOP_UNCERTAIN",
            "timed-out process tree could not be confirmed stopped",
        ) from exc
    if result.returncode != 0:
        raise LocalAdapterError(
            "L1A018_PROCESS_STOP_UNCERTAIN",
            f"taskkill did not confirm process-tree stop (returncode={result.returncode})",
        )
    try:
        process.wait(timeout=max(float(confirmation_timeout), 0.1))
    except subprocess.TimeoutExpired as exc:
        raise LocalAdapterError(
            "L1A018_PROCESS_STOP_UNCERTAIN",
            "taskkill succeeded but the parent process was not reaped",
        ) from exc
    if process.poll() is None:
        _fail(
            "L1A018_PROCESS_STOP_UNCERTAIN",
            "taskkill succeeded but the parent process remains live",
        )


class ForegroundExecutor:
    """Run an exact argv without a shell and preserve attempt-local logs."""

    name = "windows-local-foreground-v1"

    def __init__(
        self,
        *,
        attempt_root: os.PathLike[str] | str,
        timeout_seconds: float,
        environment_overlay: Mapping[str, str] | None = None,
        terminate_grace_seconds: float = 5.0,
    ) -> None:
        _require_windows()
        root = Path(attempt_root)
        self.attempt_root = _absolute_normalized(root, "attempt_root")
        if (
            type(timeout_seconds) not in {int, float}
            or isinstance(timeout_seconds, bool)
            or not math.isfinite(float(timeout_seconds))
            or timeout_seconds <= 0
        ):
            _fail("L1A008_EXECUTOR_CONFIG_INVALID", "timeout_seconds must be positive")
        if (
            type(terminate_grace_seconds) not in {int, float}
            or isinstance(terminate_grace_seconds, bool)
            or not math.isfinite(float(terminate_grace_seconds))
            or terminate_grace_seconds < 0
        ):
            _fail("L1A008_EXECUTOR_CONFIG_INVALID", "terminate_grace_seconds must be non-negative")
        overlay = dict(environment_overlay or {})
        if any(
            type(key) is not str
            or not key
            or "=" in key
            or "\x00" in key
            or type(value) is not str
            or "\x00" in value
            for key, value in overlay.items()
        ):
            _fail("L1A008_EXECUTOR_CONFIG_INVALID", "environment overlay is invalid")
        self.timeout_seconds = float(timeout_seconds)
        self.terminate_grace_seconds = float(terminate_grace_seconds)
        self.environment_overlay = overlay

    def execute(self, command: LegacyRunnerCommand) -> ExecutionResult:
        _require_windows()
        if not isinstance(command, LegacyRunnerCommand):
            _fail("L1A009_COMMAND_REJECTED", "executor requires LegacyRunnerCommand")
        if command.cwd != command.cwd.resolve(strict=False) or not command.cwd.is_dir():
            _fail("L1A009_COMMAND_REJECTED", "command cwd must be an existing normalized directory")
        if command.runner_out_dir.parent != self.attempt_root:
            _fail("L1A002_INVALID_SCOPE", "command output is outside configured attempt_root")
        _inspect_existing_chain(self.attempt_root, self.attempt_root, "attempt_root")
        if not command.runner_out_dir.is_dir():
            _fail("L1A002_INVALID_SCOPE", "runner output directory does not exist")
        _inspect_existing_chain(command.runner_out_dir, self.attempt_root, "runner_out_dir")
        if not command.runner_path.is_file():
            _fail("L1A009_COMMAND_REJECTED", "runner path must be an existing file")
        _reject_reparse(command.cwd, "command cwd")
        _reject_reparse(command.runner_path, "runner path")
        try:
            actual_runner_sha256 = hash_existing_runner_file(command.runner_path)
        except LiteWrapperError as exc:
            _fail("L1A019_RUNNER_IDENTITY_DRIFT", str(exc))
        if actual_runner_sha256 != command.runner_sha256:
            _fail(
                "L1A019_RUNNER_IDENTITY_DRIFT",
                "runner SHA-256 differs from the frozen command identity",
            )
        stdout_path = self.attempt_root / "stdout.log"
        stderr_path = self.attempt_root / "stderr.log"
        stdout_handle = _open_exclusive_binary(stdout_path)
        try:
            stderr_handle = _open_exclusive_binary(stderr_path)
        except Exception:
            stdout_handle.close()
            raise
        environment = os.environ.copy()
        environment.update(self.environment_overlay)
        try:
            try:
                process = subprocess.Popen(
                    command.argv,
                    cwd=str(command.cwd),
                    env=environment,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                    shell=False,
                    close_fds=True,
                )
            except (OSError, ValueError) as exc:
                _fail("L1A010_PROCESS_START_FAILED", f"cannot start process ({exc.__class__.__name__})")
            try:
                returncode = process.wait(timeout=self.timeout_seconds)
            except subprocess.TimeoutExpired as exc:
                _stop_process_tree_windows(
                    process,
                    confirmation_timeout=self.terminate_grace_seconds,
                )
                raise LocalAdapterError(
                    "L1A011_PROCESS_TIMEOUT",
                    f"foreground process exceeded {self.timeout_seconds:g}s",
                ) from exc
            return ExecutionResult(returncode=int(returncode))
        finally:
            stdout_handle.close()
            stderr_handle.close()
            _sync_file(stdout_path)
            _sync_file(stderr_path)
            _sync_directory_windows(self.attempt_root)


class LocalAttemptWorkspace:
    """Create never-reused attempts and atomically promote verified cases."""

    def __init__(self, *, scope_root: os.PathLike[str] | str) -> None:
        _require_windows()
        self.scope_root = _absolute_normalized(Path(scope_root), "scope_root")
        if not self.scope_root.is_dir():
            _fail("L1A002_INVALID_SCOPE", "scope_root must already be a directory")
        _inspect_existing_chain(self.scope_root, self.scope_root, "scope_root")

    def _require_scoped(self, path: Path, field: str) -> Path:
        path = _absolute_normalized(path, field)
        if not _is_within(path, self.scope_root) or path == self.scope_root:
            _fail("L1A002_INVALID_SCOPE", f"{field} escapes or equals scope_root")
        return path

    def validate(self, plan: AttemptPlan) -> None:
        """Purely validate ownership/scope before the wrapper creates a ledger."""

        _require_windows()
        if not isinstance(plan, AttemptPlan):
            _fail("L1A012_PLAN_REJECTED", "validate requires AttemptPlan")
        dataset_root = self._require_scoped(plan.dataset_root, "dataset_root")
        attempt_root = self._require_scoped(plan.attempt_root, "attempt_root")
        runner_out = self._require_scoped(plan.runner_out_dir, "runner_out_dir")
        ledger_path = self._require_scoped(plan.ledger_path, "ledger_path")
        self._require_scoped(plan.promotion.source, "promotion source")
        self._require_scoped(plan.promotion.destination, "promotion destination")
        if attempt_root != dataset_root / ".attempts" / f"{plan.attempt_id}.tmp":
            _fail("L1A012_PLAN_REJECTED", "attempt root is not canonical")
        if runner_out != attempt_root / "runner-out":
            _fail("L1A012_PLAN_REJECTED", "runner output is not attempt-local")
        if ledger_path != dataset_root / ".ledger" / f"{plan.attempt_id}.jsonl":
            _fail("L1A012_PLAN_REJECTED", "ledger path is not canonical")
        _inspect_existing_chain(dataset_root, self.scope_root, "dataset_root")

    def prepare(self, plan: AttemptPlan) -> None:
        _require_windows()
        self.validate(plan)
        dataset_root = self._require_scoped(plan.dataset_root, "dataset_root")
        attempt_root = self._require_scoped(plan.attempt_root, "attempt_root")
        runner_out = self._require_scoped(plan.runner_out_dir, "runner_out_dir")
        if plan.attempt_root != plan.dataset_root / ".attempts" / f"{plan.attempt_id}.tmp":
            _fail("L1A012_PLAN_REJECTED", "attempt root is not canonical")
        if runner_out != attempt_root / "runner-out":
            _fail("L1A012_PLAN_REJECTED", "runner output is not attempt-local")
        if dataset_root != plan.dataset_root:
            _fail("L1A012_PLAN_REJECTED", "dataset root normalization drift")
        _inspect_existing_chain(dataset_root, self.scope_root, "dataset_root")
        for path in (attempt_root, runner_out):
            if path.exists() or path.is_symlink():
                _fail("L1A005_ATTEMPT_ARTIFACT_EXISTS", f"{path.name} already exists")
        attempts_parent = attempt_root.parent
        if attempts_parent.exists() or attempts_parent.is_symlink():
            _inspect_existing_chain(attempts_parent, self.scope_root, "attempts parent")
        else:
            try:
                attempts_parent.mkdir(parents=False, exist_ok=False)
            except FileExistsError:
                _fail("L1A005_ATTEMPT_ARTIFACT_EXISTS", ".attempts was created concurrently")
            except OSError as exc:
                _fail("L1A013_PREPARE_FAILED", f"cannot create .attempts ({exc.__class__.__name__})")
            _reject_reparse(attempts_parent, "attempts parent")
        try:
            attempt_root.mkdir(parents=False, exist_ok=False)
            _reject_reparse(attempt_root, "attempt_root")
            runner_out.mkdir(parents=False, exist_ok=False)
            _reject_reparse(runner_out, "runner_out_dir")
        except FileExistsError:
            _fail("L1A005_ATTEMPT_ARTIFACT_EXISTS", "attempt path was created concurrently")
        except OSError as exc:
            _fail("L1A013_PREPARE_FAILED", f"cannot create attempt ({exc.__class__.__name__})")
        _sync_directory_windows(runner_out)
        _sync_directory_windows(attempt_root)
        _sync_directory_windows(attempts_parent)
        _sync_directory_windows(dataset_root)

    def validate_promotion(
        self,
        promotion: PromotionPlan,
        *,
        manifest_path: Path,
        manifest_sha256: str,
    ) -> None:
        _require_windows()
        if not isinstance(promotion, PromotionPlan):
            _fail("L1A014_PROMOTION_REJECTED", "promote requires PromotionPlan")
        source = self._require_scoped(promotion.source, "promotion source")
        destination = self._require_scoped(promotion.destination, "promotion destination")
        source_parent = source.parent
        if promotion.destination_must_not_exist is not True:
            _fail("L1A014_PROMOTION_REJECTED", "overwrite permission is forbidden")
        if _volume_identity(source) != _volume_identity(destination):
            _fail("L1A014_PROMOTION_REJECTED", "promotion crosses a volume")
        if not source.is_dir():
            _fail("L1A014_PROMOTION_REJECTED", "promotion source is not an existing directory")
        _inspect_existing_chain(source, self.scope_root, "promotion source")
        destination_parent = destination.parent
        if not destination_parent.is_dir():
            _fail("L1A014_PROMOTION_REJECTED", "destination parent must already exist")
        _inspect_existing_chain(destination_parent, self.scope_root, "destination parent")
        if destination.exists() or destination.is_symlink():
            _fail("L1A015_DESTINATION_EXISTS", "promotion destination already exists")
        from .artifact_manifest import revalidate_manifest

        revalidate_manifest(source, manifest_path, manifest_sha256)

    def promote(
        self,
        promotion: PromotionPlan,
        *,
        manifest_path: Path,
        manifest_sha256: str,
    ) -> None:
        self.validate_promotion(
            promotion,
            manifest_path=manifest_path,
            manifest_sha256=manifest_sha256,
        )
        source = self._require_scoped(promotion.source, "promotion source")
        destination = self._require_scoped(promotion.destination, "promotion destination")
        source_parent = source.parent
        destination_parent = destination.parent
        from .artifact_manifest import revalidate_manifest

        _sync_tree(source)
        try:
            # Windows MoveFileEx without REPLACE_EXISTING: same-volume directory
            # rename, atomic with respect to namespace visibility, and no overwrite.
            os.rename(source, destination)
        except FileExistsError:
            _fail("L1A015_DESTINATION_EXISTS", "destination appeared during promotion")
        except OSError as exc:
            _fail("L1A014_PROMOTION_REJECTED", f"atomic rename failed ({exc.__class__.__name__})")
        try:
            if source.exists() or source.is_symlink() or not destination.is_dir():
                _fail("L1A016_PROMOTION_POSTCONDITION_FAILED", "rename postcondition failed")
            _reject_reparse(destination, "promotion destination")
            revalidate_manifest(destination, manifest_path, manifest_sha256)
            _sync_directory_windows(source_parent)
            _sync_directory_windows(destination)
            _sync_directory_windows(destination_parent)
        except Exception as exc:
            # The namespace may already expose destination.  Never downgrade this
            # to a normal rejection or imply that retry/reuse is safe.
            raise LocalAdapterError(
                "L1A017_PROMOTION_DURABILITY_UNCERTAIN",
                "rename completed but postcondition/durability confirmation failed",
            ) from exc


__all__ = [
    "ForegroundExecutor",
    "LocalAdapterError",
    "LocalAttemptWorkspace",
]
