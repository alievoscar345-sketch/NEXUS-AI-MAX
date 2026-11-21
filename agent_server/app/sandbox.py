"""Restricted Docker execution boundary for autonomous code tasks.

The backend process never executes task-provided commands directly. Commands run
inside a pre-provisioned, digest-pinned container with no network and a single
task-workspace bind mount. Docker must be available to the backend operator.
"""
from __future__ import annotations

import asyncio
import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any


IMAGE_ENV = "NEXUS_SANDBOX_IMAGE"
MAX_OUTPUT_BYTES = 64 * 1024
MAX_ARGUMENTS = 100
MAX_ARGUMENT_LENGTH = 2000
ALLOWED_EXECUTABLES = {
    "python", "python3", "pytest", "node", "npm", "java", "javac",
    "gradle", "./gradlew",
}
_DIGEST_IMAGE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/:@-]*@sha256:[a-f0-9]{64}$")


class SandboxUnavailable(RuntimeError):
    pass


class SandboxValidationError(ValueError):
    pass


def validate_command(command: Any) -> list[str]:
    if not isinstance(command, list) or not command or len(command) > MAX_ARGUMENTS:
        raise SandboxValidationError("command must be a non-empty argument array (maximum 100 items)")
    if any(not isinstance(part, str) or not part or len(part) > MAX_ARGUMENT_LENGTH
           or "\x00" in part for part in command):
        raise SandboxValidationError("each command argument must be a non-empty string of at most 2000 characters")
    if command[0] not in ALLOWED_EXECUTABLES:
        raise SandboxValidationError("executable is not in the sandbox allowlist")
    return command


def _docker_workspace_source(workspace: Path) -> Path:
    configured_host_root = os.getenv("NEXUS_SANDBOX_HOST_WORKSPACE_ROOT", "").strip()
    workspace_root = Path(os.getenv("NEXUS_AGENT_WORKSPACE", "./data/workspace")).resolve()
    resolved_workspace = workspace.resolve()
    try:
        relative = resolved_workspace.relative_to(workspace_root)
    except ValueError as exc:
        raise SandboxUnavailable("Task workspace is outside the configured workspace root") from exc
    if not configured_host_root:
        return resolved_workspace
    host_root = Path(configured_host_root).resolve()
    source = host_root / relative
    if "," in str(source):
        raise SandboxUnavailable("Host workspace path contains a character unsupported by Docker mount syntax")
    return source


def build_docker_argv(image: str, workspace: Path, cidfile: str,
                      command: list[str], uid: int, gid: int) -> list[str]:
    if not _DIGEST_IMAGE.fullmatch(image):
        raise SandboxUnavailable(
            f"{IMAGE_ENV} must be a preloaded image pinned by sha256 digest"
        )
    if uid <= 0 or gid < 0:
        raise SandboxUnavailable("The backend must run as a non-root user to enable code execution")
    root = _docker_workspace_source(workspace)
    if not root.is_absolute():
        raise SandboxUnavailable("Docker workspace source must be an absolute host path")
    if "," in str(root):
        raise SandboxUnavailable("Workspace path contains a character unsupported by Docker mount syntax")
    return [
        "docker", "run", "--rm", "--pull=never",
        "--cidfile", cidfile,
        "--label", "nexus.managed=true",
        "--network=none", "--ipc=none",
        "--read-only", "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--user", f"{uid}:{gid}",
        "--env", "HOME=/tmp/home",
        "--env", "GRADLE_USER_HOME=/tmp/gradle",
        "--env", "npm_config_cache=/tmp/npm",
        "--env", "PIP_CACHE_DIR=/tmp/pip",
        "--env", "JAVA_TOOL_OPTIONS=-Djava.io.tmpdir=/tmp",
        "--tmpfs", "/tmp:rw,nosuid,nodev",
        "--mount", f"type=bind,src={root},dst=/workspace",
        "--workdir", "/workspace",
        image, *command,
    ]


async def _read_capped(stream: asyncio.StreamReader, limit: int) -> bytes:
    kept = bytearray()
    while True:
        chunk = await stream.read(8192)
        if not chunk:
            break
        if len(kept) < limit:
            kept.extend(chunk[:limit - len(kept)])
    return bytes(kept)


async def _stop_container(cidfile: str) -> None:
    try:
        container_id = Path(cidfile).read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError):
        return
    if not re.fullmatch(r"[a-f0-9]{12,64}", container_id):
        return
    try:
        stop = await asyncio.create_subprocess_exec(
            "docker", "kill", container_id,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(stop.wait(), timeout=5)
    except (OSError, asyncio.TimeoutError):
        pass


async def execute(workspace: Path, command: Any, timeout_seconds: Any = None) -> dict[str, Any]:
    argv = validate_command(command)
    timeout = None
    if timeout_seconds is not None:
        try:
            timeout = int(timeout_seconds)
        except (TypeError, ValueError):
            raise SandboxValidationError("timeout_seconds must be a positive integer or null")
        if isinstance(timeout_seconds, bool) or timeout < 1:
            raise SandboxValidationError("timeout_seconds must be a positive integer or null")

    image = os.getenv(IMAGE_ENV, "").strip()
    docker = shutil.which("docker")
    if not docker:
        raise SandboxUnavailable("Docker is not installed or is not available to the backend")
    if not image or not _DIGEST_IMAGE.fullmatch(image):
        raise SandboxUnavailable(
            f"Configure {IMAGE_ENV} with a preloaded container image pinned by sha256 digest"
        )
    uid, gid = os.getuid(), os.getgid()
    if uid <= 0:
        raise SandboxUnavailable("Run the backend as a non-root user before enabling code execution")

    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="nexus-sandbox-") as temp_dir:
        cidfile = str(Path(temp_dir) / "container.id")
        docker_argv = build_docker_argv(image, workspace, cidfile, argv, uid, gid)
        docker_argv[0] = docker
        try:
            process = await asyncio.create_subprocess_exec(
                *docker_argv,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError as exc:
            raise SandboxUnavailable(f"Unable to start Docker: {exc.__class__.__name__}") from exc

        stdout_task = asyncio.create_task(_read_capped(process.stdout, MAX_OUTPUT_BYTES))
        stderr_task = asyncio.create_task(_read_capped(process.stderr, MAX_OUTPUT_BYTES))
        timed_out = False
        try:
            if timeout is None:
                await process.wait()
            else:
                await asyncio.wait_for(process.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            timed_out = True
            await _stop_container(cidfile)
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.wait()
        except asyncio.CancelledError:
            await _stop_container(cidfile)
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.wait()
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            raise
        stdout, stderr = await asyncio.gather(stdout_task, stderr_task)

    return {
        "exit_code": process.returncode,
        "stdout": stdout.decode("utf-8", errors="replace"),
        "stderr": stderr.decode("utf-8", errors="replace"),
        "output_truncated": (
            len(stdout) >= MAX_OUTPUT_BYTES or len(stderr) >= MAX_OUTPUT_BYTES
        ),
        "timed_out": timed_out,
        "duration_seconds": round(time.monotonic() - started, 3),
        "sandbox": "docker-no-network",
    }
