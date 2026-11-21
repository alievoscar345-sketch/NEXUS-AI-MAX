import tempfile
import unittest
from pathlib import Path

from app.sandbox import (
    SandboxUnavailable,
    SandboxValidationError,
    build_docker_argv,
    validate_command,
)


class SandboxValidationTests(unittest.TestCase):
    def test_allows_argument_array_for_known_executable(self):
        self.assertEqual(
            validate_command(["python3", "-m", "pytest", "-q"]),
            ["python3", "-m", "pytest", "-q"],
        )

    def test_rejects_shell_and_unknown_executables(self):
        for command in (["sh", "-c", "id"], ["bash", "-c", "echo hi"], ["curl", "https://example.com"]):
            with self.subTest(command=command), self.assertRaises(SandboxValidationError):
                validate_command(command)

    def test_rejects_non_array_empty_and_nul_arguments(self):
        for command in ("python -c pass", [], ["python", "bad\x00arg"], ["python", "x" * 2001]):
            with self.subTest(command=command), self.assertRaises(SandboxValidationError):
                validate_command(command)

    def test_requires_digest_pinned_image_and_non_root_backend(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.assertRaises(SandboxUnavailable):
                build_docker_argv("python:3.12-slim", root, "/tmp/cid", ["python", "--version"], 1000, 1000)
            with self.assertRaises(SandboxUnavailable):
                build_docker_argv("python:3.12-slim@sha256:" + "a" * 64, root, "/tmp/cid",
                                  ["python", "--version"], 0, 0)

    def test_docker_command_has_isolation_limits_and_only_workspace_mount(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            image = "registry.example/nexus-runner:1@sha256:" + "a" * 64
            argv = build_docker_argv(image, root, "/tmp/cid", ["python3", "-m", "pytest"], 1000, 1000)
            joined = " ".join(argv)
            self.assertIn("--network=none", joined)
            self.assertIn("--read-only", joined)
            self.assertIn("--cap-drop=ALL", joined)
            self.assertIn("--security-opt=no-new-privileges", joined)
            self.assertIn("--memory=1g", joined)
            self.assertIn("--pids-limit=128", joined)
            self.assertIn(f"src={root.resolve()},dst=/workspace,rw", joined)
            self.assertEqual(argv[-3:], ["python3", "-m", "pytest"])


if __name__ == "__main__":
    unittest.main()
