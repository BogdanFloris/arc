import contextlib
import importlib.util
import io
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("arc_test_runner", Path(__file__).with_name("test.py"))
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class ArgumentsTests(unittest.TestCase):
    def test_default_is_the_full_workspace(self):
        self.assertEqual(runner.command_for([]), ["cargo", "test", "--workspace"])

    def test_package_selection_does_not_keep_workspace(self):
        for package in (["-p", "arc-core"], ["--package", "arc-core"],
                        ["--package=arc-core"], ["-parc-core"]):
            arguments = [*package, "context", "--", "--nocapture"]
            self.assertEqual(runner.command_for(arguments), ["cargo", "test", *arguments])

    def test_workspace_and_harness_arguments_are_not_reinterpreted(self):
        for arguments in (["--workspace", "--exclude", "arc"],
                          ["context", "--", "--nocapture", "--skip", "-p"]):
            expected = [] if "--workspace" in arguments else ["--workspace"]
            self.assertEqual(runner.command_for(arguments), ["cargo", "test", *expected, *arguments])

    def test_shell_metacharacters_and_spaces_stay_literal(self):
        arguments = ["-p", "arc-core", "a b;$(touch /never)", "--", "--exact"]
        self.assertEqual(runner.command_for(arguments)[2:], arguments)


class SummaryTests(unittest.TestCase):
    def test_success_retains_totals_not_individual_tests(self):
        summary = runner.summarize(
            "   Compiling arc-core v0.1.0\nrunning 2 tests\n"
            "test a ... ok\ntest b ... ignored\n"
            "test result: ok. 1 passed; 0 failed; 1 ignored\n"
        )
        self.assertIn("best-effort stable libtest text", summary)
        self.assertIn("1 passed; 0 failed; 1 ignored", summary)
        self.assertNotIn("test a", summary)
        self.assertNotIn("Compiling", summary)

    def test_failure_retains_name_location_assertion_and_not_backtrace(self):
        summary = runner.summarize(
            "test context::replay ... FAILED\nfailures:\n"
            "---- context::replay stdout ----\n"
            "thread 'context::replay' panicked at src/context.rs:42:9:\n"
            "assertion `left == right` failed\n  left: 4\n right: 3\n"
            "stack backtrace:\n   0: rust_begin_unwind\n"
            "             at /rust/library/panicking.rs:1\n"
            "   1: test_function\n"
            "note: Some details are omitted, run with RUST_BACKTRACE=full\n"
            "failures:\n    context::replay\n"
            "test result: FAILED. 0 passed; 1 failed\n"
        )
        for expected in ("Failed tests:", "context::replay", "src/context.rs:42:9",
                         "left: 4", "right: 3", "1 failed", "backtrace omitted"):
            self.assertIn(expected, summary)
        self.assertNotIn("rust_begin_unwind", summary)
        self.assertNotIn("/rust/library", summary)

    def test_compile_errors_and_unknown_output_remain_visible(self):
        text = "error[E0004]: non-exhaustive patterns\n --> src/main.rs:2:3\nCUSTOM FAILURE\n"
        summary = runner.summarize(text)
        self.assertIn("No recognized libtest result", summary)
        for line in text.splitlines():
            self.assertIn(line, summary)

    def test_ansi_and_multiple_suites(self):
        summary = runner.summarize(
            "\x1b[31mtest a ... FAILED\x1b[0m\n"
            "test result: FAILED. 1 failed\n"
            "test result: ok. 2 passed\n"
        )
        self.assertIn("  a", summary)
        self.assertIn("2 passed", summary)
        self.assertIn("1 failed", summary)
        self.assertNotIn("\x1b", summary)

    def test_unknown_format_does_not_imply_success(self):
        summary = runner.summarize("...F..\nbenchmark output\n")
        self.assertIn("No recognized libtest result", summary)
        self.assertIn("...F..", summary)
        self.assertIn("benchmark output", summary)

    def test_interleaved_progress_does_not_expose_long_backtraces(self):
        summary = runner.summarize(
            "stack backtrace:\ntest other ... ok\n   0: rust_begin_unwind\n"
            "interleaved custom output\n   1: another_frame\n"
            "test result: ok. 1 passed\n"
        )
        self.assertNotIn("rust_begin_unwind", summary)
        self.assertNotIn("another_frame", summary)
        self.assertIn("interleaved custom output", summary)

    def test_large_assertions_are_explicitly_shortened(self):
        summary = runner.summarize("  left: " + "x" * 2000 + "\n right: 3\n")
        self.assertIn("assertion truncated; see full log", summary)
        self.assertIn("right: 3", summary)
        self.assertLess(len(summary), 1000)


class ExecutionTests(unittest.TestCase):
    def invoke(self, command, directory):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            code = runner.run(command, directory)
        return code, output.getvalue()

    def test_real_exit_code_and_both_streams_are_saved(self):
        with tempfile.TemporaryDirectory() as directory:
            code, output = self.invoke([
                sys.executable, "-c",
                "import sys; print('stdout'); print('stderr', file=sys.stderr); sys.exit(101)",
            ], directory)
            self.assertEqual(code, 101)
            logs = list(Path(directory).glob("*.log"))
            self.assertEqual(len(logs), 1)
            self.assertIn("stdout", logs[0].read_text())
            self.assertIn("stderr", logs[0].read_text())
            self.assertIn(str(logs[0]), output)
            self.assertIn("exit: 101", output)

    def test_logs_are_unique_and_keep_raw_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            for _ in range(2):
                code, _ = self.invoke([sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'\\xff')"], directory)
                self.assertEqual(code, 0)
            logs = list(Path(directory).glob("*.log"))
            self.assertEqual(len(logs), 2)
            self.assertTrue(all(path.read_bytes() == b"\xff" for path in logs))

    def test_signal_exit_is_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            code, output = self.invoke([
                sys.executable, "-c", "import os, signal; os.kill(os.getpid(), signal.SIGTERM)",
            ], directory)
            self.assertEqual(code, 128 + signal.SIGTERM)
            self.assertIn("terminated by signal 15", output)

    def test_missing_command_has_log_and_nonzero_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            code, output = self.invoke([str(Path(directory) / "missing")], directory)
            self.assertEqual(code, 127)
            self.assertIn("Cannot start test command", output)
            self.assertIn("Cannot start", next(Path(directory).glob("*.log")).read_text())

    def test_unwritable_log_directory_does_not_launch_tests(self):
        with tempfile.NamedTemporaryFile() as file, mock.patch.object(runner.subprocess, "Popen") as popen:
            code, output = self.invoke(["cargo", "test"], file.name)
            self.assertEqual(code, 1)
            self.assertIn("Cannot create test log", output)
            popen.assert_not_called()

    def test_just_preserves_arguments_and_passthrough_frames(self):
        with tempfile.TemporaryDirectory() as directory:
            cargo = Path(directory) / "cargo"
            cargo.write_text("#!/bin/sh\nprintf '%s\\n' \"$@\"\nprintf 'FRAME │ hello │\\n'\nexit 7\n")
            cargo.chmod(0o700)
            env = dict(os.environ, PATH=directory + os.pathsep + os.environ["PATH"],
                       ARC_TEST_PASSTHROUGH="1")
            result = subprocess.run(
                ["just", "test", "-p", "arc-core", "literal ; $HOME", "--", "--nocapture"],
                cwd=runner.ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, check=False,
            )
            self.assertEqual(result.returncode, 7, result.stdout)
            self.assertIn("test\n-p\narc-core\nliteral ; $HOME\n--\n--nocapture\n", result.stdout)
            self.assertIn("FRAME │ hello │", result.stdout)
            self.assertNotIn("Test summary", result.stdout)
            path = Path(next(line.removeprefix("Full test log: ") for line in result.stdout.splitlines()
                             if line.startswith("Full test log: ")))
            self.assertIn("FRAME │ hello │", path.read_text())


if __name__ == "__main__":
    unittest.main()
