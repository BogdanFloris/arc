import datetime
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parent.parent
LOG_DIRECTORY = ROOT / "target" / "test-logs"
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
TEST_LINE = re.compile(r"^test (.+) \.\.\. (ok|FAILED|ignored(?:, .*)?)$")
RESULT = re.compile(r"^test result: (?:ok|FAILED)\.")
BACKTRACE_LINE = re.compile(r"^\s*(?:\d+:|at |note: Some details are omitted)")


def command_for(arguments):
    cargo_arguments = arguments[:arguments.index("--")] if "--" in arguments else arguments
    selects_package = any(
        arg in ("-p", "--package", "--workspace")
        or arg.startswith("--package=")
        or (arg.startswith("-p") and len(arg) > 2)
        for arg in cargo_arguments
    )
    return ["cargo", "test", *([] if selects_package else ["--workspace"]), *arguments]


def summarize(text):
    output = []
    failed = []
    results = []
    in_backtrace = False
    for raw in text.splitlines():
        line = ANSI.sub("", raw)
        stripped = line.strip()
        if not stripped:
            continue
        if stripped == "stack backtrace:":
            in_backtrace = True
            output.append("  [backtrace omitted; see full log]")
            continue
        if in_backtrace:
            if BACKTRACE_LINE.match(line):
                continue
            if stripped == "failures:" or line.startswith(("---- ", "thread '", "test result:")):
                in_backtrace = False
        test = TEST_LINE.match(line)
        if test:
            if test.group(2) == "FAILED":
                failed.append(test.group(1))
            continue
        if RESULT.match(line):
            results.append(line)
            continue
        if re.match(r"^running \d+ tests?$", line):
            continue
        if re.match(r"^\s*(?:Compiling |Finished |Running |Doc-tests )", line):
            continue
        if stripped == "failures:" or stripped in failed:
            continue
        if stripped.startswith(("left:", "right:", "assertion ")) and len(line) > 500:
            line = line[:500] + " [assertion truncated; see full log]"
        output.append(line)
    heading = ["Test summary (best-effort stable libtest text; exit status is authoritative)"]
    if failed:
        heading.append("Failed tests:")
        heading.extend(f"  {name}" for name in dict.fromkeys(failed))
    if not results:
        heading.append("No recognized libtest result lines; diagnostics follow.")
    return "\n".join([*heading, *results, *output])


def exit_code(returncode):
    return returncode if returncode >= 0 else 128 - returncode


def run(command, log_directory=LOG_DIRECTORY, passthrough=False):
    log_directory = Path(log_directory)
    try:
        log_directory.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        descriptor, filename = tempfile.mkstemp(
            prefix=f"{timestamp}-", suffix=".log", dir=log_directory
        )
    except OSError as error:
        print(f"Cannot create test log: {error}", file=sys.stderr)
        return 1
    path = Path(filename).resolve()
    print(f"Full test log: {path}", flush=True)
    process = None
    previous_handlers = {}
    try:
        with os.fdopen(descriptor, "wb") as log:
            try:
                process = subprocess.Popen(
                    command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            except OSError as error:
                message = f"Cannot start test command: {error}\n"
                log.write(message.encode())
                print(message, end="", file=sys.stderr)
                return 127 if isinstance(error, FileNotFoundError) else 126

            def forward_signal(number, _frame):
                try:
                    os.killpg(process.pid, number)
                except ProcessLookupError:
                    pass

            for number in (signal.SIGINT, signal.SIGTERM):
                previous_handlers[number] = signal.signal(number, forward_signal)
            try:
                while chunk := process.stdout.read1(65536):
                    log.write(chunk)
                    log.flush()
                    if passthrough:
                        sys.stdout.buffer.write(chunk)
                        sys.stdout.buffer.flush()
                returncode = process.wait()
            finally:
                process.stdout.close()
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            log.flush()
            os.fsync(log.fileno())
        if not passthrough:
            print(summarize(path.read_text(errors="replace")))
        if returncode < 0:
            print(f"Test command terminated by signal {-returncode}.")
        print(f"Test command exit: {exit_code(returncode)}")
        print(f"Full test log: {path}")
        return exit_code(returncode)
    except OSError as error:
        print(f"Test runner failed: {error}\nFull test log: {path}", file=sys.stderr)
        return 1
    finally:
        for number, handler in previous_handlers.items():
            signal.signal(number, handler)


def main():
    return run(command_for(sys.argv[1:]), passthrough=os.environ.get("ARC_TEST_PASSTHROUGH") == "1")


if __name__ == "__main__":
    sys.exit(main())
