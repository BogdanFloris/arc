# Running tests

`just test` runs the full Rust workspace. It saves combined stdout and stderr to a unique file under `target/test-logs/` and prints the absolute path before and after the run. Logs remain until you delete them or run `cargo clean`.

```sh
just test
just test -p arc-core
just test -p arc-core context
just test -p arc-core provider::stream::tests -- --test-threads=1
just test --workspace --exclude arc-voice
```

Arguments go directly to Cargo without shell evaluation. A package selection (`-p` or `--package`) replaces the default `--workspace`. Other Cargo arguments and everything following `--` are preserved. Cargo and libtest still enforce their own argument rules; there is normally one positional test-name filter.

## Feedback and full output

The default summary uses **best-effort parsing of stable libtest text**, not structured Cargo test results. It retains result lines, failed test names, panic locations, assertions, compiler diagnostics, and unrecognized output. It omits successful per-test lines, routine build progress, and recognized backtrace frames. Custom harnesses, interleaved output, and changes to libtest formatting can reduce summarization; the raw log is authoritative.

The wrapper returns Cargo's exit code, not a status inferred from text. Signal exits use the shell convention `128 + signal`. Failure to start the command returns 127 for a missing executable or 126 for other launch errors. Failure to create or write a log returns nonzero rather than running without a log. Interrupts are forwarded to the Cargo process group.

To stream raw output while still saving the full log:

```sh
ARC_TEST_PASSTHROUGH=1 just test -p arc rendered_frame -- --nocapture
```

Use this mode when inspecting rendered TUI frames or when a custom test harness needs no summarization. `--nocapture` alone is also forwarded, but its output goes through the normal summary unless passthrough is enabled. Logs can contain anything a test prints; do not print credentials.

## Testing the wrapper

Python 3 and its standard library are sufficient. The argument-forwarding integration test also uses `just` and a temporary fake Cargo executable; it does not build Rust.

```sh
python3 -m unittest discover -s scripts -p '*_test.py'
```
