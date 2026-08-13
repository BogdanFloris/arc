# ARC (Autonomous Robotic Core)

ARC is a personal AI assistant harness built as an always-on daemon with thin clients (TUI, voice) talking to it over a local wire protocol. `DESIGN.md` (coming next) governs the architecture.

## Traces

Every `arcd` run writes one Perfetto trace to `data/traces/arc-<unix-seconds>.pftrace`, and says so in its first log line. Open it by dragging the file into <https://ui.perfetto.dev> — nothing is uploaded, the UI parses it in the browser. What you get: a row per session, the span tree of each turn under it (`session.send_message` → `openai.complete`), and counter tracks graphing tokens in and out.

To ask questions instead of looking, `trace_processor_shell` is in the dev shell:

```sh
nix develop --command trace_processor_shell data/traces/arc-*.pftrace
> select name, dur/1e6 as ms from slice order by dur desc limit 10;
> select t.name, c.value from counter c join counter_track t on c.track_id = t.id;
> select name, value from stats where severity = 'error' and value > 0;   -- should be empty
```

The file is flushed packet by packet, so a trace can be opened while the daemon it belongs to is still running. `RUST_LOG` decides what gets recorded — it filters the trace and the stderr log together. Traces are disposable and excluded from backups; delete them whenever.
