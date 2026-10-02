# Working on ARC web

- Read `../docs/DESIGN.md` before architecture/product changes. `docs/DEVELOPMENT.md` records commands and verification limits.
- This is a mobile-first Svelte/TypeScript static PWA, not a suite shell or a second assistant backend.
- Use the binary protobuf protocol from `../arc-proto/proto/`; regenerate committed types with `npm run proto`. Never access the daemon's database.
- Keep transport and host selection separate from views. One active host, explicit switching, host-scoped local drafts, no replication or automatic failover.
- Uncertain sends must not be automatically retried. Keep Markdown/tool output untrusted, exact WebSocket-origin enforcement, and restricted tailnet access.
- Prefer simple changes and necessary regression tests. Keep local interactions independent of network latency.
- Use the root Nix shell. Check with `npm run check`, `npm test`, `npm run build`, and `npm run test:browser` as appropriate. Rust checks are needed only for Rust changes.
- Desktop Chromium is not iPhone acceptance. Real keyboard/IME, safe areas, suspension, scrolling, installed-app updates, and long-session performance need phone tests over trusted HTTPS.
- Use `jj`, preserve unrelated work, and do not commit or push unless asked.
