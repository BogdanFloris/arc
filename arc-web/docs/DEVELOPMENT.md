# ARC web development

`arc-web` is ARC's Svelte/TypeScript desktop and mobile PWA. `arcd` owns conversations, execution, jobs, and memory. Architecture lives in [DESIGN.md](../../docs/DESIGN.md).

The client is deployed on Erebor. It supports text conversations, streamed replies and tools, cancellation and steering across clients, jobs, project/model/thinking controls, and saved drafts.

## Develop

Use the repository's Nix shell:

```sh
cd ~/arc
nix develop
cd arc-web
npm ci
npm run dev
```

Open `http://127.0.0.1:5173`. Add the daemon in Settings and configure that exact page origin in `allowed_origins` in `arc.toml`. HTTPS deployments use `/arc` over WSS automatically.

The shell supplies Node 24, Buf, librsvg, and Chromium. Dependencies are locked with npm.

## Check

```sh
npm run check
npm test
npm run build
npm run test:browser
```

Build before browser tests: Playwright runs against the production preview. Binary protobuf fixtures exercise the client without calling a model. Chromium checks do not replace real-phone testing.

Root shortcuts: `just web-dev`, `just web-build`, `just web-test`, and `just web-test-browser`.

## Deploy

```sh
just web-deploy
```

This builds the app, copies static files into `~/.local/share/arc-web/releases/`, and atomically switches `current`. It does not install or restart arcd. A custom `XDG_DATA_HOME` changes the deployment root.

Erebor's Tailscale Serve routes are:

- `/` → `~/.local/share/arc-web/current`
- `/arc` → `http://127.0.0.1:8787`

One-time setup:

```sh
sudo tailscale serve --bg "$HOME/.local/share/arc-web/current"
sudo tailscale serve --bg --https=443 --set-path=/arc http://127.0.0.1:8787
tailscale serve status
```

Keep arcd bound to localhost. Configure the exact HTTPS origin in `~/.config/arc/arc.toml` before exposing the proxy:

```toml
allowed_origins = ["https://erebor.taile59ef0.ts.net"]
```

Origin checks are not authentication. Restrict HTTPS access to approved tailnet devices; do not expose port 8787 or enable Funnel.

Use Safari's Add to Home Screen for the phone installation. Apply a waiting app update in Settings. Only the app shell is cached; ARC execution still needs the daemon.

Daemon changes need a separate build/install and restart. Wait for active turns and jobs to finish before restarting.

## Maintain

- Schemas live in `arc-proto/proto/`. After changing them, run `npm run proto`, then the checks above. Do not edit generated bindings.
- The canonical mark is `assets/logo.svg`. Run `npm run icons` after changing it.
- Keep the deployment origin, manifest identity, root scope, and `/arc` route stable so existing installations keep their state.
- Drafts and uncertain input are host-scoped browser state. Never retry sends automatically or discard saved input during a storage migration.
- Keep Markdown untrusted and user/tool text escaped.
- JetBrains Mono is bundled for offline use under its OFL license.

## Remaining work

- Image attachments and steer-queue management.
- Arbitrary fork/rewind, manual compaction, and memory browsing/review.
- Transcript search and dedicated keyboard navigation.
- Web Push notifications.
- Real-phone checks: keyboard/IME, safe areas and rotation, scrolling, suspension/network recovery, installed-app updates, and long-session performance.

Voice and backup/restore are tracked in [DESIGN.md §11](../../docs/DESIGN.md#11-phases).
