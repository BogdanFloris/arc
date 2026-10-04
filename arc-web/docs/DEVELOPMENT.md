# ARC web development

Architecture lives in the repository's `docs/DESIGN.md`. This is ARC's desktop/mobile client, not a suite shell or a separate backend.

## Run locally

On Erebor:

```sh
cd ~/arc
nix develop
cd arc-web
npm ci
npm run dev
```

Open `http://127.0.0.1:5173`. Alternatively, `direnv allow` enables the repository's development shell automatically.

The root shell provides Node 24, librsvg, and Buf. Dependencies are locked with npm. TypeScript 6 is used because the selected `svelte-check` release does not yet accept TypeScript 7.

Fresh desktop localhost has no implicit host. Add a real ARC endpoint in Settings; the daemon must approve the page's exact Origin (e.g. `http://127.0.0.1:5173` for this development URL). Browser tests use binary protobuf fixture daemons without calling a model. No Demo connection or simulation is shipped in the application.

HTTPS deployments offer a same-origin daemon profile whose endpoint is `/arc` over WSS. Existing real host selections, IDs, drafts, and uncertain inputs are preserved. Upgrading removes legacy Demo profiles and fixture-local state; a previously active Demo selection switches to the deployment profile or first saved real host.

Settings can add and switch daemon profiles. Endpoints must use `wss://`, except local `ws://localhost`, `ws://127.0.0.1`, or `ws://[::1]`. A real host loads direct sessions, ordered history/tool output, and current daemon jobs. Removing the last host leaves no active connection. New conversations select a configured project and assistant model preset beside the composer. The client creates that session explicitly before sending; existing sessions retain their recorded project/model.

The client connects with binary protobuf, streams sends on separate observation sockets, and subscribes for history/job changes on its read socket. Switching hosts closes all old-host sockets. Connection state becomes Connected only after successful session/job reads. Unavailable hosts keep local drafts editable and disable Send; no demo data is substituted.

Input is saved locally before transmission. An acknowledgement confirms daemon acceptance; lack of acknowledgement is not proof of rejection. Unknown delivery retains the original text across reload, with explicit Restore to draft and Dismiss actions. Restore never sends automatically; check authoritative history before choosing to resend. Reconnection and returning from suspension refetch current history without replaying input. Accepted execution belongs to ARC even when the browser stops observing it.

## Implemented interactions

- Desktop has a fixed sidebar header and independently scrolling session list. Jobs has one entry in the conversation header, not a duplicate below sessions.
- The plain, clickable host-status line shows only host name and connection state without a glass pill. Connecting uses a neutral dot; only unavailable uses red. Its glass panel shows concise recorded session context, model/provider/thinking, measured context with its compaction threshold, and available shared-account allowances with local reset times and observation age. Activity and refreshes do not add temporary rows. Context is the latest completed-step reading, not cumulative spend; it does not expire just because the conversation is idle. Allowances become stale after 120 seconds or when the daemon reports a failed refresh. Missing readings remain unknown.
- Generic notices and conversation/status errors live in the status panel rather than a yellow transcript banner. Unknown/rejected input retains its recovery disclosure beside the composer. Opening or refreshing status does not interrupt an observed reply.
- Startup and history loading leave the transcript blank rather than flashing connection/loading text. Genuine empty-state prompts are muted, not warning yellow; the transcript's busy state remains available to assistive technology.
- The conversation scroller fills the pane; a centered inner column keeps text readable. Desktop scrollbars are thin and subdued, while touch indicators stay native.
- User messages have a faint padded background; ARC replies stay unboxed. Both share the same left text gutter and typography.
- ARC replies and expanded handoffs render Markdown headings, emphasis, lists, quotes, links, rules, tables, and fenced/indented code. Gruvbox colours match ARC's TUI; fenced Rust, C/C++, Python, shell, JS/TS, Kotlin/Java, JSON, TOML/INI, and YAML receive syntax highlighting. Unknown languages stay plain. Code and table scrollers preserve width/indentation and support keyboard focus.
- Raw HTML is escaped. Links allow only absolute HTTP/HTTPS/mailto URLs and open with no opener/referrer. Images show alt text without fetching resources. Completed message markup is retained; active replies render at most once per 125 ms and flush on completion. Markdown layout growth does not disengage live-follow, and readers of older content are not pulled downward.
- Session lists show newest activity first, user-created sessions only; empty and abandoned sessions are hidden. Project is a styled native select with a folder icon and chevron, defaulting to All projects and populated by the daemon's ListProjects response, not historical session names. Configured projects appear even without history; removed-project conversations remain under All projects. A removed filter resets to All projects on refresh. Project selection shares state between desktop and mobile. There is no text search, ordering caption, or abandoned toggle. Hiding a row does not switch the open conversation.
- New conversation is a single 44px circular glass pencil button in the main header on desktop and phone. It is absent from the session sidebar/sheet, focuses the composer, and preserves both the previous conversation's draft and the new-conversation draft.
- Jobs displays only ListJobs/JobChanged state in daemon order, without adding archived child sessions. Its header entry remains available even when no jobs are reported.
- Opening a job closes the Jobs panel and shows a muted chevron-and-“Back” control aligned with the transcript, without a pill, border, or original title. Its accessible label is “Back to conversation.” It returns to the original conversation or new draft with its reading position and live-follow state intact. Job hops retain that original target; unrelated session/new-conversation navigation, forks, and host changes clear it. The whole job row is tappable, with a two-line title, left-aligned state, and a trailing chevron. The empty panel centers “No jobs,” or “Offline” when disconnected.
- Mobile is the default layout; the desktop sidebar and keyboard hints only appear above 700px with a fine pointer and hover support. Wide touch layouts keep mobile navigation and have no Vim modes.
- Sessions/Settings controls use warm-tinted CSS glass, and dialogs are rounded bottom sheets on touch layouts. Panels have translucency, a softened rim/inset highlight, and a 28px backdrop blur; blur, saturation, and fill opacity were left unchanged when softening the rim. The transcript and message field remain solid. This is not the native iOS Liquid Glass renderer. Opaque fallbacks cover unsupported backdrop filters and browser-reported reduced transparency, increased contrast, or forced colours.
- Sheets focus their heading when opened, avoiding a first-row focus highlight. Tab navigation shows a thin rounded orange outline against the control's edge, without a floating gap or tinted fill; closing restores focus to the opener. Higher-contrast and forced-colour modes retain stronger keyboard focus outlines.
- All panels use a native full-viewport dialog with a real background dismiss button behind a separate glass panel surface, rather than hit-testing the native pseudo-backdrop. Dismiss with a header close control, Escape, or a tap/click on the background. Inside clicks and drags across the surface boundary do not dismiss it. Unfinished host fields survive closing and reopening. The background control is excluded from Tab navigation.
- Sessions, Jobs, and Status panels have bounded native scrolling with fixed headers. Scroll is contained at panel boundaries and underlying transcript/sidebar scrolling is disabled while a dialog is open.
- Settings has grouped Connection/App sections, compact host rows, inline app-update status, and an add-host form with Cancel.
- The phone header has an opaque background and 20px of clearance below the top safe-area inset. This is a trial for Bogdan's report of a blurred title near the iPhone pill; resolution still needs phone confirmation.
- Bogdan reported picker scrolling reaching the underlying conversation, then gave positive deployed-use feedback on the scrolling/project selector, Markdown, and cleaned-up header/picker. This is user smoke feedback; Chromium touch-gesture checks and that feedback do not establish full iPhone acceptance.
- Session switches preserve draft and in-memory scroll state. Drafts and selected sessions also survive reload.
- The composer remains editable while a reply streams. Enter sends; Shift+Enter adds a newline. Composition Enter does not submit.
- A local send shows three subdued pulsing dots beneath the user message until the first nonempty text delta or tool starts. Acknowledgement does not hide them. They stop on completion, failure, or loss of observation, follow the active conversation, and stay static with reduced motion. The connection header remains unchanged.
- Project context replaces the ARC label inside the composer toolbar. Model and thinking are quiet, unboxed text controls beside it, not a separate labelled settings row. Native select appearance is disabled while touch targets stay 44px. New-conversation project and preset choices are host-scoped and survive reload, independently of the session-list filter. Unavailable saved choices stay explicit rather than silently falling back. Choosing thinking before a first message deliberately creates an empty session to query its supported levels; changing that empty conversation's context stages a replacement while preserving its draft.
- An effective thinking level remains visible when the daemon supplies no editable levels. It is read-only, not labelled unavailable; its tooltip explains that distinction. The browser never invents supported levels or changes the recorded effort.
- Existing project context stays fixed. Selecting a model preset requires an explicit fork confirmation at the last durable user/assistant message. Cancelling leaves the original pin unchanged; confirming preserves the draft in both conversations. Thinking choices come only from session status, apply to the next turn, and are disabled during live work.
- Host or conversation navigation during session creation cannot redirect a send or replace the destination's draft. If the draft changes while creation waits, it stays unsent. Failed creation retains the draft; it is never automatically retried.
- The textarea and 44px circular Send control share one rounded surface. Draft height grows up to 180px and is restored after reload or session switching. Desktop keyboard hints are hidden on the phone layout.
- Send and action buttons, host checks, and the Jobs chevron retain orange. Focus outlines hug rounded control edges. Text controls have horizontal padding inside the ring, and the project wrapper does not clip it. The composer brightens slightly while editing; its border stays neutral. Added toolbar-control padding preserves the left text gutter, and the thinking control fits `medium` at 320px.
- Scrolling upward disengages live following. The compact down-arrow resumes following; it has a 28px visible mark and 44px touch target.
- Tool output expands through adjacent borderless native disclosure rows with keyboard support and no paragraph-sized gaps. Touch summaries remain 44px high; desktop mouse summaries are 32px. Expanded output is indented and limited to a 240px scroll region. Exact daemon job handbacks are collapsed subject/body disclosures without SYSTEM/HANDOFF labels; other internal system prompts are hidden. User input, tool arguments, and tool output remain escaped literal text.

## Checks

Inside the development shell:

```sh
npm run check
npm test
npm run build
npm run test:browser
```

From the repository root, `just web-dev`, `just web-build`, `just web-test`, `just web-test-browser`, `just web-proto`, and `just web-deploy` wrap frontend commands. Rust commands remain independent; publishing the web app does not restart arcd.

Browser tests run against the production build, so build first. They use the system Chrome configured by the Nix shell through `PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH`. Outside that shell, set the variable to a suitable Chromium executable or install Playwright's browser separately.

Coverage includes:

- Host/session-scoped drafts, restoration, and legacy Demo removal without losing real-host state.
- Phone-sized and desktop layout, touch-target dimensions, header-action spacing, New conversation placement/draft preservation, dialog focus, and simulated top safe-area clearance.
- Portrait and landscape touch navigation, initial sheet focus, close-fitting orange keyboard outlines, composer focus spacing at 320px, explicit higher-contrast/forced-colour focus indicators, and the opaque higher-contrast glass fallback.
- Outside-click dismissal and real background hit-target checks for all panels, simulated touch taps for each panel, both directions of drag across the surface boundary, and preservation of unfinished host fields.
- Long session-list scrolling, portrait/landscape touch scrolling inside Sessions and Jobs with fixed headers and no background scrolling, outer-edge conversation scrollbars, and the compact jump control.
- Enter/Shift+Enter behavior and synthetic IME key-event guards.
- Typing the next draft during streaming and reading older content without forced scrolling.
- Waiting-reply dots through acknowledgement, replacement by text/tool activity, navigation, failure/completion cleanup, and reduced-motion rendering.
- Composer height restoration and keyboard disclosure of bounded long tool output.
- Escaped message content, failed daemon connections, and locally preserved drafts.
- Markdown structures and TUI colour values, keyboard scrolling of wide tables/code on narrow and desktop layouts, hostile HTML/URLs/attributes, no automatic image loads, unfinished fences during streaming, live-follow, reader-position preservation, and stable completed-message DOM.
- Actual binary protobuf fixtures: session/history loading, stream/tool events, durable reconciliation, and host switching.
- Config-backed project lists, explicit creation under selected project/preset, supported thinking updates, confirmed model forks, preserved drafts, and creation/navigation races.
- Status readings, unknown context, allowance freshness and reset formatting, fresh low-allowance attention, and recovery controls remaining outside the status panel.
- Job return for existing/new drafts, preserved scroll/follow through a live reply and durable reconciliation, independent status refresh, and route clearing after unrelated navigation, forks, or host changes.
- Transport request correlation, concurrent reads, connection timeouts, malformed frames, and accepted/uncertain disconnects.
- Stale history/host responses, uncertain input restoration, and reconnect without send retries.
- TUI source classification, nanosecond-precise activity ordering, missing-date/ID ties, empty/abandoned filters, and explicit project selection defaulting to All projects. Project selection survives session switches and new conversations, resets on host switches, and is shared between desktop and mobile.
- Daemon-only job lists, compact adjacent tool rows, collapsed handbacks, and omission of internal system prompts.
- Production manifest/icons and app-shell reload with the browser offline.

These are Chromium checks, not proof of iPhone keyboard, Safari, installed-app behavior, or responsiveness under real ARC traffic.

### Waiting-reply indicator verified October 4, 2026

The transcript now shows three quiet dots during the gap between a local send and its first text or tool activity. The indicator is transient browser state, not a durable message or daemon-execution claim. It disappears immediately when observation ends, even while history reconciliation is pending. Active-conversation routing and live-follow are preserved.

All 60 unit tests and 65 Chromium browser tests pass, including acknowledgement/empty-delta handling, first text/tool replacement, completion/failure cleanup, conversation/host navigation, editable next drafts, and reduced-motion rendering. Type checks, the production build, root `just fmt`, and root `just lint` pass. The static release is published; live HTTPS index and service worker match it. The phone-sized fixture screenshot is `/tmp/arc-web-waiting-reply-phone.png`. No daemon restart or automated live send was used.

Bogdan sent a test message and approved the deployed indicator on October 4, 2026. This is user smoke feedback; the device was not specified, and controlled installed-iPhone acceptance remains open.

### Quiet controls and status verified October 4, 2026

Orange control accents use thin rounded outlines with no gap outside the control and no tinted focus fill. Text controls have internal horizontal padding; the project wrapper no longer clips the ring. The composer brightens while its border stays neutral. Higher-contrast and forced-colour modes retain stronger keyboard outlines. The header stays host/connection-only during replies, refreshes, and allowance warnings. The status panel omits transient activity rows and explanatory copy while preserving readings, freshness, and actual errors. Connecting uses a neutral dot rather than red. Startup no longer inserts yellow connection/loading placeholders at the top of the transcript.

All 56 unit tests and 64 Chromium browser tests pass, including close-fitting orange outlines, padded/unclipped composer controls at 320px, higher-contrast/forced-colour indicators, blank startup/history loading, muted genuine empty states, and stable connection-section height during a reply/status refresh. Svelte/TypeScript checks, the production build, root `just fmt`, and root `just lint` pass. The static release is published; live HTTPS index and service worker match it. A read-only Chromium smoke connected to Erebor and verified the outline and control padding without overflow at 320/390/1440px or page errors. Earlier startup checks observed no connection/loading transcript rows. Screenshots are under `/tmp/arc-web-close-outline-*.png`. No daemon restart or live send was used. Installed-iPhone appearance and update acceptance remain unverified.

### Conversation controls and status verified October 3, 2026

Bogdan's phone feedback rejected the extra pill and settings-row treatment. The quieter revision restores the plain host/connection line, moves project context into the existing composer toolbar, and makes model/effort controls unboxed. All 56 unit and 60 Chromium browser tests pass, including a regression for recorded `high` with an empty supported-level list. The running daemon reports exactly that combination for `gpt-6.1-sol`; its support table does not recognize that model name. The UI preserves the current value read-only. Enabling effort changes for that pin still needs a daemon-side support update; no daemon restart was made for this revision.

The configured-project picker, composer choices, unified status panel, and job return/alignment changes pass Svelte/TypeScript checks with zero errors/warnings, all 56 unit tests, all 58 Chromium browser tests, and the production build. Root `just fmt` and `just lint` pass. No Rust code, protocol schema, dependency, role default, or daemon configuration changed.

The return/streaming regression caught a transient transcript shrink during durable reconciliation. The client now retains the completed live transcript until authoritative history arrives. If that read fails, it keeps the visible reply and refuses a model fork until history reconciles.

The checked static release is published through `~/.local/share/arc-web/current`. Live HTTPS assets, manifest, and service worker match the build. A fresh Chromium context connected over WSS and read real sessions, history, recorded model/thinking, measured context, and provider allowance. Desktop and 390px layout checks found no horizontal overflow or browser errors; the status touch target is 44px high. Screenshots are under `/tmp/arc-web-parity-*.png`. This was read-only: no live message, session creation, or fork was used for the smoke check. Installed-iPhone controls, keyboard, update, and lifecycle acceptance remain open.

Arcd was not restarted. Its running ListProjects snapshot still includes `cairn`, despite the config-file removal recorded below. That option disappears after the next safe daemon restart, not through a browser hard-coded exception. The browser no longer adds project names merely because historical sessions contain them.

### Repository move verified October 2, 2026

The app sources, tests, assets, scripts, and development docs now live in `arc-web` alongside the Rust components. The root Nix shell supplies the frontend toolchain; locked dependencies install cleanly with `npm ci`. Protobuf regeneration reads the local schemas and produces identical files on repeat. No daemon code, protocol schema, or dependency version changed.

Svelte/TypeScript checks pass with zero errors/warnings, all 43 unit tests and 46 Chromium browser tests pass, and the production build succeeds. New coverage checks legacy state migration, new-key precedence, preservation on failed storage writes, no automatic input retry, ARC metadata, and generated icon dimensions/white-background/orange-mark pixels. Root `just fmt` and `just lint` also pass.

A separate desktop Chromium check served the previous production build, installed its service worker, entered an unsent draft, then switched the same origin to the new build and deliberately applied the update. The saved host, draft, and uncertain input survived; storage moved to the new key, and the ARC shell/draft reloaded offline. This does not verify an installed-iPhone update or name/icon refresh.

A checked release is published under `~/.local/share/arc-web/current`. Bogdan applied the privileged static-route switch; Tailscale now serves that path at `/` and preserves the `/arc` proxy to localhost. Live HTTPS returns the ARC title and manifest, and the served manifest, service worker, and all four installation icons match the checked build. A fresh desktop Chromium context connected over WSS, listed real conversations, and loaded selected conversation history without sending a message. The original checkout and release were retained until deployed-use acceptance, then cleaned up as recorded below. Arcd was not restarted. Controlled installed-iPhone update/name/icon acceptance remains open.

### Earlier deployment verification, October 2, 2026

The scrolling/project-selector update was published to the existing tailnet HTTPS deployment, and the served JavaScript/CSS asset names matched the new production build. Bogdan reported that the update looked good before requesting a commit.

On Erebor, the production build and Svelte/TypeScript checks pass with zero errors or warnings. All 40 unit tests and 44 Chromium browser tests pass. Markdown streaming/reader-position checks also passed four repeated runs each. The production-dependency audit reported no known vulnerabilities. The Markdown update and subsequent quieter session picker/header New conversation cleanup were published to the existing tailnet HTTPS deployment, with served JavaScript/CSS assets matching each new build. Bogdan gave positive feedback on both published updates; full iPhone acceptance remains pending.

Earlier desktop smoke tests against the running daemon verified real conversation loading, creation, a small model reply, and authoritative restoration after reload. A later fresh Chromium session connected through deployed HTTPS/WSS, loaded project-scoped sessions and collapsed handbacks with no Demo profile or SYSTEM labels, and rejected an unapproved-Origin browser connection. A direct daemon handshake with `Origin: null` returned HTTP 403; the Serve proxy surfaced the failed upgrade as HTTP 502. This confirms deployed read connectivity and active Origin enforcement, not real iPhone behavior. ARC's approved-Origin change passes 91 daemon tests, formatting/lint, and its release build.

The offline check uses the production service worker on desktop localhost. Bogdan reported that the initial Tailscale deployment worked on the iPhone and preferred the refined composer/tool rows. The earlier native pseudo-backdrop dismissal passed Chromium checks but failed for all three panels on his iPhone. Subsequent picker updates received positive deployed-use feedback, but a systematic on-device dismissal/drag checklist has not been recorded; the precise WebKit cause has not been reproduced locally. This is user-reported smoke and visual feedback, not a measured performance or full acceptance result. Detailed keyboard/IME behavior, safe-area cropping, and suspension remain unverified. Installed-iPhone update acceptance remains open; the later desktop upgrade check is recorded above.

## PWA and assets

```sh
npm run build
npm run preview
```

The preview uses `http://127.0.0.1:4173`. The production build registers its service worker; normal Vite development does not. Settings shows offline readiness and offers explicit application of a waiting update.

Only the static application shell is precached. This does not supply offline ARC execution or persist conversations.

The app name, page title, and Apple installation title are ARC; the package is `arc-web`. The approved mark is an orange square core between two opposing angular frame/gripper arms. The canonical `assets/logo.svg` is transparent: the desktop sidebar shows only this mark against the app background, without a wordmark. Phone conversations do not show a brand header. PNG generation adds white for home-screen installation; it does not duplicate the artwork. The conversation UI retains its dark Gruvbox theme. Regenerate the committed PNG installation assets after changing the mark:

```sh
npm run icons
```

`public/icons/` contains the 180px Apple touch icon, 192/512px regular icons, and a 512px maskable icon. The icon crop is provisional until tested on the phone.

The core-and-frame mark was published on October 2, 2026. Build/type checks and all 47 Chromium browser tests pass, including transparent SVG pixels, opaque white installation icons, no desktop wordmark, and no phone-layout brand header. The served artwork, four PNGs, and service worker match the build. A fresh live browser verified the icon-only sidebar, transparency, phone-sized layout, and real daemon connection. Bogdan approved the appearance and reported that everything worked well. This is deployed-use feedback, not a controlled installed-phone icon/lifecycle acceptance run.

### Browser-state migration

Browser state uses `arc-web.local.v1`. If it is absent, initialization reads the legacy `cairn.local.v1`, preserving real hosts, selections, drafts, and uncertain inputs. The old key is removed only after saving the new state successfully; failed writes leave the original data intact. If both exist, the new key is authoritative. Previously pending inputs become uncertain and are never automatically resent.

The existing HTTPS origin, manifest ID `/`, start URL `/`, scope `/`, service-worker location, and `/arc` WebSocket route stay unchanged. This is an update to the existing installation, not a second browser origin. An installed icon/name may not refresh immediately; check the existing installation before removing it, because uninstalling can discard local state.

## Phone testing

### Production deployment on Erebor

Tailscale is enabled through dotfiles. Enroll Erebor and the iPhone in the same tailnet, enable MagicDNS and HTTPS, and restrict access to Erebor's TCP 443 to approved devices in the tailnet policy. Network permissions are configured in Tailscale, not by this repository.

Inside the development shell:

```sh
npm run deploy
sudo tailscale serve --bg "$HOME/.local/share/arc-web/current"
tailscale serve status
```

`npm run deploy` runs the production build and type checks, copies only the generated `dist/` files into a new release under `${XDG_DATA_HOME:-$HOME/.local/share}/arc-web/releases/`, and atomically switches the `current` symlink. A failed build leaves the published release unchanged. When using a custom `XDG_DATA_HOME`, use the Serve command printed by the script.

Serve directly hosts these static files over trusted HTTPS within the tailnet. No Vite process, separate web server, Cloudflare DNS change, or public Funnel endpoint is needed. The `--bg` configuration persists across terminal closure and reboot. Subsequent deployments only require `npm run deploy`, not another Serve command. Old release directories are retained; remove unused ones manually, never the directory referenced by `current`.

### One-time repository move

Publish the new release, then replace only the static `/` route using the Serve command above. `/arc` must continue proxying to `http://127.0.0.1:8787`. The HTTPS URL remains unchanged; no daemon restart or conversation migration is required. Changing the route needs root or an explicitly configured Tailscale operator. Without that permission the release is published, but the live site stays on its previous release.

On Erebor, cleanup completed after Bogdan approved the deployed app on October 2, 2026. The original source and complete Git/`jj` metadata were archived to `~/.local/share/arc/archives/cairn-2026-10-02.tar.gz`, excluding rebuildable dependencies, builds, and test output. The archive was compared against the original before removing the checkout and retired static deployment. Its history is preserved in the archive rather than imported into ARC.

The retired project entry was removed from `~/.config/arc/arc.toml`; ARC's project description now includes `arc-web`. This takes effect on the daemon's next restart. Restart only after active turns/jobs finish. Historical conversations and legacy browser-state migration were not removed.

### Connect the real daemon

Keep ARC bound to localhost. Before exposing its WebSocket, install the ARC build with approved-Origin enforcement and configure the exact PWA origin at the top level of `~/.config/arc/arc.toml`:

```toml
allowed_origins = ["https://erebor.taile59ef0.ts.net"]
```

Native clients without Origin remain allowed; this is not application authentication. The reviewed tailnet policy grants only the approved iPhone access to Erebor on TCP 443. Keep that restriction rather than opening port 8787 or a Funnel endpoint.

Restart ARC **after any important turns/jobs finish**, from an independent terminal. ARC backs these assistant sessions, so a restart during an assistant turn can interrupt it. Then add a separate Serve route, leaving the static `/` route intact:

```sh
systemctl --user restart arcd
systemctl --user is-active arcd
sudo tailscale serve --bg --https=443 --set-path=/arc http://127.0.0.1:8787
tailscale serve status
```

Expected Serve routes: `/` serves the published ARC web directory; `/arc` proxies to localhost ARC. The client uses `wss://erebor.taile59ef0.ts.net/arc`. Approved-Origin enforcement must be active **before** adding the proxy.

Use the HTTPS address printed by Serve in Safari and add ARC to the home screen. Apply a waiting app update in Settings and reopen if necessary. Legacy Demo selections migrate to the real deployment profile. Verify that real conversations load, send a small message, and test locking/reopening the phone. The deployed proxy has been checked from desktop Chromium; phone acceptance remains unverified.

To remove only the WebSocket proxy:

```sh
sudo tailscale serve --https=443 --set-path=/arc off
```

To stop serving entirely:

```sh
sudo tailscale serve --https=443 off
```

Deployment scripts publish the app shell only; they do not install/restart ARC or configure the WebSocket proxy. Successful localhost checks alone do not establish tailnet HTTPS connectivity or actual iPhone behavior; deployed HTTPS connectivity has been checked separately.

### Development server alternative

You can also expose the local development/preview server through trusted HTTPS on the tailnet. Desktop localhost is a secure development exception; a plain HTTP LAN address is not sufficient for equivalent PWA tests.

When reverse-proxying Vite, explicitly allow the chosen hostname through Vite's `__VITE_ADDITIONAL_SERVER_ALLOWED_HOSTS` environment variable. Do not disable host checking for every hostname.

Test both Safari and the home-screen installation on the actual iPhone. Keyboard placement, text selection, scrolling during streaming, app suspension, safe-area cropping, and push notifications remain unverified.

The deployment command publishes files only; it does not enroll devices, change the tailnet policy, or enable Serve itself.

## Protocol maintenance and remaining work

After changing ARC's schemas, regenerate browser wire types inside the development shell:

```sh
npm run proto
npm run check
npm test
```

Generation reads `../arc-proto/proto/` using Buf and the locally pinned Protobuf-ES plugin. Commit the generated types alongside their client changes; never maintain a copied schema. Display types are separate from the wire contract.

Live job state is distinct from durable child-session history. The Jobs list contains only daemon-reported jobs; it does not enumerate archived child sessions or infer that execution survived a daemon restart. Opening a reported job loads durable history. Direct conversations updated by another client refresh on durable notifications; this protocol does not broadcast that client's transient token deltas.

### Missing UI

- **New-conversation project/model selection.** The project picker filters existing sessions only. New conversation currently sends without project/model choices and takes ARC defaults. Configured projects/models are not fetched as selection menus.
- **Stop/cancel controls.** There is no Stop action for an active assistant reply or Cancel action for a live job. ARC exposes `CancelTurn` and `CancelJob`; the web client has not wired them. Fork/rewind and manual compaction are also not exposed.
- **Desktop keyboard navigation.** Native Tab/focus, Enter/Shift+Enter composition, and dialog Escape work. There are no dedicated session-navigation/new-conversation shortcuts or Vim-like command bindings yet.
- **Provider-specific citation presentation.** Ordinary Markdown links work; provider citation markers do not have dedicated widgets.

Assistant prose renders safe Markdown; user input, arguments, and tool output remain escaped plain text. Image alt text rather than automatic image loading is intentional, not an unfinished image renderer.

### Acceptance still needed

- Installed-iPhone keyboard/IME, selection/copy, safe areas, rotation, and wide Markdown code/table scrolling.
- A recorded phone send/stream and return-to-history run, including locking/suspension mid-turn and switching networks. Verify authoritative result restoration, draft preservation, and no automatic duplicate send.
- Realistic long-session responsiveness. Full history is fetched at once because ARC has no history pagination request; Chromium fixtures do not measure phone performance.
- A controlled installed-iPhone app-shell upgrade with an unsent draft, new name/icon refresh, and recorded outside-tap/drag dismissal checks for all panels.

### Later scope

Web Push notifications are not implemented and need ARC-side support; they are not merely an untested checkbox. Voice and replication/execution handoff are later work. The broader suite, Capture/Writing, the shared Rust document core, GTK4, and document sync are paused, not part of ARC web.

No automatic uncertain-send retries or client-side replication are planned for this milestone.
