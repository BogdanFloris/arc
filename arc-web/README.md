# ARC web

ARC's Svelte/TypeScript client for desktop browsers and phone home-screen installation. The daemon owns conversations and execution; this client owns presentation, connection profiles, and local drafts.

- [Architecture](../docs/DESIGN.md): repository-wide design and boundaries.
- [Development](docs/DEVELOPMENT.md): local setup, checks, tailnet deployment, and remaining work.

From the repository root:

```sh
nix develop
cd arc-web
npm ci
npm run dev
```

The approved mark puts an orange core between two angular robotic arms. The desktop sidebar uses the transparent mark without a wordmark; installation icons add a white background. The conversation UI retains its dark Gruvbox theme.
