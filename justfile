default: build

build:
    cargo build --workspace

# The binary the systemd unit runs (target/release/arcd). After this:
# `systemctl --user restart arcd`.
build-release:
    cargo build --workspace --release

# Rebuild and restart together so the running daemon never drifts behind the code.
deploy: install

# The real install (task 7.2): both binaries on a stable path, the unit
# enabled, the daemon restarted. `arc` is then a command, not a cargo path.
install: build-release
    mkdir -p ~/.local/bin ~/.config/systemd/user
    install -m755 target/release/arcd ~/.local/bin/arcd
    install -m755 target/release/arc ~/.local/bin/arc
    cp arcd/arcd.service ~/.config/systemd/user/arcd.service
    systemctl --user daemon-reload
    systemctl --user enable arcd
    systemctl --user restart arcd

test:
    cargo test --workspace

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

lint:
    cargo clippy --workspace --all-targets

check:
    cargo check --workspace

# Install the systemd user unit. Enabling it is a separate, deliberate step:
# `systemctl --user enable --now arcd`.
install-service:
    mkdir -p ~/.config/systemd/user
    cp arcd/arcd.service ~/.config/systemd/user/arcd.service
    systemctl --user daemon-reload
    @echo "installed. enable with: systemctl --user enable --now arcd"

# Download the default local model (Qwen3-8B Q4_K_M GGUF, ~5GB) to where the
# default config expects it. Resumes a partial download.
model:
    mkdir -p ~/.local/state/arc/models
    curl -L -C - -o ~/.local/state/arc/models/Qwen3-8B-Q4_K_M.gguf \
        "https://huggingface.co/Qwen/Qwen3-8B-GGUF/resolve/main/Qwen3-8B-Q4_K_M.gguf"
