default: build

build:
    cargo build --workspace

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
    mkdir -p data/models
    curl -L -C - -o data/models/Qwen3-8B-Q4_K_M.gguf \
        "https://huggingface.co/Qwen/Qwen3-8B-GGUF/resolve/main/Qwen3-8B-Q4_K_M.gguf"
