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

# Download the default local model (Qwen3-8B Q4_K_M GGUF, ~5GB) to where the
# default config expects it. Resumes a partial download.
model:
    mkdir -p data/models
    curl -L -C - -o data/models/Qwen3-8B-Q4_K_M.gguf \
        "https://huggingface.co/Qwen/Qwen3-8B-GGUF/resolve/main/Qwen3-8B-Q4_K_M.gguf"
