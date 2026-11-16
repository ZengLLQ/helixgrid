# Building HelixGrid from Source

## Prerequisites (Verified Environment)

- **Operating System:** Windows 11 x64 (`10.0.26300` tested)
- **Rust Toolchain:** `stable-x86_64-pc-windows-msvc` (`rustc 1.98.0`, `cargo 1.98.0` tested; `rust-toolchain.toml` pins `stable` with `clippy` and `rustfmt`; MSRV `1.95` from Slint `1.19.0`)
- **C/C++ Build Tools:** Microsoft Visual Studio Build Tools / Community C++ workload + Windows 11 SDK (`10.0.26100.0` tested) and `cmake.exe` (for `libsqlite3-sys` bundled SQLite C compilation and Win32 resource linking)
- **Installer Packaging (Optional, for MSI generation):** WiX Toolset CLI v5 (`wix 5.0.2` tested)

> **Why LLVM/Clang is NOT required:** HelixGrid configures `slint` with `renderer-femtovg` + `renderer-software` (`default-features = false`), avoiding `skia` and keeping builds pure MSVC + Cargo.

## Common Build & Verification Commands

```powershell
# Format check and strict Clippy lint gate across all workspace targets
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings

# Run all unit tests and Windows process/IPC/DB/stress integration tests
cargo test --workspace

# Build optimized stripped release binaries (target\release\helixgrid.exe & helixgrid-worker.exe)
cargo build --release -p helixgrid-worker -p helixgrid-studio

# Run headless end-to-end smoke test against release binaries
.\target\release\helixgrid.exe --smoke-test

# Launch GUI explicitly under hardware OpenGL (FemtoVG) or CPU software fallback
$env:SLINT_BACKEND = "winit-femtovg"
.\target\release\helixgrid.exe

$env:SLINT_BACKEND = "winit-software"
.\target\release\helixgrid.exe

# Run Criterion micro-benchmarks
cargo bench -p helixgrid-benchmarks
```

## Runtime CLI Options (`helixgrid.exe`)

- `--data-dir <PATH>` — Override local data root (defaults to `%LocalAppData%\HelixGrid\HelixGrid\data`).
- `--worker-exe <PATH>` — Override `helixgrid-worker.exe` binary path (defaults to resolving sibling `helixgrid-worker.exe` next to `helixgrid.exe`).
- `--smoke-test` — Initialize coordinator + worker pool, submit and complete a real CPU benchmark workload, shut down gracefully, and exit (`0` on pass).
