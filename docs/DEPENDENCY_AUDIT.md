# HelixGrid Dependency Audit

All workspace dependencies in `Cargo.toml` were verified against crates.io metadata (`cargo info`), checked for Windows 11 x64 compatibility via `cargo clippy --workspace --all-targets -- -D warnings` and `cargo build --release`, and inspected via `cargo tree -e normal,build -p helixgrid-studio -p helixgrid-worker`.

*(Note on Windows NTFS: This file serves canonically as `docs/DEPENDENCY_AUDIT.md`, as Windows NTFS treats `DEPENDENCY_AUDIT.md` and `dependency-audit.md` as the same file.)*

## Direct Workspace Dependencies

| Crate | Resolved Version | License | Purpose in HelixGrid | Audit Decision & Notes |
| --- | --- | --- | --- | --- |
| `tokio` | `1.54.1` | `MIT` | Multi-threaded async runtime, Windows Named Pipes, bounded `mpsc`/`watch` channels, timers | Retained; minimal required feature set (`fs`, `io-util`, `macros`, `net`, `process`, `rt`, `rt-multi-thread`, `signal`, `sync`, `time`) |
| `serde` | `1.0.229` | `MIT OR Apache-2.0` | Domain, config, and IPC message serialization | Retained (`derive` feature only) |
| `serde_json` | `1.0.154` | `MIT OR Apache-2.0` | IPC JSON payloads, result metadata, structured diagnostic exports | Retained |
| `uuid` | `1.29.2` | `Apache-2.0 OR MIT` | Strongly typed v4 IDs (`WorkloadId`, `WorkerId`, `ExecutionId`, `WorkflowId`) + CSPRNG IPC token entropy | Retained (`v4`, `serde`) |
| `thiserror` | `2.0.21` | `MIT OR Apache-2.0` | Typed error enums in library crates | Retained |
| `anyhow` | `1.0.104` | `MIT OR Apache-2.0` | Top-level error propagation in `helixgrid` and `helixgrid-worker` binaries | Retained in binaries only |
| `tracing` | `0.1.44` | `MIT` | Structured diagnostic instrumentation | Retained |
| `tracing-subscriber` | `0.3.23` | `MIT` | JSON rolling log formatting (`fmt`, `env-filter`, `json`) | Retained |
| `time` | `0.3.55` | `MIT OR Apache-2.0` | UTC timestamping (`OffsetDateTime`) and formatting | Retained (`formatting`, `macros`, `parsing`, `serde`) |
| `toml` | `1.2.1` (`1.2.1+spec-1.1.0`) | `MIT OR Apache-2.0` | Human-editable `config.toml` configuration load/save | Retained |
| `directories` | `6.0.0` | `MIT OR Apache-2.0` | Known-folder resolution (`%LocalAppData%\HelixGrid`) | Retained |
| `clap` | `4.6.7` | `MIT OR Apache-2.0` | CLI flag parsing (`--data-dir`, `--worker-exe`, `--smoke-test` in Studio; worker pipe flags in Worker) | Retained (`derive`) |
| `rusqlite` | `0.43.0` | `MIT` | Embedded SQLite WAL persistence + online consistent backup | Retained with `features = ["bundled", "backup"]` compiling native C `libsqlite3-sys 0.40.0` (Public Domain) |
| `slint` | `1.19.0` | `GPL-3.0-only OR LicenseRef-Slint-Royalty-free-2.0 OR LicenseRef-Slint-Software-3.0` | Declarative desktop UI runtime | Retained (`backend-winit`, `renderer-femtovg`, `renderer-software`, `accessibility`, `compat-1-2`). Compliance via `AboutSlint` component in `HxAboutDialog` (`LICENSE` updated). `skia` omitted to avoid LLVM/Clang build dependency on MSVC hosts. |
| `slint-build` | `1.19.0` | Same as `slint` | Build-time `.slint` compiler (`build.rs`) | Retained as `[build-dependencies]` of `helixgrid-studio` |
| `windows` | `0.62.2` | `MIT OR Apache-2.0` | Official Microsoft Win32/COM bindings (Job Objects, `CreateProcessW`, SDDL Named Pipes, `GlobalMemoryStatusEx`, `GetSystemTimes`, `GetProcessMemoryInfo`, `IFileOpenDialog`/`IFileSaveDialog`, `SPI_GETCLIENTAREAANIMATION`) | Retained with strict minimal feature flags in `Cargo.toml` |
| `windows-sys` | *(transitive only: `0.52.0` / `0.61.2`)* | `MIT OR Apache-2.0` | Not declared directly by HelixGrid | Pulled transitively by `tokio`, `mio`, `winit`, and `clipboard-win` |
| `winresource` | `0.1.34` | `MIT` | Embeds `.ico` icon & PE version metadata (`ProductName`, `FileDescription`, `FileVersion`) into `.exe` resources | Retained as build dependency for both binaries |
| `proptest` | `1.12.0` | `MIT OR Apache-2.0` | Property-based testing for IPC framing and DAG/state invariants | Dev dependency only (`helixgrid-protocol`, `tests`) |
| `tempfile` | `3.27.0` | `MIT OR Apache-2.0` | Isolated temporary directories in integration & unit tests | Dev/test/bench dependency only |
| `criterion` | `0.8.2` | `Apache-2.0 OR MIT` | Micro-benchmarks for scheduler, pipe IPC, SQLite, and solvers | Benchmark crate (`benchmarks`) only |

## Font Assets & Transitive License Summary

- **Poppins Font Family** (`assets/fonts/Poppins-{Regular,Medium,SemiBold,Bold}.ttf`): Licensed under **SIL Open Font License 1.1** (`assets/fonts/OFL.txt`, packaged into MSI `licenses\OFL.txt`).
- **Transitive Dependency Audit (`cargo tree`):** All transitive crates in `helixgrid-studio` and `helixgrid-worker` resolve strictly under permissive licenses (`MIT`, `Apache-2.0`, `BSD-2-Clause`, `BSD-3-Clause`, `BSL-1.0`, `Zlib`, `Unlicense`, `Unicode-3.0`, `0BSD`), plus SQLite (`Public Domain`) and `slint` (`LicenseRef-Slint-Royalty-free-2.0` / `GPL-3.0-only` dual option). No GPL-only or AGPL transitive libraries exist in the compiled binaries.
- **Explicitly Rejected Dependencies:** Electron, Tauri, WebView2, Node.js, Python, C#, and `tokio` full-feature bloat are excluded.
