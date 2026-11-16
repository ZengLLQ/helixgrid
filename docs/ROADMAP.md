# HelixGrid Roadmap

## v1.0 Foundation *(Completed in Workspace)*

- Modular Rust workspace with isolated domain, protocol, config, security, storage, resource, scheduler, executor, worker runtime, core actor, and Slint desktop crates.
- Real out-of-process Windows execution via suspended `CreateProcessW` + `WindowsJobObject` + SDDL-protected Named Pipe IPC + SQLite WAL persistence.
- Four production workload categories (`CpuBenchmark`, `BatchProcessing`, `NumericalComputation`, `RegisteredExecutable`) plus Kahn-validated DAG workflows.
- Desktop Studio (`Poppins`-only typography, Dark & Light themes, 8 screens, native file pickers, diagnostic export, online backup) + per-user WiX v5 MSI installer.

## Future Evolution Beyond v1

1. **Stronger Windows Process Isolation:** Optional AppContainer / Restricted Token launch profiles and scoped directory ACLs for untrusted external binaries.
2. **Plugin Compute Backends:** Trait-backed local GPU acceleration (Vulkan Compute or DirectML) integrating cleanly behind `GpuCapabilityStatus` once hardware capabilities are enumerated and verified.
3. **Artifact Streaming & Retention Policies:** Configurable compressed artifact archiving, disk-quota enforcement, and workflow JSON import/export templates.
4. **Signed Releases & Auto-Update:** Authenticode signing pipeline for `.exe`/`.msi` artifacts and hash-verified update checks.
5. **Distributed Transport Layer (Future Major Release):** Extending `helixgrid-protocol` (`PROTOCOL_VERSION >= 2`) and `SystemTelemetryProvider` / remote worker adapters to coordinate mTLS-authenticated peer nodes when multi-machine clustering is explicitly introduced.
