# HelixGrid Security Model & Trust Boundaries

HelixGrid follows defense-in-depth tailored to single-machine Windows workstation execution, paired with explicit documentation of what OS isolation primitives do and do not guarantee.

## 1. Local IPC Isolation (Windows Named Pipes)

- Named Pipes are created in `crates/helixgrid-platform-windows/src/pipe.rs` (`create_secure_pipe_server`).
- **Network Rejection:** `ServerOptions::reject_remote_clients(true)` blocks all SMB/network client connections at the kernel pipe layer.
- **Current-User DACL:** Queries the running user's SID via `OpenProcessToken` + `GetTokenInformation(TokenUser)` + `ConvertSidToStringSidW`, builds an SDDL DACL `D:(A;;GA;;;{user_sid})`, and applies it directly at pipe creation time via `ConvertStringSecurityDescriptorToSecurityDescriptorW` + `SECURITY_ATTRIBUTES`.
- **Single Instance Ownership:** Uses `first_pipe_instance(true)` on the primary server instance and `max_instances(1)` to prevent pipe-squatting attacks.
- **256-Bit Session Authentication:** Every spawned worker receives a CSPRNG-backed 256-bit hex token (`generate_ipc_token`) and authenticates via `WorkerRegistration`. `WorkerPoolSupervisor` checks tokens in constant time (`constant_time_eq`) and rejects mismatches (`WorkerIpcAuthRejected`).
- **Bounded Message Framing:** `helixgrid-protocol` enforces `PROTOCOL_VERSION == 1`, validates `[u32 LE length][payload]`, rejects frames exceeding `MAX_FRAME_BYTES` (`4 MiB`), and caps per-worker Tokio channel depth at `MAX_IPC_CHANNEL_CAPACITY` (`256`).

## 2. Process Containment & Honest Job Object Limitations

- Workers are launched suspended via `CreateProcessW(..., CREATE_NO_WINDOW | CREATE_SUSPENDED, ...)` and assigned to an unnamed Win32 `WindowsJobObject` (`AssignProcessToJobObject`) **before** `ResumeThread` allows worker user-mode code to execute.
- Configured Job Object limits:
  - `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` — Guarantees no orphaned workers or child processes survive coordinator exit or crash.
  - `JOB_OBJECT_LIMIT_PROCESS_MEMORY` — Enforces hard committed per-process memory cap.
  - `JOB_OBJECT_LIMIT_ACTIVE_PROCESS` — Caps active processes inside the job object.
  - `JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP` — Caps worker job CPU share percentage.
- **Explicit Limitation:** Win32 Job Objects provide resource governance and process-tree lifecycle binding; **they are NOT a complete OS security sandbox**. Workers still execute under the invoking user's standard token without AppContainer virtualization, filesystem redirection, or Restricted Token SID stripping.

## 3. Path & Input Validation (`crates/helixgrid-security`)

- **Path Traversal Guard (`safe_join_under`):** Rejects empty strings, NUL bytes, Windows root prefixes (`\`, `C:\`), parent directory components (`..`), and symlink/junction escapes outside the designated workspace/artifact root.
- **Allowlisted External Executables (`validate_registered_executable`):**
  - When `require_explicit_executable_allowlist = true` (default), external executables must match an `enabled` entry in `AppConfig.registered_executables`.
  - Resolves and checks canonical paths for both executable and `working_dir` (requiring `working_dir` to reside strictly within `entry.allowed_working_dir`), verifies target is an existing regular file, and blocks control characters (`\r`, `\n`, `\0`) in arguments.
  - **Disallowed Shell Interpreters:** Explicitly blocks shell and scripting hosts by basename (even if an administrator attempts to register them): `cmd.exe`, `cmd`, `powershell.exe`, `powershell`, `pwsh.exe`, `pwsh`, `wscript.exe`, `cscript.exe`, `mshta.exe`, `sh.exe`, `bash.exe`, `wsl.exe`.

## 4. Environment Scrubbing & Safe Retry Rules

- **Scrubbed Process Environment (`build_sanitized_env`):** External executables do **not** inherit the full parent process environment. Only `SAFE_INHERITED_ENV_VARS` (`SYSTEMROOT`, `SYSTEMDRIVE`, `WINDIR`, `OS`, `COMSPEC`, `NUMBER_OF_PROCESSORS`, `PROCESSOR_ARCHITECTURE`) pass through by default, while `env_overrides` reject overrides matching sensitive prefixes (`AWS_`, `AZURE_`, `GCP_`, `GOOGLE_`, `GITHUB_`, `GITLAB_`, `OPENAI_`, `ANTHROPIC_`, `STRIPE_`, `DATABASE_URL`, `SECRET`, `TOKEN`, `PASSWORD`, `PRIVATE_KEY`) as well as `PATH` / `COMSPEC` hijacks.
- **Retry Safety Policy (`verify_safe_retry`):** External executables with `max_retries > 0` are rejected unless explicitly classified `RetrySafety::SafeIdempotent`, preventing accidental double-execution of side-effecting binaries after a worker crash.

## 5. Artifact Integrity Verification

- Workers compute FIPS 180-4 SHA-256 digests (`sha256_bytes` / `sha256_file_hex`) for every generated result artifact and transmit digests alongside byte sizes so persisted artifacts in SQLite carry tamper-evident cryptographic hashes.
