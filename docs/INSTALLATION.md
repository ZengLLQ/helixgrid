# Installing & Uninstalling HelixGrid

HelixGrid ships as a signed-ready, per-user Windows x64 MSI installer (`helixgrid-0.1.0-x64.msi`) generated with WiX Toolset v5 (`packaging/helixgrid.wxs` and `packaging/build-msi.ps1`).

## Building the Installer

```powershell
# Full release build + MSI packaging
powershell -NoProfile -ExecutionPolicy Bypass -File .\packaging\build-msi.ps1

# Package previously built release binaries
powershell -NoProfile -ExecutionPolicy Bypass -File .\packaging\build-msi.ps1 -SkipCargoBuild
```

Output is written to `packaging\out\helixgrid-0.1.0-x64.msi` (`9,601,024 bytes` / `~9.16 MiB`).

## What Gets Installed & Where

- **Scope:** `perUser` (installs cleanly without UAC administrator elevation).
- **Program Directory:** `%LocalAppData%\Programs\HelixGrid\`
  - `helixgrid.exe` (Desktop Studio application)
  - `helixgrid-worker.exe` (Isolated compute worker runtime)
  - `LICENSE` (HelixGrid dual license & third-party component notices)
  - `licenses\OFL.txt` (SIL Open Font License 1.1 for bundled Poppins fonts)
- **Shortcuts:** Start Menu (`HelixGrid` + `Uninstall HelixGrid`) and Desktop (`HelixGrid`).
- **HKCU Registry Marker:** `HKCU\Software\HelixGrid\InstallDir` (used for MSI key paths and clean component removal).

## Silent Installation & Uninstallation (Verified)

```powershell
# Silent per-user install
msiexec /i .\packaging\out\helixgrid-0.1.0-x64.msi /qn

# Verify installed CLI smoke test
& "$env:LOCALAPPDATA\Programs\HelixGrid\helixgrid.exe" --smoke-test

# Silent uninstall (cleanly removes Programs\HelixGrid and shortcuts)
msiexec /x .\packaging\out\helixgrid-0.1.0-x64.msi /qn
```

## User Data Preservation

Runtime SQLite databases (`helixgrid.db`), `config.toml`, rotating JSON logs (`logs\`), execution artifacts (`artifacts\`), consistent online backups (`backups\`), and diagnostic bundles (`exports\`) are stored under `%LocalAppData%\HelixGrid\HelixGrid\data\` and are **preserved** across MSI upgrades and uninstalls so historical job records and artifacts are never silently deleted.
