use crate::PlatformError;
use std::path::PathBuf;
use windows::Win32::Foundation::HWND;
use windows::Win32::System::Com::{
    CLSCTX_ALL, COINIT_APARTMENTTHREADED, CoCreateInstance, CoInitializeEx, CoTaskMemFree,
    CoUninitialize,
};
use windows::Win32::UI::Shell::{
    FOS_FILEMUSTEXIST, FOS_FORCEFILESYSTEM, FOS_OVERWRITEPROMPT, FOS_PICKFOLDERS, FileOpenDialog,
    FileSaveDialog, IFileOpenDialog, IFileSaveDialog, SIGDN_FILESYSPATH,
};
use windows::core::PCWSTR;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileDialogMode {
    OpenFile,
    OpenFolder,
    SaveFile,
}

/// Opens a native Windows File Dialog (`IFileOpenDialog` / `IFileSaveDialog`) on a dedicated STA thread
/// so callers never block or fail due to MTA/STA conflicts.
pub fn show_native_file_dialog(
    mode: FileDialogMode,
    title: &str,
    default_filename: Option<&str>,
) -> Result<Option<PathBuf>, PlatformError> {
    let title = title.to_string();
    let default_filename = default_filename.map(str::to_string);

    let handle =
        std::thread::spawn(move || run_dialog_sta(mode, &title, default_filename.as_deref()));
    handle
        .join()
        .map_err(|_| PlatformError::InvalidArgument("dialog thread panicked".to_string()))?
}

fn run_dialog_sta(
    mode: FileDialogMode,
    title: &str,
    default_filename: Option<&str>,
) -> Result<Option<PathBuf>, PlatformError> {
    // SAFETY: Dedicated thread initializes COM STA and balances with CoUninitialize.
    let hr = unsafe { CoInitializeEx(None, COINIT_APARTMENTTHREADED) };
    let need_uninit = hr.is_ok();

    let result = (|| -> Result<Option<PathBuf>, PlatformError> {
        let title_w: Vec<u16> = title.encode_utf16().chain(Some(0)).collect();
        match mode {
            FileDialogMode::OpenFile | FileDialogMode::OpenFolder => {
                // SAFETY: COM STA initialized; creates standard FileOpenDialog CLSID.
                let dialog: IFileOpenDialog =
                    unsafe { CoCreateInstance(&FileOpenDialog, None, CLSCTX_ALL) }?;
                unsafe {
                    let mut opts = dialog.GetOptions()?;
                    opts |= FOS_FORCEFILESYSTEM | FOS_FILEMUSTEXIST;
                    if mode == FileDialogMode::OpenFolder {
                        opts |= FOS_PICKFOLDERS;
                    }
                    dialog.SetOptions(opts)?;
                    dialog.SetTitle(PCWSTR(title_w.as_ptr()))?;
                    if let Err(e) = dialog.Show(Some(HWND::default())) {
                        // ERROR_CANCELLED = 0x800704C7
                        if e.code().0 as u32 == 0x8007_04C7 {
                            return Ok(None);
                        }
                        return Err(PlatformError::Win32(e));
                    }
                    let item = dialog.GetResult()?;
                    let pwstr = item.GetDisplayName(SIGDN_FILESYSPATH)?;
                    let path_str = pwstr.to_string().unwrap_or_default();
                    CoTaskMemFree(Some(pwstr.0.cast()));
                    if path_str.is_empty() { Ok(None) } else { Ok(Some(PathBuf::from(path_str))) }
                }
            }
            FileDialogMode::SaveFile => {
                let dialog: IFileSaveDialog =
                    unsafe { CoCreateInstance(&FileSaveDialog, None, CLSCTX_ALL) }?;
                unsafe {
                    let mut opts = dialog.GetOptions()?;
                    opts |= FOS_FORCEFILESYSTEM | FOS_OVERWRITEPROMPT;
                    dialog.SetOptions(opts)?;
                    dialog.SetTitle(PCWSTR(title_w.as_ptr()))?;
                    if let Some(name) = default_filename {
                        let name_w: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
                        dialog.SetFileName(PCWSTR(name_w.as_ptr()))?;
                    }
                    if let Err(e) = dialog.Show(Some(HWND::default())) {
                        if e.code().0 as u32 == 0x8007_04C7 {
                            return Ok(None);
                        }
                        return Err(PlatformError::Win32(e));
                    }
                    let item = dialog.GetResult()?;
                    let pwstr = item.GetDisplayName(SIGDN_FILESYSPATH)?;
                    let path_str = pwstr.to_string().unwrap_or_default();
                    CoTaskMemFree(Some(pwstr.0.cast()));
                    if path_str.is_empty() { Ok(None) } else { Ok(Some(PathBuf::from(path_str))) }
                }
            }
        }
    })();

    if need_uninit {
        // SAFETY: Matches CoInitializeEx above.
        unsafe { CoUninitialize() };
    }
    result
}
