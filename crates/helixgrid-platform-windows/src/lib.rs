pub mod dialogs;
pub mod job;
pub mod pipe;
pub mod process;
pub mod telemetry;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PlatformError {
    #[error("Windows API error: {0}")]
    Win32(#[from] windows::core::Error),

    #[error("platform I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("unsupported platform operation: {0}")]
    Unsupported(String),
}

/// Checks Windows system accessibility preference for client-area animations.
/// Returns `true` when the user prefers reduced motion.
#[must_use]
pub fn is_system_reduced_motion_enabled() -> bool {
    #[cfg(windows)]
    {
        use windows::Win32::UI::WindowsAndMessaging::{
            SPI_GETCLIENTAREAANIMATION, SYSTEM_PARAMETERS_INFO_UPDATE_FLAGS, SystemParametersInfoW,
        };
        let mut animations_enabled: i32 = 1;
        // SAFETY: Passing a valid mutable pointer to a 4-byte BOOL/i32 for SPI_GETCLIENTAREAANIMATION.
        let ok = unsafe {
            SystemParametersInfoW(
                SPI_GETCLIENTAREAANIMATION,
                0,
                Some((&mut animations_enabled as *mut i32).cast()),
                SYSTEM_PARAMETERS_INFO_UPDATE_FLAGS(0),
            )
        };
        ok.is_ok() && animations_enabled == 0
    }
    #[cfg(not(windows))]
    {
        false
    }
}

/// Returns human-readable Windows OS version info for Diagnostics.
#[must_use]
pub fn windows_platform_summary() -> String {
    format!("Windows {} ({})", std::env::consts::OS, std::env::consts::ARCH)
}
