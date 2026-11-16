use crate::PlatformError;
use std::ffi::c_void;
use tokio::net::windows::named_pipe::{NamedPipeServer, PipeMode, ServerOptions};
use windows::Win32::Foundation::{CloseHandle, HANDLE, HLOCAL, LocalFree};
use windows::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
};
use windows::Win32::Security::{
    GetTokenInformation, PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER,
    TokenUser,
};
use windows::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
use windows::core::{PCWSTR, PWSTR};

/// Queries the current process user SID in SDDL string form so named pipes can restrict
/// access exclusively to the current Windows user account.
pub fn current_user_sid_string() -> Result<String, PlatformError> {
    let mut token = HANDLE::default();
    // SAFETY: GetCurrentProcess() returns pseudo-handle requiring no closing; OpenProcessToken opens query handle.
    unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) }?;

    let mut needed: u32 = 0;
    let _ = unsafe { GetTokenInformation(token, TokenUser, None, 0, &mut needed) };
    if needed == 0 {
        unsafe {
            let _ = CloseHandle(token);
        }
        return Err(PlatformError::InvalidArgument(
            "GetTokenInformation returned zero size".to_string(),
        ));
    }

    let mut buf = vec![0u8; needed as usize];
    let info_res = unsafe {
        GetTokenInformation(
            token,
            TokenUser,
            Some(buf.as_mut_ptr().cast::<c_void>()),
            needed,
            &mut needed,
        )
    };
    unsafe {
        let _ = CloseHandle(token);
    }
    info_res?;

    // SAFETY: `buf` is populated by GetTokenInformation(TokenUser) and begins with TOKEN_USER.
    let token_user = unsafe { &*(buf.as_ptr().cast::<TOKEN_USER>()) };
    let mut sid_pwstr = PWSTR::null();
    // SAFETY: `token_user.User.Sid` points into `buf`; ConvertSidToStringSidW allocates `sid_pwstr`.
    unsafe { ConvertSidToStringSidW(token_user.User.Sid, &mut sid_pwstr) }?;

    let sid_str = unsafe { sid_pwstr.to_string() }
        .map_err(|e| PlatformError::InvalidArgument(format!("SID UTF-16 decode error: {e}")))?;

    // SAFETY: Free string allocated by ConvertSidToStringSidW.
    unsafe {
        let _ = LocalFree(Some(HLOCAL(sid_pwstr.0.cast::<c_void>())));
    }
    Ok(sid_str)
}

/// Creates a Windows Named Pipe server instance bound strictly to the local machine
/// (`reject_remote_clients`), restricted via SDDL DACL to the current user SID,
/// and optionally asserting `first_pipe_instance`.
pub fn create_secure_pipe_server(
    pipe_name: &str,
    first_instance: bool,
) -> Result<NamedPipeServer, PlatformError> {
    let user_sid = current_user_sid_string()?;
    let sddl = format!("D:(A;;GA;;;{user_sid})");
    let sddl_wide: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();

    let mut sd = PSECURITY_DESCRIPTOR::default();
    // SAFETY: `sddl_wide` is null-terminated UTF-16; `sd` receives a LocalAlloc security descriptor.
    unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            PCWSTR(sddl_wide.as_ptr()),
            SDDL_REVISION_1,
            &mut sd,
            None,
        )
    }?;

    let mut sa = SECURITY_ATTRIBUTES {
        nLength: u32::try_from(std::mem::size_of::<SECURITY_ATTRIBUTES>()).unwrap_or(0),
        lpSecurityDescriptor: sd.0,
        bInheritHandle: false.into(),
    };

    let mut opts = ServerOptions::new();
    opts.pipe_mode(PipeMode::Byte)
        .reject_remote_clients(true)
        .first_pipe_instance(first_instance)
        .max_instances(1)
        .in_buffer_size(64 * 1024)
        .out_buffer_size(64 * 1024);

    // SAFETY: `sa` and its `lpSecurityDescriptor` stay valid through `create_with_security_attributes_raw`.
    let pipe_res = unsafe {
        opts.create_with_security_attributes_raw(
            pipe_name,
            (&mut sa as *mut SECURITY_ATTRIBUTES).cast(),
        )
    };

    // SAFETY: Security descriptor allocated by ConvertStringSecurityDescriptorToSecurityDescriptorW must be freed via LocalFree.
    unsafe {
        let _ = LocalFree(Some(HLOCAL(sd.0)));
    }

    Ok(pipe_res?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn secure_named_pipe_accepts_same_user_client() {
        let pipe_name = format!(r"\\.\pipe\helixgrid-test-{}", Uuid::new_v4());
        let server = create_secure_pipe_server(&pipe_name, true).unwrap();
        let client =
            tokio::net::windows::named_pipe::ClientOptions::new().open(&pipe_name).unwrap();
        server.connect().await.unwrap();
        drop(client);
    }
}
