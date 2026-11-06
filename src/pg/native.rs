use crate::contracts::{Error, ErrorKind, Result};
use std::ffi::c_void;

pub(super) fn check() -> Result<()> {
    // SAFETY: RTLD_DEFAULT searches loaded objects; the symbol name is a static C string.
    let marker = unsafe { libc::dlsym(libc::RTLD_DEFAULT, c"PQrpgmigrateCapabilities".as_ptr()) };
    validate_marker(marker)
}

fn validate_marker(marker: *mut c_void) -> Result<()> {
    if marker.is_null() {
        return Err(configuration_error());
    }
    let connect = pq_sys::PQconnectStart as *const () as *const c_void;
    let mut marker_info = std::mem::MaybeUninit::<libc::Dl_info>::uninit();
    let mut connect_info = std::mem::MaybeUninit::<libc::Dl_info>::uninit();
    // SAFETY: both addresses come from loaded symbols, and the outputs have Dl_info storage.
    let found = unsafe {
        libc::dladdr(marker, marker_info.as_mut_ptr()) != 0
            && libc::dladdr(connect, connect_info.as_mut_ptr()) != 0
    };
    if !found {
        return Err(configuration_error());
    }
    // SAFETY: successful dladdr calls initialized both outputs; no pathname is read or logged.
    let (marker_info, connect_info) =
        unsafe { (marker_info.assume_init(), connect_info.assume_init()) };
    if marker_info.dli_fbase.is_null() || marker_info.dli_fbase != connect_info.dli_fbase {
        return Err(configuration_error());
    }
    // SAFETY: this non-null symbol belongs to libpq and has the agreed unsigned int(void) C ABI.
    let capabilities = unsafe {
        let function: unsafe extern "C" fn() -> libc::c_uint = std::mem::transmute(marker);
        function()
    };
    validate_capabilities(capabilities)
}

fn validate_capabilities(capabilities: libc::c_uint) -> Result<()> {
    if capabilities & 1 == 0 {
        return Err(configuration_error());
    }
    Ok(())
}

fn configuration_error() -> Error {
    Error {
        kind: ErrorKind::BadConfig,
        message: "native libpq lacks the required passfile redaction capability".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn missing_native_marker_is_a_configuration_failure() {
        let failure = validate_marker(std::ptr::null_mut()).unwrap_err();
        assert_eq!(failure.kind, ErrorKind::BadConfig);
    }

    #[test]
    fn unrelated_marker_is_rejected_without_being_called() {
        static CALLED: AtomicBool = AtomicBool::new(false);
        unsafe extern "C" fn foreign_marker() -> libc::c_uint {
            CALLED.store(true, Ordering::Relaxed);
            1
        }
        let failure = validate_marker(foreign_marker as *const () as *mut c_void).unwrap_err();
        assert_eq!(failure.kind, ErrorKind::BadConfig);
        assert!(!CALLED.load(Ordering::Relaxed));
    }

    #[test]
    fn only_the_required_capability_allows_connection() {
        for capabilities in [0, 2, 4] {
            assert_eq!(
                validate_capabilities(capabilities).unwrap_err().kind,
                ErrorKind::BadConfig
            );
        }
        assert!(validate_capabilities(1).is_ok());
        assert!(validate_capabilities(3).is_ok());
    }
}
