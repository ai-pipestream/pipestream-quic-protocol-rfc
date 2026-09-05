//! Non-default SQLite VFS shim. No SQL or application callback runs here.
//!
//! Safety boundary: SQLite supplies valid, appropriately aligned callback
//! arguments. A File is initialized only by open, and retains one Arc until
//! close. The underlying file uses SQLite's allocator and original Unix VFS.
//! Registered VFS storage lives for the process lifetime. No callback panics on
//! a lock failure or uses a caller path to open an unregistered file.

use super::*;
use rusqlite::ffi::*;
use std::{
    ffi::{CStr, c_char, c_int, c_void},
    ptr,
};

static REGISTERED: OnceLock<Result<(), c_int>> = OnceLock::new();

#[repr(C)]
struct Vfs {
    api: sqlite3_vfs,
    parent: *mut sqlite3_vfs,
}

#[repr(C)]
struct BoundedFile {
    api: sqlite3_file,
    real: *mut sqlite3_file,
    guard: *const Guard,
    limit: i64,
}

pub(super) fn register() -> Result<(), StoreError> {
    let result = REGISTERED.get_or_init(|| {
        // Unix's SHM mapper rounds 32 KiB regions to OS pages. Refuse platforms
        // outside the checked range instead of falling back to unbounded I/O.
        if !supported_pages() {
            return Err(SQLITE_CANTOPEN);
        }
        // SAFETY: SQLite initializes its global registry under its own mutex.
        // The named built-in Unix VFS remains registered for process lifetime.
        unsafe {
            let parent = sqlite3_vfs_find(c"unix".as_ptr());
            if parent.is_null()
                || (*parent).iVersion < 2
                || !sqlite3_vfs_find(c"pipestream-bounded-unix-v1".as_ptr()).is_null()
            {
                return Err(SQLITE_CANTOPEN);
            }
            // Do not copy the original structure: SQLite can change its pNext
            // concurrently while registering other VFS objects.
            let api = sqlite3_vfs {
                iVersion: 2,
                szOsFile: std::mem::size_of::<BoundedFile>() as c_int,
                mxPathname: (*parent).mxPathname,
                pNext: ptr::null_mut(),
                zName: c"pipestream-bounded-unix-v1".as_ptr(),
                pAppData: ptr::null_mut(),
                xOpen: Some(open),
                xDelete: Some(delete),
                xAccess: Some(access),
                xFullPathname: Some(full_path),
                xDlOpen: None,
                xDlError: None,
                xDlSym: None,
                xDlClose: None,
                xRandomness: Some(randomness),
                xSleep: Some(sleep),
                xCurrentTime: Some(current_time),
                xGetLastError: Some(last_error),
                xCurrentTimeInt64: Some(current_time_int64),
                xSetSystemCall: None,
                xGetSystemCall: None,
                xNextSystemCall: None,
            };
            let allocation = Box::into_raw(Box::new(Vfs { api, parent }));
            let result = sqlite3_vfs_register(ptr::addr_of_mut!((*allocation).api), 0);
            if result != SQLITE_OK {
                drop(Box::from_raw(allocation));
                return Err(result);
            }
        }
        Ok(())
    });
    result.map_err(|code| corrupt(&format!("bounded SQLite Unix VFS unavailable ({code})")))
}

#[cfg(unix)]
fn supported_pages() -> bool {
    unsafe extern "C" {
        fn getpagesize() -> c_int;
    }
    // SAFETY: getpagesize takes no arguments and returns an integer.
    let page = unsafe { getpagesize() };
    page > 0 && page <= 65536 && (page as u32).is_power_of_two()
}

#[cfg(not(unix))]
fn supported_pages() -> bool {
    false
}

// Each pass-through calls the original VFS with its own pointer and app data,
// not the wrapper. Required methods are part of the built-in Unix VFS ABI.
macro_rules! forward_vfs {
    ($name:ident, $method:ident, ($($arg:ident: $ty:ty),*) -> $ret:ty, $fallback:expr) => {
        unsafe extern "C" fn $name(vfs: *mut sqlite3_vfs, $($arg: $ty),*) -> $ret {
            // SAFETY: SQLite passes the registered Vfs object; delegated arguments are unchanged.
            unsafe {
                let parent = (*(vfs.cast::<Vfs>())).parent;
                match (*parent).$method { Some(call) => call(parent, $($arg),*), None => $fallback }
            }
        }
    };
}
forward_vfs!(delete, xDelete, (path: *const c_char, sync: c_int) -> c_int, SQLITE_IOERR_DELETE);
forward_vfs!(access, xAccess, (path: *const c_char, flags: c_int, out: *mut c_int) -> c_int, SQLITE_IOERR_ACCESS);
forward_vfs!(full_path, xFullPathname, (path: *const c_char, count: c_int, out: *mut c_char) -> c_int, SQLITE_CANTOPEN);
forward_vfs!(randomness, xRandomness, (count: c_int, out: *mut c_char) -> c_int, 0);
forward_vfs!(sleep, xSleep, (micros: c_int) -> c_int, 0);
forward_vfs!(current_time, xCurrentTime, (out: *mut f64) -> c_int, SQLITE_ERROR);
forward_vfs!(last_error, xGetLastError, (count: c_int, out: *mut c_char) -> c_int, 0);
forward_vfs!(current_time_int64, xCurrentTimeInt64, (out: *mut i64) -> c_int, SQLITE_ERROR);

fn lookup(path: &Path) -> Option<(Arc<Guard>, u64)> {
    let stores = STORES.get()?.lock().ok()?;
    for guard in stores.values().filter_map(Weak::upgrade) {
        // SHM is opened only by xShmMap. Temporary files, ATTACH aliases and
        // super-journals have no budget and cannot be opened through this VFS.
        for (file, limit) in guard.paths[..3].iter().zip(guard.limits.values()) {
            if file == path {
                return Some((guard.clone(), limit));
            }
        }
    }
    None
}

unsafe extern "C" fn open(
    vfs: *mut sqlite3_vfs,
    name: sqlite3_filename,
    file: *mut sqlite3_file,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    // SAFETY: SQLite allocates szOsFile bytes and supplies a terminated filename
    // valid until xClose. The real file is allocated separately with SQLite's
    // allocator to preserve its alignment independently of wrapper layout.
    unsafe {
        (*file).pMethods = ptr::null();
        if name.is_null() {
            return SQLITE_CANTOPEN;
        }
        let Ok(path) = CStr::from_ptr(name).to_str() else {
            return SQLITE_CANTOPEN;
        };
        let Some((guard, limit)) = lookup(Path::new(path)) else {
            return SQLITE_CANTOPEN;
        };
        match checked_length(Path::new(path)) {
            Ok(None) => {}
            Ok(Some(size)) if size <= limit => {}
            _ => return SQLITE_CANTOPEN,
        }
        let parent = (*(vfs.cast::<Vfs>())).parent;
        let Some(call) = (*parent).xOpen else {
            return SQLITE_CANTOPEN;
        };
        let real = sqlite3_malloc64((*parent).szOsFile as u64).cast::<sqlite3_file>();
        if real.is_null() {
            return SQLITE_NOMEM;
        }
        ptr::write_bytes(real.cast::<u8>(), 0, (*parent).szOsFile as usize);
        let result = call(parent, name, real, flags, out);
        if result != SQLITE_OK || (*real).pMethods.is_null() {
            if !(*real).pMethods.is_null()
                && let Some(close) = (*(*real).pMethods).xClose
            {
                close(real);
            }
            sqlite3_free(real.cast());
            return if result == SQLITE_OK {
                SQLITE_CANTOPEN
            } else {
                result
            };
        }
        ptr::write(
            file.cast::<BoundedFile>(),
            BoundedFile {
                api: sqlite3_file { pMethods: &METHODS },
                real,
                guard: Arc::into_raw(guard),
                limit: limit as i64,
            },
        );
        // Disable underlying preallocation rounding and database mappings before
        // SQLite can use size hints. WAL shared memory has its own guarded path.
        let mut chunk: c_int = 0;
        let mut mmap: i64 = 0;
        control(
            file,
            SQLITE_FCNTL_CHUNK_SIZE,
            ptr::addr_of_mut!(chunk).cast(),
        );
        control(file, SQLITE_FCNTL_MMAP_SIZE, ptr::addr_of_mut!(mmap).cast());
        SQLITE_OK
    }
}

unsafe extern "C" fn close(file: *mut sqlite3_file) -> c_int {
    // SAFETY: every successfully initialized wrapper owns exactly one real
    // file and Arc; SQLite calls xClose once, including failed-open cleanup.
    unsafe {
        let wrapper = &mut *file.cast::<BoundedFile>();
        let result = match (*(*wrapper.real).pMethods).xClose {
            Some(call) => call(wrapper.real),
            None => SQLITE_IOERR_CLOSE,
        };
        sqlite3_free(wrapper.real.cast());
        drop(Arc::from_raw(wrapper.guard));
        wrapper.api.pMethods = ptr::null();
        result
    }
}

macro_rules! forward_file {
    ($name:ident, $method:ident, ($($arg:ident: $ty:ty),*) -> $ret:ty, $fallback:expr) => {
        unsafe extern "C" fn $name(file: *mut sqlite3_file, $($arg: $ty),*) -> $ret {
            // SAFETY: a live wrapper owns the underlying file and its methods.
            unsafe {
                let real = (*file.cast::<BoundedFile>()).real;
                match (*(*real).pMethods).$method { Some(call) => call(real, $($arg),*), None => $fallback }
            }
        }
    };
}
forward_file!(read, xRead, (buffer: *mut c_void, count: c_int, offset: i64) -> c_int, SQLITE_IOERR_READ);
forward_file!(sync, xSync, (flags: c_int) -> c_int, SQLITE_IOERR_FSYNC);
forward_file!(file_size, xFileSize, (out: *mut i64) -> c_int, SQLITE_IOERR_FSTAT);
forward_file!(lock, xLock, (level: c_int) -> c_int, SQLITE_IOERR_LOCK);
forward_file!(unlock, xUnlock, (level: c_int) -> c_int, SQLITE_IOERR_UNLOCK);
forward_file!(reserved, xCheckReservedLock, (out: *mut c_int) -> c_int, SQLITE_IOERR_CHECKRESERVEDLOCK);
forward_file!(sector_size, xSectorSize, () -> c_int, 4096);
forward_file!(characteristics, xDeviceCharacteristics, () -> c_int, 0);
forward_file!(shm_lock, xShmLock, (offset: c_int, count: c_int, flags: c_int) -> c_int, SQLITE_IOERR_SHMLOCK);
forward_file!(shm_barrier, xShmBarrier, () -> (), ());
forward_file!(shm_unmap, xShmUnmap, (delete: c_int) -> c_int, SQLITE_IOERR_SHMMAP);

unsafe extern "C" fn write(
    file: *mut sqlite3_file,
    buffer: *const c_void,
    count: c_int,
    offset: i64,
) -> c_int {
    // SAFETY: only validated growth is forwarded; SQLite owns the buffer.
    unsafe {
        let wrapper = &*file.cast::<BoundedFile>();
        if count < 0
            || offset < 0
            || offset
                .checked_add(i64::from(count))
                .is_none_or(|end| end > wrapper.limit)
        {
            return SQLITE_FULL;
        }
        match (*(*wrapper.real).pMethods).xWrite {
            Some(call) => call(wrapper.real, buffer, count, offset),
            None => SQLITE_IOERR_WRITE,
        }
    }
}

unsafe extern "C" fn truncate(file: *mut sqlite3_file, size: i64) -> c_int {
    // SAFETY: the underlying chunk size is held at zero, so there is no
    // upward rounding after this check, including truncate-as-enlarge calls.
    unsafe {
        let wrapper = &*file.cast::<BoundedFile>();
        if size < 0 || size > wrapper.limit {
            return SQLITE_FULL;
        }
        match (*(*wrapper.real).pMethods).xTruncate {
            Some(call) => call(wrapper.real, size),
            None => SQLITE_IOERR_TRUNCATE,
        }
    }
}

unsafe extern "C" fn control(file: *mut sqlite3_file, op: c_int, arg: *mut c_void) -> c_int {
    // SAFETY: the documented SQLite opcode determines the argument type.
    // Internal growth hints never bypass the pre-write guard.
    unsafe {
        let wrapper = &*file.cast::<BoundedFile>();
        let Some(call) = (*(*wrapper.real).pMethods).xFileControl else {
            return SQLITE_NOTFOUND;
        };
        match op {
            SQLITE_FCNTL_SIZE_HINT => {
                let size = *arg.cast::<i64>();
                if size < 0 || size > wrapper.limit {
                    return SQLITE_FULL;
                }
                // A hint is optional. Ignore it instead of letting the backend
                // perform fallocate/mmap writes outside xWrite/xTruncate.
                SQLITE_OK
            }
            SQLITE_FCNTL_CHUNK_SIZE => {
                let mut zero: c_int = 0;
                call(wrapper.real, op, ptr::addr_of_mut!(zero).cast())
            }
            SQLITE_FCNTL_MMAP_SIZE => {
                let mut zero: i64 = 0;
                let result = call(wrapper.real, op, ptr::addr_of_mut!(zero).cast());
                *arg.cast::<i64>() = 0;
                result
            }
            // Only the audited non-growth Unix controls pass through. Future
            // backend controls are opt-in, not silently inherited by the guard.
            SQLITE_FCNTL_LOCKSTATE
            | SQLITE_FCNTL_LAST_ERRNO
            | SQLITE_FCNTL_PERSIST_WAL
            | SQLITE_FCNTL_POWERSAFE_OVERWRITE
            | SQLITE_FCNTL_VFSNAME
            | SQLITE_FCNTL_HAS_MOVED
            | SQLITE_FCNTL_LOCK_TIMEOUT
            | SQLITE_FCNTL_EXTERNAL_READER => call(wrapper.real, op, arg),
            _ => SQLITE_NOTFOUND,
        }
    }
}

unsafe extern "C" fn shm_map(
    file: *mut sqlite3_file,
    page: c_int,
    size: c_int,
    extend: c_int,
    out: *mut *mut c_void,
) -> c_int {
    // SAFETY: only Unix's 32 KiB WAL-index regions are supported. Registration
    // verifies OS pages <=64 KiB; rounding up to 64 KiB bounds its real mapping.
    unsafe {
        *out = ptr::null_mut();
        let wrapper = &*file.cast::<BoundedFile>();
        let guard = &*wrapper.guard;
        let end = (i64::from(page) + 1) * i64::from(size);
        let rounded = ((end + 65535) / 65536) * 65536;
        if page < 0 || size != 32768 || rounded as u64 > guard.limits.shared_memory_bytes {
            return SQLITE_FULL;
        }
        match checked_length(&guard.paths[3]) {
            Ok(None) => {}
            Ok(Some(length)) if length <= guard.limits.shared_memory_bytes => {}
            _ => return SQLITE_IOERR_SHMMAP,
        }
        let methods = &*(*wrapper.real).pMethods;
        if methods.iVersion < 2 {
            return SQLITE_IOERR_SHMMAP;
        }
        match methods.xShmMap {
            Some(call) => call(wrapper.real, page, size, extend, out),
            None => SQLITE_IOERR_SHMMAP,
        }
    }
}

// Version 2 deliberately has no database mmap/fetch path.
static METHODS: sqlite3_io_methods = sqlite3_io_methods {
    iVersion: 2,
    xClose: Some(close),
    xRead: Some(read),
    xWrite: Some(write),
    xTruncate: Some(truncate),
    xSync: Some(sync),
    xFileSize: Some(file_size),
    xLock: Some(lock),
    xUnlock: Some(unlock),
    xCheckReservedLock: Some(reserved),
    xFileControl: Some(control),
    xSectorSize: Some(sector_size),
    xDeviceCharacteristics: Some(characteristics),
    xShmMap: Some(shm_map),
    xShmLock: Some(shm_lock),
    xShmBarrier: Some(shm_barrier),
    xShmUnmap: Some(shm_unmap),
    xFetch: None,
    xUnfetch: None,
};
