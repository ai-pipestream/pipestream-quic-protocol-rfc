use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
};

// Each importing test executable contains one test. This measures Rust heap,
// not native allocations, and does not equate allocated Rust bytes with RSS.
struct Allocator;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static LARGEST: AtomicUsize = AtomicUsize::new(0);
fn acquired(size: usize) {
    let live = LIVE.fetch_add(size, Ordering::SeqCst) + size;
    PEAK.fetch_max(live, Ordering::SeqCst);
    LARGEST.fetch_max(size, Ordering::SeqCst);
}
// SAFETY: all operations and layouts pass unchanged to System. Counters observe
// successful allocation without owning the returned pointers.
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            acquired(layout.size());
        }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            acquired(layout.size());
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        unsafe {
            System.dealloc(p, layout);
        }
        LIVE.fetch_sub(layout.size(), Ordering::SeqCst);
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(p, layout, size) };
        if !result.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::SeqCst);
            acquired(size);
        }
        result
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

pub struct Sample {
    baseline: usize,
}
impl Sample {
    pub fn start() -> Self {
        let baseline = LIVE.load(Ordering::SeqCst);
        PEAK.store(baseline, Ordering::SeqCst);
        LARGEST.store(0, Ordering::SeqCst);
        Self { baseline }
    }
    pub fn finish(self) -> (usize, usize) {
        (
            PEAK.load(Ordering::SeqCst).saturating_sub(self.baseline),
            LARGEST.load(Ordering::SeqCst),
        )
    }
}
