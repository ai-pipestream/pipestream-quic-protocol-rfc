//! Hold the last Arc's destructor after its strong count reaches zero.
use super::*;
use std::{sync::mpsc, thread, time::Duration};

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Dropping,
    Waiting,
    Opened,
}

struct Gate {
    events: mpsc::Sender<Event>,
    release: Mutex<Option<mpsc::Receiver<()>>>,
}

fn gates() -> &'static Mutex<BTreeMap<PathBuf, Arc<Gate>>> {
    static GATES: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Gate>>>> = OnceLock::new();
    GATES.get_or_init(Mutex::default)
}

pub(crate) fn before_drop(path: &Path) {
    let gate = gates().lock().unwrap().get(path).cloned();
    if let Some(gate) = gate {
        let release = gate.release.lock().unwrap().take();
        if let Some(release) = release {
            gate.events.send(Event::Dropping).unwrap();
            release.recv_timeout(DEADLINE).unwrap();
        }
    }
}

pub(crate) fn before_wait(path: &Path) {
    let gate = gates().lock().unwrap().get(path).cloned();
    if let Some(gate) = gate {
        gate.events.send(Event::Waiting).unwrap();
    }
}

#[test]
fn reopen_waits_for_last_handle_to_release_its_os_lock() {
    let dir = tempfile::tempdir().unwrap();
    let root = RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap();
    install(&root, None, "retained", 1, b"retained body").unwrap();
    let usage = root.usage(None).unwrap();
    let path = root.path.clone();
    let (events, observed) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    gates().lock().unwrap().insert(
        path.clone(),
        Arc::new(Gate {
            events: events.clone(),
            release: Mutex::new(Some(blocked)),
        }),
    );
    let dropping = thread::spawn(move || drop(root));
    assert_eq!(observed.recv_timeout(DEADLINE).unwrap(), Event::Dropping);
    let openings: Vec<_> = (0..2)
        .map(|_| {
            let opening_path = path.clone();
            let events = events.clone();
            thread::spawn(move || {
                let result = RetainedRoot::open(opening_path, Some(tiny()));
                events.send(Event::Opened).unwrap();
                result
            })
        })
        .collect();
    // The old implementation returns WouldBlock immediately; the corrected
    // implementation reports that it is waiting for this local finalizer.
    let transitions = [
        observed.recv_timeout(DEADLINE).unwrap(),
        observed.recv_timeout(DEADLINE).unwrap(),
    ];
    // A finalizer for this path does not monopolize the global registry.
    let unrelated = tempfile::tempdir().unwrap();
    drop(RetainedRoot::open(unrelated.path().to_owned(), Some(tiny())).unwrap());
    release.send(()).unwrap();
    dropping.join().unwrap();
    let reopened: Vec<_> = openings.into_iter().map(|t| t.join().unwrap()).collect();
    gates().lock().unwrap().remove(&path);
    let reopened: Vec<_> = reopened
        .into_iter()
        .map(|r| r.expect("reopen must coordinate with the retiring local owner"))
        .collect();
    assert_eq!(transitions, [Event::Waiting, Event::Waiting]);
    assert!(Arc::ptr_eq(&reopened[0], &reopened[1]));
    assert_eq!(reopened[0].usage(None).unwrap(), usage);
    assert_eq!(reopened[0].limits(), tiny());
}
