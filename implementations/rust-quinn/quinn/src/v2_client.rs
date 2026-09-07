//! Version-2 asynchronous client components. The journal worker is independent
//! of control I/O; it is not itself a network connection or an authenticator.

pub mod journal;
