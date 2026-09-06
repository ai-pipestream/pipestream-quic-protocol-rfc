This document specifies PipeStream, a recursive scatter-gather streaming
protocol for hierarchical task decomposition and distributed processing
over QUIC transport. PipeStream enables the decomposition (scattering)
of complex, arbitrary workloads into constituent sub-tasks, their
transmission across distributed processing nodes, and subsequent
rehydration (gathering) at destination endpoints.

PipeStream defines a hierarchical work state machine in an application
protocol directly over QUIC. It uses independent Entity Streams for
payload transmission and a Control Stream for lifecycle reports,
completion barriers, and recovery coordination.

Version 1 defines a generic 2-bit Data Layer field for entity
representation, leaving the concrete payload semantics to application
profiles. Checkpoints and completion policies distinguish pending work,
successful completion, and partial outcomes. Status digests summarize
reported completion; they are not proofs of correct computation. The
draft records implementation coverage and remaining interoperability
questions. Version 2 defines a reduced mandatory Core and separately
negotiated durable-work and result-delivery profiles, including authenticated
replay, non-reusable logical identities, fenced attempts, sealed closure,
and output streams and references. A new major mapping preserves the meaning
of existing version-1 implementations rather than silently weakening their
mandatory behavior.
