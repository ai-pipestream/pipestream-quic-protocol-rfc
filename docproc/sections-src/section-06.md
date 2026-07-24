# IANA Considerations

This document requests no IANA actions.

## Profile Identification

This profile is identified by the case-sensitive string `DOCPROC`.
Implementations MAY advertise this identifier in out-of-band
configuration, capability metadata, or application-specific routing
tables.

The initial profile schema version defined by this document is `1`.
Receivers that do not support the advertised `profile-version` in a
PipeDoc payload SHOULD reject that payload at the application layer
rather than attempting best-effort interpretation.
