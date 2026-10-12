# Protocol Layer Capability Matrix (Informative)

| Feature | Layer 0 | Layer 1 | Layer 2 |
|---------|---------|---------|---------|
| Unified status frame (128-bit base) | X | X | X |
| Entity streaming | X | X | X |
| PENDING/PROCESSING/COMPLETE/FAILED | X | X | X |
| DEHYDRATING/REHYDRATING | X | X | X |
| Checkpoint blocking | X | X | X |
| Assembly Manifest | X | X | X |
| Cursor-based ID recycling | X | X | X |
| Scoped status fields (Scope ID, depth) | | X | X |
| Hierarchical scopes | | X | X |
| Scope digest (Merkle) | | X | X |
| Barrier (subtree sync) | | X | X |
| YIELDED status | | | X |
| DEFERRED status | | | X |
| Claim checks | | | X |
| Completion policies | | | X |
| SKIPPED/ABANDONED statuses | | | X |

This matrix describes version 1. Version 2 (Section 12) defines no
layers; it negotiates the profiles in Section 11.10.
