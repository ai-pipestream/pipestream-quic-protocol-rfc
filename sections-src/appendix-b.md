## Appendix B: Protocol Layer Capability Matrix

| Feature | Layer 0 | Layer 1 | Layer 2 |
|---------|---------|---------|---------|
| Unified status frame (64-bit) | ✓ | ✓ | ✓ |
| Entity streaming | ✓ | ✓ | ✓ |
| PENDING/PROCESSING/COMPLETE/FAILED | ✓ | ✓ | ✓ |
| Checkpoint blocking | ✓ | ✓ | ✓ |
| Assembly Manifest | ✓ | ✓ | ✓ |
| Cursor-based ID recycling | ✓ | ✓ | ✓ |
| Scoped status fields (Scope ID, depth) | | ✓ | ✓ |
| Hierarchical scopes | | ✓ | ✓ |
| Scope digest (Merkle) | | ✓ | ✓ |
| Barrier (subtree sync) | | ✓ | ✓ |
| YIELDED status | | | ✓ |
| DEFERRED status | | | ✓ |
| Claim checks | | | ✓ |
| Completion policies | | | ✓ |
| SKIPPED/ABANDONED statuses | | | ✓ |
