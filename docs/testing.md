# Testing Strategy

This project now includes a focused suite of unit tests for the storage engine (`internal/store/store_test.go`). The tests aim to validate the most error-prone parts of the Bitcask lifecycle.

## Covered Scenarios

- **Basic CRUD** – `TestFileStorePutGetDelete` exercises inserts, reads, and tombstone handling.
- **Durability** – `TestFileStorePersistsAcrossOpen` ensures data written to disk survives a full close/reopen cycle.
- **Hint Files** – `TestFileStoreWriteHint` verifies that hint files are emitted to the configured data directory and remain usable across restarts.
- **Compaction** – `TestCompactOnceReclaimsObsoleteSegments` forces multiple segment rotations with large payloads, then confirms compaction eliminates sealed segments, removes temp files, and preserves the latest values.

Each test uses `t.TempDir()` to avoid polluting the repository and relies on real file I/O to mirror production usage. Running `go test ./...` from the repo root covers all packages, including the new storage tests.
