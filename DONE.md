# DONE

- Productionized S3 bad-chunk retry handling by removing diagnostic stderr output, preserving wrapped underlying errors, and retrying invalid/decompression failures through a bounded S3 helper capped at two retries beyond the first read.
- Added focused S3 retry tests covering recovery after a bad stored chunk and the two-retry cap for repeated validation/decompression failures.
- Added `LocalFSOptions.NoXattrs` and `desync tar --no-xattrs` to skip local extended-attribute reads when creating catar/index output from disk.
- Added LocalFS/tar and CLI tests for the no-xattrs path.
- Added opt-in debug counters for long desync runs via `DESYNC_DEBUG_STATS` and optional Go pprof output via `DESYNC_CPU_PROFILE` / `DESYNC_MEM_PROFILE`.
