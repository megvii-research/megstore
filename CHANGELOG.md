## 0.0.2 - 2026.08.25

- feat
  - Added `buffer_size` to indexed writers; the default `0` disables buffering.
    - Replaced `_append()` with `_serialize()` for buffered writers.
    - Kept legacy `_append()` compatible when buffering is disabled.

## 0.0.1 - 2025.11.25

- First release of `megstore`
