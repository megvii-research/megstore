# About Indexed Module

The `megstore.indexed` module provides a framework for file formats that support random access via an index file. This document explains the base classes and mechanisms used for indexing.

## Overview

Indexed storage allows for efficient random access to large files by maintaining a separate index file (usually with a `.idx` extension) that stores the offsets of records in the data file.

The core components are:

*   **BaseIndexedReader**: Abstract base class for reading indexed files.
*   **BaseIndexedWriter**: Abstract base class for writing indexed files.
*   **IndexHandler**: Manages the index file itself.

## Base Classes

### BaseIndexedReader

`BaseIndexedReader` provides the interface for reading data from an indexed file.

Key features:
*   **Automatic Index Verification**: Checks if the index file exists and is valid (correct header, matching file size).
*   **Index Rebuilding**: If the index is missing or invalid, it can automatically rebuild it from the data file.
*   **Random Access**: Supports `get(index)` to retrieve specific records.

```python
class BaseIndexedReader(BaseReader[T], ABC):
    def __init__(
        self,
        fp_data: BinaryIO,
        fp_index_path: Optional[str] = None,
        *,
        close_fileobj_when_close: bool = False,
        index_file_mode: str = "rb",
        index_build_callback: Optional[Callable[[Any], None]] = None,
    ) -> None:
        ...
```

### BaseIndexedWriter

`BaseIndexedWriter` handles writing data and updating the index simultaneously.

Key features:
*   **Append Mode**: Supports appending to existing files and updating the index.
*   **Index Synchronization**: Ensures the index is updated as data is written.

```python
class BaseIndexedWriter(BaseWriter[T], ABC):
    def __init__(
        self,
        fp_data: BinaryIO,
        fp_index_path: str,
        *,
        append_mode: bool = False,
        close_fileobj_when_close: bool = False,
    ):
        ...
```

## Index File Format

The index file typically contains:
1.  **Header**: A header string (default "IDV1") and format information.
2.  **Offsets**: A sequence of file offsets (usually 64-bit unsigned integers) pointing to the start of each record in the data file.

### IndexHandler

The `IndexHandler` class (and its subclasses `IndexHandlerReader` and `IndexHandlerWriter`) manages the low-level operations on the index file.

*   `check_index_file_header`: Validates the index file header.
*   `write_header`: Writes the index file header.
*   `get(index)`: Retrieves the offset for a given index.
*   `put(index, value)`: Writes an offset for a given index.

## Usage

To implement a new indexed format, you typically need to:

1.  Inherit from `BaseIndexedReader` and implement `_build_index` and `_get`.
2.  Inherit from `BaseIndexedWriter` and implement `_append` and `_commit`.

See `megstore.indexed.jsonline` or `megstore.indexed.msgpack` for concrete implementations.
