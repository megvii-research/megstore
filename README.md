megstore - Common Formats. Uncommon Speed.
---

[![Build](https://github.com/megvii-research/megstore/actions/workflows/run-tests.yml/badge.svg?branch=main)](https://github.com/megvii-research/megstore/actions/workflows/run-tests.yml)
[![Documents](https://github.com/megvii-research/megstore/actions/workflows/publish-docs.yml/badge.svg)](https://github.com/megvii-research/megstore/actions/workflows/publish-docs.yml)
[![Codecov](https://img.shields.io/codecov/c/gh/megvii-research/megstore)](https://app.codecov.io/gh/megvii-research/megstore/)
[![Latest version](https://img.shields.io/pypi/v/megstore.svg)](https://pypi.org/project/megstore/)
[![Support python versions](https://img.shields.io/pypi/pyversions/megstore.svg)](https://pypi.org/project/megstore/)
[![License](https://img.shields.io/pypi/l/megstore.svg)](https://github.com/megvii-research/megstore/blob/main/LICENSE)

* Docs: http://megvii-research.github.io/megstore

`megstore` is a Python library that provides a unified interface for file operations across various file formats. It aims to simplify file handling, while also enhancing performance and reliability.

## Why megstore

* Faster file read and write operations, the random read performance is also very fast with index support.
* Low memory usage with streaming read and write support.
* Supports popular file formats and easy to use.

## Quick Start

### Installation

```bash
pip3 install megstore

# for msgpack support
pip3 install 'megstore[msgpack]'
```

### Examples

- indexed jsonline

```python
from megstore import indexed_jsonline_open

with indexed_jsonline_open("data.jsonl", "w") as writer:
    writer.append({"key": "value"})
    writer.append({"number": 123})

with indexed_jsonline_open("data.jsonl", "r") as reader:
    second_item = reader[1]
    second_to_last_items = reader[1:]
    total_count = len(reader)
    for item in reader:
        print(item)
```

- indexed msgpack

```python
from megstore import indexed_msgpack_open

with indexed_msgpack_open("data.msg", "w") as writer:
    writer.append({"key": "value"})
    writer.append({"number": 123})

with indexed_msgpack_open("data.msg", "r") as reader:
    second_item = reader[1]
    second_to_last_items = reader[1:]
    total_count = len(reader)
    for item in reader:
        print(item)
```

- indexed txt

```python
from megstore import indexed_txt_open

with indexed_txt_open("data.txt", "w") as writer:
    writer.append("Hello, World!")
    writer.append("This is a test.")

with indexed_txt_open("data.txt", "r") as reader:
    second_item = reader[1]
    second_to_last_items = reader[1:]
    total_count = len(reader)
    for line in reader:
        print(line)
```

## How to Contribute
* We welcome everyone to contribute code to the `megstore` project, but the contributed code needs to meet the following conditions as much as possible:

    *You can submit code even if the code doesn't meet conditions. The project members will evaluate and assist you in making code changes*

    * **Code format**: Your code needs to pass **code format check**. `megstore` uses `ruff` as lint tool
    * **Static check**: Your code needs complete **type hint**. `megstore` uses `pytype` as static check tool. If `pytype` failed in static check, use `# pytype: disable=XXX` to disable the error and please tell us why you disable it.

    * **Test**: Your code needs complete **unit test** coverage. `megstore` uses `pyfakefs` and `moto` as local file system and s3 virtual environment in unit tests. The newly added code should have a complete unit test to ensure the correctness

* You can help to improve `megstore` in many ways:
    * Write code.
    * Improve [documentation](https://github.com/megvii-research/megstore/blob/main/docs).
    * Report or investigate [bugs and issues](https://github.com/megvii-research/megstore/issues).
    * If you find any problem or have any improving suggestion, [submit a new issuse](https://github.com/megvii-research/megstore/issues) as well. We will reply as soon as possible and evaluate whether to adopt.
    * Review [pull requests](https://github.com/megvii-research/megstore/pulls).
    * Star `megstore` repo.
    * Recommend `megstore` to your friends.
    * Any other form of contribution is welcomed.
