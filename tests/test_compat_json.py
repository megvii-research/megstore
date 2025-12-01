import pytest

from megstore.utils.compat_json import (
    JSONDecodeError,
    JSONEncodeError,
    dumps,
    loads,
)


def test_dumps_basic_types():
    """Test dumps with basic Python types"""
    assert dumps(1) == b"1"
    assert dumps(1.5) == b"1.5"
    assert dumps("string") == b'"string"'
    assert dumps([1, 2, 3]) == b"[1,2,3]"
    assert dumps({"a": 1}) == b'{"a":1}'
    assert dumps(True) == b"true"
    assert dumps(False) == b"false"
    assert dumps(None) == b"null"


def test_dumps_tuple():
    """Test dumps with tuple - should convert to list"""
    result = dumps((1, 2, 3))
    # orjson should handle tuple via our default function
    assert loads(result) == [1, 2, 3]


def test_dumps_tuple_nested():
    """Test dumps with nested structures containing tuples"""
    # tuple inside list
    result = dumps([(1, 2), (3, 4)])
    parsed = loads(result)
    assert parsed == [[1, 2], [3, 4]]


def test_create_default_func_with_tuple():
    """Test create_default_func directly with tuple to cover the tuple branch"""
    from megstore.utils.compat_json import create_default_func

    default_func = create_default_func(None)
    # Directly call the default function with a tuple
    result = default_func((1, 2, 3))
    assert result == (1, 2, 3)


def test_dumps_nested_tuple():
    """Test dumps with nested tuple"""
    result = dumps({"key": (1, 2, 3)})
    assert loads(result) == {"key": [1, 2, 3]}


def test_dumps_with_custom_default():
    """Test dumps with custom default callback"""

    class CustomClass:
        def __init__(self, value):
            self.value = value

    def custom_default(obj):
        if isinstance(obj, CustomClass):
            return {"custom": obj.value}
        raise TypeError

    result = dumps(CustomClass(42), default=custom_default)
    assert loads(result) == {"custom": 42}


def test_dumps_with_custom_default_none():
    """Test dumps with default=None and unsupported type"""

    class UnsupportedClass:
        pass

    with pytest.raises(TypeError):
        dumps(UnsupportedClass())


def test_dumps_without_numpy(mocker):
    """Test dumps when numpy is not available"""
    # Mock the np variable to None to simulate numpy not being installed
    import megstore.utils.compat_json as compat_json

    original_np = compat_json.np
    compat_json.np = None

    try:
        # Should still work for basic types
        result = dumps({"key": "value"})
        assert loads(result) == {"key": "value"}
    finally:
        compat_json.np = original_np


def test_dumps_with_custom_default_raises_type_error():
    """Test dumps when custom default raises TypeError"""

    class UnsupportedClass:
        pass

    def custom_default(obj):
        raise TypeError("Cannot serialize")

    with pytest.raises(TypeError):
        dumps(UnsupportedClass(), default=custom_default)


def test_loads_basic():
    """Test loads with basic JSON"""
    assert loads(b"1") == 1
    assert loads(b"1.5") == 1.5
    assert loads(b'"string"') == "string"
    assert loads(b"[1,2,3]") == [1, 2, 3]
    assert loads(b'{"a":1}') == {"a": 1}


def test_loads_invalid_json():
    """Test loads with invalid JSON raises JSONDecodeError"""
    with pytest.raises(JSONDecodeError):
        loads(b"invalid json")


def test_json_encode_error():
    """Test that JSONEncodeError is available"""
    # Just check that the error class is exported
    assert JSONEncodeError is not None


# Tests for numpy types - only run if numpy is available
try:
    import numpy as np

    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_dumps_numpy_bool():
    """Test dumps with numpy bool types"""
    result = dumps(np.bool_(True))
    assert loads(result) is True

    result = dumps(np.bool_(False))
    assert loads(result) is False


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_dumps_numpy_integer():
    """Test dumps with numpy integer types"""
    # Test various numpy integer types
    int_types = [np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32]
    for dtype in int_types:
        result = dumps(dtype(42))
        assert loads(result) == 42


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_dumps_numpy_floating():
    """Test dumps with numpy floating types"""
    # Test various numpy float types
    for dtype in [np.float32, np.float64]:
        result = dumps(dtype(3.14))
        assert abs(loads(result) - 3.14) < 0.001


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_dumps_numpy_array_in_dict():
    """Test dumps with numpy types inside dict"""
    data = {
        "int": np.int64(42),
        "float": np.float64(3.14),
        "bool": np.bool_(True),
    }
    result = dumps(data)
    parsed = loads(result)
    assert parsed["int"] == 42
    assert abs(parsed["float"] - 3.14) < 0.001
    assert parsed["bool"] is True


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_dumps_numpy_with_custom_default():
    """Test dumps with numpy types and custom default callback"""

    class CustomClass:
        def __init__(self, value):
            self.value = value

    def custom_default(obj):
        if isinstance(obj, CustomClass):
            return {"custom": obj.value}
        raise TypeError

    # Should handle both numpy types and custom class
    data = {"np_int": np.int64(42), "custom": CustomClass(100)}
    result = dumps(data, default=custom_default)
    parsed = loads(result)
    assert parsed["np_int"] == 42
    assert parsed["custom"] == {"custom": 100}
