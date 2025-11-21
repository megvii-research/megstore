try:
    import numpy as np
except ImportError:
    np = None
import orjson
from orjson import JSONDecodeError, JSONEncodeError, loads

__all__ = [
    "JSONDecodeError",
    "JSONEncodeError",
    "dumps",
    "loads",
]


def create_default_func(callback):
    def default(obj):
        if isinstance(obj, tuple):
            return tuple(obj)
        if np:
            if isinstance(obj, np.generic):
                if isinstance(obj, np.bool_):
                    return bool(obj)
                if isinstance(obj, np.integer):
                    return int(obj)
                if isinstance(obj, np.floating):
                    return float(obj)
        if callback is not None:
            return callback(obj)
        raise TypeError

    return default


def dumps(obj, default=None):
    return orjson.dumps(obj, default=create_default_func(default))
