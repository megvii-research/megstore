from functools import partial

from megfile import smart_open

smart_limited_seekable_open = partial(smart_open, limited_seekable=True)


def full_class_name(obj):
    # obj.__module__ + "." + obj.__class__.__qualname__ is an example in
    # this context of H.L. Mencken's "neat, plausible, and wrong."
    # Python makes no guarantees as to whether the __module__ special
    # attribute is defined, so we take a more circumspect approach.
    # Alas, the module name is explicitly excluded from __qualname__
    # in Python 3.

    module = obj.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return obj.__class__.__name__  # Avoid reporting __builtin__
    else:
        return module + "." + obj.__class__.__name__


def full_error_message(error):
    return "%s: %s" % (full_class_name(error), str(error))
