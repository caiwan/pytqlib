from functools import wraps


def bind_function(fn, obj):
    @wraps(fn)
    def _wrapped_fn(*args, **kwargs):
        return fn(obj, *args, **kwargs)

    _wrapped_fn._binding_fn = fn
    _wrapped_fn._bound_obj = obj
    return _wrapped_fn
