from functools import wraps
from time import time


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        delta = te - ts
        delta = round(delta, 2)
        print(f"{f.__name__} takes {delta}")
        return result

    return wrap
