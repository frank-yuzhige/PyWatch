from typing import Any, Final, Generic, Optional, TypeVar

import pickle
import random

A = TypeVar("A")

def delegate(methods, prop):
    def decorate(cls):
        for method in methods:
            setattr(cls, method,
                lambda self, *args, **kwargs:
                    getattr(getattr(self, prop), method)(*args, **kwargs))
        return cls
    return decorate

@delegate([method for method in dir(str) + dir(int) if method[:2] == "__" \
    and method not in ["__class__", "__new__", "__init__", "__getattribute__", "__format__", "__str__", "__index__", "__bool__"]]
    , "instance")
class Blackhole:
    """
    BLACKHOLE object, used in resolving static dependencies from arbitrary task callback (see Task.dependencies())
    Corresponds to the Applicative instance of the `Const` functor (see paper).
    This is pure dark arts, forbidden in Hogwarts, banned by Ministry of Magic, do not use it elsewhere!
    """
    def __init__(self) -> None:
        super().__init__()

    @property
    def instance(self):
        return self

    def __getattribute__(self, name: str) -> Any:
        return self

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self

    def __str__(self) -> str:
        return "Blackhole"

    def __index__(self) -> int:
        return 42

BLACKHOLE: Final[Blackhole] = Blackhole()


class Ignore:
    def __str__(self) -> str:
        return "IGNORED"

def hashcode(obj: Any) -> Optional[int]:
    if isinstance(obj, Ignore):
        return random.getrandbits(128)
    try:
        return hash(pickle.dumps(obj))
    except TypeError:
        # TODO Return more sensible hashcode for un-pickle-dumpable objects
        return random.getrandbits(128)

def if_present(obj: A) -> A: # Trick python typehint
    """
    Use case:
        if_present(object).some_attr
    , will return None instead of throwing NameError when some_attr not present in object.
    """
    return _IfPresent(obj)

class _IfPresent(Generic[A]):
    def __init__(self, obj: A) -> None:
        super().__init__()
        self._obj = obj

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__getattribute__("_obj"), name, None)

