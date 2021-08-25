import asyncio

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from types import TracebackType

from .debug import DEBUG
from .util import Ignore
from .builder import StaticTaskOf, Task, TaskOf, Tasks, StaticTaskOf
from .rebuilders import VTRebuilder
from .schedulers import SuspendingScheduler
from .errors import PyWatchError
from typing import Any, Callable, Collection, Dict, Final, Generic, Iterable, List, Optional, Set, Sized, Tuple, Type, TypeVar

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")
A = TypeVar("A")
B = TypeVar("B")


class PyWatchType(Enum):
    ATOM = "ATOM"
    SELECTOR = "SELECTOR"
    EVENT = "EVENT"


@dataclass(frozen=True, eq=True)
class PyWatchID(Generic[T]):
    """
    Unique ID of the PyWatchRule (per PyWatch)
    """
    type: PyWatchType
    seq_num: int
    name: Optional[str] = None
    name_seq: Optional[int] = None  # name and name_seq should both present or be None
    internal_use: bool = field(default=False, hash=False)


    def is_internal_name(self) -> bool:
        return self.internal_use

    def to_attr_form(self) -> str:
        if not self.name:
            return f"<{self.type}, {self.seq_num}>::auto"
        else:
            return self.name + ("" if self.name_seq == 0 else f"${self.name_seq}")

    def __str__(self) -> str:
        if not self.name:
            return f"<{self.type}, {self.seq_num}>::auto"
        else:
            suffix = "" if self.name_seq == 0 else f"${self.name_seq}"
            return f"<{self.name}{suffix}>"

FetchFn = Callable[[PyWatchID[K]], K]


class PyWatchIDSupplier(object):
    """
    Automatic PyWatchID generator for PyWatchRule.
    """
    def __init__(self) -> None:
        super().__init__()
        self._watch_id_supply: Dict[PyWatchType, int] = defaultdict(int)
        self._registered_names: Dict[str, int] = defaultdict(int)

    def make_watch_id(self,
                      type: PyWatchType,
                      name: Optional[str] = None) -> PyWatchID:
        seq = self._watch_id_supply[type]
        self._watch_id_supply[type] += 1
        return PyWatchID(type, seq, name, self.register_name(name))

    def name_is_registered(self, name: str) -> bool:
        return self._registered_names[name] > 0

    def register_name(self, name: str) -> int:
        seq = self._registered_names[name]
        self._registered_names[name] += 1
        return seq

class PyWatchRule(Generic[T]):
    """
    Entries which the PyWatch will try to build.
    """
    def __init__(self, watch_id: PyWatchID[T]) -> None:
        super().__init__()
        self._watch_id = watch_id

    @property
    def watch_id(self) -> PyWatchID[T]:
        return self._watch_id

    @abstractmethod
    def as_task(self) -> Task[PyWatchID[T], T]:
        pass


class PyWatchAtom(PyWatchRule[T]):
    """
    Atom rule. This rule will simply return the object it is referring.
    Only works when "default" is an object.
    """
    def __init__(self, watch_id: PyWatchID[T], default: T):
        super().__init__(watch_id)
        self._default = default
        self._memoized_task: Task[PyWatchID[T], T] = None

    @property
    def default(self) -> T:
        return self._default

    def as_task(self) -> Task[PyWatchID[T], T]:
        return TaskOf(lambda fetch: self._default)


class PyWatchSelector(PyWatchRule[T]):
    """
    Selector rule. This rule takes a FetchFn, and calculates the latest value. (Similar to a Task)
    """
    def __init__(self, watch_id: PyWatchID[T]) -> None:
        super().__init__(watch_id)


class PyWatchSelectorOf(PyWatchSelector[T]):
    """
    Wraps Callable into PyWatchSelector
    """
    def __init__(self, watch_id: PyWatchID[T], select: Callable[[FetchFn[T]], T]) -> None:
        super().__init__(watch_id)
        self._select = select

    def as_task(self) -> Task[PyWatchID[T], T]:
        return TaskOf(self._select)

class PyWatchSelectorCapture(PyWatchSelector[T]):
    """
    PyWatchSelector created via capture. Generates Task with static dependencies.
    """
    def __init__(self,
                 watch_id: PyWatchID[T],
                 predicate: Callable[[], T],
                 captured_deps: Optional[Set[PyWatchID]]) -> None:
        super().__init__(watch_id)
        self._predicate = predicate
        self._captured_deps = frozenset(captured_deps or set())

    def as_task(self) -> Task[PyWatchID[T], T]:
        return StaticTaskOf(self._predicate, self._captured_deps)


WR = TypeVar("WR", bound=PyWatchRule)
WA = TypeVar("WA", bound=PyWatchAtom)


class PyWatchAPI(ABC):
    """
    API for registering various PyWatchEntry.
    """
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def _get_name(self) -> Optional[str]:
        """
        Get the given name of the rule.
        """
        pass

    @abstractmethod
    def _get_py_watch(self) -> "PyWatch":
        """
        Get the binding PyWatch
        """
        pass

    @abstractmethod
    def watch(self, atom: A) -> PyWatchID[A]:
        """
        Watch an object. Generates an Atom rule.
        """
        pass

    @abstractmethod
    def select(self, select: Callable[[FetchFn[A]], K]) -> PyWatchID[K]:
        """
        Generate a Selector rule based on the callback. Selectors are rules that depends on other rules.
        """
        pass

    @abstractmethod
    def capture(self, *captured_objects: Any) -> "_CapturedAPI":
        """
        Creates a Selector rule with static dependency.
        """
        pass

    @abstractmethod
    def event(self, watch_id: PyWatchID[bool], equals: bool = True, timeout: float = 10) -> asyncio.Event:
        """
        Creates an asyncio.Event as a Selector rule.
        """
        pass

    # Combinatorial API

    def AND(self, *watch_ids: PyWatchID[bool]) -> PyWatchID[bool]:
        name = self._get_name() or " AND ".join([str(wid) for wid in watch_ids])
        return self._get_py_watch().create_select(lambda get: all(map(get, watch_ids)), name)

    def OR(self, *watch_ids: PyWatchID[bool]) -> PyWatchID[bool]:
        name = self._get_name() or " OR ".join([str(wid) for wid in watch_ids])
        return self._get_py_watch().create_select(lambda get: any(map(get, watch_ids)), name)

    def NOT(self, watch_id: PyWatchID[bool]) -> PyWatchID[bool]:
        name = self._get_name() or "NOT " + str(watch_id)
        return self._get_py_watch().create_select(lambda get: not get(watch_id), name)

    def EQ(self, *watch_ids: PyWatchID[A]) -> PyWatchID[bool]:
        name = self._get_name() or " == ".join([str(wid) for wid in watch_ids])
        return self._get_py_watch().create_select(lambda get: len(set(map(get, watch_ids))) == 1, name)

    def EXPECT(self, watch_id: PyWatchID[A], *one_of: A) -> PyWatchID[bool]:
        name = self._get_name()
        if not name:
            if len(one_of) == 1:
                name = f"{watch_id} == {one_of[0]}"
            else:
                name = f"{watch_id} IN {list(one_of)}"
        return self._get_py_watch().create_select((lambda get: get(watch_id) in one_of), name)

    def IS(self, watch_id: PyWatchID[A], to_be: A) -> PyWatchID[bool]:
        name = self._get_name() or f"{watch_id} IS {to_be}"
        return self._get_py_watch().create_select(lambda get: get(watch_id) is to_be, name)

    def IN(self, item_id: PyWatchID[A], collection_id: PyWatchID[Collection[A]]) -> PyWatchID[bool]:
        name = self._get_name() or f"{item_id} IN {collection_id}"
        return self._get_py_watch().create_select(lambda get: get(item_id) in get(collection_id), name)

    def COUNT(self, watch_id: PyWatchID[Sized]) -> PyWatchID[int]:
        name = self._get_name() or f"COUNT {watch_id}"
        return self._get_py_watch().create_select(lambda get: len(get(watch_id)), name)

    # Private methods


@dataclass
class _CapturedAPI:
    """
    API for creating statically-dependent Selectors.
    """
    captured_ids: Set[PyWatchID]
    watch_record: Dict[PyWatchID, Any]
    id_supplier: PyWatchIDSupplier
    name: Optional[str] = None

    def gets(self, predicate: Callable[[], T]) -> PyWatchID[T]:
        watch_id = self.id_supplier.make_watch_id(PyWatchType.SELECTOR, self.name)
        self.watch_record[watch_id] = PyWatchSelectorCapture(watch_id, predicate, self.captured_ids)
        return watch_id

class PyWatch(PyWatchAPI):
    """
    The "overseer" responsible for maintaining rules, building values as required, and stores values in its storage.
    """

    def __init__(self) -> None:
        super().__init__()

        self._id_supplier = PyWatchIDSupplier()
        self._builder = SuspendingScheduler().make_builder(VTRebuilder())
        self._watch_record: Dict[PyWatchID, PyWatchRule] = {}
        self._store: Dict[PyWatchID, Any] = {}
        self._captured_ref_to_watch_id: Dict[int, PyWatchID] = {}
        self._watched_events: Set[PyWatchID[asyncio.Event]] = set()
        self._visit_generated_keys: Dict[Tuple[PyWatchID, str], PyWatchID] = {}

    @dataclass
    class _NamedAPI(PyWatchAPI):
        """
        PyWatchAPI for creating named rules.
        """
        _manager: "PyWatch"
        _name: Optional[str]

        # PyWatchAPI mixin

        def watch(self, atom: A) -> PyWatchID[A]:
            return self._manager.create_watch(atom, self._name)

        def select(self, select: Callable[[FetchFn[A]], K]) -> PyWatchID[K]:
            return self._manager.create_select(select, self._name)

        def capture(self, *captured_objects: Any) -> "_CapturedAPI":
            return _CapturedAPI(captured_ids=self._manager._captured_objects_to_ids(captured_objects),
                                watch_record=self._manager._watch_record,
                                id_supplier=self._manager._id_supplier,
                                name=self._name)

        def event(self, watch_id: PyWatchID[bool], equals: bool = True, timeout: float = 10) -> asyncio.Event:
            return self._manager.create_event(watch_id, equals, self._name, timeout)

        def _get_name(self) -> Optional[str]:
            return self._name

        def _get_py_watch(self) -> "PyWatch":
            return self._manager

    @property
    def id_supplier(self) -> PyWatchIDSupplier:
        return self._id_supplier

    # PyWatchAPI mixin

    def watch(self, atom: A) -> PyWatchID[A]:
        return self.create_watch(atom)

    def select(self, get: Callable[[FetchFn[A]], K]) -> PyWatchID[K]:
        return self.create_select(get)

    def capture(self, *objects: Any) -> _CapturedAPI:
        return _CapturedAPI(captured_ids=self._captured_objects_to_ids(objects),
                            watch_record=self._watch_record,
                            id_supplier=self._id_supplier)

    def event(self, watch_id: PyWatchID[bool], equals: bool = True, timeout: float = 10) -> "PyWatchEvent":
        return self.create_event(watch_id, equals, None, timeout)

    def _get_name(self) -> Optional[str]:
        return None

    def _get_py_watch(self) -> "PyWatch":
        return self

    # Companion tasks

    async def auto_builder_companion(self, every: float = 0.5) -> None:
        while True:
            self.build_events()
            await asyncio.sleep(every)

    # Class-specific methods

    def name(self, name: str) -> PyWatchAPI:
        if not name:
            raise PyWatchError("Given name is empty or None!")
        if name[0] == "#":
            raise PyWatchError(f"Cannot use {name} as a name! Names start with '#' is reserved for internal use in PyWatch!")
        for reserved in ("$", "<", ">"):
            if reserved in name:
                raise PyWatchError(f"Cannot use {name} as a name! Names with '{reserved}' is reserved for internal use in PyWatch!")
        return self._name(name)

    def visit(self, attr_visit: "PyWatchAttrVisit") -> PyWatchID[T]:
        """
        Automatically creates relevant rules from the context provided by the given PyWatchAttrVisit object.
        """
        namespace = PyWatchAttrVisit.get_namespace(attr_visit)
        key = PyWatchAttrVisit.get_start(attr_visit)
        current_name = key.to_attr_form()
        for item in namespace:
            current_name += "." + item
            if reuseable_key := self._visit_generated_keys.get((key, item)):
                key = reuseable_key
                continue
            new_key = self._name(current_name).select(lambda get, key=key, item=item: getattr(get(key), item, None))
            self._visit_generated_keys[(key, item)] = new_key
            key = new_key
        return key

    def create_watch(self, atom: T, name: Optional[str] = None) -> PyWatchID[T]:
        watch_id = self._id_supplier.make_watch_id(PyWatchType.ATOM, name)
        self._watch_record[watch_id] = PyWatchAtom(watch_id, atom)
        self._captured_ref_to_watch_id[id(atom)] = watch_id
        return watch_id

    def create_select(self, get: Callable[[FetchFn[K]], K], name: Optional[str] = None) -> PyWatchID[T]:
        watch_id = self._id_supplier.make_watch_id(PyWatchType.SELECTOR, name)
        self._watch_record[watch_id] = PyWatchSelectorOf(watch_id, get)
        return watch_id

    def create_event(self, depend_id: PyWatchID[bool],
                           equals: bool = True,
                           name: Optional[str] = None,
                           timeout: float = 10) -> "PyWatchEvent":
        def selector(get: FetchFn[bool]) -> Ignore:
            if get(depend_id) == equals:
                event.set()
                self.unwatch(depend_id)
            return Ignore()
        name = name or ("#Event" + (" NOT " if not equals else " ") + str(depend_id))
        watch_id = self._id_supplier.make_watch_id(PyWatchType.EVENT, name)
        self._watch_record[watch_id] = PyWatchSelectorOf(watch_id, selector)
        event = PyWatchEvent(self, watch_id=watch_id, depends=depend_id, timeout=timeout)
        self._watched_events.add(watch_id)
        return event

    def find_watch_id(self, watch_id: PyWatchID[K]) -> Optional[PyWatchRule[K]]:
        return self._watch_record.get(watch_id)

    def unwatch(self, watch_id: PyWatchID[K]) -> None:
        """
        De-register an PyWatchID, removes it from Py_WATCH's storage.
        NOTE This method is UNSAFE, since it does not check whether other rules depends on it.
        """
        self._watch_record.pop(watch_id, None)
        self._store.pop(watch_id, None)
        self._watched_events.discard(watch_id)

    def static_gc(self) -> None:
        raise PyWatchError("Not Implemented")  # TODO static garbage collection

    def build(self, input: PyWatchID[T]) -> T:
        """
        Force build an PyWatchID
        """
        result = self._builder.build(PyWatchTasks(self), input, self._store)
        if DEBUG:
            print(f"Building {input} complete!")
            print(self.dumps_store())
        return result.get(input)

    def build_events(self) -> None:
        """
        Force build all watched events.
        """
        if self._watched_events:
            barrier = self.barrier(*self._watched_events)
            self.build(barrier)
            self.unwatch(barrier)

    def get_value(self, watch_id: PyWatchID[T]) -> Optional[T]:
        return self._store.get(watch_id)

    def dumps_rules(self) -> str:
        entries = [str(k) for k in self._watch_record.keys() if not k.is_internal_name()]
        return "Rules:\n" + "\n".join(entries)

    def dumps_store(self) -> str:
        entries = [f"{str(k): <40} : {v}" for k, v in self._store.items() if not k.is_internal_name() and not isinstance(v, Ignore)]
        return "Store:\n" + "\n".join(entries)

    def barrier(self, *watch_ids: PyWatchID) -> PyWatchID[None]:
        """
        Creates a barrier rule. A barrier rule force builds its captured PyWatchIDs.
        """
        return PyWatch._NamedAPI(self, "#BARRIER").capture(*watch_ids).gets(Ignore)

    def get_object_watch_id(self, obj: Any) -> Optional[PyWatchID]:
        return self._captured_ref_to_watch_id.get(id(obj))

    # Private methods

    def _name(self, name: str) -> _NamedAPI:
        return PyWatch._NamedAPI(self, name)

    def _captured_objects_to_ids(self, objects: Iterable[Any]) -> Set[PyWatchID]:
        """
        Translate objects in an Py_WATCH.capture(...) call into corresponding WatchID, when they are not WatchIDs per se.
        If the object is not being watched, watch it automatically.
        """
        ids: Set[PyWatchID] = set()
        for o in objects:
            if isinstance(o, PyWatchID):
                ids.add(o)
            else:
                ref = id(o)
                if ref in self._captured_ref_to_watch_id:
                    ids.add(self._captured_ref_to_watch_id[ref])
                else:
                    ids.add(self.watch(o))
        return ids

class PyWatchTasks(Tasks[PyWatchID[K], K]):
    """
    Main Tasks of PyWatch
    """
    def __init__(self, overseer: PyWatch) -> None:
        super().__init__()
        self._overseer = overseer

    def get(self, key: PyWatchID[K]) -> Optional[Task[PyWatchID[K], K]]:
        rule = self._overseer.find_watch_id(key)
        return rule.as_task() if rule else None


# TODO A bit too hacky, change a new design later.
def path(wid: PyWatchID[A]) -> A: # Actually, PyWatchAttrVisit
    return PyWatchAttrVisit([], wid)


@dataclass
class PyWatchAttrVisit(Generic[A]):
    """
    Attribute visit record object. Records the attribute accessing order.
    """

    def __init__(self, namespace: List[str], start_wid: PyWatchID[A]) -> None:
        super().__init__()
        self._namespace = namespace
        self._wid = start_wid

    @staticmethod
    def get_namespace(attr_visit: "PyWatchAttrVisit") -> List[str]:
        return attr_visit.__getattribute__("_namespace")

    @staticmethod
    def get_start(attr_visit: "PyWatchAttrVisit") -> PyWatchID[A]:
        return attr_visit.__getattribute__("_wid")

    def __getattr__(self, name: str) -> Any:
        if name[:2] == "__":    # Ignore reserved
            return self.__getattribute__(name)
        namespace: List[str] = PyWatchAttrVisit.get_namespace(self)
        namespace.append(name)
        return PyWatchAttrVisit(namespace, self.__getattribute__("_wid"))

    def __hasattr__(self, name: str) -> bool:
        if name == "something":
            return True
        return False


"""
PyWatch-specialized asyncio synchronous primitives
"""

class PyWatchSyncPrim(ABC):

    def __init__(self, overseer: PyWatch, watch_id: PyWatchID) -> None:
        super().__init__()
        self._overseer = overseer
        self._watch_id = watch_id


class PyWatchEvent(PyWatchSyncPrim, asyncio.Event):
    def __init__(self, overseer: PyWatch,
                       watch_id: PyWatchID["PyWatchEvent"],
                       depends: PyWatchID[bool],
                       timeout: float = 10) -> None:
        super().__init__(overseer, watch_id)
        self._depends = depends
        self._timeout = timeout


    # State Manager

    def __enter__(self) -> "PyWatchEvent":
        return self

    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> bool:
        if exc_value is not None:  # Re-raise exception if any
            return False
        asyncio.get_event_loop().run_until_complete(self.wait())
        return True

    async def wait(self) -> bool:
        try:
            return await asyncio.wait_for(asyncio.Event.wait(self), self._timeout)
        except asyncio.TimeoutError:
            raise PyWatchError(
                f"Expecting an event that depends on {self._depends} to be set within {self._timeout}s\n"
                f"But the event has timed out\n"
                f"Event ID: {self._watch_id}"
            )
