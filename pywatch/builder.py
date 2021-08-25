import functools

from abc import ABC, abstractmethod, abstractstaticmethod
from dataclasses import dataclass
from typing import Any, AnyStr, Callable, Dict, FrozenSet, Generic, List, Mapping, MutableMapping, Optional, Set, Tuple, TypeVar

from .util import BLACKHOLE

K = TypeVar("K")
V = TypeVar("V")


Store = MutableMapping
Fetch = Callable[[K], V]
TaskFn = Callable[[Fetch[K, V]], V]

class Task(ABC, Generic[K, V]):
    """
    A Task takes a fetch callback which fetches the values of its dependencies (if any), and calculates the value.
    For now, we support Task with either static or dynamic dependencies.
      * A Task with static dependencies means its dependencies does not depend on any runtime info.
            (Per "Applicative" Task in paper).
      * A Task with dynamic dependencies requires runtime info to resolve its dependencies. For example, the task:
            x = fetch("k1")
            return fetch("k" + x)
        has dynamic dependencies. Because the key in 2nd fetch call depends on the first fetch call.
            (Per "Monad" Task in paper)
    """
    @abstractmethod
    def run(self, fetch: Fetch[K, V]) -> V:
        pass


    def track(self, fetch: Fetch[K, V]) -> Tuple[V, Dict[K, V]]:
        """
        Run and track the fetched value of dependent keys
        """
        track_dict: Dict[K, V] = {}
        def track_fetch(k: K) -> V:
            v = fetch(k)
            track_dict[k] = v
            return v
        v = self.run(track_fetch)
        return v, track_dict


    @functools.lru_cache(maxsize=1)
    def dependencies(self) -> FrozenSet[K]:
        """
        Get the static dependencies of the task.
        This method will behave unexpectedly if the dependencies are dynamic.
        """
        deps: Set[K] = set()
        def fetch(k: K) -> V:
            deps.add(k)
            return BLACKHOLE
        self.run(fetch)
        return frozenset(deps)

    # For lru_cache
    def __hash__(self) -> int:
        return id(self)


class TaskOf(Task[K, V]):
    """
    Wraps Callable into Task
    """
    def __init__(self, run: TaskFn[K, V]) -> None:
        super().__init__()
        self._run = run

    def run(self, fetch: Fetch[K, V]) -> V:
        return self._run(fetch)


class StaticTaskOf(Task[K, V]):
    """
    A Task with user-defined static dependencies.
    """
    def __init__(self, predicate: Callable[[], V], deps: Set[K]) -> None:
        super().__init__()
        self._predicate = predicate
        self._deps = frozenset(deps)

    def run(self, fetch: Fetch[K, V]) -> V:
        for k in self._deps:
            fetch(k)                # Manually track from all static dependencies.
        return self._predicate()

    def dependencies(self) -> FrozenSet[K]:
        return self._deps

TasksFn = Callable[[K], Optional[Task[K, V]]]


class Tasks(ABC, Generic[K, V]):
    """
    A "Task-bundle", gets the corresponding Task from the key.
    """
    @abstractmethod
    def get(self, key: K) -> Optional[Task[K, V]]:
        pass

@dataclass
class TasksOf(Tasks[K, V]):
    """
    Wraps Callable into "Task-bundle".
    """
    _get: TasksFn[K, V]

    def get(self, key: K) -> Optional[Task[K, V]]:
        return self._get(key)


BuilderFn = Callable[[Tasks[K, V], K, Store[K, V]], Store[K, V]]

class Builder(ABC, Generic[K, V]):
    """
    Builder, builds relevant values from the given tasks spec and target key, and stores
    results into store mapping.
    """
    @abstractmethod
    def build(self, tasks: Tasks[K, V], target: K, store: Store[K, V]) -> Store[K, V]:
        pass

    def build_and_get(self, tasks: Tasks[K, V], target: K, store: Store[K, V]) -> V:
        s = self.build(tasks, target, store)
        return s[target]

@dataclass
class BuilderOf(Builder[K, V]):
    """
    Wraps Callable into Builder
    """
    _build: BuilderFn
    def build(self, tasks: Tasks[K, V], target: K, store: Store[K, V]) -> Store[K, V]:
        return self._build(tasks, target, store)

class Rebuilder(ABC, Generic[K, V]):
    """
    Part of the builder that is "responsible for deciding whether a key needs to be rebuilt".
    See: section 4.2 of the paper.
    """
    @abstractmethod
    def rebuild(self, key: K, value: V, task: Task[K, V]) -> Task[K, V]:
        pass

class Scheduler(ABC, Generic[K, V]):
    """
    Part of the builder that is "responsible for scheduling tasks in the dependency order".
    See: section 4.1 of the paper.
    """
    @abstractstaticmethod
    def make_builder(rebuilder: Rebuilder[K, V]) -> Builder[K, V]:
        pass
