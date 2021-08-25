from abc import ABC
from datetime import datetime, time

from .debug import DEBUG
from .util import hashcode

from .trace import Hash, VerifyingTrace
from .builder import Fetch, Rebuilder, Task, TaskOf, Tasks
from typing import Dict, Final, TypeVar


K = TypeVar("K")
V = TypeVar("V")

class BusyRebuilder(Rebuilder[K, V]):
    """
    Rebuilder, naively, diligently.
    """
    def rebuild(self, key: K, value: V, task: Task[K, V]) -> Task[K, V]:
        return task

class ModTimeRebuilder(Rebuilder[K, V]):
    """
    Rebuilder that based on the modification time of the key.
    """

    LOGGER_NAME = "rebuilder.mod.time"

    def __init__(self) -> None:
        super().__init__()
        self._mod_times: Dict[K, datetime] = {}

    def rebuild(self, key: K, value: V, task: Task[K, V]) -> Task[K, V]:
        def task_fn(fetch: Fetch[K, V]) -> V:
            now = datetime.now()
            last_mod = self._mod_times.get(key)
            if last_mod is None:
                dirty = True
            else:
                for dep in task.dependencies():
                    if self._mod_times[dep] > last_mod:
                        dirty = True
                        break
            if not dirty:
                return value
            self._mod_times[key] = now
            return task.run(fetch)
        return TaskOf(task_fn)


class VTRebuilder(Rebuilder[K, V]):
    """
    Rebuilder based on verifying trace
    See: Section 4.2.2 of the paper.
    """
    LOGGER_NAME: Final[str] = "rebuilder.vt"

    def __init__(self, v_trace: VerifyingTrace = VerifyingTrace()) -> None:
        super().__init__()
        self._v_trace = v_trace

    def rebuild(self, key: K, value: V, task: Task[K, V]) -> Task[K, V]:
        def task_fn(fetch: Fetch[K, V]) -> V:
            up2date = self._v_trace.verify_vt(key, hashcode(value), lambda k: hashcode(fetch(k)))
            if up2date:
                if DEBUG:
                    print(f"Key {key} is up2date")
                return value
            new_value, deps = task.track(fetch)
            self._v_trace.record_vt(key, hashcode(new_value), {k: hashcode(v) for k, v in deps.items()})
            if DEBUG:
                print(f"Key {key} new value = {new_value}")
            return new_value
        return TaskOf(task_fn)
