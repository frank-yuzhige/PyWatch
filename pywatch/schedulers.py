from .builder import Builder, BuilderOf, Rebuilder, Scheduler, Store, Task, Tasks
from typing import Dict, Final, List, Set, TypeVar


K = TypeVar("K")
V = TypeVar("V")

Graph = Dict[K, List[K]]
class TopologicalScheduler(Scheduler[K, V]):
    """
    Scheduler that resolves dependencies in their topological order.
    Can only handle static dependencies.
    """
    @staticmethod
    def _reachable(complete_graph: Graph[K], start: K) -> Graph[K]:
        reach: Graph[K] = {}
        fringes = [start]
        while fringes:
            current = fringes.pop(0)
            reach[current] = complete_graph[current]
            fringes += reach[current]
        return reach

    @staticmethod
    def _topological_sort(graph: Graph[K]) -> List[K]:
        l = []
        edge_count = 0
        incoming: Dict[K, int] = dict([(k, 0) for k in graph.keys()])
        for out in graph.values():
            for node in out:
                incoming[node] += 1
                edge_count += 1
        s = [node for (node, n) in incoming.items() if n == 0]
        while s:
            n = s.pop()
            l.append(n)
            for m in graph[n]:
                edge_count -= 1
                incoming[m] -= 1
                if incoming[m] == 0:
                    s.append(m)
        if edge_count != 0:
            raise Exception("Cyclic graph")
        return l

    def make_builder(self, rebuilder: Rebuilder[K, V]) -> Builder[K, V]:
        def builder(tasks: Tasks[K, V], target: K, store: Store[K, V]) -> Store[K, V]:
            # calculate order
            graph: Graph[K] = {}
            r = set([target])
            while r:
                n = r.pop()
                next_task = tasks.get(n)
                deps = next_task.dependencies() if next_task else []
                graph[n] = deps
                r.update(deps)
            order = TopologicalScheduler._topological_sort(graph)
            order.reverse()
            # build
            for key in order:
                task = tasks.get(key)
                if task is not None:
                    v = store.get(key)
                    new_task = rebuilder.rebuild(key, v, task)
                    new_v = new_task.run(store.get)
                    store[key] = new_v
            return store
        return BuilderOf(builder)


class SuspendingScheduler(Scheduler[K, V]):
    """
    Scheduler that builds dependencies when requested, suspending the current running task.
    Can handle dynamic dependencies.
    """
    def make_builder(self, rebuilder: Rebuilder[K, V]) -> Builder[K, V]:
        def builder(tasks: Tasks[K, V], target: K, store: Store[K, V]) -> Store[K, V]:
            done: Set[K] = set()
            def fetch(key: K) -> V:
                if key not in done:
                    task = tasks.get(key)
                    if task:
                        value = store.get(key)
                        new_value = rebuilder.rebuild(key, value, task).run(fetch)
                        done.add(key)
                        store[key] = new_value
                return store.get(key)
            fetch(target)
            return store
        return BuilderOf(builder)

