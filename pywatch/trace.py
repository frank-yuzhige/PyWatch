from dataclasses import dataclass
from typing import Callable, Dict, Generic, Hashable, List, Optional, TypeVar

"""
Various traces utilized by corresponding rebuilders.
See section 4.2 of the paper.
"""

K = TypeVar("K")
V = TypeVar("V", bound=Hashable)
R = TypeVar("R")

# Hash = None means force recompute
Hash = Optional[int]

def hash_equals(p: Hash, q: Hash) -> bool:
    return p is None or p == q

@dataclass
class Trace(Generic[K, V, R]):
    key: K
    depends: Dict[K, Hash]
    result: R


class VerifyingTrace(Generic[K, V]):

    def __init__(self, traces: List[Trace[K, V, Hash]] = []) -> None:
        super().__init__()
        self._traces = traces

    @property
    def traces(self) -> Trace:
        return self._traces

    def record_vt(self,
                  k: K,
                  v_hash: Hash,
                  depends: Dict[K, Hash]) -> None:
        self._traces.append(Trace(k, depends, v_hash))

    def verify_vt(self,
                  k: K,
                  v_hash: Hash,
                  fetch_hash: Callable[[K], Hash]) -> bool:
        for trace in self._traces:
            if trace.key == k and hash_equals(trace.result, v_hash):
                mismatch = [dep_k for dep_k, dep_hash in trace.depends.items() if not hash_equals(fetch_hash(dep_k), dep_hash)]
                if not mismatch:
                    return True
        return False
