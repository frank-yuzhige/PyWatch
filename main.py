import asyncio
from pywatch.errors import PyWatchError
from pywatch.watch import path
from typing import List
from pywatch import PY_WATCH as P
from dataclasses import dataclass, field

"""
Try change DEBUG in debug.py to True to see more logs
"""
@dataclass
class Person:
    name: str
    gender: str
    age: int
    likes: List["Person"] = field(default_factory=list)


def main() -> None:
    loop = asyncio.get_event_loop()

    alice = Person(name="Alice", gender="F", age=20)
    bob = Person(name="Bob", gender="M", age=25)
    chris = Person(name="Chris", gender="M", age=50)

    # Watch a single mutable object.
    i_alice = P.watch(alice)
    i_bob = P.watch(bob)
    # Use a selector callback to select watched object's property.
    i_alice_age = P.select(lambda get: get(i_alice).age)
    # Complex selector callback
    i_alice_bob_both_old = P.select(lambda get: get(i_alice_age) > 22 and get(P.watch(bob)).age > 22)
    # Event from boolean PyWatchID
    e_alice_bob_both_old = P.event(i_alice_bob_both_old, timeout=0.5)
    # Explicitly build all registered events
    P.build_events()
    try:
        loop.run_until_complete(e_alice_bob_both_old.wait())
        raise Exception("Should timeout!")
    except PyWatchError:
        pass

    # Now alice is growing old
    alice.age = 23
    P.build_events()
    loop.run_until_complete(e_alice_bob_both_old.wait())

    # Use (experimental) visit-path api for quickly generate object-field accessing rule
    i_bob_likes = P.visit(path(i_bob).likes)
    # A simpler way to write rules is to use combinatorial APIs
    e_alice_bob_like_each_other = P.event(
        P.AND(P.IN(i_alice, i_bob_likes),
              P.IN(i_bob, P.visit(path(i_alice).likes))))
    # Make them like each other
    bob.likes.append(alice)
    alice.likes.append(bob)
    P.build_events()
    loop.run_until_complete(e_alice_bob_like_each_other.wait())


if __name__ == "__main__":
    main()
