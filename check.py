from enum import IntEnum, unique

@unique
class State(IntEnum):
    FOLLOWER = -1,
    CANDIDATE = 0,
    LEADER = 1


d = State(1)

if d == State.LEADER:
    print(d.name)
else:
    print(f"d is not {State.LEADER.name}, but {d.name}")

d = State.CANDIDATE
print(d)