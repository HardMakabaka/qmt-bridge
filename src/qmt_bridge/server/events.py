from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, TypedDict


class EventCursorPayload(TypedDict):
    epoch: str
    sequence: int


class EventTransportStatus(TypedDict):
    transport: str
    listener_alive: bool
    replay_gap: bool
    cursor: EventCursorPayload | None


@dataclass(frozen=True, slots=True)
class EventCursor:
    epoch: str
    sequence: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> EventCursor | None:
        epoch = value.get("epoch")
        sequence = value.get("sequence")
        if not isinstance(epoch, str) or not epoch:
            return None
        if not isinstance(sequence, int) or isinstance(sequence, bool):
            return None
        return cls(epoch=epoch, sequence=sequence)

    def to_payload(self) -> EventCursorPayload:
        return {"epoch": self.epoch, "sequence": self.sequence}


def build_event_transport_status(
    *,
    cursor: Mapping[str, object] | None,
    replay_gap: bool,
    listener_alive: bool,
) -> EventTransportStatus:
    parsed = EventCursor.from_mapping(cursor) if cursor is not None else None
    return {
        "transport": "zmq",
        "listener_alive": listener_alive,
        "replay_gap": replay_gap,
        "cursor": parsed.to_payload() if parsed is not None else None,
    }
