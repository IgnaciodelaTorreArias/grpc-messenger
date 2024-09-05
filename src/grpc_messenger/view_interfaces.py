from dataclasses import dataclass, field
from collections import deque
from typing import Protocol


@dataclass(frozen=True, slots=True)
class ViewBuffers:
    # In case the gui/cli you are using is NOT thread safe
    connecting: deque[str] = field(init=False, default_factory=deque)
    connected: deque[str] = field(init=False, default_factory=deque)
    failed: deque[str] = field(init=False, default_factory=deque)
    new_message: deque[tuple[str, str]] = field(  # sender, message
        init=False, default_factory=deque
    )
    disconnected: deque[str] = field(init=False, default_factory=deque)


class ViewUpdate(Protocol):
    # Callbacks used to update your gui/cli
    def connecting(self, connection: str) -> None:
        """Called when either you try to connect with someone else or someone else tries to connect with you

        Args:
            connection (str): the address of who is trying to connect
        """
        ...

    def connected(self, connection: str) -> None:
        """Called when the connection described before is successful

        Args:
            connection (str): the address of the successful connection
        """
        ...

    def failed(self, connection: str) -> None:
        """Called when the connection described before failed

        Args:
            connection (str): the address of the failed connection
        """
        ...

    def new_message(self, connection: str, message: str) -> None:
        """Called when you receive a message from someone

        Args:
            connection (str): the address from who you are receiving the message
            message (str): the message you received
        """
        ...

    def disconnected(self, connection: str) -> None:
        """Called when a disconnection happens

        Args:
            connection (str): the address of who disconnected
        """
        ...
