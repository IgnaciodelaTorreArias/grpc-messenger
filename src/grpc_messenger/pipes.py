from dataclasses import dataclass, field
import logging
from contextlib import contextmanager
from typing import Generator, Any

from grpc_messenger.view_interfaces import ViewBuffers, ViewUpdate

@dataclass
class Pipes:
    logger = logging.getLogger("Pipe")
    pending_connections: dict[str, int] = field(init=False, default_factory=dict)
    current_connections: set[str] = field(init=False, default_factory=set)
    pending_validations: dict[str, bytes] = field(init=False, default_factory=dict)
    def connecting(self, connection: str) -> bool:
        self.logger.info("%s connecting...", connection)
        if self.pending_connections.get(connection, None) is None:
            self.pending_connections[connection] = 1
            return True
        self.pending_connections[connection] += 1
        return False
    def connected(self, connection: str) -> None:
        self.logger.info("%s connected!!!", connection)
        self.current_connections.add(connection)
        self.pending_connections.pop(connection)
    def failed(self, connection: str) -> bool:
        self.logger.info("%s connection failed", connection)
        if connection in self.current_connections:
            # If one of the connections was successful ignore the failed ones
            return False
        self.pending_connections[connection] -= 1
        if self.pending_connections[connection] == 0:
            self.pending_connections.pop(connection)
            return True
        return False
    def new_message(self, connection: str, message: str) -> None:
        self.logger.info("new message from %s: %s", connection, message)
    def disconnected(self, connection: str) -> None:
        self.logger.info("%s disconnected", connection)
        self.current_connections.discard(connection)

@dataclass
class PipesBuffering(Pipes):
    front_buffer: ViewBuffers = field(init=False, default_factory=ViewBuffers)
    back_buffer: ViewBuffers = field(init=False, default_factory=ViewBuffers)
    buffer_swap: bool = field(init=False, default=False)
    # Swaps between the back and front buffer, this to make the view consistent for non thread safe gui/cli

    @contextmanager
    def render(self) -> Generator[ViewBuffers, Any, None]:
        self.buffer_swap = not self.buffer_swap
        if self.buffer_swap:
            render_buffer = self.back_buffer
        else:
            render_buffer = self.front_buffer
        try:
            yield render_buffer
        finally:
            # Clear buffers
            render_buffer.connecting.clear()
            render_buffer.connected.clear()
            render_buffer.failed.clear()
            render_buffer.new_message.clear()
            render_buffer.disconnected.clear()

    @property
    def draw_buffer(self) -> ViewBuffers:
        if self.buffer_swap:
            return self.front_buffer
        else:
            return self.back_buffer

    def connecting(self, connection: str) -> bool:
        render = super().connecting(connection)
        if render:
            self.draw_buffer.connecting.append(connection)
        return render

    def connected(self, connection: str) -> None:
        super().connected(connection)
        self.draw_buffer.connected.append(connection)

    def failed(self, connection: str) -> bool:
        render =  super().failed(connection)
        if render:
            self.draw_buffer.failed.append(connection)
        return render

    def new_message(self, connection: str, message: str) -> None:
        super().new_message(connection, message)
        self.draw_buffer.new_message.append((connection, message))

    def disconnected(self, connection: str) -> None:
        super().disconnected(connection)
        self.draw_buffer.disconnected.append(connection)

@dataclass
class PipesImmediateUpdate(Pipes):
    interface: ViewUpdate
    def connecting(self, connection: str) -> bool:
        render =  super().connecting(connection)
        if render:
            self.interface.connecting(connection)
        return render

    def connected(self, connection: str) -> None:
        super().connected(connection)
        self.interface.connected(connection)

    def failed(self, connection: str) -> bool:
        render =  super().failed(connection)
        if render:
            self.interface.failed(connection)
        return render

    def new_message(self, connection: str, message: str) -> None:
        super().new_message(connection, message)
        self.interface.new_message(connection, message)

    def disconnected(self, connection: str) -> None:
        super().disconnected(connection)
        self.interface.disconnected(connection)
