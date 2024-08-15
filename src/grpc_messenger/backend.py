import threading
import asyncio
import secrets
from collections import defaultdict
import logging
from typing import  AsyncGenerator, Protocol

import grpc
import grpc_messenger.protos.Messager_pb2 as pb2
import grpc_messenger.protos.Messager_pb2_grpc as pb2_grpc

from grpc_messenger.view_interfaces import ViewUpdate
from grpc_messenger.pipes import Pipes, PipesBuffering, PipesImmediateUpdate

# NOTE: theoretically if 2 people try starting a connection with each other
# at a similar time (practically at the same time) 2 connections will be created, this is a feature :)

class ServerI(Protocol):
    @property
    def address(self) -> str:
        ...
    def send_message(self, connection: str, message: str) -> None:
        ...

class Server(pb2_grpc.MessengerServicer):
    logger = logging.getLogger("Server")
    def __init__(self, address: str, pipes: Pipes) -> None:
        super().__init__()
        self._address = address
        self._pipes = pipes

        self._clients_servers: dict[str, str]
        self._authenticated_servers: set[str]
        self._to_send_messages: dict[str, asyncio.Queue[str | None]]
        self._thread: threading.Thread
        self._loop: asyncio.AbstractEventLoop
        
        self._server_initialized: threading.Event
        self._server_stop: threading.Event
        
        self._started: bool
        self._inside_context: bool = False

    @property
    def address(self) -> str:
        return self._address

    async def _start_server(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._server = grpc.aio.server()
        pb2_grpc.add_MessengerServicer_to_server(self, self._server)
        try:
            self._server.add_insecure_port(self._address)
        except Exception:
            self.logger.error("Binding error")
            self._server_initialized.set()
            return
        await self._server.start()
        self.logger.info("Server started")
        self._started = True
        self._server_initialized.set()
        await self._server.wait_for_termination()
        self._server_stop.set()

    def __enter__(self) -> ServerI | None:
        self._server_initialized = threading.Event()
        self._server_stop = threading.Event()

        self._clients_servers = dict()
        self._authenticated_servers = set()
        self._to_send_messages = defaultdict(asyncio.Queue)

        self._started = False

        self._thread = threading.Thread(target=asyncio.run, args=(self._start_server(),))
        self._thread.start()
        self._server_initialized.wait() # wait for it to truly start or fail
        if not self._started:
            return None
        self._inside_context = True
        return self

    def __exit__(self, exc_type, exc_val, tb) -> None:
        if self._started:
            for queue in self._to_send_messages.values():
                asyncio.run_coroutine_threadsafe(queue.put(None), self._loop)
            while len(self._to_send_messages):
                pass # Wait for all connections to be terminated gracefully
            asyncio.run_coroutine_threadsafe(self._server.stop(None), self._loop)
            # Since the initialization indicates that the server started correctly,
            # now we wait for it to stop
            self._server_stop.wait()
            self._started = False
        self._thread.join()
        self._inside_context = False

    def send_message(self, connection: str, message: str) -> None:
        if not self._inside_context:
            raise RuntimeError("Operation only valid inside context")
        if self._to_send_messages.get(connection) is None:
            return
        asyncio.run_coroutine_threadsafe(self._to_send_messages[connection].put(message), self._loop)

    # ----- grpc methods ----- #

    async def timed_connection_authenticity(self, client:str) -> None:
        # Limit connection, avoiding attacks of memory leak and limit the time an authentication is valid
        await asyncio.sleep(10)
        if self._clients_servers.pop(client, None) is not None:
            self.logger.debug("Authenticity of connection from %s, timeout", client)
        self._authenticated_servers.discard(client)

    async def RequestConnection(
        self,
        request: pb2.Server,
        context: grpc.aio.ServicerContext
    ) -> pb2.Void:
        if request.identification in self._pipes.current_connections or request.identification in self._authenticated_servers:
            # This limits the amount of connections/requests of connections that can be done per server
            await context.abort(
                code=grpc.StatusCode.ALREADY_EXISTS,
                details="You have access to a connection"
            )
        self.logger.debug("New request from %s with server %s", context.peer(), request.identification)
        # NOTE: keeping the track of the peer's ip on a set would allow to immediately
        # deny the ip from having multiple requests, making sure they only connect once, therefore protecting
        # ourselves from a DOS attack from the peer, but since the string format of the peer is determined
        # by gRPC runtime, how can this be achieved consistently?, also what if the peer is inside a different
        # network that uses NAT
        self._pipes.connecting(request.identification)
        try:
            async with grpc.aio.insecure_channel(request.identification) as channel:
                # Validating if the server proportioned by this peer is reachable
                await asyncio.wait_for(channel.channel_ready(), timeout=7)
                stub = pb2_grpc.MessengerStub(channel)
                await asyncio.wait_for(stub.Authenticate(pb2.Server(identification=self._address, token=request.token)), timeout=7)
        except asyncio.TimeoutError:
            self.logger.debug("Request from %s with server %s, timeout", context.peer(), request.identification)
            self._pipes.failed(request.identification)
            await context.abort(
                code=grpc.StatusCode.DEADLINE_EXCEEDED,
                details="The server you proportioned took too long to respond"
            )
        except grpc.RpcError as e:
            self.logger.debug("Authentication from %s with server %s, rejected", context.peer(), request.identification)
            self._pipes.failed(request.identification)
            await context.abort(
                code=grpc.StatusCode.PERMISSION_DENIED,
                details="The server you proportioned denied your identity"
            )
        if request.identification in self._authenticated_servers:
            # An attacker could start a lot of requests with the same o different clients
            # and authenticate all of them in their own server, we will only accept one
            # the firs conditional limits the amount of authenticated/current connections
            # but if multiple requests enter before the first one of them is authenticated
            # this could lead to multiple connections.
            # This conditional avoids that case
            self._pipes.failed(request.identification)
            await context.abort(
                code=grpc.StatusCode.ALREADY_EXISTS,
                details="You have access to a connection don't make another"
            )
        self.logger.debug("Authentication from %s with server %s, accepted", context.peer(), request.identification)
        self._clients_servers[context.peer()] = request.identification
        self._authenticated_servers.add(request.identification)
        asyncio.create_task(self.timed_connection_authenticity(context.peer()))
        return pb2.Void()

    async def Authenticate(
        self,
        request: pb2.Server,
        context: grpc.aio.ServicerContext
    ) -> pb2.Void:
        try:
            self.logger.debug("Validating connection with %s", request.identification)
            if secrets.compare_digest(self._pipes.pending_validations[request.identification], request.token):
                self.logger.debug("Connection with %s validated", request.identification)
                self._pipes.pending_validations.pop(request.identification, None)
                return pb2.Void()
            await context.abort(
                code=grpc.StatusCode.PERMISSION_DENIED,
                details="Theres is no pending connection with you"
                # Technically a better response would be:
                # The validation key you proportioned is invalid
                # but this would give information to the attacker
            )
        except KeyError:
            await context.abort(
                code=grpc.StatusCode.PERMISSION_DENIED,
                details="Theres is no pending connection with you"
            )

    async def messages(self, request_iterator: AsyncGenerator[pb2.Message, None], connection: str) -> None:
        async for message in request_iterator:
            self._pipes.new_message(connection, message.message)
        await self._to_send_messages[connection].put(None) # If the clients stream terminates we terminate ours too

    async def Connect(
        self,
        request_iterator: AsyncGenerator[pb2.Message, None],
        context: grpc.aio.ServicerContext
    ) -> AsyncGenerator[pb2.Message, None]:
        identification = self._clients_servers.pop(context.peer(), None)
        if identification is None:
            await context.abort(
                code=grpc.StatusCode.FAILED_PRECONDITION,
                details="You need to request a connection first"
            )
        self.logger.debug("%s initiated the connection", identification)
        self._pipes.connected(identification)
        queue = self._to_send_messages[identification]
        t = asyncio.create_task(self.messages(request_iterator, identification))
        try:
            while True:
                message = await queue.get()
                if message is None:
                    break
                yield pb2.Message(message=message)
            return
        finally:
            t.cancel()
            self._pipes.disconnected(identification)
            self._to_send_messages.pop(identification, None)

class ClientI(Protocol):
    @property
    def address(self) -> str:
        ...
    def connect(self, connection: str) -> None:
        ...
    def send_message(self, connection: str, message: str) -> None:
        ...

class Client:
    logger = logging.getLogger("Client")
    def __init__(self, address: str, pipes: Pipes) -> None:
        self._address: str = address
        self._pipes = pipes

        self._to_send_messages: dict[str, asyncio.Queue[str | None]]
        self._thread: threading.Thread
        self._loop: asyncio.AbstractEventLoop

        self._ready: threading.Event
        self._stop_event: asyncio.Event

        self._inside_context: bool = False

    @property
    def address(self) -> str:
        return self._address

    async def _start_clients(self):
        self._loop = asyncio.get_running_loop()
        self._ready.set()
        await self._stop_event.wait()

    def __enter__(self) -> ClientI:
        self._to_send_messages = defaultdict(asyncio.Queue)

        self._ready = threading.Event()
        self._stop_event = asyncio.Event()

        self._thread = threading.Thread(target=asyncio.run, args=(self._start_clients(),))
        self._thread.start()
        self._ready.wait()
        self._inside_context = True
        return self

    def __exit__(self, exc_type, exc_val, tb) -> None:
        for queue in self._to_send_messages.values():
            asyncio.run_coroutine_threadsafe(queue.put(None), self._loop)
        while len(self._to_send_messages):
            pass # wait for all connections to gracefully terminate
        self._loop.call_soon_threadsafe(self._stop_event.set)
        self._thread.join()
        self._inside_context = False

    def connect(self, connection: str) -> None:
        if not self._inside_context:
            raise RuntimeError("Operation only valid inside context")
        if connection in self._pipes.current_connections:
            return
        asyncio.run_coroutine_threadsafe(self._connect(connection), self._loop)

    def send_message(self, connection: str, message: str) -> None:
        if not self._inside_context:
            raise RuntimeError("Operation only valid inside context")
        if self._to_send_messages.get(connection) is None:
            return
        asyncio.run_coroutine_threadsafe(self._to_send_messages[connection].put(message), self._loop)

    async def delete_validation_token(self, connection: str) -> None:
        await asyncio.sleep(15)
        if self._pipes.pending_validations.pop(connection, None) is not None:
            self.logger.debug("Validation token for %s, timeout", connection)

    async def _message_generator(self, connection: str) -> AsyncGenerator[pb2.Message, None]:
        queue = self._to_send_messages[connection]
        while True:
            message = await queue.get()
            if message is None:
                return
            yield pb2.Message(message=message)

    async def _connect(
        self,
        connection: str,
    ) -> None:
        token = secrets.token_bytes(32)
        self._pipes.pending_validations[connection] = token
        asyncio.create_task(self.delete_validation_token(connection))
        try:
            async with grpc.aio.insecure_channel(connection) as channel:
                self.logger.debug("Connecting to %s", connection)
                await asyncio.wait_for(channel.channel_ready(), timeout=10)
                self.logger.debug("Channel to %s, opened", connection)
                stub = pb2_grpc.MessengerStub(channel)
                await stub.RequestConnection(pb2.Server(identification=self._address, token=token))
                self.logger.debug("Connection to %s, successful", connection)
                self._pipes.connected(connection)
                async for message in stub.Connect(self._message_generator(connection)):
                    self._pipes.new_message(connection, message.message)
                self.logger.debug("Terminated: %s", connection)
        except asyncio.TimeoutError:
            self.logger.error("Connection to %s, failed, deadline", connection)
            self._pipes.failed(connection)
        except grpc.RpcError as e:
            self.logger.error("Connection to %s, failed, %s", connection, e.details()) # type: ignore
            self._pipes.failed(connection)
        finally:
            await self._to_send_messages[connection].put(None)
            self._pipes.disconnected(connection)
            self._to_send_messages.pop(connection, None)

class BackendI(Protocol):
    @property
    def address(self) -> str:
        ...
    @property
    def public_facing_address(self) -> str:
        ...
    def render(self) -> None:
        """To render from the buffer only when you indicate that `thread_safe_view=False` (the default)
        """
        ...
    def connect(self, connection: str) -> None:
        """To who you want to initiate a connection

        Args:
            connection (str): the address of the destiny's server
        """
        ...
    def send_message(self, connection: str, message: str) -> None:
        """To who you want to send a message and the message itself

        Args:
            connection (str): the address of the destiny's server
            message (str): the message you want to send
        """        
        ...

class Backend:
    def __init__(self, address: str, view_updater: ViewUpdate, thread_safe_view: bool = False, public_facing_address: str | None = None) -> None:
        # TODO: see what can be done for using public facing addresses
        self._server_address = address
        self._client_address = address

        self._view = view_updater
        self._thread_safe_view = thread_safe_view
        self._pipes: Pipes

        self._server: Server
        self._server_initiated = False
        self._client: Client

        self._server_interface: ServerI
        self._client_interface: ClientI

        self._inside_context: bool = False

    @property
    def address(self) -> str:
        return self._server_address

    @property
    def public_facing_address(self) -> str:
        return self._client_address

    def render(self) -> None:
        if not self._inside_context:
            raise RuntimeError("Operation only valid inside context")
        if self._thread_safe_view:
            raise RuntimeError("You provided a 'thread safe' ViewUpdater that is already being used")
        with self._pipes.render() as buffers: # type: ignore
            for connecting in buffers.connecting:
                self._view.connecting(connecting)
            for connected in buffers.connected:
                self._view.connected(connected)
            for failed in buffers.failed:
                self._view.failed(failed)
            for new_message in buffers.new_message:
                self._view.new_message(*new_message)
            for disconnected in buffers.disconnected:
                self._view.disconnected(disconnected)

    def __enter__(self) -> BackendI | None:
        if self._thread_safe_view:
            self._pipes = PipesImmediateUpdate(self._view)
        else:
            self._pipes = PipesBuffering()

        self._server = Server(self._server_address, self._pipes)
        interface = self._server.__enter__()
        if interface is None:
            self._server_initiated = False
            return None
        self._server_initiated = True
        self._server_interface = interface

        self._client = Client(self._client_address, self._pipes)
        self._client_interface = self._client.__enter__()

        self._inside_context = True
        return self

    def __exit__(self, exc_type, exc_val, tb) -> None:
        self._server.__exit__(exc_type, exc_val, tb)
        if self._server_initiated:
            self._client.__exit__(exc_type, exc_val, tb)

        self._inside_context = False

    def connect(self, connection: str) -> None:
        if not self._inside_context:
            raise RuntimeError("Operation only valid inside context")
        if connection in self._pipes.current_connections:
            return
        self._pipes.connecting(connection)
        self._client_interface.connect(connection)

    def send_message(self, connection: str, message: str) -> None:
        if not self._inside_context:
            raise RuntimeError("Operation only valid inside context")
        self._client_interface.send_message(connection, message)
        self._server_interface.send_message(connection, message)