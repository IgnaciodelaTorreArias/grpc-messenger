# GRPC-MESSENGER

![Image](/assets/bear-256.png)

![PyPI - License](https://img.shields.io/pypi/l/grpc-messenger)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/grpc-messenger)
![PyPI - Version](https://img.shields.io/pypi/v/grpc-messenger)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![Statically typed: mypy](https://img.shields.io/badge/statically%20typed-mypy-039dfc)

---

Send text messages over the network, using grpc.

---

# Installation

> python -m pip install grpc-messenger

# Tech stack

- [grpc](https://pypi.org/project/grpcio/)
- [protobuf](https://pypi.org/project/protobuf/)

# How's

## How it works in the background

When you create a backend context 2 threads are created. One for managing grpc clients (the ones used when you initiate a connection with someone else) and one for the grpc server (the one used by other to initiate a connection with you).

Inside each thread an asyncio event loop is running, both the grpc server and the grpc clients are asynchronous, for each connection made a bidirectional streaming is created.

Each application running is identified by it's server's address, so before the client connects to our server an identification process happens where we receive the direction of the client's server, and authenticate that the server provided really belongs to that client.

Once authenticated the client can start the bidirectional streaming.

## How to use with a gui/cli library

The objective of this library is to be a backend/core/module for a front-end gui/cli application.

### The basic

```python
import grpc_messenger

# Create an class inheriting from ViewUpdate protocol
class MyView(grpc_messenger.ViewUpdate):
   ...
```

The requiere methods are:

- connecting. Called when either you try to connect with someone else or someone else tries to connect with you
- connected. Called when the connection described before is successful
- failed. Called when the connection described before failed
- new_message. Called when you receive a message from someone
- disconnected. Called when a disconnection happens

### Thread safe gui/cli library

```python
interface = MyView()
with Backend("[::1]:50051", interface, thread_safe_view=True) as b:
    if b is not None:
        interface.start() # The method that starts your gui/cli library
```

this will call the methods described before immediately (from the background threads)

### NOT thread safe gui/cli library with access to the render loop

```python
interface = MyView()
with Backend("[::1]:50051", interface) as b:
    if b is not None:
        while interface.running(): 
            interface.render_frame() 
            b.render()
```

this will keep all events (like receiving a new message) on a buffer and will only call the methods when you use the render() method from the background

### Neither of the cases described before

You could probably hack your way around using one of the methods described before

### Give orders

With the guide described before you can see things that happen, now you need to know how to make things happen.

When you initiate the context for the backend it returns either None or a BackendI.

- None. When the server could not be initiated, probably because the interface and port you selected for your server is already in use.
- BackendI. When everything is fine.

The BackendI provides the following methods:

- render. To render from the buffer only when you indicate that `thread_safe_view=False` (the default)
- connect. To who you want to initiate a connection
- send_message. To who you want to send a message and the message itself

### Bind the orders to callbacks inside your gui/cli

```python
class MyView(ViewUpdate):
    backend: BackendI
interface = MyView()
with Backend(address, interface) as b:
    if b is not None:
        interface.backend = b
```

this might allow you to do something like

```python
class MyView(ViewUpdate):
    def on_press_button(self):
        self.backend.send_message("[::1]:50051", "hello")
```

This should be safe because when you start the view you also set the backend
