"""A2A over gRPC: the a2a-sdk's gRPC binding, beside the JSON-RPC and REST ones.

The platform turns it on for a deployment it can route gRPC to: it sets
``HOPSWORKS_AGENT_GRPC_URL`` to the address a client dials (the agent's
hostname and the gateway's port) and ``HOPSWORKS_AGENT_GRPC_PORT`` to the port
the pod listens on. The server here answers on that port with the same request
handler the HTTP bindings use, so a task sent over gRPC is read, followed and
cancelled over HTTP and the other way round.

Both service names are served: ``lf.a2a.v1.A2AService`` (A2A 1.0) and
``a2a.v1.A2AService`` (0.3), as the HTTP bindings serve both versions.

The gateway authenticates a gRPC call as it does an HTTP request and sends the
caller's principal as metadata (``x-hopsworks-principal``); a private agent
reads it from there, and refuses a call without it, like ``scope._PrincipalGate``
refuses an HTTP request.

This module imports ``grpc``: ``a2a`` imports it only when the platform asked
for gRPC, and an environment without ``grpcio`` serves the HTTP bindings alone.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING, Any

import grpc
import grpc.aio
from a2a.compat.v0_3 import a2a_v0_3_pb2_grpc
from a2a.compat.v0_3.grpc_handler import CompatGrpcHandler
from a2a.server.request_handlers.grpc_handler import (
    DefaultGrpcServerCallContextBuilder,
    GrpcHandler,
)
from a2a.types import a2a_pb2_grpc

from .scope import (
    ANONYMOUS,
    _header_principal,
    _on_platform,
    _principal,
)


if TYPE_CHECKING:
    from a2a.server.context import ServerCallContext
    from a2a.server.request_handlers.request_handler import RequestHandler


_logger = logging.getLogger(__name__)

#: Where a client reaches this agent over gRPC, as the card names it (``host:port``).
GRPC_URL_ENV = "HOPSWORKS_AGENT_GRPC_URL"
#: The port the pod's gRPC server listens on.
GRPC_PORT_ENV = "HOPSWORKS_AGENT_GRPC_PORT"
#: The port when the platform names none.
DEFAULT_GRPC_PORT = 9090
#: How long calls still open at shutdown get, once the drain is over.
STOP_GRACE_SECONDS = 5.0


def _grpc_port() -> int:
    """The port to listen on: the platform's, or 9090."""
    value = os.environ.get(GRPC_PORT_ENV, "").strip()
    if not value:
        return DEFAULT_GRPC_PORT
    try:
        port = int(value)
    except ValueError:
        _logger.warning(
            "%s=%r is not a port; gRPC listens on %d",
            GRPC_PORT_ENV,
            value,
            DEFAULT_GRPC_PORT,
        )
        return DEFAULT_GRPC_PORT
    return port if 0 <= port <= 65535 else DEFAULT_GRPC_PORT


def _metadata_headers(metadata: Any) -> dict[str, str]:
    """A call's text metadata as the HTTP bindings see a request's headers: lower-cased names."""
    headers: dict[str, str] = {}
    for key, value in metadata or ():
        name = str(key).lower()
        if name.endswith("-bin") or not isinstance(value, str):
            continue
        headers[name] = value
    return headers


class _MetadataContextBuilder(DefaultGrpcServerCallContextBuilder):
    """The call context of a gRPC call, carrying its metadata where the stores read headers.

    The owner resolver, the executor and the extended card read
    ``state["headers"]``; over HTTP the SDK puts the request's headers there,
    and here the call's metadata takes their place. In a private agent the
    caller is also set for the stores, for the rest of the call: each call runs
    in its own task, so the caller of one never leaks into another.
    """

    def __init__(self, private: bool):
        self._private = private

    def build(self, context: grpc.aio.ServicerContext) -> ServerCallContext:
        call = super().build(context)
        headers = _metadata_headers(context.invocation_metadata())
        call.state["headers"] = headers
        if self._private:
            # the interceptor refused a call on a deployment without one
            _principal.set(_header_principal(headers) or ANONYMOUS)
        return call


class _PrincipalInterceptor(grpc.aio.ServerInterceptor):
    """A private agent on a deployment refuses a gRPC call that does not name its caller."""

    MESSAGE = (
        "This agent keeps each caller's conversations apart and the call does "
        "not say who the caller is. Reach the agent through the Hopsworks gateway."
    )

    async def intercept_service(self, continuation: Any, details: Any) -> Any:
        handler = await continuation(details)
        if handler is None or not _on_platform():
            return handler
        headers = _metadata_headers(details.invocation_metadata)
        if _header_principal(headers) is not None:
            return handler
        return _refusing(handler, self.MESSAGE)


def _refusing(handler: Any, message: str) -> Any:
    """A handler of the same shape as ``handler`` that refuses the call as unauthenticated."""

    async def refuse(request: Any, context: grpc.aio.ServicerContext) -> Any:
        await context.abort(grpc.StatusCode.UNAUTHENTICATED, message)

    async def refuse_stream(request: Any, context: grpc.aio.ServicerContext) -> Any:
        await context.abort(grpc.StatusCode.UNAUTHENTICATED, message)
        yield  # pragma: no cover - abort raises

    serializers = {
        "request_deserializer": handler.request_deserializer,
        "response_serializer": handler.response_serializer,
    }
    if handler.unary_stream is not None:
        return grpc.unary_stream_rpc_method_handler(refuse_stream, **serializers)
    if handler.stream_unary is not None:
        return grpc.stream_unary_rpc_method_handler(refuse, **serializers)
    if handler.stream_stream is not None:
        return grpc.stream_stream_rpc_method_handler(refuse_stream, **serializers)
    return grpc.unary_unary_rpc_method_handler(refuse, **serializers)


class _GrpcServer:
    """The gRPC server of one ``A2AServer``: started with the app, stopped after the drain."""

    def __init__(self, handler: RequestHandler, *, private: bool, port: int):
        self._handler = handler
        self._private = private
        #: The port asked for; the one bound once started (0 asks for a free one).
        self.port = port
        self._server: grpc.aio.Server | None = None
        self._stopping: asyncio.Future | None = None

    async def _start(self) -> None:
        """Listen on the port and serve both A2A versions.

        Raises:
            RuntimeError: When the port cannot be bound.
        """
        server = grpc.aio.server(
            interceptors=[_PrincipalInterceptor()] if self._private else None
        )
        builder = _MetadataContextBuilder(self._private)
        a2a_pb2_grpc.add_A2AServiceServicer_to_server(
            GrpcHandler(self._handler, context_builder=builder), server
        )
        a2a_v0_3_pb2_grpc.add_A2AServiceServicer_to_server(
            CompatGrpcHandler(self._handler, context_builder=builder), server
        )
        bound = server.add_insecure_port(f"[::]:{self.port}")
        if not bound:
            raise RuntimeError(f"could not listen on port {self.port}")
        self.port = bound
        await server.start()
        self._server = server
        _logger.info("A2A over gRPC is served on port %d", bound)

    def _stop_accepting(self, grace: float) -> None:
        """The pod was told to stop: take no new calls, and give the open ones ``grace`` seconds."""
        if self._server is None or self._stopping is not None:
            return
        self._stopping = asyncio.ensure_future(self._server.stop(max(0.0, grace)))

    async def _stop(self) -> None:
        """At shutdown, after the drain: end what is still open and close the port."""
        if self._server is None:
            return
        self._stop_accepting(STOP_GRACE_SECONDS)
        stopping = self._stopping
        if stopping is not None and not stopping.done():
            # the drain already gave the calls their time; this is the remainder
            await asyncio.gather(self._server.stop(0), return_exceptions=True)
        if stopping is not None:
            await asyncio.gather(stopping, return_exceptions=True)
        self._server = None
