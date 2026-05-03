# backend/otel_config.py

import os
import socket
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.django import DjangoInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource


def _is_reachable(url: str, timeout: int = 2) -> bool:
    """Quick TCP check — avoids hanging Django startup if SigNoz is down."""
    try:
        host_port = url.replace("http://", "").replace("https://", "")
        host, port = host_port.split(":")
        socket.create_connection((host, int(port)), timeout=timeout)
        return True
    except Exception:
        return False


def setup_telemetry():
    """
    Configure OpenTelemetry to send traces to SigNoz.

    Port guide:
      4317 → OTLP gRPC  (what we use — faster, binary protocol)
      4318 → OTLP HTTP  (alternative)

    Your docker-compose maps:
      host:4317 → signoz_container:4317  (gRPC)
      host:4318 → signoz_container:4318  (HTTP)

    So Django on the host sends to localhost:4317 via gRPC.
    """
    if os.getenv("DISABLE_TELEMETRY", "False").lower() == "true":
        print("⚠️ Telemetry disabled via DISABLE_TELEMETRY env var")
        return

    # OTLP gRPC endpoint — must NOT have http:// prefix for gRPC exporter
    # The env var should be set as: OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317
    grpc_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")

    # Strip http:// if someone accidentally included it
    grpc_endpoint = grpc_endpoint.replace("http://", "").replace("https://", "")

    # Check reachability before trying to connect
    host, port = grpc_endpoint.split(":") if ":" in grpc_endpoint else (grpc_endpoint, "4317")
    if not _is_reachable(f"http://{host}:{port}"):
        print(f"⚠️ SigNoz not reachable at {host}:{port} — telemetry skipped")
        return

    service_name = os.getenv("OTEL_SERVICE_NAME", "estatemind-django")
    environment  = os.getenv("ENVIRONMENT", "development")

    resource = Resource(attributes={
        SERVICE_NAME: service_name,
        "deployment.environment": environment,
        "service.version": "1.0.0",
    })

    provider = TracerProvider(resource=resource)

    try:
        exporter = OTLPSpanExporter(
            endpoint=grpc_endpoint,
            insecure=True,   # no TLS for local dev
        )
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        print(f"✅ OpenTelemetry → SigNoz at {grpc_endpoint}")
    except Exception as e:
        print(f"⚠️ Failed to configure OTLP exporter: {e}")
        return

    # Instrument Django request/response cycle
    try:
        DjangoInstrumentor().instrument()
        print("✅ Django instrumented")
    except Exception as e:
        print(f"⚠️ DjangoInstrumentor failed: {e}")

    # Instrument outgoing HTTP requests (to Supabase, Ollama, etc.)
    try:
        RequestsInstrumentor().instrument()
        print("✅ Requests instrumented")
    except Exception as e:
        print(f"⚠️ RequestsInstrumentor failed: {e}")