# backend/otel_config.py
import os
import socket


def _is_reachable(host: str, port: int, timeout: int = 2) -> bool:
    try:
        socket.create_connection((host, port), timeout=timeout)
        return True
    except Exception:
        return False


def setup_telemetry():
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.django import DjangoInstrumentor
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource

    if os.getenv("DISABLE_TELEMETRY", "False").lower() == "true":
        print("⚠️ Telemetry disabled")
        return

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
    endpoint = endpoint.replace("http://", "").replace("https://", "")
    host, port = endpoint.split(":") if ":" in endpoint else (endpoint, "4317")

    if not _is_reachable(host, int(port)):
        print(f"⚠️ OTEL collector not reachable at {host}:{port} — skipped")
        return

    resource = Resource(attributes={
        SERVICE_NAME: os.getenv("OTEL_SERVICE_NAME", "estatemind-django"),
        "deployment.environment": os.getenv("ENVIRONMENT", "development"),
    })

    provider = TracerProvider(resource=resource)
    try:
        exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        print(f"✅ OpenTelemetry → {endpoint}")
    except Exception as e:
        print(f"⚠️ OTEL exporter failed: {e}")
        return

    try:
        DjangoInstrumentor().instrument()
        RequestsInstrumentor().instrument()
        print("✅ Django + Requests instrumented")
    except Exception as e:
        print(f"⚠️ Instrumentation failed: {e}")