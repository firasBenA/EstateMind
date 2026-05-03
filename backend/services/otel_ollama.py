# services/otel_ollama.py
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
import httpx
import time
from django.conf import settings

tracer = trace.get_tracer(__name__)

class OllamaInstrumentor:
    """Instrument Ollama calls for tracing in SigNoz"""
    
    @staticmethod
    async def trace_ollama_call(model: str, prompt: str, response: str = None, error: str = None):
        """Create a span for Ollama LLM call"""
        with tracer.start_as_current_span("ollama.generate") as span:
            span.set_attribute("llm.model", model)
            span.set_attribute("llm.prompt_length", len(prompt))
            span.set_attribute("llm.provider", "ollama")
            
            if response:
                span.set_attribute("llm.response_length", len(response))
            
            if error:
                span.set_status(Status(StatusCode.ERROR, error))
            else:
                span.set_status(Status(StatusCode.OK))
            
            return span
    
    @staticmethod
    async def call_ollama_with_tracing(model: str, prompt: str):
        """Call Ollama with automatic tracing"""
        import time
        from django.conf import settings
        
        start_time = time.time()
        
        with tracer.start_as_current_span("ollama.generate") as span:
            span.set_attribute("llm.model", model)
            span.set_attribute("llm.prompt_length", len(prompt))
            span.set_attribute("llm.provider", "ollama")
            
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        f"{settings.OLLAMA_BASE_URL}/api/generate",
                        json={"model": model, "prompt": prompt, "stream": False},
                        timeout=120
                    )
                    
                    duration_ms = (time.time() - start_time) * 1000
                    span.set_attribute("llm.duration_ms", duration_ms)
                    
                    if response.status_code == 200:
                        result = response.json()
                        span.set_status(Status(StatusCode.OK))
                        return result
                    else:
                        span.set_status(Status(StatusCode.ERROR, f"HTTP {response.status_code}"))
                        return None
                        
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

# Decorator pour tracer automatiquement les fonctions
def trace_function(name: str = None):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            span_name = name or func.__name__
            with tracer.start_as_current_span(span_name) as span:
                # Ajouter les arguments comme attributs
                for i, arg in enumerate(args):
                    span.set_attribute(f"arg.{i}", str(arg)[:100])
                
                try:
                    result = await func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise
        return wrapper
    return decorator