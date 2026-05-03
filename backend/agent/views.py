"""
SSE Chat endpoint and session views.
Streams agent responses in real-time to frontend.
"""
from decimal import Decimal
from django.core.serializers.json import DjangoJSONEncoder
import json
import uuid
import logging
from typing import Generator
from django.http import StreamingHttpResponse, JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.models import AnonymousUser
from django.middleware.csrf import get_token
from django.core.cache import cache

from agent.models import ChatSession, ChatMessage
from agent.agent import AgentOrchestrator
from agent.validators import InputValidator

_agent_instance = None

def _get_agent() -> AgentOrchestrator:
    global _agent_instance
    if _agent_instance is None:
        _agent_instance = AgentOrchestrator()
    return _agent_instance


class SSEJSONEncoder(DjangoJSONEncoder):
    """Custom JSON encoder that handles Decimal + Django types for SSE streaming."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)  # Convert Decimal → float for JSON
        return super().default(obj)

logger = logging.getLogger(__name__)

# Rate limiting: requests per minute per user/session
RATE_LIMIT_PER_MINUTE = 10


def _get_or_create_session(request, session_id=None):
    """Get or create a chat session."""
    if session_id:
        try:
            return ChatSession.objects.get(session_id=session_id, is_active=True)
        except ChatSession.DoesNotExist:
            pass

    # Create new session
    user = request.user if request.user.is_authenticated else None
    session_id = str(uuid.uuid4())

    session = ChatSession.objects.create(
        user=user,
        session_id=session_id,
        title=f"Chat {session_id[:8]}",
    )

    return session


def _check_rate_limit(session_id: str) -> tuple[bool, str]:
    """Check if user has exceeded rate limit."""
    cache_key = f"chat_rate:{session_id}"
    count = cache.get(cache_key, 0)

    if count >= RATE_LIMIT_PER_MINUTE:
        return False, "Rate limit exceeded. Please wait before sending another message."

    cache.set(cache_key, count + 1, 60)  # Expire after 60 seconds
    return True, ""


def _sse_stream(generator) -> Generator:
    """Convert generator output to SSE format with proper JSON encoding."""
    for data in generator:
        try:
            # 🔑 Use custom encoder to handle Decimal, datetime, UUID, etc.
            yield f"data: {json.dumps(data, cls=SSEJSONEncoder)}\n\n"
        except Exception as e:
            logger.error(f"SSE encoding error: {e}")
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)}, cls=SSEJSONEncoder)}\n\n"


@csrf_exempt
@require_http_methods(["POST", "GET"])
def chat_endpoint(request):
    """
    POST /api/chat/ - Send message, get SSE stream response
    GET /api/chat/ - Get CSRF token

    Request body (POST):
        {
            "message": "Search apartments in Tunis",
            "session_id": "uuid (optional)"
        }

    Response (SSE stream):
        data: {"type": "token", "content": "..."}
        data: {"type": "tool_call", "tool": "search_listings", "result": {...}}
        data: {"type": "action_required", "action": "create_listing", "preview": {...}}
        data: {"type": "error", "content": "..."}
        data: {"type": "end", "content": ""}
    """

    # GET: Return CSRF token
    if request.method == "GET":
        return JsonResponse({
            "csrf_token": get_token(request),
        })

    # POST: Process message
    try:
        data = json.loads(request.body.decode("utf-8"))
    except (json.JSONDecodeError, ValueError):
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    user_message = data.get("message", "").strip()
    session_id = data.get("session_id")

    # Validate input
    if not user_message or len(user_message) < 3:
        return JsonResponse(
            {"error": "Message must be at least 3 characters"},
            status=400
        )

    if len(user_message) > 1000:
        return JsonResponse(
            {"error": "Message too long (max 1000 characters)"},
            status=400
        )

    # Get or create session
    try:
        session = _get_or_create_session(request, session_id)
    except Exception as e:
        logger.error(f"Session creation error: {e}")
        return JsonResponse(
            {"error": "Failed to create chat session"},
            status=500
        )

    # Check rate limit
    allowed, error_msg = _check_rate_limit(session.session_id)
    if not allowed:
        return JsonResponse({"error": error_msg}, status=429)

    # Sanitize user message
    user_message = InputValidator.sanitize_string(user_message)

    # Save user message to DB
    try:
        ChatMessage.objects.create(
            session=session,
            role="user",
            content=user_message,
        )
    except Exception as e:
        logger.error(f"Failed to save user message: {e}")

    # Initialize agent
    try:
        agent = _get_agent()
    except Exception as e:
        logger.error(f"Agent initialization error: {e}")
        return JsonResponse(
            {"error": "Agent not available. Check backend logs."},
            status=503
        )

    # Get session message history for context
    history = list(
        list(session.messages.values("role", "content").order_by("created_at"))[-10:]
    )

    # Process message with agent (streaming)
    def response_generator():
        try:    
            agent_response_text = ""

            yield {
            "type":       "session",
            "session_id": session.session_id,
            }

            for chunk in agent.process_message(
                user_message=user_message,
                session_id=session.session_id,   # ← add this line
                session_messages=history,
                user_id=str(request.user.id) if request.user.is_authenticated else None,
            ):
                if chunk.get("type") == "token":
                    agent_response_text += chunk.get("content", "")

                # Save assistant message on end
                if chunk.get("type") == "end":
                    try:
                        ChatMessage.objects.create(
                            session=session,
                            role="assistant",
                            content=agent_response_text,
                        )
                    except Exception as e:
                        logger.error(f"Failed to save assistant message: {e}")

                yield chunk

        except Exception as e:
            logger.error(f"Stream processing error: {e}", exc_info=True)
            yield {
                "type": "error",
                "content": f"Stream error: {str(e)}",
            }

    # Return SSE response
    response = StreamingHttpResponse(
        _sse_stream(response_generator()),
        content_type="text/event-stream",
    )
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"

    return response


@require_http_methods(["GET"])
def get_sessions(request):
    """GET /api/chat/sessions/ - Get user's chat sessions."""
    if not request.user.is_authenticated:
        return JsonResponse({"error": "Unauthorized"}, status=401)

    sessions = ChatSession.objects.filter(
        user=request.user,
        is_active=True,
    ).order_by("-updated_at")[:20]

    return JsonResponse({
        "sessions": [
            {
                "session_id": s.session_id,
                "title": s.title,
                "updated_at": s.updated_at.isoformat(),
                "message_count": s.messages.count(),
            }
            for s in sessions
        ]
    })


@require_http_methods(["GET"])
def get_session_messages(request, session_id):
    """GET /api/chat/sessions/{session_id}/messages/ - Get messages in session."""
    try:
        session = ChatSession.objects.get(session_id=session_id, is_active=True)

        # Optional: Check ownership if authenticated
        if request.user.is_authenticated and session.user_id != request.user.id:
            return JsonResponse({"error": "Unauthorized"}, status=403)

        messages = ChatMessage.objects.filter(session=session).order_by("created_at")

        return JsonResponse({
            "session_id": session_id,
            "messages": [
                {
                    "role": m.role,
                    "content": m.content,
                    "timestamp": m.created_at.isoformat(),
                    "tool_calls": m.tool_calls,
                }
                for m in messages
            ]
        })

    except ChatSession.DoesNotExist:
        return JsonResponse({"error": "Session not found"}, status=404)


@require_http_methods(["DELETE"])
def delete_session(request, session_id):
    """DELETE /api/chat/sessions/{session_id}/ - Delete a session."""
    try:
        session = ChatSession.objects.get(session_id=session_id, is_active=True)

        # Check ownership
        if request.user.is_authenticated and session.user_id != request.user.id:
            return JsonResponse({"error": "Unauthorized"}, status=403)

        session.is_active = False
        session.save()

        logger.info(f"Session {session_id} deleted")

        return JsonResponse({"success": True})

    except ChatSession.DoesNotExist:
        return JsonResponse({"error": "Session not found"}, status=404)

