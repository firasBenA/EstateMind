# config/middleware.py
from django.db import connections


class CloseOldConnectionsMiddleware:
    """
    Close connections that have exceeded their max age or are unhealthy.

    IMPORTANT: This is NOT the same as closing after every request.
    Django's CONN_MAX_AGE=60 means a connection is kept alive for 60s and
    reused across requests within that window. This middleware only evicts
    connections that have already expired or failed their health check.

    What we fixed:
      - Removed the post-response close_old_connections() call.
        Calling it after EVERY response defeats CONN_MAX_AGE entirely —
        you were paying the TCP handshake cost on every single request.
      - The pre-request call is still here because a worker may have been
        idle long enough for Supabase's server-side idle timeout to drop
        the connection. Checking before the request (not after) catches
        that case without evicting healthy connections prematurely.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Only close connections that are past CONN_MAX_AGE or failed
        # health check — healthy connections within their max age are kept.
        for conn in connections.all():
            conn.close_if_unusable_or_obsolete()

        return self.get_response(request)