"""
LangChain Agent Orchestrator for EstateMind.
Handles tool orchestration, streaming, session state, and interactive clarification.

Session state is stored server-side in a dict keyed by session_id.
The FastAPI endpoint must pass session_id on every request and persist nothing on the client.
"""
import re
import json
import logging
from typing import Generator, Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory session store (replace with Redis in production)
# ---------------------------------------------------------------------------
SESSION_STORE: Dict[str, Dict[str, Any]] = {}


def get_session(session_id: str) -> Dict[str, Any]:
    if session_id not in SESSION_STORE:
        SESSION_STORE[session_id] = {
            "pending_search": None,
            "last_tool":      None,
            "last_result":    None,
            "refinement_count": 0,  # 🔹 NEW: Track refinement rounds
        }
    return SESSION_STORE[session_id]


def clear_pending(session_id: str) -> None:
    session = get_session(session_id)
    session["pending_search"] = None
    session["refinement_count"] = 0  # 🔹 Reset counter when clearing


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CITIES = [
    "tunis", "sfax", "ariana", "sousse", "manouba",
    "bizerte", "nabeul", "monastir", "gabes", "gafsa",
]

CLARIFICATION_THRESHOLD = 50
MAX_DISPLAY_LISTINGS    = 5
MAX_REFINEMENT_ROUNDS   = 3  # 🔹 NEW: Auto-show after this many refinements
AUTO_SHOW_THRESHOLD     = 500  # 🔹 NEW: If results <= this, offer to show


class AgentOrchestrator:
    """
    Agent orchestrator with per-session state and interactive clarification.
    """

    TOOLS_SCHEMA = [
        {
            "name": "search_listings",
            "description": "Search for property listings with filters",
            "parameters": {
                "type": "object",
                "properties": {
                    "query":            {"type": "string"},
                    "city":             {"type": "string"},
                    "region":           {"type": "string"},
                    "min_price":        {"type": "number"},
                    "max_price":        {"type": "number"},
                    "min_surface":      {"type": "number"},
                    "max_surface":      {"type": "number"},
                    "rooms":            {"type": "integer"},
                    "property_type":    {"type": "string"},
                    "transaction_type": {"type": "string"},
                    "page":             {"type": "integer"},
                    "page_size":        {"type": "integer"},
                },
                "required": [],
            },
        },
        {
            "name": "predict_price",
            "description": "Predict property price using ML model",
            "parameters": {
                "type": "object",
                "properties": {
                    "property_type":     {"type": "string"},
                    "city":              {"type": "string"},
                    "surface":           {"type": "number"},
                    "rooms":             {"type": "integer"},
                    "region":            {"type": "string"},
                    "transaction_type":  {"type": "string"},
                    "reliability_score": {"type": "number"},
                    "reliability_level": {"type": "string"},
                },
                "required": ["property_type", "city", "surface", "rooms"],
            },
        },
        {
            "name": "create_listing",
            "description": "Create a new property listing",
            "parameters": {
                "type": "object",
                "properties": {
                    "title":               {"type": "string"},
                    "price":               {"type": "number"},
                    "property_type":       {"type": "string"},
                    "transaction_type":    {"type": "string"},
                    "city":                {"type": "string"},
                    "description":         {"type": "string"},
                    "surface":             {"type": "number"},
                    "rooms":              {"type": "integer"},
                    "region":              {"type": "string"},
                    "action_confirmation": {"type": "boolean"},
                },
                "required": ["title", "price", "property_type", "transaction_type", "city"],
            },
        },
        {
            "name": "get_analytics",
            "description": "Get market analytics and statistics",
            "parameters": {
                "type": "object",
                "properties": {
                    "city":          {"type": "string"},
                    "region":        {"type": "string"},
                    "property_type": {"type": "string"},
                    "days":          {"type": "integer"},
                },
                "required": [],
            },
        },
    ]

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------
    def __init__(self):
        try:
            from langchain_together import ChatTogether
            self.llm = ChatTogether(
                model="meta-llama/Llama-3.1-405B-Instruct",
                temperature=0.7,
                max_tokens=1024,
            )
            self.initialized = True
            logger.info("✅ LangChain agent initialized with Together AI")
        except ImportError as e:
            logger.error(f"❌ Failed to import LangChain: {e}")
            self.initialized = False
            raise

    # ------------------------------------------------------------------
    # Tool registry
    # ------------------------------------------------------------------
    def _get_tool_handler(self, tool_name: str):
        from agent.tools.search        import search_listings
        from agent.tools.predict_price import predict_price
        from agent.tools.crud          import create_listing
        from agent.tools.analytics     import get_analytics

        return {
            "search_listings": search_listings,
            "predict_price":   predict_price,
            "create_listing":  create_listing,
            "get_analytics":   get_analytics,
        }.get(tool_name)

    # ------------------------------------------------------------------
    # Intent detection  (specific → general, CREATE before SEARCH)
    # ------------------------------------------------------------------
    def _detect_intent(self, message: str) -> Optional[str]:
        msg = message.lower()

        # 1. Create listing — must come before search to avoid 'list' collision
        if any(w in msg for w in [
            "create listing", "post listing", "add listing",
            "new listing", "publish listing", "create a property",
        ]):
            return "create_listing"

        # 2. Price prediction
        if any(w in msg for w in [
            "predict", "estimate", "how much", "price of",
            "what's the price", "valuation", "value of",
        ]):
            return "predict_price"

        # 3. Analytics
        if any(w in msg for w in [
            "stats", "analytics", "market", "metrics",
            "fraud", "quality", "statistics", "overview",
        ]):
            return "get_analytics"

        # 4. Search — broad, checked last
        if any(w in msg for w in [
            "search", "find", "show", "list", "display",
            "apartments", "villas", "properties", "houses",
            "looking for", "i want", "give me", "all",
        ]):
            return "search_listings"

        return None

    # ------------------------------------------------------------------
    # 🔹 NEW: Check for "show results" commands
    # ------------------------------------------------------------------
    def _is_show_command(self, message: str) -> bool:
        """Check if user wants to see results now instead of refining."""
        msg = message.lower().strip()
        show_phrases = [
            "show", "show me", "give me", "top", "just show", "enough",
            "ok show", "display", "see results", "that's enough", "stop asking",
            "i'm done", "show results", "show listings", "just display"
        ]
        return any(phrase in msg for phrase in show_phrases)

    def _extract_top_n(self, message: str) -> int:
        """Extract 'top N' number from message (e.g., 'show top 5' → 5)."""
        import re
        match = re.search(r'(?:top|show|give)\s*(\d+)', message.lower())
        return int(match.group(1)) if match else MAX_DISPLAY_LISTINGS

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------
    def process_message(
        self,
        user_message: str,
        session_id: str,
        session_messages: List[Dict[str, str]] = None,
        user_id: str = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Process user message and yield streaming chunks.

        Chunk types:
            {"type": "token",           "content": "..."}
            {"type": "action_required", "action": "...", "preview": {...}, "message": "..."}
            {"type": "clarification",   "content": "...", "pending_params": {...}}
            {"type": "error",           "content": "..."}
            {"type": "end",             "content": ""}
        """
        if not self.initialized:
            yield {"type": "error", "content": "Agent not initialized. Check logs."}
            return

        try:
            state   = get_session(session_id)
            msg_low = user_message.lower().strip()

            # 🔹 NEW: Check for reset command FIRST
            if any(w in msg_low for w in ["reset", "start over", "new search", "clear filters", "clear"]):
                clear_pending(session_id)
                yield {"type": "token", "content": "🔄 Search cleared! What are you looking for now?\n\n"}
                yield {"type": "token", "content": (
                    "💡 Try:\n"
                    "• *'3-bedroom apartments in Tunis under 200k'*\n"
                    "• *'Villas for rent in Sousse'*\n"
                    "• *'Land in Ariana'*\n\n"
                    "What would you like to find? 🏠"
                )}
                yield {"type": "end", "content": ""}
                return

            # ==============================================================
            # A. PENDING SEARCH — checked FIRST, before any intent detection.
            # ==============================================================
            if state["pending_search"] is not None:
                
                # 🔹 NEW: Check if user wants to SEE RESULTS now
                if self._is_show_command(user_message):
                    page_size = min(self._extract_top_n(user_message), 30)  # Cap at 30
                    
                    pending = state["pending_search"].copy()
                    pending["page"] = 1
                    pending["page_size"] = page_size
                    
                    yield {"type": "token", "content": f"📄 Showing top {page_size} results:\n\n"}
                    
                    handler = self._get_tool_handler("search_listings")
                    result = handler(**pending)
                    
                    # 🔹 Pass refinement_count for smart messaging
                    yield from self._format_search_response(
                        result, session_id, pending, 
                        refinement_count=state.get("refinement_count", 0),
                        force_show=True  # 🔹 NEW: Skip clarification, just show
                    )
                    
                    # Clear pending after showing
                    clear_pending(session_id)
                    yield {"type": "end", "content": ""}
                    return

                # User wants to refine further
                refinement = self._extract_search_params(user_message)
                refinement.pop("page", None)
                refinement.pop("page_size", None)

                # Merge: prior params as base, new params on top
                merged = {**state["pending_search"], **refinement}
                merged["page"] = 1
                merged["page_size"] = state["pending_search"].get("page_size", 10)

                # 🔹 Increment refinement counter
                state["refinement_count"] = state.get("refinement_count", 0) + 1
                state["pending_search"] = merged

                yield {"type": "token", "content": "🔍 Refining your search...\n\n"}

                handler = self._get_tool_handler("search_listings")
                result = handler(**merged)
                
                # 🔹 Pass refinement_count for smart messaging
                yield from self._format_search_response(
                    result, session_id, merged,
                    refinement_count=state["refinement_count"]
                )
                yield {"type": "end", "content": ""}
                return

            # ==============================================================
            # B. FRESH MESSAGE — intent detection
            # ==============================================================
            intent = self._detect_intent(user_message)

            if not intent:
                yield {"type": "token", "content": (
                    "🤔 I'm not sure what you're looking for.\n\n"
                    "I can help you with:\n"
                    "• 🔍 **Search listings** — *'Find 3-bedroom apartments in Tunis'*\n"
                    "• 💰 **Price prediction** — *'How much is a 120m² house in Sfax?'*\n"
                    "• 📊 **Market analytics** — *'Show me market stats for Sousse'*\n"
                    "• 📝 **Create a listing** — *'Create a new listing'*\n\n"
                    "Could you rephrase your request?"
                )}
                yield {"type": "end", "content": ""}
                return

            # Extract params for detected intent
            extractors = {
                "search_listings": self._extract_search_params,
                "predict_price":   self._extract_predict_params,
                "create_listing":  self._extract_listing_params,
                "get_analytics":   self._extract_analytics_params,
            }
            tool_args = extractors[intent](user_message)

            handler = self._get_tool_handler(intent)
            if not handler:
                yield {"type": "error", "content": f"Tool '{intent}' not found."}
                return

            # Loading indicator
            loading_msgs = {
                "search_listings": "🔍 Searching listings...\n\n",
                "predict_price":   "🤖 Running price prediction model...\n\n",
                "create_listing":  "📝 Preparing listing...\n\n",
                "get_analytics":   "📊 Fetching market analytics...\n\n",
            }
            yield {"type": "token", "content": loading_msgs.get(intent, "⏳ Processing...\n\n")}

            result = handler(**tool_args)

            # Update session
            state["last_tool"]   = intent
            state["last_result"] = result

            # Confirmation gate for create_listing
            if result.get("requires_confirmation"):
                yield {
                    "type":    "action_required",
                    "action":  result.get("action"),
                    "preview": result.get("preview"),
                    "message": result.get("message"),
                }
                return

            # Format and stream response
            if intent == "search_listings":
                # 🔹 Reset refinement count for fresh searches
                state["refinement_count"] = 0
                yield from self._format_search_response(result, session_id, tool_args)
            elif intent == "predict_price":
                yield from self._format_predict_response(result)
            elif intent == "get_analytics":
                yield from self._format_analytics_response(result)
            elif intent == "create_listing":
                yield {"type": "token", "content": result.get("message", "✅ Listing created!")}

            yield {"type": "end", "content": ""}

        except Exception as e:
            logger.error(f"Agent processing error: {e}", exc_info=True)
            yield {"type": "error", "content": f"Error processing request: {e}"}

    # ------------------------------------------------------------------
    # Parameter extractors (unchanged - working well)
    # ------------------------------------------------------------------
    def _extract_search_params(self, message: str) -> Dict[str, Any]:
        params  = {}
        msg_low = message.lower()

        # City
        for city in CITIES:
            if city in msg_low:
                params["city"] = city.capitalize()
                break

        # Max price
        max_price_match = re.search(
            r'(?:under|below|max|maximum|less than)\s*(\d[\d,]*)\s*(k\b)?'
            r'\s*(?:tnd|dinars?|dt)?',
            msg_low,
        )
        if max_price_match:
            raw = max_price_match.group(1).replace(",", "")
            try:
                price = int(raw)
                if max_price_match.group(2):
                    price *= 1000
                elif price < 10_000:
                    price *= 1000
                params["max_price"] = price
            except ValueError:
                pass

        # Min price
        min_price_match = re.search(
            r'(?:above|over|min|minimum|more than)\s*(\d[\d,]*)\s*(k\b)?'
            r'\s*(?:tnd|dinars?|dt)?',
            msg_low,
        )
        if min_price_match:
            raw = min_price_match.group(1).replace(",", "")
            try:
                price = int(raw)
                if min_price_match.group(2):
                    price *= 1000
                elif price < 10_000:
                    price *= 1000
                params["min_price"] = price
            except ValueError:
                pass

        # Rooms
        room_match = re.search(r'(\d+)\s*(?:rooms?|bedrooms?|beds?|chambres?)', msg_low)
        if room_match:
            params["rooms"] = int(room_match.group(1))

        # Surface → min_surface / max_surface
        surface_match = re.search(
            r'(\d+(?:\.\d+)?)\s*(?:m2|m²|sqm|square\s*meters?)', msg_low
        )
        if surface_match:
            surface_val = float(surface_match.group(1))
            if any(w in msg_low for w in ["at least", "minimum", "min", "more than", "above", "bigger", "over"]):
                params["min_surface"] = surface_val
            elif any(w in msg_low for w in ["at most", "maximum", "max", "less than", "below", "under", "smaller"]):
                params["max_surface"] = surface_val
            else:
                params["min_surface"] = round(surface_val * 0.8, 1)
                params["max_surface"] = round(surface_val * 1.2, 1)

        # Property type
        if any(t in msg_low for t in ["apartment", "appart", "appartement", "flat"]):
            params["property_type"] = "apartment"
        elif any(t in msg_low for t in ["house", "villa", "maison"]):
            params["property_type"] = "house"
        elif any(t in msg_low for t in ["land", "terrain", "plot"]):
            params["property_type"] = "land"
        elif any(t in msg_low for t in ["commercial", "shop", "store", "bureau", "office"]):
            params["property_type"] = "commercial"

        # Transaction type
        if any(t in msg_low for t in ["rent", "rental", "louer", "location", "to let"]):
            params["transaction_type"] = "rent"
        elif any(t in msg_low for t in ["sale", "sell", "vente", "acheter", "buy", "purchase", "for sale"]):
            params["transaction_type"] = "sale"

        params.setdefault("page", 1)
        params.setdefault("page_size", 10)

        return params

    def _extract_predict_params(self, message: str) -> Dict[str, Any]:
        msg_low = message.lower()
        params  = {
            "property_type":     "apartment",
            "city":              "Tunis",
            "surface":           100.0,
            "rooms":             2,
            "reliability_level": "GOOD",
        }

        surface_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:m2|m²|sqm|square\s*meters?)', msg_low)
        if surface_match:
            params["surface"] = float(surface_match.group(1))

        room_match = re.search(r'(\d+)\s*(?:rooms?|bedrooms?|beds?|chambres?)', msg_low)
        if room_match:
            params["rooms"] = int(room_match.group(1))

        for city in CITIES:
            if city in msg_low:
                params["city"] = city.capitalize()
                break

        if any(t in msg_low for t in ["house", "villa", "maison"]):
            params["property_type"] = "house"
        elif any(t in msg_low for t in ["land", "terrain", "plot"]):
            params["property_type"] = "land"
        elif any(t in msg_low for t in ["commercial", "shop", "store", "bureau", "office"]):
            params["property_type"] = "commercial"

        if any(t in msg_low for t in ["rent", "rental", "louer", "location"]):
            params["transaction_type"] = "rent"
        elif any(t in msg_low for t in ["sale", "sell", "vente", "acheter", "buy"]):
            params["transaction_type"] = "sale"

        return params

    def _extract_listing_params(self, message: str) -> Dict[str, Any]:
        msg_low = message.lower()
        params  = {}

        price_match = re.search(r'(\d[\d,]*)\s*(k\b)?\s*(?:tnd|dinars?|dt)?', msg_low)
        if price_match:
            raw = price_match.group(1).replace(",", "")
            try:
                price = int(raw)
                if price_match.group(2):
                    price *= 1000
                params["price"] = price
            except ValueError:
                pass

        surface_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:m2|m²|sqm)', msg_low)
        if surface_match:
            params["surface"] = float(surface_match.group(1))

        room_match = re.search(r'(\d+)\s*(?:rooms?|bedrooms?|beds?)', msg_low)
        if room_match:
            params["rooms"] = int(room_match.group(1))

        for city in CITIES:
            if city in msg_low:
                params["city"] = city.capitalize()
                break

        if any(t in msg_low for t in ["apartment", "appart", "flat"]):
            params["property_type"] = "apartment"
        elif any(t in msg_low for t in ["house", "villa", "maison"]):
            params["property_type"] = "house"
        elif any(t in msg_low for t in ["land", "terrain", "plot"]):
            params["property_type"] = "land"
        elif any(t in msg_low for t in ["commercial", "shop", "office"]):
            params["property_type"] = "commercial"

        if any(t in msg_low for t in ["rent", "rental", "louer"]):
            params["transaction_type"] = "rent"
        else:
            params["transaction_type"] = "sale"

        params["action_confirmation"] = any(
            w in msg_low for w in ["yes", "confirm", "go ahead", "proceed", "ok", "sure"]
        )

        return params

    def _extract_analytics_params(self, message: str) -> Dict[str, Any]:
        msg_low = message.lower()
        params  = {"days": 30}

        for city in CITIES:
            if city in msg_low:
                params["city"] = city.capitalize()
                break

        if any(t in msg_low for t in ["apartment", "appart", "flat"]):
            params["property_type"] = "apartment"
        elif any(t in msg_low for t in ["house", "villa", "maison"]):
            params["property_type"] = "house"
        elif any(t in msg_low for t in ["land", "terrain", "plot"]):
            params["property_type"] = "land"

        days_match = re.search(r'(?:last|past)\s*(\d+)\s*days?', msg_low)
        if days_match:
            params["days"] = int(days_match.group(1))

        return params

    # ------------------------------------------------------------------
    # 🔹 UPDATED: Response formatters with smart thresholds
    # ------------------------------------------------------------------
    def _format_search_response(
        self,
        result:          Dict,
        session_id:      str,
        tool_args:       Dict,
        refinement_count: int = 0,      # 🔹 NEW param
        force_show:      bool = False,  # 🔹 NEW param
    ) -> Generator[Dict, None, None]:

        state = get_session(session_id)
        count   = result.get("count", 0)
        results = result.get("results", [])
        page    = result.get("page", 1)
        pages   = result.get("pages", 1)

        # Error
        if "error" in result:
            clear_pending(session_id)
            yield {"type": "token", "content": f"❌ {result['error']}\n\n"}
            yield {"type": "token", "content": "💡 Try: *'apartments in Tunis under 200,000 TND'*"}
            return

        # No results
        if count == 0:
            clear_pending(session_id)
            yield {"type": "token", "content": "🔍 No listings found matching your criteria.\n\n"}
            yield {"type": "token", "content": (
                "💡 Tips:\n"
                "• Widen your price range\n"
                "• Try a different city or property type\n"
                "• Remove some filters\n\n"
                "Or say *'reset'* to start a fresh search! 🏠"
            )}
            return

        # 🔹 SMART THRESHOLD: Auto-show if refined enough OR results manageable
        should_ask = (
            count > CLARIFICATION_THRESHOLD 
            and refinement_count < MAX_REFINEMENT_ROUNDS
            and count > AUTO_SHOW_THRESHOLD
            and not force_show
        )

        if should_ask:
            # Store pending for next refinement round
            state["pending_search"] = tool_args
            
            # Positive, encouraging language
            progress_msg = {
                0: "🏠 Found many listings! Let's narrow it down.",
                1: "✨ Great progress! Getting closer.",
                2: "🎯 Almost there! Just a bit more.",
            }.get(refinement_count, "💡 Let's refine a bit more.")
            
            yield {"type": "token", "content": f"{progress_msg}\n\n"}
            yield {"type": "token", "content": "Please answer one or more of these:\n\n"}
            yield {"type": "token", "content": (
                "**💰 Budget?**\n"
                "   e.g. *'under 150,000 TND'* or *'between 80k and 200k'*\n\n"
                "**🛏️ Bedrooms?**\n"
                "   e.g. *'2 bedrooms'* or *'3 rooms'*\n\n"
                "**📐 Size?**\n"
                "   e.g. *'under 100m²'* or *'at least 80m²'*\n\n"
                "**🏠 Property type?**\n"
                "   apartment / house / villa / land / commercial\n\n"
                "**📍 More specific area?**\n"
                "   e.g. *'in Sfax'* or *'in Sousse'*\n\n"
                "💡 You can combine them: *'2 bedrooms under 150,000 in Sfax'*\n\n"
                "Or say *'show top 5'* to see the best matches now! ✨\n"
                "Or *'reset'* to start over.\n"
            )}
            yield {
                "type":           "clarification",
                "content":        f"Awaiting refinement for {count} results.",
                "pending_params": tool_args,
            }
            return

        # 🔹 Show results! (either forced, or below threshold, or refined enough)
        clear_pending(session_id)

        # Header
        yield {"type": "token", "content": f"🏠 Found **{count} listing{'s' if count != 1 else ''}**"}
        if pages > 1:
            yield {"type": "token", "content": f" (Page {page}/{pages})"}
        yield {"type": "token", "content": ":\n\n"}

        TYPE_EMOJI = {
            "apartment":  "🏢",
            "house":      "🏡",
            "villa":      "🏰",
            "land":       "🌾",
            "commercial": "🏪",
        }

        for i, listing in enumerate(results[:MAX_DISPLAY_LISTINGS], 1):
            title      = listing.get("title", "Untitled")
            price      = listing.get("price", 0)
            city       = listing.get("city", "Unknown")
            surface    = listing.get("surface")
            rooms      = listing.get("rooms")
            prop_type  = (listing.get("property_type") or "").lower()
            listing_id = listing.get("id")
            url        = listing.get("url", "")

            listing_url = (
                f"http://localhost:8081/listing/{listing_id}"
                if listing_id
                else f"http://localhost:8081/listing/{url}"
            )

            price_str    = f"{price:,.0f} TND" if price else "Price on request"
            detail_parts = []
            if surface:
                detail_parts.append(f"{int(surface)}m²")
            if rooms:
                detail_parts.append(f"{rooms} bed")
            details_str = " • ".join(detail_parts) if detail_parts else "Details on request"
            emoji       = TYPE_EMOJI.get(prop_type, "🏠")
            title       = title[:47] + "..." if len(title) > 50 else title

            yield {"type": "token", "content": (
                f"{i}. {emoji} **{title}**\n"
                f"   💰 {price_str} • 📍 {city}\n"
                f"   📐 {details_str}\n"
                f"   🔗 [View listing →]({listing_url})\n\n"
            )}

        # 🔹 Smart footer based on count and refinement history
        if count > MAX_DISPLAY_LISTINGS:
            yield {"type": "token", "content": f"🎯 **Showing {MAX_DISPLAY_LISTINGS} of {count} results.**\n\n"}

        # Options footer
        if pages > 1 and page < pages:
            yield {"type": "token", "content": (
                "💬 **What next?**\n"
                "• *'Show next page'* — more results\n"
                "• *'Cheaper options'* — reduce price\n"
                "• *'Bigger properties'* — increase size\n"
                "• *'Different city'* — change location\n\n"
            )}
        elif count > AUTO_SHOW_THRESHOLD and refinement_count < MAX_REFINEMENT_ROUNDS and not force_show:
            # Still many results, but offer escape hatch
            yield {"type": "token", "content": (
                "💡 **Still quite a few!** You can:\n"
                "• Say *'show top 10'* to see more now\n"
                "• Or refine: *'with balcony'*, *'newly built'*, *'near metro'*\n"
                "• Or *'reset'* to start fresh 🔄\n\n"
            )}
        else:
            # Good number of results → encourage exploration
            yield {"type": "token", "content": (
                "💬 **Want to refine?** Try:\n"
                "• *'With garden'* — add amenity\n"
                "• *'Under 100,000'* — adjust budget\n"
                "• *'Details on #1'* — learn more about a listing\n\n"
            )}

        # Cross-sell other capabilities
        yield {"type": "token", "content": (
            "❤️ I can also **predict prices**, show **market stats**, or **create a listing**!"
        )}

    def _format_predict_response(self, result: Dict) -> Generator[Dict, None, None]:
        if "error" in result:
            yield {"type": "token", "content": f"❌ {result['error']}\n"}
            return

        price      = result.get("predicted_price", 0)
        confidence = result.get("confidence", 0)
        min_est    = result.get("min_estimate", 0)
        max_est    = result.get("max_estimate", 0)
        reasoning  = result.get("reasoning", "")

        yield {"type": "token", "content": (
            f"💰 **Predicted Price: {price:,.0f} TND**\n\n"
            f"📊 Range: {min_est:,.0f} – {max_est:,.0f} TND\n"
            f"🎯 Confidence: {confidence * 100:.0f}%\n"
        )}
        if reasoning:
            yield {"type": "token", "content": f"\n💡 {reasoning}\n"}

    def _format_analytics_response(self, result: Dict) -> Generator[Dict, None, None]:
        if "error" in result:
            yield {"type": "token", "content": f"❌ {result['error']}\n"}
            return

        yield {"type": "token", "content": (
            f"📊 **Market Analytics**\n\n"
            f"🏠 Total Listings:  {result.get('total_listings', 0):,}\n"
            f"💰 Avg Price:       {result.get('avg_price', 0):,.0f} TND\n"
            f"📐 Avg Price/m²:    {result.get('avg_price_per_m2', 0):,.0f} TND\n"
            f"🚨 Fraud Rate:      {result.get('fraud_rate', 0):.1f}%\n"
            f"📈 Outlier Rate:    {result.get('outlier_rate', 0):.1f}%\n"
        )}