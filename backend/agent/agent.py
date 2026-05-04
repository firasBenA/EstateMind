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
import os
from agent.tools.llm_extractor import extract_predict_price_params_with_llm

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
            "refinement_count": 0,
        }
    return SESSION_STORE[session_id]


def clear_pending(session_id: str) -> None:
    session = get_session(session_id)
    session["pending_search"] = None
    session["refinement_count"] = 0


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CITIES = [
    "tunis", "sfax", "ariana", "sousse", "manouba",
    "bizerte", "nabeul", "monastir", "gabes", "gafsa",
]
CITY_ALIASES = {
    "ariena": "Ariana",
}

CLARIFICATION_THRESHOLD = 50
MAX_DISPLAY_LISTINGS    = 5
MAX_REFINEMENT_ROUNDS   = 3
AUTO_SHOW_THRESHOLD     = 500


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
        {
            "name": "generate_contract",
            "description": "Generate a legal contract (sales agreement, rental agreement, etc.)",
            "parameters": {
                "type": "object",
                "properties": {
                    "contract_type": {"type": "string", "description": "compromis_de_vente, promesse_de_vente, contrat_de_location, or acte_de_vente"},
                    "seller_name": {"type": "string"},
                    "seller_cin": {"type": "string"},
                    "seller_address": {"type": "string"},
                    "buyer_name": {"type": "string"},
                    "buyer_cin": {"type": "string"},
                    "buyer_address": {"type": "string"},
                    "listing_id": {"type": "string"},
                    "listing_title": {"type": "string"},
                    "listing_address": {"type": "string"},
                    "surface": {"type": "number"},
                    "price": {"type": "number"},
                    "transaction_date": {"type": "string"},
                    "transaction_type": {"type": "string"},
                },
                "required": ["contract_type", "buyer_name", "buyer_cin"],
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


    def _extract_contract_params(self, message: str) -> Dict[str, Any]:
        """Extract contract parameters from natural language."""
        import re
        from datetime import datetime
        
        params = {}
        msg_low = message.lower()
        
        # Contract type
        if any(t in msg_low for t in ["sales agreement", "compromis"]):
            params["contract_type"] = "compromis_de_vente"
        elif "promesse" in msg_low:
            params["contract_type"] = "promesse_de_vente"
        elif any(t in msg_low for t in ["rental", "location"]):
            params["contract_type"] = "contrat_de_location"
        elif any(t in msg_low for t in ["acte de vente", "final deed"]):
            params["contract_type"] = "acte_de_vente"
        else:
            params["contract_type"] = "compromis_de_vente"  # Default
        
        # Extract buyer name from common phrasing like 'with X' or 'for X'
        buyer_match = re.search(
            r'(?:with|for|buyer|client)\s+([^,\d]+?)(?:,|\bcin\b|\bid\b|$)',
            message,
            flags=re.IGNORECASE,
        )
        if buyer_match:
            buyer_name = buyer_match.group(1).strip(' .')
            if buyer_name:
                params["buyer_name"] = buyer_name

        # Extract CIN/ID
        cin_match = re.search(r'(?:cin|id)[:\s]+(\d{6,})', msg_low)
        if cin_match:
            params["buyer_cin"] = cin_match.group(1)

        # Extract city or listing location
        for city in CITIES:
            if re.search(rf'\b{re.escape(city)}\b', msg_low):
                params["listing_address"] = city.capitalize()
                params.setdefault("listing_title", f"Property in {city.capitalize()}")
                break
        
        # Extract property details if mentioned
        surface_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:m2|m²|sqm)', msg_low)
        if surface_match:
            params["surface"] = float(surface_match.group(1))
        
        price_match = re.search(r'(\d[\d,]*)\s*(?:tnd|dinars?|dt)?', msg_low)
        if price_match:
            try:
                price = int(price_match.group(1).replace(',', ''))
                params["price"] = price * 1000 if price < 10000 else price
            except ValueError:
                pass
        
        # Default values
        params.setdefault("seller_name", "")
        params.setdefault("seller_cin", "")
        params.setdefault("seller_address", "")
        params.setdefault("buyer_address", "")
        params.setdefault("listing_id", "")
        params.setdefault("listing_title", "")
        params.setdefault("transaction_date", datetime.now().strftime("%Y-%m-%d"))
        params.setdefault("transaction_type", "sale")
        
        return params

    # ------------------------------------------------------------------
    # Tool registry
    # ------------------------------------------------------------------
    def _get_tool_handler(self, tool_name: str):
        from agent.tools.search        import search_listings
        from agent.tools.predict_price import predict_price
        from agent.tools.crud          import create_listing
        from agent.tools.analytics     import get_analytics

        try:
            from agent.tools.contract import generate_contract
            contract_handler = generate_contract
        except ImportError as e:
            logger.warning(f"⚠️ Contract tool not available: {e}")
            contract_handler = None

        return {
                "search_listings": search_listings,
                "predict_price":   predict_price,
                "create_listing":  create_listing,
                "get_analytics":   get_analytics,
                "generate_contract": contract_handler,
            }.get(tool_name)

    # ------------------------------------------------------------------
    # LLM-powered conversational fallback
    # ------------------------------------------------------------------
    CHAT_SYSTEM_PROMPT = """You are EstateMind, a warm and friendly real estate assistant specialised in the Tunisian property market.

Your personality:
- Conversational, helpful, and encouraging — like a knowledgeable friend
- You use light emojis naturally (don't overdo it)
- You keep replies concise (2-5 sentences max for chat messages)
- You speak English, French, and Darija (Tunisian Arabic) naturally

Your strict boundaries:
- You ONLY discuss real estate topics: buying, renting, selling, prices, neighbourhoods, contracts, market trends, property types, renovation tips, investment advice in Tunisia
- If someone asks about anything unrelated (sports, cooking, politics, weather, general coding, etc.), gently redirect them back to real estate
- Never make up property listings or prices — those come from the database

Your capabilities you can mention:
- Search real listings (apartments, houses, villas, land, commercial)
- Predict property prices with an ML model
- Show market analytics and trends
- Generate legal contracts (sales, rental)
- Create new listings

Always end chat messages with a soft nudge toward what you can actually do for them."""

    def _llm_chat(self, user_message: str, history: List[Dict] = None) -> Generator[Dict, None, None]:
        """
        Stream a conversational response from the LLM.
        Used for greetings, small talk, clarifications, and anything
        that doesn't map to a specific tool call.
        """
        try:
            from langchain_core.messages import SystemMessage, HumanMessage, AIMessage

            messages = [SystemMessage(content=self.CHAT_SYSTEM_PROMPT)]

            # Include recent conversation history for context
            if history:
                for msg in history[-6:]:  # last 3 turns
                    role    = msg.get("role", "")
                    content = msg.get("content", "")
                    if role == "user":
                        messages.append(HumanMessage(content=content))
                    elif role == "assistant":
                        messages.append(AIMessage(content=content))

            messages.append(HumanMessage(content=user_message))

            # Stream response token by token
            for chunk in self.llm.stream(messages):
                token = chunk.content
                if token:
                    yield {"type": "token", "content": token}

        except Exception as e:
            logger.warning(f"LLM chat fallback error: {e}")
            # Graceful degradation — still better than a cold error
            yield {"type": "token", "content": (
                "Hey! 😊 I'm EstateMind, your real estate assistant for Tunisia.\n\n"
                "I can help you find properties, estimate prices, or generate contracts.\n"
                "What are you looking for? 🏠"
            )}

    # ------------------------------------------------------------------
    # Intent detection  (specific → general, CREATE before SEARCH)
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    def _detect_intent(self, message: str) -> Optional[str]:
        msg = message.lower()

        # 1. Create listing
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

        # 4. Contract generation
        if any(phrase in msg for phrase in [
            "generate contract", "create contract", "make contract",
            "sales agreement", "compromis de vente", "promesse de vente",
            "rental agreement", "contrat de location", "acte de vente",
            "final deed", "deed of sale", "generate a contract",
            "i need a contract", "prepare a contract", "draft a contract",
        ]):
            return "generate_contract"

        # 5. Search
        if any(w in msg for w in [
            "search", "find", "show", "list", "display",
            "apartments", "villas", "properties", "houses",
            "looking for", "i want", "give me", "all",
        ]):
            return "search_listings"

        return None

    # ------------------------------------------------------------------
    # Check for "show results" commands
    # ------------------------------------------------------------------
    def _is_show_command(self, message: str) -> bool:
        """Check if user wants to see results now instead of refining."""
        msg = message.lower().strip()
        show_phrases = [
            "show", "show me", "give me", "best", "top", "just show", "enough",
            "ok show", "display", "see results", "that's enough", "stop asking",
            "i'm done", "show results", "show listings", "just display"
        ]
        return any(phrase in msg for phrase in show_phrases)

    def _extract_top_n(self, message: str) -> int:
        """Extract 'top N' number from message (e.g., 'show top 5' or 'best 3' → 5/3)."""
        match = re.search(r'(?:top|show|give|best)\s*(?:the\s*)?(\d+)', message.lower())
        return int(match.group(1)) if match else MAX_DISPLAY_LISTINGS

    # ------------------------------------------------------------------
    # Smart clarification question builder
    # ------------------------------------------------------------------
    def _build_clarification_message(
        self,
        count: int,
        params: Dict[str, Any],
        refinement_count: int,
    ) -> str:
        """
        Build a targeted, friendly clarification question based on what
        filters are already set vs. what's still missing.
        Asks at most ONE focused question + offers an escape hatch.
        """
        has_price   = "min_price" in params or "max_price" in params
        has_rooms   = "rooms" in params
        has_surface = "min_surface" in params or "max_surface" in params
        has_city    = "city" in params
        has_tx      = "transaction_type" in params
        prop_type   = params.get("property_type", "")

        # --- Pick the single most-impactful missing filter ---

        # 1. Transaction type unknown → buying or renting?
        if not has_tx:
            return (
                f"🏠 Great, I found **{count} houses** in {params.get('city', 'the area')}! "
                f"Are you looking to **buy** or **rent**?\n\n"
                f"💬 *Reply:* \"buy\" or \"rent\" — or say **\"show top 3\"** to see the best matches now."
            )

        # 2. No budget set → what's the price range?
        if not has_price:
            tx_label = "rent" if params.get("transaction_type") == "rent" else "budget"
            examples = (
                "e.g. *\"under 800 TND/month\"*"
                if params.get("transaction_type") == "rent"
                else "e.g. *\"under 300k TND\"* or *\"between 150k and 400k\"*"
            )
            return (
                f"💰 Found **{count} results** — that's quite a range! "
                f"Do you have a {tx_label} in mind?\n\n"
                f"💬 {examples} — or say **\"show top 3\"** to jump straight in."
            )

        # 3. No room count → how many bedrooms?
        if not has_rooms:
            return (
                f"🛏️ **{count} listings** match so far. How many bedrooms are you looking for?\n\n"
                f"💬 e.g. *\"3 bedrooms\"*, *\"at least 2 rooms\"* — or say **\"show top 5\"** to see results now."
            )

        # 4. No surface constraint → preferred size?
        if not has_surface:
            return (
                f"📐 **{count} results** found. Any preference on size?\n\n"
                f"💬 e.g. *\"at least 150m²\"*, *\"around 200m²\"* — or say **\"show top 5\"** if size doesn't matter."
            )

        # 5. All main filters set but still many results → offer to sort/show
        price_hint = ""
        if "max_price" in params:
            price_hint = f" under {params['max_price']:,.0f} TND"

        rooms_hint = f", {params['rooms']}+ beds" if has_rooms else ""

        return (
            f"🎯 I've narrowed it down to **{count} listings**{price_hint}{rooms_hint}. "
            f"That's still a fair number — want me to sort by **best value**, "
            f"**newest**, or just **show top 5** now?\n\n"
            f"💬 *Reply:* \"best value\", \"newest\", \"show top 5\" — or give me another filter!"
        )

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

            # Check for reset command FIRST
            if any(w in msg_low for w in ["reset", "start over", "new search", "clear filters", "clear"]):
                clear_pending(session_id)
                yield {"type": "token", "content": "🔄 Search cleared! What are you looking for?\n\n"}
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
                
                # Check if user wants to SEE RESULTS now (top N / show / best)
                if self._is_show_command(user_message):
                    page_size = min(self._extract_top_n(user_message), 30)
                    
                    pending = state["pending_search"].copy()
                    pending["page"] = 1
                    pending["page_size"] = page_size

                    # Natural acknowledgment based on what they asked for
                    top_n_match = re.search(r'\d+', user_message)
                    if top_n_match:
                        yield {"type": "token", "content": f"👍 Here are the top **{page_size}** results:\n\n"}
                    else:
                        yield {"type": "token", "content": f"👍 Showing the best results for you:\n\n"}
                    
                    handler = self._get_tool_handler("search_listings")
                    result = handler(**pending)
                    
                    yield from self._format_search_response(
                        result, session_id, pending, 
                        refinement_count=state.get("refinement_count", 0),
                        force_show=True
                    )
                    
                    clear_pending(session_id)
                    yield {"type": "end", "content": ""}
                    return

                # User wants to refine further — extract new params and merge
                refinement = self._extract_search_params(user_message)
                refinement.pop("page", None)
                refinement.pop("page_size", None)

                # Merge: prior params as base, new params on top
                merged = {**state["pending_search"], **refinement}
                merged["page"] = 1
                merged["page_size"] = state["pending_search"].get("page_size", 10)

                # Increment refinement counter
                state["refinement_count"] = state.get("refinement_count", 0) + 1
                state["pending_search"] = merged

                yield {"type": "token", "content": "🔍 Refining your search...\n\n"}

                handler = self._get_tool_handler("search_listings")
                result = handler(**merged)
                
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

            # ── Conversational fallback: let the LLM handle it naturally ──
            if not intent:
                yield from self._llm_chat(user_message, history=session_messages)
                yield {"type": "end", "content": ""}
                return

            # Extract params for detected intent
            extractors = {
                "search_listings": self._extract_search_params,
                "predict_price":   self._extract_predict_params,
                "create_listing":  self._extract_listing_params,
                "get_analytics":   self._extract_analytics_params,
                "generate_contract": self._extract_contract_params,
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
                "generate_contract": "📝 Generating contract...\n\n",
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
                state["refinement_count"] = 0
                yield from self._format_search_response(result, session_id, tool_args)
            elif intent == "predict_price":
                yield from self._format_predict_response(result)
            elif intent == "get_analytics":
                yield from self._format_analytics_response(result)
            elif intent == "create_listing":
                yield {"type": "token", "content": result.get("message", "✅ Listing created!")}
            elif intent == "generate_contract":
                logger.info(f"📝 Agent: Detected contract intent, params: {tool_args}")
                if "error" in result:
                    logger.error(f"📝 Agent: Contract generation error: {result['error']}")
                    yield {"type": "error", "content": result["error"]}
                else:
                    logger.info(f"📝 Agent: Contract generated successfully, text length: {len(result.get('contract_text', ''))}")
                    yield {"type": "token", "content": result.get("contract_text", "")}
                    yield {
                        "type": "action_required",
                        "action": "save_contract",
                        "preview": {
                            "contract_type": result.get("contract_type"),
                            "params": result.get("params"),
                        },
                        "message": "Contract generated successfully! Would you like to save it?",
                    }

            yield {"type": "end", "content": ""}

        except Exception as e:
            logger.error(f"Agent processing error: {e}", exc_info=True)
            yield {"type": "error", "content": f"Error processing request: {e}"}

    # ------------------------------------------------------------------
    # Parameter extractors
    # ------------------------------------------------------------------
    def _extract_search_params(self, message: str) -> Dict[str, Any]:
        params  = {}
        msg_low = message.lower()

        # City alias handling first
        for alias, canonical in CITY_ALIASES.items():
            if alias in msg_low:
                params["city"] = canonical
                break
        else:
            for city in CITIES:
                if city in msg_low:
                    params["city"] = city.capitalize()
                    break

        # Top N / best results
        top_n_match = re.search(r'(?:top|best|show)\s*(?:the\s*)?(\d+)', msg_low)
        if top_n_match:
            params["page_size"] = min(int(top_n_match.group(1)), 30)

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
        # Detect "200m² or more" / "at least 200m²" patterns first
        surface_match = re.search(
            r'(\d+(?:\.\d+)?)\s*(?:m2|m²|sqm|square\s*meters?)', msg_low
        )
        if surface_match:
            surface_val = float(surface_match.group(1))
            # Check full message for directional keywords (before OR after the number)
            is_min = any(w in msg_low for w in [
                "at least", "minimum", "min", "more than", "above",
                "bigger", "over", "or more", "+", "≥",
            ])
            is_max = any(w in msg_low for w in [
                "at most", "maximum", "max", "less than", "below",
                "under", "smaller", "or less", "≤",
            ])
            if is_min:
                params["min_surface"] = surface_val
            elif is_max:
                params["max_surface"] = surface_val
            else:
                # Default: treat bare "200m²" as a minimum (user intent is usually "at least this big")
                params["min_surface"] = surface_val

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

    def _detect_llm_provider(self) -> str:
        """Detect which LLM provider was used for extraction."""
        if os.getenv("OPENAI_API_KEY") and os.getenv("OPENAI_API_KEY").startswith("sk-proj-"):
            return "openai"
        elif os.getenv("GROQ_API_KEY"):
            return "groq"
        return "unknown"

    def _extract_predict_params(self, message: str) -> Dict[str, Any]:
        msg_low = message.lower()
        params  = {
            "property_type":     "apartment",
            "city":              "Tunis",
            "surface":           100.0,
            "rooms":             2,
            "reliability_level": "GOOD",
        }

        llm_params = extract_predict_price_params_with_llm(message)
        if llm_params:
            params.update({k: v for k, v in llm_params.items() if v is not None})
            params["_extraction_method"] = "llm"
            params["_llm_provider"] = self._detect_llm_provider()
            logger.info(f"🎯 [AGENT] LLM extraction successful via {params['_llm_provider']}")
        else:
            logger.info("🎯 [AGENT] LLM extraction failed, falling back to regex")
            
        if params.get("surface") in (None, 100.0):
            surface_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:m2|m²|sqm|square\s*meters?)', msg_low)
            if surface_match:
                params["surface"] = float(surface_match.group(1))

        if params.get("rooms") in (None, 2):
            room_match = re.search(r's\+\s*(\d+)', msg_low)
            if not room_match:
                room_match = re.search(r'(\d+)\s*(?:rooms?|bedrooms?|beds?|chambres?)', msg_low)
            if not room_match:
                room_match = re.search(r'(\d+)\s*(?:pi[èe]ces?)', msg_low)
            if room_match:
                params["rooms"] = int(room_match.group(1))

        if params.get("city") == "Tunis":
            for city in CITIES:
                if city.lower() in msg_low:
                    params["city"] = city.capitalize()
                    break

        if params.get("property_type") == "apartment":
            if any(t in msg_low for t in ["house", "villa", "maison"]):
                params["property_type"] = "house"
            elif any(t in msg_low for t in ["land", "terrain", "plot"]):
                params["property_type"] = "land"
            elif any(t in msg_low for t in ["commercial", "shop", "store", "bureau", "office"]):
                params["property_type"] = "commercial"

        if "transaction_type" not in params or params.get("transaction_type") not in {"rent", "sale"}:
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
    # Response formatters
    # ------------------------------------------------------------------
    def _format_search_response(
        self,
        result:          Dict,
        session_id:      str,
        tool_args:       Dict,
        refinement_count: int = 0,
        force_show:      bool = False,
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
                "💡 Try:\n"
                "• Widen your price range\n"
                "• Try a different city or property type\n"
                "• Remove some filters\n\n"
                "Or say *'reset'* to start a fresh search! 🏠"
            )}
            return

        TYPE_EMOJI = {
            "apartment":  "🏢",
            "house":      "🏡",
            "villa":      "🏰",
            "land":       "🌾",
            "commercial": "🏪",
        }

        # ---------------------------------------------------------------
        # SMART THRESHOLD: Ask for refinement when result set is broad
        # ---------------------------------------------------------------
        should_ask = (
            count > CLARIFICATION_THRESHOLD
            and refinement_count < MAX_REFINEMENT_ROUNDS
            and not force_show
        )

        if should_ask:
            state["pending_search"] = tool_args

            # Always show a small preview (top 3) so the user isn't in the dark
            preview_count = min(3, len(results))
            yield {"type": "token", "content": (
                f"🏠 I found **{count} listings** — here's a quick preview:\n\n"
            )}

            for i, listing in enumerate(results[:preview_count], 1):
                title      = listing.get("title", "Untitled")
                price      = listing.get("price", 0)
                city       = listing.get("city", "Unknown")
                surface    = listing.get("surface")
                rooms      = listing.get("rooms")
                prop_type  = (listing.get("property_type") or "").lower()
                listing_id = listing.get("id")

                listing_url = f"http://localhost:8081/listing/{listing_id}" if listing_id else "#"
                price_str   = f"{price:,.0f} TND" if price else "Price on request"
                parts       = []
                if surface:
                    parts.append(f"{int(surface)}m²")
                if rooms:
                    parts.append(f"{rooms} bed")
                details_str = " • ".join(parts) if parts else "Details on request"
                emoji       = TYPE_EMOJI.get(prop_type, "🏠")
                title       = title[:47] + "..." if len(title) > 50 else title

                yield {"type": "token", "content": (
                    f"{i}. {emoji} **{title}**\n"
                    f"   💰 {price_str} • 📍 {city}\n"
                    f"   📐 {details_str}\n"
                    f"   🔗 [View →]({listing_url})\n\n"
                )}

            # Build and yield the targeted clarification question
            clarification_msg = self._build_clarification_message(
                count, tool_args, refinement_count
            )
            yield {"type": "token", "content": f"---\n{clarification_msg}"}

            yield {
                "type":           "clarification",
                "content":        clarification_msg,
                "pending_params": tool_args,
            }
            return

        # ---------------------------------------------------------------
        # Show results (forced, below threshold, or refined enough)
        # ---------------------------------------------------------------
        clear_pending(session_id)

        # Header
        yield {"type": "token", "content": f"🏠 Found **{count} listing{'s' if count != 1 else ''}**"}
        if pages > 1:
            yield {"type": "token", "content": f" (Page {page}/{pages})"}
        yield {"type": "token", "content": ":\n\n"}

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

        # Smart footer based on count
        if count > MAX_DISPLAY_LISTINGS:
            yield {"type": "token", "content": f"🎯 **Showing {min(MAX_DISPLAY_LISTINGS, len(results))} of {count} results.**\n\n"}

        if pages > 1 and page < pages:
            yield {"type": "token", "content": (
                "💬 **What next?**\n"
                "• *'Show next page'* — more results\n"
                "• *'Cheaper options'* — reduce price\n"
                "• *'Bigger properties'* — increase size\n\n"
            )}
        else:
            yield {"type": "token", "content": (
                "💬 **Want to refine?** Try:\n"
                "• *'Under 200k'* — adjust budget\n"
                "• *'3 bedrooms'* — filter by rooms\n"
                "• *'reset'* — start a new search 🔄\n\n"
            )}

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
        
        debug_mode        = os.getenv("DEBUG_EXTRACTED_PARAMS", "false").lower() == "true"
        extraction_method = result.get("_extraction_method", "regex")
        llm_provider      = result.get("_llm_provider", "unknown")
        
        if debug_mode and extraction_method == "llm" and result.get("_extracted_params"):
            extracted = result["_extracted_params"]
            yield {"type": "token", "content": (
                f"🔍 **Extracted via {llm_provider.upper()}:**\n"
                f"• 🏠 Type: {extracted.get('property_type', 'N/A')}\n"
                f"• 📍 City: {extracted.get('city', 'N/A')}\n"
                f"• 📐 Surface: {extracted.get('surface', 'N/A')} m²\n"
                f"• 🛏️ Rooms: {extracted.get('rooms', 'N/A')}\n"
                f"• 💱 Transaction: {extracted.get('transaction_type', 'N/A')}\n\n"
            )}
        elif debug_mode:
            yield {"type": "token", "content": "🔍 **Extracted via REGEX** (LLM unavailable)\n\n"}

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