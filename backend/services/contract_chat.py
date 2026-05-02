"""
Contract generation chat assistant.
Uses the existing AgentOrchestrator to extract contract parameters.
"""

import json
from typing import Dict, Any, Optional
from agent.agent import AgentOrchestrator

# Contract-specific prompts
CONTRACT_INTENT_PROMPT = """
You are a contract generation assistant for EstateMind.

Your job is to extract the following information from user messages:
- contract_type: compromis_de_vente, promesse_de_vente, contrat_de_location, or acte_de_vente
- listing_id: The property listing ID (if mentioned)
- listing_title: Property description (if no ID)
- buyer_name: Full name of buyer
- buyer_cin: Buyer's national ID
- buyer_address: Buyer's address
- seller_name: Full name of seller
- seller_cin: Seller's national ID
- seller_address: Seller's address
- transaction_date: Date of transaction
- price: Property price (if mentioned)
- surface: Property surface area (if mentioned)

Current conversation context: {context}

User message: "{message}"

Extract as JSON. If a field is not mentioned, omit it or set to null.
Also include:
- "missing_fields": list of required fields still needed
- "ready_to_generate": true if all required fields are present
- "confirmation_message": message to show user before generating

Required fields for generation:
- contract_type
- listing_id OR (listing_title + price + surface)
- buyer_name
- buyer_cin
- seller_name
- seller_cin
"""


class ContractChatAssistant:
    """Handles conversational contract generation."""
    
    def __init__(self):
        self.orchestrator = AgentOrchestrator()
    
    def process_message(self, message: str, current_draft: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Process user message and extract contract parameters.
        
        Returns:
        {
            "response": str,  # Natural language response to user
            "extracted_params": dict,  # Extracted parameters
            "missing_fields": list,  # Fields still needed
            "ready_to_generate": bool,  # Whether we can generate now
        }
        """
        if current_draft is None:
            current_draft = {}
        
        # Build context from current draft
        context = self._build_context(current_draft)
        
        # Use LLM to extract parameters
        prompt = CONTRACT_INTENT_PROMPT.format(
            context=context,
            message=message
        )
        
        try:
            # Use the orchestrator's LLM
            from langchain_together import ChatTogether
            llm = ChatTogether(
                model="meta-llama/Llama-3.1-405B-Instruct",
                temperature=0.0,
                max_tokens=500,
            )
            
            result = llm.invoke(prompt)
            extracted = json.loads(result.content)
            
            # Merge with current draft
            merged_draft = {**current_draft, **extracted}
            
            # Check what's missing
            required_fields = [
                "contract_type", "buyer_name", "buyer_cin", 
                "seller_name", "seller_cin"
            ]
            
            # Need either listing_id OR listing details
            has_listing = (
                merged_draft.get("listing_id") or 
                (merged_draft.get("listing_title") and 
                 merged_draft.get("price") and 
                 merged_draft.get("surface"))
            )
            
            if not has_listing:
                required_fields.append("listing_id or property details")
            
            missing = [f for f in required_fields if not merged_draft.get(f)]
            
            # Generate response
            if merged_draft.get("ready_to_generate") and not missing:
                response = "Perfect! I have all the information needed. Generating your contract now..."
            elif missing:
                response = self._ask_for_missing_fields(missing, merged_draft)
            else:
                response = extracted.get("response", "Got it! What's next?")
            
            return {
                "response": response,
                "extracted_params": {k: v for k, v in extracted.items() if k not in ["response", "missing_fields", "ready_to_generate"]},
                "missing_fields": missing,
                "ready_to_generate": len(missing) == 0,
                "draft": merged_draft,
            }
            
        except Exception as e:
            return {
                "response": f"I encountered an error: {str(e)}. Please try again.",
                "extracted_params": {},
                "missing_fields": required_fields,
                "ready_to_generate": False,
                "draft": current_draft,
            }
    
    def _build_context(self, draft: Dict[str, Any]) -> str:
        """Build context string from current draft."""
        lines = []
        if draft.get("contract_type"):
            lines.append(f"Contract type: {draft['contract_type']}")
        if draft.get("listing_title"):
            lines.append(f"Property: {draft['listing_title']}")
        if draft.get("buyer_name"):
            lines.append(f"Buyer: {draft['buyer_name']}")
        if draft.get("seller_name"):
            lines.append(f"Seller: {draft['seller_name']}")
        return "\n".join(lines) if lines else "No information collected yet."
    
    def _ask_for_missing_fields(self, missing: list, draft: Dict[str, Any]) -> str:
        """Generate a friendly question for missing fields."""
        if "contract_type" in missing:
            return "What type of contract would you like? I can generate:\n- Sales Agreement (Compromis de Vente)\n- Promise to Sell (Promesse de Vente)\n- Rental Agreement (Contrat de Location)\n- Final Deed of Sale (Acte de Vente)"
        
        if "listing_id or property details" in missing:
            if draft.get("user_listings"):
                return "Which property is this for? You can tell me the listing ID or describe it (e.g., 'my land in Bizerte')."
            return "Which property is this for? Please provide the listing ID or describe it (title, price, surface area)."
        
        if "buyer_name" in missing:
            return "What's the buyer's full name?"
        
        if "buyer_cin" in missing:
            return "What's the buyer's CIN (National ID number)?"
        
        if "seller_name" in missing:
            return "What's the seller's full name? (I can use your profile name if you'd like)"
        
        if "seller_cin" in missing:
            return "What's the seller's CIN (National ID number)?"
        
        return f"I still need: {', '.join(missing)}. Please provide this information."


# Create singleton instance
assistant = ContractChatAssistant()


def process_contract_chat(message: str, current_draft: dict = None) -> dict:
    """Main entry point for contract chat."""
    return assistant.process_message(message, current_draft)