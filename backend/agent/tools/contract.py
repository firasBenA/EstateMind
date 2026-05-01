"""
Contract generation tool.
Generates legal contracts using the RAG engine.
"""
from typing import Dict, Any, Optional
import logging
import os
from pathlib import Path
import sys

logger = logging.getLogger(__name__)


def _get_contract_engine():
    """Import the RAG engine for contract generation."""
    # 1. Determine the absolute path to the 'backend' folder
    # contract.py is in: backend/agent/tools/
    # .parent (tools) -> .parent (agent) -> .parent (backend)
    backend_dir = Path(__file__).resolve().parent.parent.parent

    # 2. Define the likely locations for your RAG engine
    data_dir = backend_dir.parent / "data" / "MODELS" / "RAG" / "Reports"
    rag_dir = data_dir / "rag"
    backend_data_dir = backend_dir / "data" / "MODELS" / "RAG" / "Reports"

    target_dirs = [rag_dir, data_dir, backend_data_dir]

    target_dir = None
    for candidate in target_dirs:
        if candidate.exists():
            target_dir = candidate
            break

    if not target_dir:
        logger.error("❌ Contract engine directory not found!")
        logger.error(
            f"Checked:\n 1. {rag_dir}\n 2. {data_dir}\n 3. {backend_data_dir}"
        )
        return None

    if str(data_dir) not in sys.path:
        sys.path.insert(0, str(data_dir))
    if str(rag_dir) not in sys.path:
        sys.path.insert(0, str(rag_dir))

    # Load environment from the engine directory if present (same as dashboard path)
    env_file = data_dir / ".env"
    if env_file.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(env_file, override=False)
        except ImportError:
            logger.warning("⚠️ python-dotenv not installed, skipping .env load")

    logger.info(f"✅ Found engine directory: {target_dir}")

    try:
        from engine import generate_contract_stream
        return generate_contract_stream
    except ImportError as e:
        logger.error(f"❌ 'engine.py' not found in {target_dir}: {e}")
        return None

def generate_contract(
    contract_type: str,
    **kwargs,
) -> Dict[str, Any]:
    """
    Generate a contract using the RAG engine.

    Args:
        contract_type: compromis_de_vente, promesse_de_vente, contrat_de_location, or acte_de_vente
        **kwargs: All other contract parameters (buyer_name, seller_cin, listing_address, etc.)

    Returns:
        {
            "contract_text": str,
            "contract_type": str,
            "params": dict,
            "error": str (if any)
        }
    """
    logger.info(f"🔄 Starting contract generation: type={contract_type}, params={kwargs}")
    try:
        generate = _get_contract_engine()
        if not generate:
            logger.error("❌ Contract engine not available")
            return {
                "error": "Contract generation engine not available",
                "contract_text": "",
                "contract_type": contract_type,
                "params": kwargs,  # ← Pass kwargs as params
            }

        logger.info("✅ Contract engine loaded, starting generation...")
        # Stream the contract generation
        contract_text = ""
        token_count = 0
        for token in generate(contract_type, kwargs):  # ← Pass kwargs to engine
            contract_text += token
            token_count += 1
            if token_count % 100 == 0:  # Log every 100 tokens
                logger.info(f"📝 Generated {token_count} tokens, current length: {len(contract_text)}")

        logger.info(f"✅ Contract generation complete: {len(contract_text)} chars, {token_count} tokens")
        return {
            "contract_text": contract_text,
            "contract_type": contract_type,
            "params": kwargs,
        }

    except Exception as e:
        logger.error(f"❌ Contract generation failed: {e}", exc_info=True)
        return {
            "error": f"Failed to generate contract: {str(e)}",
            "contract_text": "",
            "contract_type": contract_type,
            "params": kwargs,
        }