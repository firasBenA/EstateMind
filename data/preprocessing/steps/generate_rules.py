# estateMindPi/generate_rules.py
#!/usr/bin/env python
"""
Generate normalization rules using clean data examples
Run from project root: python generate_rules.py
"""

import sys
from pathlib import Path

# Add project root (data directory) to path so 'preprocessing' module can be found
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Now import with correct path
from rule_generator import get_rules_generator

def main():
    """Generate normalization rules"""
    print("=" * 60)
    print("Generating Normalization Rules")
    print("=" * 60)
    print()
    
    print("📝 Sending clean examples to LLM...")
    generator = get_rules_generator()
    rules = generator.generate_rules_from_clean_data()
    
    print(f"💾 Saving rules to data/normalization_rules.json...")
    generator.save_rules()
    
    print()
    print("=" * 60)
    print("✅ Rules generated successfully!")
    print(f"   Fields: {list(rules.keys())}")
    print("=" * 60)

if __name__ == "__main__":
    main()