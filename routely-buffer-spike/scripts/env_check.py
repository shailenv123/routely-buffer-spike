#!/usr/bin/env python3
"""
Environment file integrity checker for RDM API key.
Detects UTF-8 BOM, quotes, whitespace issues that can cause 403 errors.
"""

import os
import sys
from dotenv import load_dotenv


def check_env_file():
    """Check .env file for common issues that cause API key problems."""
    env_path = ".env"
    
    if not os.path.exists(env_path):
        print(f"❌ .env file not found at {env_path}")
        return False
    
    print(f"📁 Checking {env_path}")
    
    # Read as bytes to detect BOM
    with open(env_path, 'rb') as f:
        raw_bytes = f.read()
    
    # Check for UTF-8 BOM
    has_bom = raw_bytes.startswith(b'\xef\xbb\xbf')
    if has_bom:
        print("⚠️  UTF-8 BOM detected - this can cause API key issues")
        # Remove BOM for analysis
        raw_bytes = raw_bytes[3:]
    else:
        print("✅ No UTF-8 BOM found")
    
    # Decode to text
    try:
        content = raw_bytes.decode('utf-8')
    except UnicodeDecodeError as e:
        print(f"❌ Cannot decode .env file as UTF-8: {e}")
        return False
    
    # Check for RDM_API_KEY line
    rdm_key_line = None
    for line_num, line in enumerate(content.splitlines(), 1):
        if line.strip().startswith('RDM_API_KEY'):
            rdm_key_line = line
            break
    
    if not rdm_key_line:
        print("❌ RDM_API_KEY not found in .env file")
        return False
    
    print(f"📋 Found RDM_API_KEY on line {line_num}")
    
    # Parse the key value
    if '=' not in rdm_key_line:
        print("❌ Invalid format: no '=' found in RDM_API_KEY line")
        return False
    
    key, value = rdm_key_line.split('=', 1)
    
    # Check for issues with the value
    original_value = value
    print(f"🔍 Raw value: '{original_value}' (length: {len(original_value)})")
    
    # Check for surrounding quotes
    has_quotes = False
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        has_quotes = True
        print("⚠️  Value is surrounded by quotes - this may cause issues")
        value = value[1:-1]  # Remove quotes
    
    # Check for whitespace
    stripped_value = value.strip()
    if len(stripped_value) != len(value):
        print(f"⚠️  Value has leading/trailing whitespace")
        value = stripped_value
    
    # Mask the key for display
    if len(value) >= 12:
        masked = value[:6] + "…" + value[-6:]
    else:
        masked = "[SHORT_KEY]"
    
    print(f"🔑 Cleaned value: {masked} (length: {len(value)})")
    
    # Now test with dotenv
    load_dotenv()
    loaded_key = os.getenv("RDM_API_KEY")
    
    if not loaded_key:
        print("❌ dotenv failed to load RDM_API_KEY")
        return False
    
    # Mask loaded key
    if len(loaded_key) >= 12:
        loaded_masked = loaded_key[:6] + "…" + loaded_key[-6:]
    else:
        loaded_masked = "[SHORT_KEY]"
    
    print(f"📦 dotenv loaded: {loaded_masked} (length: {len(loaded_key)})")
    
    # Compare
    if value == loaded_key:
        print("✅ Raw parsing matches dotenv loading")
    else:
        print("⚠️  Raw parsing differs from dotenv loading!")
        print(f"   Raw: {len(value)} chars")
        print(f"   Loaded: {len(loaded_key)} chars")
    
    # Fix file if needed
    if has_bom or has_quotes or len(stripped_value) != len(original_value):
        print("\n🔧 Issues detected - creating fixed .env file")
        
        # Rebuild content with fixes
        fixed_lines = []
        for line in content.splitlines():
            if line.strip().startswith('RDM_API_KEY'):
                # Use the cleaned value without quotes
                fixed_lines.append(f"RDM_API_KEY={stripped_value}")
            else:
                fixed_lines.append(line)
        
        fixed_content = '\n'.join(fixed_lines)
        
        # Write without BOM
        with open(env_path, 'w', encoding='utf-8') as f:
            f.write(fixed_content)
        
        print("✅ Fixed .env file written")
        
        # Test the fix
        print("\n🧪 Testing fixed file...")
        os.environ.pop('RDM_API_KEY', None)  # Clear cached value
        load_dotenv(override=True)
        test_key = os.getenv("RDM_API_KEY")
        
        if test_key and len(test_key) >= 12:
            test_masked = test_key[:6] + "…" + test_key[-6:]
            print(f"✅ Fixed file loads: {test_masked} (length: {len(test_key)})")
        else:
            print("❌ Fixed file still has issues")
            return False
    
    return True


if __name__ == "__main__":
    success = check_env_file()
    if success:
        print("\n🎉 .env file appears to be clean")
        sys.exit(0)
    else:
        print("\n💥 .env file has issues that need manual fixing")
        sys.exit(1)
