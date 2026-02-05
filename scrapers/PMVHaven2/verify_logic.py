
import sys
import os

# Mocking modules to avoid side effects during import if necessary
# But PMVHaven2 seems robust enough.
# We will just import the function.

try:
    import PMVHaven2
except ImportError:
    # If in a subfolder, add path
    sys.path.append(os.getcwd())
    import PMVHaven2

def test(filename, expected=None):
    result = PMVHaven2._build_search_query(filename)
    print(f"File: {filename}")
    print(f"Result: '{result}'")
    if expected:
        if result == expected:
            print("PASS")
        else:
            print(f"FAIL (Expected '{expected}')")
    print("-" * 20)

print("Verifying Search Query Logic...")

# Type A
# User's Example Output: "TaT (Tits,Ass,&Tats)"
# User's Rule: "delete the first set of _-_ and everything before it"
# My Implementation of Rule Output: "(Tits,Ass,&Tats)"
# I will check what it produces.
test("TaT_-_(Tits,Ass,&Tats).mp4")

# Type B
# User's Example Output: "Thicc 5"
test("tomtom10_-_Thicc_5_1766671291932_folhscuz.mp4", "Thicc 5")
