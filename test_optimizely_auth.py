import os
from load_optimizely_decisions import fetch_optimizely_temp_creds

# Make sure your .env file or environment has OPTIMIZELY_PAT set
pat = os.getenv("OPTIMIZELY_PAT")

if not pat:
    raise RuntimeError("Missing OPTIMIZELY_PAT in environment or .env file")

# Try fetching temporary AWS credentials
creds = fetch_optimizely_temp_creds(pat, "1h", verbose=True)
print("✅ Successfully fetched temporary credentials:")
print(creds)
