import os
import sys
from supabase import create_client

RPC_NAME = "refresh_card_usage_monthly"  # must match the SQL function name

def main() -> int:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        print("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr)
        return 2

    supabase = create_client(url, key)

    try:
        # For RETURNS void, Supabase typically returns data=None or [] depending on version
        res = supabase.rpc(RPC_NAME, {}).execute()
        print(f"RPC '{RPC_NAME}' executed successfully. Returned: {res.data}")
        return 0
    except Exception as e:
        print(f"RPC '{RPC_NAME}' failed: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
