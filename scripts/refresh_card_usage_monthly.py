import os
import sys
from supabase import create_client

def main() -> int:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        print("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr)
        return 2

    supabase = create_client(url, key)

    res = supabase.rpc("refresh_card_usage_monthly").execute()

    if res.error:
        print(res.error, file=sys.stderr)
        return 1

    print("Refresh completed successfully.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
