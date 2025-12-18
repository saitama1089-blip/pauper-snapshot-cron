import os
import sys
import psycopg2
from psycopg2.extras import execute_values

SQL = """
INSERT INTO public.v_card_usage_monthly (
  month_start, section, card_name,
  total_copies, decks_playing_it, avg_per_deck_playing_it, computed_at
)
WITH bounds AS (
  SELECT date_trunc('month', current_date)::date AS this_month_start
),
events AS (
  SELECT cdr.deck_id, cdr.date AS event_date
  FROM public.challenge_deck_results cdr
  JOIN bounds b ON cdr.date >= (b.this_month_start - interval '1 month')::date
  UNION ALL
  SELECT plr.id AS deck_id, plr.event_date
  FROM public.pauper_league_results plr
  JOIN bounds b ON plr.event_date >= (b.this_month_start - interval '1 month')::date
),
exploded AS (
  SELECT date_trunc('month', e.event_date)::date AS month_start,
         dc.deck_id, x.section, x.card_name, x.copies
  FROM events e
  JOIN public.deck_cache dc ON dc.deck_id = e.deck_id
  CROSS JOIN LATERAL (
    SELECT 'mainboard'::text AS section, c->>'name' AS card_name, (c->>'count')::int AS copies
    FROM jsonb_array_elements(dc.json_decklist->'mainboard') c
    UNION ALL
    SELECT 'sideboard'::text, c->>'name', (c->>'count')::int
    FROM jsonb_array_elements(COALESCE(dc.json_decklist->'sideboard','[]'::jsonb)) c
  ) x
  WHERE x.card_name NOT IN ('Mountain','Island','Plains','Swamp','Forest')
),
per_deck AS (
  SELECT month_start, deck_id, section, card_name, SUM(copies) AS copies_in_deck
  FROM exploded
  GROUP BY month_start, deck_id, section, card_name
),
aggregated AS (
  SELECT month_start, section, card_name,
         SUM(copies_in_deck) AS total_copies,
         COUNT(*) AS decks_playing_it,
         ROUND(AVG(copies_in_deck)::numeric, 2) AS avg_per_deck_playing_it
  FROM per_deck
  GROUP BY month_start, section, card_name
)
SELECT month_start, section, card_name,
       total_copies, decks_playing_it, avg_per_deck_playing_it,
       now() AS computed_at
FROM aggregated;
"""

def main() -> int:
    db_url = os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        print("Missing DATABASE_URL (or SUPABASE_DB_URL) env var.", file=sys.stderr)
        return 2

    # Connect & run in a single transaction
    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(SQL)
                # rowcount for INSERT..SELECT is sometimes -1; still print something helpful
                print("Refresh completed successfully.")
    finally:
        conn.close()

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
