"""Main entrypoint for Shipping ETL."""
from __future__ import annotations
import sys
from sqlalchemy import text
from config import load_config
from etl import (
    pg_engine, sf_connect, extract_all, transform_dimensions, transform_fact,
    ensure_session, load_dimensions, fetch_surrogate_mappings, load_fact
)

def run():
    cfg = load_config()
    print("Starting Shipping ETL ...")

    # Postgres connectivity check
    pg = pg_engine(cfg)
    try:
        with pg.connect() as c:
            c.execute(text("SELECT 1"))
    except Exception as e:
        print("Abort: Postgres not reachable:", e)
        sys.exit(1)

    dfs = extract_all(pg, cfg.postgres.schema)
    cust_df, ship_df, port_df = transform_dimensions(dfs)
    fact_df = transform_fact(dfs)
    print("Dimension sizes:", len(cust_df), len(ship_df), len(port_df))
    print("Fact candidate rows:", len(fact_df))

    # Snowflake
    try:
        sf = sf_connect(cfg)
    except Exception as e:
        print("Abort: Snowflake connection failure:", e)
        sys.exit(2)

    ensure_session(sf, cfg)
    load_dimensions(sf, cust_df, ship_df, port_df, cfg)
    mappings = fetch_surrogate_mappings(sf)
    load_fact(sf, fact_df, mappings, cfg)
    print("ETL complete.")

if __name__ == "__main__":  
    run()
